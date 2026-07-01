package syncer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/crawlkit/progress"

	"github.com/openclaw/discrawl/internal/store"
)

func (s *Syncer) syncMessageChannels(
	ctx context.Context,
	guildID string,
	channels []*discordgo.Channel,
	opts SyncOptions,
) (int, error) {
	messageChannels := filterMessageChannels(channels, opts.ChannelIDs)
	if len(messageChannels) == 0 {
		return 0, nil
	}
	progress := newMessageSyncProgress(s, guildID, len(messageChannels), opts)
	workers := opts.Concurrency
	if workers <= 1 {
		total, err := s.syncMessageChannelsSerial(ctx, guildID, messageChannels, opts, progress)
		if progress != nil {
			progress.finish(err)
		}
		return total, err
	}
	total, err := s.syncMessageChannelsConcurrent(ctx, guildID, messageChannels, opts, workers, progress)
	if progress != nil {
		progress.finish(err)
	}
	return total, err
}

func filterMessageChannels(channels []*discordgo.Channel, requested []string) []*discordgo.Channel {
	requestedSet := makeGuildSet(requested)
	channelByID := make(map[string]*discordgo.Channel, len(channels))
	for _, channel := range channels {
		if channel != nil {
			channelByID[channel.ID] = channel
		}
	}
	out := make([]*discordgo.Channel, 0, len(channels))
	for _, channel := range channels {
		if !isMessageChannel(channel) {
			continue
		}
		if len(requestedSet) > 0 && !requestedMessageTarget(channel, channelByID, requestedSet) {
			continue
		}
		out = append(out, channel)
	}
	return out
}

func requestedMessageTarget(channel *discordgo.Channel, channelByID map[string]*discordgo.Channel, requested map[string]struct{}) bool {
	if channel == nil {
		return false
	}
	if _, ok := requested[channel.ID]; ok {
		return true
	}
	if !isThreadChannel(channel) {
		return false
	}
	if _, ok := requested[channel.ParentID]; !ok {
		return false
	}
	parent := channelByID[channel.ParentID]
	return parent != nil && parent.Type == discordgo.ChannelTypeGuildForum
}

func (s *Syncer) syncMessageChannelsSerial(ctx context.Context, guildID string, channels []*discordgo.Channel, opts SyncOptions, progress *messageSyncProgress) (int, error) {
	total := 0
	for _, channel := range channels {
		progress.start(channel)
		channelCtx, cancel := s.messageChannelContext(ctx)
		count, err := s.syncChannelMessages(channelCtx, guildID, channel, opts.Full, opts.Embeddings, opts.Since, opts.LatestOnly, progress)
		cancel()
		total += count
		if err != nil {
			if s.skipSyncError(ctx, channel, err) {
				if recordErr := s.recordChannelFailure(ctx, guildID, channel.ID, err); recordErr != nil {
					s.logger.Warn("record channel failure", "channel_id", channel.ID, "err", recordErr)
				}
				progress.recordSkip(channel, err)
				continue
			}
			return total, fmt.Errorf("sync channel %s: %w", channel.ID, withFailureRecordError(err, s.recordChannelFailure(ctx, guildID, channel.ID, err)))
		}
		if err := s.clearUnavailableChannel(ctx, channel.ID); err != nil {
			return total, withFailureRecordError(err, s.recordChannelFailure(ctx, guildID, channel.ID, err))
		}
		if err := s.resolveChannelFailures(ctx, guildID, channel.ID); err != nil {
			return total, err
		}
		progress.record(channel, count)
	}
	return total, nil
}

func (s *Syncer) syncMessageChannelsConcurrent(
	ctx context.Context,
	guildID string,
	channels []*discordgo.Channel,
	opts SyncOptions,
	workers int,
	progress *messageSyncProgress,
) (int, error) {
	type result struct {
		channelID string
		channel   *discordgo.Channel
		count     int
		err       error
		skipped   error
	}

	ctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	jobs := make(chan *discordgo.Channel)
	results := make(chan result, len(channels))
	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			for channel := range jobs {
				if ctx.Err() != nil {
					return
				}
				progress.start(channel)
				channelCtx, cancelChannel := s.messageChannelContext(ctx)
				count, err := s.syncChannelMessages(channelCtx, guildID, channel, opts.Full, opts.Embeddings, opts.Since, opts.LatestOnly, progress)
				cancelChannel()
				succeeded := err == nil
				var skipped error
				if err != nil && s.skipSyncError(ctx, channel, err) {
					skipped = err
					err = nil
				}
				if succeeded {
					err = s.clearUnavailableChannel(ctx, channel.ID)
					if err == nil {
						err = s.resolveChannelFailures(ctx, guildID, channel.ID)
					}
				}
				if skipped != nil {
					if recordErr := s.recordChannelFailure(ctx, guildID, channel.ID, skipped); recordErr != nil {
						s.logger.Warn("record channel failure", "channel_id", channel.ID, "err", recordErr)
					}
				} else if err != nil {
					err = withFailureRecordError(err, s.recordChannelFailure(ctx, guildID, channel.ID, err))
				}
				select {
				case results <- result{channelID: channel.ID, channel: channel, count: count, err: err, skipped: skipped}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					cancelAll()
					return
				}
			}
		})
	}

	go func() {
		defer close(jobs)
		for _, channel := range channels {
			select {
			case jobs <- channel:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	total := 0
	var firstErr error
	for result := range results {
		total += result.count
		if result.skipped != nil {
			progress.recordSkip(result.channel, result.skipped)
		} else {
			progress.record(result.channel, result.count)
		}
		if result.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("sync channel %s: %w", result.channelID, result.err)
		}
	}
	return total, firstErr
}

func (s *Syncer) clearUnavailableChannel(ctx context.Context, channelID string) error {
	if s == nil || s.store == nil || channelID == "" {
		return nil
	}
	return s.store.DeleteSyncState(ctx, channelMessageUnavailableScope(channelID))
}

func (s *Syncer) messageChannelContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if s == nil || s.messageChannelTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, s.messageChannelTimeout)
}

func (s *Syncer) syncChannelMessages(ctx context.Context, guildID string, channel *discordgo.Channel, full bool, embeddings bool, since time.Time, latestOnly bool, progress *messageSyncProgress) (int, error) {
	state, err := s.loadChannelSyncState(ctx, channel.ID)
	if err != nil {
		return 0, err
	}
	if full {
		if err := s.seedChannelSyncState(ctx, channel.ID, &state); err != nil {
			return 0, err
		}
		if shouldSkipChannelSync(channel, state) {
			return 0, nil
		}
		return s.syncFullChannelHistory(ctx, channel, state, embeddings, since, progress)
	}
	if latestOnly && state.Latest == "" {
		return 0, nil
	}
	if shouldSkipChannelSync(channel, state) || (latestOnly && shouldSkipLatestOnlyChannelSync(channel, state)) {
		return 0, nil
	}
	return s.syncIncrementalChannelHistory(ctx, channel, state, embeddings, since, progress)
}

type channelSyncState struct {
	Latest           string
	StoredLatest     string
	BackfillCursor   string
	BackfillComplete bool
}

func shouldSkipChannelSync(channel *discordgo.Channel, state channelSyncState) bool {
	if !state.BackfillComplete || channel == nil {
		return false
	}
	if channel.LastMessageID == "" {
		return state.Latest == ""
	}
	if state.Latest == "" {
		return false
	}
	return maxSnowflake(state.Latest, channel.LastMessageID) == state.Latest
}

func shouldSkipLatestOnlyChannelSync(channel *discordgo.Channel, state channelSyncState) bool {
	if channel == nil || state.Latest == "" || channel.LastMessageID == "" {
		return false
	}
	return maxSnowflake(state.Latest, channel.LastMessageID) == state.Latest
}

func (s *Syncer) loadChannelSyncState(ctx context.Context, channelID string) (channelSyncState, error) {
	latest, err := s.store.GetSyncState(ctx, channelLatestScope(channelID))
	if err != nil {
		return channelSyncState{}, err
	}
	backfillCursor, err := s.store.GetSyncState(ctx, channelBackfillScope(channelID))
	if err != nil {
		return channelSyncState{}, err
	}
	backfillComplete, err := s.store.GetSyncState(ctx, channelHistoryCompleteScope(channelID))
	if err != nil {
		return channelSyncState{}, err
	}
	return channelSyncState{
		Latest:           latest,
		StoredLatest:     latest,
		BackfillCursor:   backfillCursor,
		BackfillComplete: backfillComplete != "",
	}, nil
}

func (s *Syncer) seedChannelSyncState(ctx context.Context, channelID string, state *channelSyncState) error {
	if state.Latest == "" || state.BackfillCursor == "" {
		oldestStored, newestStored, err := s.store.ChannelMessageBounds(ctx, channelID)
		if err != nil {
			return err
		}
		if state.Latest == "" && newestStored != "" {
			state.Latest = newestStored
		}
		if state.BackfillCursor == "" && oldestStored != "" {
			state.BackfillCursor = oldestStored
		}
	}
	if state.StoredLatest != "" && state.BackfillCursor == "" && !state.BackfillComplete {
		if err := s.store.SetSyncState(ctx, channelHistoryCompleteScope(channelID), "1"); err != nil {
			return err
		}
		state.BackfillComplete = true
	}
	return nil
}

func (s *Syncer) syncFullChannelHistory(ctx context.Context, channel *discordgo.Channel, state channelSyncState, embeddings bool, since time.Time, progress *messageSyncProgress) (int, error) {
	messageCount := 0
	newest := state.Latest
	if state.Latest != "" {
		count, latest, err := s.syncForwardPages(ctx, channel, state.Latest, embeddings, progress)
		messageCount += count
		if err != nil {
			return messageCount, err
		}
		newest = maxSnowflake(newest, latest)
		if err := s.advanceChannelLatest(ctx, channel.ID, newest); err != nil {
			return messageCount, err
		}
	}
	if !state.BackfillComplete {
		before := state.BackfillCursor
		if before == "" && state.Latest != "" {
			before = state.Latest
		}
		count, latest, err := s.syncBackfillPages(ctx, channel, before, newest, channel.Name, embeddings, since, progress)
		messageCount += count
		newest = maxSnowflake(newest, latest)
		if err != nil {
			return messageCount, err
		}
	}
	if newest != "" || state.Latest != "" || !state.BackfillComplete {
		if newest == "" {
			if err := s.store.EnsureChannelLatestMessageState(ctx, channel.ID); err != nil {
				return messageCount, err
			}
			return messageCount, nil
		}
		if err := s.advanceChannelLatest(ctx, channel.ID, newest); err != nil {
			return messageCount, err
		}
	}
	return messageCount, nil
}

func (s *Syncer) syncIncrementalChannelHistory(ctx context.Context, channel *discordgo.Channel, state channelSyncState, embeddings bool, since time.Time, progress *messageSyncProgress) (int, error) {
	if state.Latest == "" {
		return s.bootstrapChannelHistory(ctx, channel, embeddings, since, progress)
	}
	count, newest, err := s.syncForwardPages(ctx, channel, state.Latest, embeddings, progress)
	if err != nil {
		return count, err
	}
	if newest == "" && state.Latest == "" {
		return count, nil
	}
	if err := s.advanceChannelLatest(ctx, channel.ID, maxSnowflake(state.Latest, newest)); err != nil {
		return count, err
	}
	return count, nil
}

func (s *Syncer) bootstrapChannelHistory(ctx context.Context, channel *discordgo.Channel, embeddings bool, since time.Time, progress *messageSyncProgress) (int, error) {
	messageCount := 0
	before := ""
	newest := ""
	for {
		page, err := s.client.ChannelMessages(ctx, channel.ID, 100, before, "")
		if err != nil {
			return messageCount, err
		}
		if len(page) == 0 {
			break
		}
		eligible, reachedSince := filterMessagesSince(page, since)
		pageNewest, err := s.persistMessagePage(ctx, eligible, channel.Name, channel.GuildID, embeddings)
		if err != nil {
			return messageCount, err
		}
		progress.touch(channel, len(eligible))
		newest = maxSnowflake(newest, pageNewest)
		messageCount += len(eligible)
		if reachedSince {
			if len(eligible) > 0 {
				before = eligible[len(eligible)-1].ID
				if err := s.store.SetSyncState(ctx, channelBackfillScope(channel.ID), before); err != nil {
					return messageCount, err
				}
			}
			break
		}
		before = page[len(page)-1].ID
		if len(page) < 100 {
			if newest != "" {
				if err := s.store.SetSyncState(ctx, channelHistoryCompleteScope(channel.ID), "1"); err != nil {
					return messageCount, err
				}
			}
			break
		}
	}
	if newest != "" {
		if err := s.advanceChannelLatest(ctx, channel.ID, newest); err != nil {
			return messageCount, err
		}
	}
	return messageCount, nil
}

func (s *Syncer) syncForwardPages(ctx context.Context, channel *discordgo.Channel, after string, embeddings bool, progress *messageSyncProgress) (int, string, error) {
	messageCount := 0
	newest := after
	for {
		page, err := s.client.ChannelMessages(ctx, channel.ID, 100, "", after)
		if err != nil {
			return messageCount, newest, err
		}
		if len(page) == 0 {
			break
		}
		pageNewest, err := s.persistMessagePage(ctx, page, channel.Name, channel.GuildID, embeddings)
		if err != nil {
			return messageCount, newest, err
		}
		progress.touch(channel, len(page))
		after = maxSnowflake(after, pageNewest)
		newest = maxSnowflake(newest, pageNewest)
		messageCount += len(page)
		if err := s.advanceChannelLatest(ctx, channel.ID, newest); err != nil {
			return messageCount, newest, err
		}
		if len(page) < 100 {
			break
		}
	}
	return messageCount, newest, nil
}

func (s *Syncer) syncBackfillPages(ctx context.Context, channel *discordgo.Channel, before, latestFloor, channelName string, embeddings bool, since time.Time, progress *messageSyncProgress) (int, string, error) {
	messageCount := 0
	newest := ""
	for {
		page, err := s.client.ChannelMessages(ctx, channel.ID, 100, before, "")
		if err != nil {
			return messageCount, newest, err
		}
		if len(page) == 0 {
			if err := s.store.SetSyncState(ctx, channelHistoryCompleteScope(channel.ID), "1"); err != nil {
				return messageCount, newest, err
			}
			break
		}
		eligible, reachedSince := filterMessagesSince(page, since)
		pageNewest, err := s.persistMessagePage(ctx, eligible, channelName, channel.GuildID, embeddings)
		if err != nil {
			return messageCount, newest, err
		}
		progress.touch(channel, len(eligible))
		newest = maxSnowflake(newest, pageNewest)
		messageCount += len(eligible)
		// Backfill pages are older than any previously synced head, so only
		// checkpoint the latest pointer when it actually advances (backfill
		// starting from the channel head); persisting backfill-region ids
		// over a stored head would make resumed full syncs re-crawl the
		// whole span between the backfill cursor and the head.
		if newest != "" && maxSnowflake(latestFloor, newest) == newest {
			if err := s.advanceChannelLatest(ctx, channel.ID, newest); err != nil {
				return messageCount, newest, err
			}
		}
		if reachedSince {
			if len(eligible) > 0 {
				before = eligible[len(eligible)-1].ID
				if err := s.store.SetSyncState(ctx, channelBackfillScope(channel.ID), before); err != nil {
					return messageCount, newest, err
				}
			}
			break
		}
		before = page[len(page)-1].ID
		if err := s.store.SetSyncState(ctx, channelBackfillScope(channel.ID), before); err != nil {
			return messageCount, newest, err
		}
		if len(page) < 100 {
			if err := s.store.SetSyncState(ctx, channelHistoryCompleteScope(channel.ID), "1"); err != nil {
				return messageCount, newest, err
			}
			break
		}
	}
	return messageCount, newest, nil
}

func (s *Syncer) advanceChannelLatest(ctx context.Context, channelID, cursor string) error {
	return s.store.AdvanceChannelLatestMessageID(ctx, channelID, cursor)
}

func (s *Syncer) persistMessagePage(ctx context.Context, messages []*discordgo.Message, channelName string, fallbackGuildID string, embeddings bool) (string, error) {
	if len(messages) == 0 {
		return "", nil
	}
	mutations, newest, err := buildMessageMutations(ctx, messages, channelName, fallbackGuildID, embeddings, s.attachmentTextEnabled)
	if err != nil {
		return "", err
	}
	if err := s.store.UpsertMessages(ctx, mutations); err != nil {
		return "", err
	}
	return newest, nil
}

func buildMessageMutations(ctx context.Context, messages []*discordgo.Message, channelName string, fallbackGuildID string, embeddings bool, attachmentText bool) ([]store.MessageMutation, string, error) {
	mutations := make([]store.MessageMutation, 0, len(messages))
	newest := ""
	for _, message := range messages {
		mutation, err := buildMessageMutation(ctx, message, channelName, fallbackGuildID, embeddings, attachmentText)
		if err != nil {
			return nil, "", err
		}
		mutations = append(mutations, mutation)
		newest = maxSnowflake(newest, message.ID)
	}
	return mutations, newest, nil
}

func filterMessagesSince(messages []*discordgo.Message, since time.Time) ([]*discordgo.Message, bool) {
	if since.IsZero() || len(messages) == 0 {
		return messages, false
	}
	out := make([]*discordgo.Message, 0, len(messages))
	reachedSince := false
	for _, message := range messages {
		if message.Timestamp.Before(since) {
			reachedSince = true
			break
		}
		out = append(out, message)
	}
	return out, reachedSince
}

type messageSyncProgress struct {
	syncer                *Syncer
	guildID               string
	totalChannels         int
	startedAt             time.Time
	lastLogAt             time.Time
	lastProgressAt        time.Time
	processed             int
	messages              int
	deferredRetryable     int
	skippedMissingAccess  int
	skippedUnknownChannel int
	logEvery              time.Duration
	waitEvery             time.Duration
	done                  chan struct{}
	once                  sync.Once
	mu                    sync.Mutex
	inflight              map[string]messageSyncInFlight
}

type messageSyncInFlight struct {
	id           string
	name         string
	startedAt    time.Time
	lastPageAt   time.Time
	pageCount    int
	pageMessages int
}

func newMessageSyncProgress(s *Syncer, guildID string, totalChannels int, opts SyncOptions) *messageSyncProgress {
	if s == nil || s.logger == nil || totalChannels == 0 {
		return nil
	}
	now := time.Now()
	progress := &messageSyncProgress{
		syncer:         s,
		guildID:        guildID,
		totalChannels:  totalChannels,
		startedAt:      now,
		lastLogAt:      now,
		lastProgressAt: now,
		logEvery:       s.messageSyncLogEvery,
		waitEvery:      s.messageSyncWaitEvery,
		done:           make(chan struct{}),
		inflight:       make(map[string]messageSyncInFlight, totalChannels),
	}
	s.logger.Info(
		"message sync started",
		"guild_id", guildID,
		"channels", totalChannels,
		"full", opts.Full,
		"concurrency", max(1, opts.Concurrency),
		"channel_timeout", timeoutLabel(s.messageChannelTimeout),
	)
	go progress.runWaitHeartbeat()
	return progress
}

func (p *messageSyncProgress) start(channel *discordgo.Channel) {
	if p == nil || channel == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inflight[channel.ID] = messageSyncInFlight{
		id:         channel.ID,
		name:       channel.Name,
		startedAt:  time.Now(),
		lastPageAt: time.Now(),
	}
}

func (p *messageSyncProgress) touch(channel *discordgo.Channel, messages int) {
	if p == nil || channel == nil {
		return
	}
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.inflight[channel.ID]
	if !ok {
		entry = messageSyncInFlight{
			id:        channel.ID,
			name:      channel.Name,
			startedAt: now,
		}
	}
	entry.lastPageAt = now
	entry.pageCount++
	entry.pageMessages += messages
	p.inflight[channel.ID] = entry
	p.lastProgressAt = now
}

func (p *messageSyncProgress) record(channel *discordgo.Channel, count int) {
	p.complete(channel, count, "ok")
}

func (p *messageSyncProgress) recordSkip(channel *discordgo.Channel, err error) {
	if p == nil {
		return
	}
	outcome := syncErrorOutcome(err)
	p.mu.Lock()
	switch outcome {
	case "deferred_retryable":
		p.deferredRetryable++
	case "skipped_missing_access":
		p.skippedMissingAccess++
	case "skipped_unknown_channel":
		p.skippedUnknownChannel++
	}
	p.mu.Unlock()
	p.complete(channel, 0, outcome)
}

func (p *messageSyncProgress) complete(channel *discordgo.Channel, count int, outcome string) {
	if p == nil || p.syncer == nil || p.syncer.logger == nil {
		return
	}
	now := time.Now()
	p.mu.Lock()
	if channel != nil {
		delete(p.inflight, channel.ID)
	}
	p.processed++
	p.messages += count
	p.lastProgressAt = now
	shouldLog := p.processed == p.totalChannels ||
		p.processed == 1 ||
		p.processed%100 == 0 ||
		now.Sub(p.lastLogAt) >= p.logEvery
	if !shouldLog {
		p.mu.Unlock()
		return
	}
	p.lastLogAt = now
	channelID := ""
	channelName := ""
	if channel != nil {
		channelID = channel.ID
		channelName = channel.Name
	}
	activeChannels := len(p.inflight)
	deferred := p.deferredRetryable
	missingAccess := p.skippedMissingAccess
	unknownChannel := p.skippedUnknownChannel
	processed := p.processed
	totalChannels := p.totalChannels
	messages := p.messages
	elapsed := now.Sub(p.startedAt).Round(time.Second).String()
	percent := progress.Percent(int64(processed), int64(totalChannels))
	completion := progress.Completion(int64(processed), int64(totalChannels))
	p.mu.Unlock()
	p.syncer.logger.Info(
		"message sync progress",
		"guild_id", p.guildID,
		"processed_channels", processed,
		"total_channels", totalChannels,
		"remaining_channels", totalChannels-processed,
		"percent", percent,
		"completion", completion,
		"active_channels", activeChannels,
		"messages_written", messages,
		"deferred_channels", deferred,
		"skipped_missing_access_channels", missingAccess,
		"skipped_unknown_channel_channels", unknownChannel,
		"last_channel_id", channelID,
		"last_channel_name", channelName,
		"last_outcome", outcome,
		"elapsed", elapsed,
	)
}

func (p *messageSyncProgress) finish(err error) {
	if p == nil || p.syncer == nil || p.syncer.logger == nil {
		return
	}
	p.once.Do(func() {
		close(p.done)
		now := time.Now()
		p.mu.Lock()
		activeChannels := len(p.inflight)
		deferred := p.deferredRetryable
		missingAccess := p.skippedMissingAccess
		unknownChannel := p.skippedUnknownChannel
		processed := p.processed
		totalChannels := p.totalChannels
		messages := p.messages
		elapsed := now.Sub(p.startedAt).Round(time.Second).String()
		percent := progress.Percent(int64(processed), int64(totalChannels))
		completion := progress.Completion(int64(processed), int64(totalChannels))
		oldestID, oldestName, oldestElapsed, oldestIdle, oldestPages, oldestPageMessages := oldestInflightDetails(p.inflight, now)
		p.mu.Unlock()
		attrs := []any{
			"guild_id", p.guildID,
			"processed_channels", processed,
			"total_channels", totalChannels,
			"remaining_channels", totalChannels - processed,
			"percent", percent,
			"completion", completion,
			"active_channels", activeChannels,
			"messages_written", messages,
			"deferred_channels", deferred,
			"skipped_missing_access_channels", missingAccess,
			"skipped_unknown_channel_channels", unknownChannel,
			"elapsed", elapsed,
		}
		if oldestID != "" {
			attrs = append(attrs,
				"oldest_active_channel_id", oldestID,
				"oldest_active_channel_name", oldestName,
				"oldest_active_elapsed", oldestElapsed,
				"oldest_active_idle_for", oldestIdle,
				"oldest_active_pages", oldestPages,
				"oldest_active_page_messages", oldestPageMessages,
			)
		}
		if err != nil {
			attrs = append(attrs, "err", err)
			p.syncer.logger.Warn("message sync finished with error", attrs...)
			return
		}
		p.syncer.logger.Info("message sync finished", attrs...)
	})
}

func (p *messageSyncProgress) runWaitHeartbeat() {
	if p == nil || p.waitEvery <= 0 {
		return
	}
	ticker := time.NewTicker(p.waitEvery)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.logWaitHeartbeat()
		}
	}
}

func (p *messageSyncProgress) logWaitHeartbeat() {
	if p == nil || p.syncer == nil || p.syncer.logger == nil {
		return
	}
	now := time.Now()
	p.mu.Lock()
	if len(p.inflight) == 0 || now.Sub(p.lastProgressAt) < p.waitEvery {
		p.mu.Unlock()
		return
	}
	activeChannels := len(p.inflight)
	deferred := p.deferredRetryable
	missingAccess := p.skippedMissingAccess
	unknownChannel := p.skippedUnknownChannel
	processed := p.processed
	totalChannels := p.totalChannels
	messages := p.messages
	idleFor := now.Sub(p.lastProgressAt).Round(time.Second).String()
	elapsed := now.Sub(p.startedAt).Round(time.Second).String()
	percent := progress.Percent(int64(processed), int64(totalChannels))
	completion := progress.Completion(int64(processed), int64(totalChannels))
	oldestID, oldestName, oldestElapsed, oldestIdle, oldestPages, oldestPageMessages := oldestInflightDetails(p.inflight, now)
	p.mu.Unlock()
	p.syncer.logger.Info(
		"message sync waiting",
		"guild_id", p.guildID,
		"processed_channels", processed,
		"total_channels", totalChannels,
		"remaining_channels", totalChannels-processed,
		"percent", percent,
		"completion", completion,
		"active_channels", activeChannels,
		"messages_written", messages,
		"deferred_channels", deferred,
		"skipped_missing_access_channels", missingAccess,
		"skipped_unknown_channel_channels", unknownChannel,
		"idle_for", idleFor,
		"oldest_active_channel_id", oldestID,
		"oldest_active_channel_name", oldestName,
		"oldest_active_elapsed", oldestElapsed,
		"oldest_active_idle_for", oldestIdle,
		"oldest_active_pages", oldestPages,
		"oldest_active_page_messages", oldestPageMessages,
		"elapsed", elapsed,
	)
}

func oldestInflightDetails(channels map[string]messageSyncInFlight, now time.Time) (string, string, string, string, int, int) {
	var oldest messageSyncInFlight
	found := false
	for _, channel := range channels {
		if !found || channel.startedAt.Before(oldest.startedAt) {
			oldest = channel
			found = true
		}
	}
	if !found {
		return "", "", "", "", 0, 0
	}
	return oldest.id,
		oldest.name,
		now.Sub(oldest.startedAt).Round(time.Second).String(),
		now.Sub(oldest.lastPageAt).Round(time.Second).String(),
		oldest.pageCount,
		oldest.pageMessages
}

func syncErrorOutcome(err error) string {
	switch unavailableReason(err) {
	case "missing_access":
		return "skipped_missing_access"
	case "unknown_channel":
		return "skipped_unknown_channel"
	}
	if isRetryableSyncError(context.TODO(), err) {
		return "deferred_retryable"
	}
	return "skipped"
}
