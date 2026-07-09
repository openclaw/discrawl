package syncer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/store"
)

func (s *Syncer) SetRepairOffset(offset time.Duration) {
	if s == nil {
		return
	}
	if offset <= 0 {
		offset = 0
	}
	s.tailRepairOffsetMu.Lock()
	s.tailRepairOffset = offset
	s.tailRepairOffsetMu.Unlock()
}

func (s *Syncer) repairOffset() time.Duration {
	if s == nil {
		return 0
	}
	s.tailRepairOffsetMu.RLock()
	defer s.tailRepairOffsetMu.RUnlock()
	return s.tailRepairOffset
}

func (s *Syncer) RunTail(ctx context.Context, guildIDs []string, repairEvery time.Duration) error {
	if err := s.importTailMessageFailureFallbacks(ctx); err != nil {
		return err
	}
	handler := &tailHandler{
		guilds:                 makeGuildSet(guildIDs),
		store:                  s.store,
		client:                 s.client,
		attachmentTextEnabled:  s.attachmentTextEnabled,
		onReady:                s.tailReady,
		logger:                 s.logger,
		exclusions:             s.channelExclusions,
		channels:               map[string]tailChannel{},
		kindExcludedChannelIDs: map[string]struct{}{},
	}
	if err := handler.seedChannelExclusions(ctx); err != nil {
		return fmt.Errorf("seed tail channel exclusions: %w", err)
	}
	tailCtx, cancelTail := context.WithCancel(ctx)
	defer cancelTail()
	tailDone := make(chan error, 1)
	go func() {
		tailDone <- s.client.Tail(tailCtx, handler)
	}()

	var repairTimer *time.Timer
	var repairC <-chan time.Time
	defer func() {
		if repairTimer != nil {
			repairTimer.Stop()
		}
	}()
	repairOffset := s.repairOffset()
	scheduleRepair := func() {
		if repairEvery <= 0 {
			repairC = nil
			return
		}
		delay := nextTailRepairDelay(time.Now(), repairEvery, repairOffset)
		if repairTimer == nil {
			repairTimer = time.NewTimer(delay)
		} else {
			if !repairTimer.Stop() {
				select {
				case <-repairTimer.C:
				default:
				}
			}
			repairTimer.Reset(delay)
		}
		repairC = repairTimer.C
	}
	scheduleRepair()

	var activeRepair *tailRepairRun
	var repairDone <-chan tailRepairResult
	for {
		select {
		case <-ctx.Done():
			return s.finishTailRun(
				cancelTail,
				tailDone,
				activeRepair,
				nil,
				false,
				true,
				"parent_shutdown",
			)
		case err := <-tailDone:
			return s.finishTailRun(
				cancelTail,
				nil,
				activeRepair,
				err,
				true,
				false,
				"tail_return",
			)
		case <-repairC:
			repairC = nil
			activeRepair = s.startTailRepair(tailCtx, guildIDs)
			if activeRepair == nil {
				scheduleRepair()
				continue
			}
			repairDone = activeRepair.done
		case result := <-repairDone:
			s.logTailRepairResult(result)
			activeRepair = nil
			repairDone = nil
			// Schedule from completion so an overrun cannot immediately replay stale ticks.
			scheduleRepair()
		}
	}
}

func (s *Syncer) finishTailRun(
	cancelTail context.CancelFunc,
	tailDone <-chan error,
	repair *tailRepairRun,
	tailErr error,
	tailFinished bool,
	requestedShutdown bool,
	repairJoinReason string,
) error {
	cancelTail()

	timeout := s.tailShutdownTimeout
	if timeout <= 0 {
		timeout = defaultTailShutdownTimeout
	}
	startedAt := time.Now()
	repairErr := s.joinTailRepair(repair, repairJoinReason, timeout)

	var closeDone <-chan error
	if closeable, ok := s.client.(closeableClient); ok {
		done := make(chan error, 1)
		closeDone = done
		go func() {
			done <- closeable.Close()
		}()
	}

	if tailFinished {
		tailDone = nil
	}
	if tailDone == nil && closeDone == nil {
		return tailRunResult(requestedShutdown, tailErr, repairErr, nil, nil)
	}

	remaining := timeout - time.Since(startedAt)
	if remaining <= 0 {
		return tailRunResult(
			requestedShutdown,
			tailErr,
			repairErr,
			nil,
			fmt.Errorf("tail shutdown timed out after %s", timeout),
		)
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()

	var closeErr error
	for tailDone != nil || closeDone != nil {
		select {
		case err := <-tailDone:
			if tailErr == nil {
				tailErr = err
			}
			tailDone = nil
		case err := <-closeDone:
			closeErr = err
			closeDone = nil
		case <-timer.C:
			return tailRunResult(
				requestedShutdown,
				tailErr,
				repairErr,
				closeErr,
				fmt.Errorf("tail shutdown timed out after %s", timeout),
			)
		}
	}
	return tailRunResult(requestedShutdown, tailErr, repairErr, closeErr, nil)
}

func tailRunResult(
	requestedShutdown bool,
	tailErr error,
	repairErr error,
	closeErr error,
	shutdownErr error,
) error {
	if requestedShutdown && !discordclient.IsFatalTailError(tailErr) {
		tailErr = nil
	}
	return errors.Join(tailErr, repairErr, closeErr, shutdownErr)
}

const defaultTailRepairJoinTimeout = 5 * time.Second

type tailRepairResult struct {
	err     error
	elapsed time.Duration
}

type tailRepairRun struct {
	cancel context.CancelFunc
	done   <-chan tailRepairResult
}

func (s *Syncer) importTailMessageFailureFallbacks(ctx context.Context) error {
	if s == nil || s.store == nil {
		return nil
	}
	imported, err := s.store.ImportTailMessageFailureFallbacks(ctx)
	if err != nil {
		return fmt.Errorf("import tail message failure fallbacks: %w", err)
	}
	if imported > 0 && s.logger != nil {
		s.logger.Info("tail message failure fallbacks imported", "count", imported)
	}
	return nil
}

func (s *Syncer) startTailRepair(ctx context.Context, guildIDs []string) *tailRepairRun {
	if s == nil || !s.tailRepairRunMu.TryLock() {
		if s != nil && s.logger != nil {
			s.logger.Warn("tail repair start skipped", "reason", "repair_already_running")
		}
		return nil
	}
	repairCtx, cancel := context.WithCancel(ctx)
	done := make(chan tailRepairResult, 1)
	startedAt := time.Now()
	go func() {
		defer s.tailRepairRunMu.Unlock()
		_, err := s.runTailRepair(repairCtx, tailRepairSyncOptions(guildIDs))
		done <- tailRepairResult{err: err, elapsed: time.Since(startedAt)}
	}()
	return &tailRepairRun{cancel: cancel, done: done}
}

func (s *Syncer) runTailRepair(ctx context.Context, opts SyncOptions) (SyncStats, error) {
	if s.tailRepair != nil {
		return s.tailRepair(ctx, opts)
	}
	return s.Sync(ctx, opts)
}

func (s *Syncer) joinTailRepair(repair *tailRepairRun, reason string, maxWait time.Duration) error {
	if repair == nil {
		return nil
	}
	startedAt := time.Now()
	repair.cancel()
	timeout := s.tailRepairJoinTimeout
	if timeout <= 0 {
		timeout = defaultTailRepairJoinTimeout
	}
	if maxWait > 0 && timeout > maxWait {
		timeout = maxWait
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	outcome := "timed_out"
	var result tailRepairResult
	select {
	case result = <-repair.done:
		outcome = "joined"
	case <-timer.C:
	}
	if s.logger != nil {
		s.logger.Info(
			"tail repair join completed",
			"reason", reason,
			"outcome", outcome,
			"join_elapsed", time.Since(startedAt),
		)
	}
	if outcome == "joined" {
		s.logTailRepairResult(result)
		return nil
	}
	return fmt.Errorf("%w: scheduled tail repair join timed out", discordclient.ErrFatalTail)
}

func (s *Syncer) logTailRepairResult(result tailRepairResult) {
	if result.err == nil || errors.Is(result.err, context.Canceled) || s == nil || s.logger == nil {
		return
	}
	failureKind := "returned_error"
	if errors.Is(result.err, context.DeadlineExceeded) {
		failureKind = "timeout"
	}
	s.logger.Warn(
		"tail repair failed",
		"failure_kind", failureKind,
		"elapsed", result.elapsed,
	)
}

func tailRepairSyncOptions(guildIDs []string) SyncOptions {
	return SyncOptions{
		GuildIDs:     append([]string(nil), guildIDs...),
		Full:         false,
		SkipMembers:  true,
		LatestOnly:   true,
		RepairReason: "tail_repair",
	}
}

func nextTailRepairDelay(now time.Time, repairEvery, repairOffset time.Duration) time.Duration {
	if repairEvery <= 0 {
		return 0
	}
	if repairOffset <= 0 {
		return repairEvery
	}
	normalizedOffset := repairOffset % repairEvery
	civilNow := civilTimelineTime(now)
	nextCivil := civilNow.Truncate(repairEvery).Add(normalizedOffset)
	if !nextCivil.After(civilNow) {
		nextCivil = nextCivil.Add(repairEvery)
	}
	for {
		// A candidate that does not round-trip is a nonexistent civil slot.
		next := time.Date(
			nextCivil.Year(),
			nextCivil.Month(),
			nextCivil.Day(),
			nextCivil.Hour(),
			nextCivil.Minute(),
			nextCivil.Second(),
			nextCivil.Nanosecond(),
			now.Location(),
		)
		if civilTimelineTime(next).Equal(nextCivil) && next.After(now) {
			return next.Sub(now)
		}
		nextCivil = nextCivil.Add(repairEvery)
	}
}

func civilTimelineTime(value time.Time) time.Time {
	return time.Date(
		value.Year(),
		value.Month(),
		value.Day(),
		value.Hour(),
		value.Minute(),
		value.Second(),
		value.Nanosecond(),
		time.UTC,
	)
}

type tailHandler struct {
	guilds                 map[string]struct{}
	store                  *store.Store
	client                 Client
	attachmentTextEnabled  bool
	failureLedgerTimeout   time.Duration
	onReady                func(context.Context) error
	logger                 *slog.Logger
	exclusions             channelExclusions
	exclusionMu            sync.RWMutex
	channels               map[string]tailChannel
	kindExcludedChannelIDs map[string]struct{}
}

type tailChannel struct {
	parentID string
	kind     string
}

func (t *tailHandler) OnTailReady(ctx context.Context) error {
	if t.onReady == nil {
		return nil
	}
	return t.onReady(ctx)
}

func (t *tailHandler) OnTailFailure(failure discordclient.TailFailure) {
	if t == nil || t.logger == nil {
		return
	}
	attrs := []any{
		"event_type", failure.EventType,
		"failure_kind", failure.Kind,
	}
	if failure.GuildID != "" {
		attrs = append(attrs, "guild_id", failure.GuildID)
	}
	if failure.ChannelID != "" {
		attrs = append(attrs, "channel_id", failure.ChannelID)
	}
	if failure.MessageID != "" {
		attrs = append(attrs, "message_id", failure.MessageID)
	}
	if failure.UserID != "" {
		attrs = append(attrs, "user_id", failure.UserID)
	}
	t.logger.Warn("tail event handler failed", attrs...)
}

func (t *tailHandler) OnMessageCreate(ctx context.Context, msg *discordgo.Message) error {
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageHandler)
	if msg == nil || !t.allowGuild(msg.GuildID) || t.excludeChannel(msg.ChannelID) {
		return nil
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageMessageBuild)
	mutation, err := buildMessageMutation(ctx, msg, "", "", false, t.attachmentTextEnabled)
	if err != nil {
		return err
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageCanonicalWrite)
	if err := t.store.UpsertMessages(ctx, []store.MessageMutation{mutation}); err != nil {
		return err
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageEventAppend)
	if err := t.store.AppendMessageEvent(ctx, msg.GuildID, msg.ChannelID, msg.ID, "create", msg); err != nil {
		return err
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageStateUpdate)
	if err := t.store.SetSyncState(ctx, "tail:last_event", msg.ID); err != nil {
		return err
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageCursorAdvance)
	if err := t.store.AdvanceChannelLatestMessageID(ctx, msg.ChannelID, msg.ID); err != nil {
		return err
	}
	return t.resolveMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, "create")
}

func (t *tailHandler) OnMessageUpdate(ctx context.Context, msg *discordgo.Message) error {
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageHandler)
	if msg == nil {
		return nil
	}
	if t.excludeChannel(msg.ChannelID) || (msg.GuildID != "" && !t.allowGuild(msg.GuildID)) {
		return nil
	}
	var err error
	msg, err = t.messageUpdateSnapshot(ctx, msg)
	if err != nil {
		return err
	}
	if msg == nil || !t.allowGuild(msg.GuildID) || t.excludeChannel(msg.ChannelID) {
		return nil
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageMessageBuild)
	mutation, err := buildMessageMutation(ctx, msg, "", "", false, t.attachmentTextEnabled)
	if err != nil {
		return err
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageCanonicalWrite)
	if err := t.store.UpsertMessages(ctx, []store.MessageMutation{mutation}); err != nil {
		return err
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageEventAppend)
	if err := t.store.AppendMessageEvent(ctx, msg.GuildID, msg.ChannelID, msg.ID, "update", msg); err != nil {
		return err
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageStateUpdate)
	if err := t.store.SetSyncState(ctx, "tail:last_event", msg.ID); err != nil {
		return err
	}
	return t.resolveMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, "update")
}

func (t *tailHandler) messageUpdateSnapshot(ctx context.Context, msg *discordgo.Message) (*discordgo.Message, error) {
	if t.client == nil || msg.ChannelID == "" || msg.ID == "" {
		if isPartialMessageUpdate(msg) {
			return nil, nil
		}
		return msg, nil
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageMessageUpdateRefetch)
	full, err := t.client.ChannelMessage(ctx, msg.ChannelID, msg.ID)
	if err != nil {
		return nil, fmt.Errorf("fetch message update %s/%s: %w", msg.ChannelID, msg.ID, err)
	}
	if full != nil {
		if err := validateMessageUpdateSnapshotIdentity(msg, full); err != nil {
			return nil, err
		}
		if full.ID == "" {
			full.ID = msg.ID
		}
		if full.GuildID == "" {
			full.GuildID = msg.GuildID
		}
		if full.ChannelID == "" {
			full.ChannelID = msg.ChannelID
		}
		discordclient.EnrichTailFailureMetadata(ctx, full)
		return full, nil
	}
	if isPartialMessageUpdate(msg) {
		return nil, nil
	}
	return msg, nil
}

func validateMessageUpdateSnapshotIdentity(partial, full *discordgo.Message) error {
	switch {
	case partial == nil || full == nil:
		return nil
	case full.ID != "" && partial.ID != "" && full.ID != partial.ID:
		return fmt.Errorf(
			"fetched message update returned different message id: event=%s fetched=%s",
			partial.ID,
			full.ID,
		)
	case full.ChannelID != "" && partial.ChannelID != "" && full.ChannelID != partial.ChannelID:
		return fmt.Errorf(
			"fetched message update returned different channel id: event=%s fetched=%s",
			partial.ChannelID,
			full.ChannelID,
		)
	case full.GuildID != "" && partial.GuildID != "" && full.GuildID != partial.GuildID:
		return fmt.Errorf(
			"fetched message update returned different guild id: event=%s fetched=%s",
			partial.GuildID,
			full.GuildID,
		)
	default:
		return nil
	}
}

func isPartialMessageUpdate(msg *discordgo.Message) bool {
	return msg == nil || msg.Author == nil || msg.Timestamp.IsZero()
}

func (t *tailHandler) OnMessageDelete(ctx context.Context, evt *discordgo.MessageDelete) error {
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageHandler)
	if evt == nil || !t.allowGuild(evt.GuildID) || t.excludeChannel(evt.ChannelID) {
		return nil
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageCanonicalDelete)
	if err := t.store.MarkMessageDeleted(ctx, evt.GuildID, evt.ChannelID, evt.ID, evt); err != nil {
		return err
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageStateUpdate)
	if err := t.store.SetSyncState(ctx, "tail:last_event", evt.ID); err != nil {
		return err
	}
	return t.resolveMessageFailure(ctx, evt.GuildID, evt.ChannelID, evt.ID, "delete")
}

func (t *tailHandler) OnChannelUpsert(ctx context.Context, channel *discordgo.Channel) error {
	if channel == nil || !t.allowGuild(channel.GuildID) {
		return nil
	}
	t.trackChannelExclusion(channel)
	return t.store.UpsertChannel(ctx, toChannelRecord(channel, marshalJSONString(channel, "{}")))
}

func (t *tailHandler) OnMemberUpsert(ctx context.Context, guildID string, member *discordgo.Member) error {
	if !t.allowGuild(guildID) || member == nil || member.User == nil {
		return nil
	}
	return t.store.UpsertMember(ctx, toMemberRecord(guildID, member))
}

func (t *tailHandler) OnMemberDelete(ctx context.Context, guildID, userID string) error {
	if !t.allowGuild(guildID) {
		return nil
	}
	return t.store.DeleteMember(ctx, guildID, userID)
}

func (t *tailHandler) TailAllowsGuild(guildID string) bool {
	return t.allowGuild(guildID)
}

func (t *tailHandler) allowGuild(guildID string) bool {
	if len(t.guilds) == 0 {
		return true
	}
	_, ok := t.guilds[guildID]
	return ok
}

func (t *tailHandler) seedChannelExclusions(ctx context.Context) error {
	if t.store == nil {
		return nil
	}
	channels, err := t.store.Channels(ctx, "")
	if err != nil {
		return err
	}
	t.exclusionMu.Lock()
	defer t.exclusionMu.Unlock()
	t.channels = make(map[string]tailChannel, len(channels))
	if t.kindExcludedChannelIDs == nil {
		t.kindExcludedChannelIDs = map[string]struct{}{}
	}
	for _, channel := range channels {
		t.channels[channel.ID] = tailChannel{
			parentID: storedChannelParentID(channel),
			kind:     channel.Kind,
		}
	}
	t.rebuildChannelExclusionsLocked()
	return nil
}

func (t *tailHandler) excludeChannel(channelID string) bool {
	if t.exclusions.excludesID(channelID) {
		return true
	}
	t.exclusionMu.RLock()
	defer t.exclusionMu.RUnlock()
	_, ok := t.kindExcludedChannelIDs[channelID]
	return ok
}

func (t *tailHandler) trackChannelExclusion(channel *discordgo.Channel) {
	if channel == nil {
		return
	}
	t.exclusionMu.Lock()
	defer t.exclusionMu.Unlock()
	if t.channels == nil {
		t.channels = map[string]tailChannel{}
	}
	t.channels[channel.ID] = tailChannel{
		parentID: channel.ParentID,
		kind:     channelKind(channel),
	}
	t.rebuildChannelExclusionsLocked()
}

func (t *tailHandler) rebuildChannelExclusionsLocked() {
	if t.kindExcludedChannelIDs == nil {
		t.kindExcludedChannelIDs = map[string]struct{}{}
	}
	clear(t.kindExcludedChannelIDs)
	decisions := make(map[string]bool, len(t.channels))
	var excludes func(string, map[string]struct{}) bool
	excludes = func(channelID string, visiting map[string]struct{}) bool {
		if decision, ok := decisions[channelID]; ok {
			return decision
		}
		if t.exclusions.excludesID(channelID) {
			decisions[channelID] = true
			return true
		}
		channel, ok := t.channels[channelID]
		if !ok {
			decisions[channelID] = false
			return false
		}
		if t.exclusions.excludesKind(channel.kind) {
			decisions[channelID] = true
			return true
		}
		if channel.parentID == "" {
			decisions[channelID] = false
			return false
		}
		if visiting == nil {
			visiting = map[string]struct{}{}
		}
		if _, ok := visiting[channelID]; ok {
			return false
		}
		visiting[channelID] = struct{}{}
		excluded := excludes(channel.parentID, visiting)
		delete(visiting, channelID)
		decisions[channelID] = excluded
		return excluded
	}
	for channelID := range t.channels {
		if excludes(channelID, nil) {
			t.kindExcludedChannelIDs[channelID] = struct{}{}
		}
	}
}
