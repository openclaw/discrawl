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

func (s *Syncer) RunTail(ctx context.Context, guildIDs []string, repairEvery time.Duration) error {
	if err := s.importTailMessageFailureFallbacks(ctx); err != nil {
		return err
	}
	handler := &tailHandler{
		guilds:                makeGuildSet(guildIDs),
		store:                 s.store,
		client:                s.client,
		attachmentTextEnabled: s.attachmentTextEnabled,
		onReady:               s.tailReady,
		logger:                s.logger,
	}
	if repairEvery <= 0 {
		return s.client.Tail(ctx, handler)
	}
	tailCtx, cancelTail := context.WithCancel(ctx)
	defer cancelTail()
	var closeOnce sync.Once
	closeClient := func() {
		if closeable, ok := s.client.(closeableClient); ok {
			_ = closeable.Close()
		}
	}
	tailDone := make(chan error, 1)
	go func() {
		tailDone <- s.client.Tail(tailCtx, handler)
	}()
	ticker := time.NewTicker(repairEvery)
	defer ticker.Stop()
	var activeRepair *tailRepairRun
	var repairDone <-chan tailRepairResult
	for {
		select {
		case <-ctx.Done():
			cancelTail()
			repairErr := s.joinTailRepair(activeRepair, "parent_shutdown")
			closeOnce.Do(closeClient)
			tailErr := <-tailDone
			if discordclient.IsFatalTailError(tailErr) {
				if repairErr != nil {
					return errors.Join(tailErr, repairErr)
				}
				return tailErr
			}
			if repairErr != nil {
				return repairErr
			}
			return nil
		case err := <-tailDone:
			cancelTail()
			repairErr := s.joinTailRepair(activeRepair, "tail_return")
			closeOnce.Do(closeClient)
			if repairErr != nil {
				return errors.Join(err, repairErr)
			}
			return err
		case result := <-repairDone:
			s.logTailRepairResult(result)
			activeRepair = nil
			repairDone = nil
		case <-ticker.C:
			if activeRepair != nil {
				continue
			}
			activeRepair = s.startTailRepair(ctx, guildIDs)
			if activeRepair != nil {
				repairDone = activeRepair.done
			}
		}
	}
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
	if s == nil || !s.tailRepairMu.TryLock() {
		if s != nil && s.logger != nil {
			s.logger.Warn("tail repair start skipped", "reason", "repair_already_running")
		}
		return nil
	}
	repairCtx, cancel := context.WithCancel(ctx)
	done := make(chan tailRepairResult, 1)
	startedAt := time.Now()
	go func() {
		defer s.tailRepairMu.Unlock()
		_, err := s.runTailRepair(repairCtx, SyncOptions{
			GuildIDs:     guildIDs,
			Full:         false,
			RepairReason: "tail_repair",
		})
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

func (s *Syncer) joinTailRepair(repair *tailRepairRun, reason string) error {
	if repair == nil {
		return nil
	}
	startedAt := time.Now()
	repair.cancel()
	timeout := s.tailRepairJoinTimeout
	if timeout <= 0 {
		timeout = defaultTailRepairJoinTimeout
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

type tailHandler struct {
	guilds                map[string]struct{}
	store                 *store.Store
	client                Client
	attachmentTextEnabled bool
	failureLedgerTimeout  time.Duration
	onReady               func(context.Context) error
	logger                *slog.Logger
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
	if !t.allowGuild(msg.GuildID) {
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
	if msg.GuildID != "" && !t.allowGuild(msg.GuildID) {
		return nil
	}
	var err error
	msg, err = t.messageUpdateSnapshot(ctx, msg)
	if err != nil {
		return err
	}
	if msg == nil || !t.allowGuild(msg.GuildID) {
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
	if !t.allowGuild(evt.GuildID) {
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
	if !t.allowGuild(channel.GuildID) {
		return nil
	}
	return t.store.UpsertChannel(ctx, toChannelRecord(channel, marshalJSONString(channel, "{}")))
}

func (t *tailHandler) OnGuildUpsert(ctx context.Context, guild *discordgo.Guild) error {
	if guild == nil || guild.Unavailable || !t.allowGuild(guild.ID) {
		return nil
	}
	return t.store.UpsertGuild(ctx, store.GuildRecord{
		ID:      guild.ID,
		Name:    guild.Name,
		Icon:    guild.Icon,
		RawJSON: marshalJSONString(guild, "{}"),
	})
}

func (t *tailHandler) OnGuildDelete(ctx context.Context, guild *discordgo.Guild) error {
	if guild == nil || guild.Unavailable || !t.allowGuild(guild.ID) {
		return nil
	}
	return t.store.MarkGuildDeleted(ctx, guild.ID, "discord-gateway", "guild-delete-event")
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
	return t.store.MarkMemberDeleted(ctx, guildID, userID, "discord-gateway", "member-remove-event")
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
