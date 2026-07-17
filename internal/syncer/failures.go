package syncer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/store"
)

const syncMessagesFailureOperation = "sync_messages"

const (
	tailMessageFailureOperation     = "tail_message"
	tailMessageFailureRelatedKind   = "message_event"
	tailMessageFailureLedgerTimeout = 250 * time.Millisecond
)

var (
	errTailMessageHandlerReturned = errors.New("tail message handler returned an error")
	errTailMessageHandlerPanic    = errors.New("tail message handler panicked")
	errTailMessageHandlerTimeout  = errors.New("tail message handler timed out")
)

func (s *Syncer) recordChannelFailure(ctx context.Context, guildID, channelID string, failure error) error {
	if s == nil || s.store == nil || failure == nil {
		return nil
	}
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	return s.store.RecordFailure(ledgerCtx, store.FailureRef{
		Operation: syncMessagesFailureOperation,
		Source:    "discord",
		GuildID:   guildID,
		ChannelID: channelID,
	}, failure)
}

func (s *Syncer) resolveChannelFailures(ctx context.Context, guildID, channelID string) error {
	if s == nil || s.store == nil {
		return nil
	}
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	return s.store.ResolveFailures(ledgerCtx, store.FailureRef{
		Operation: syncMessagesFailureOperation,
		Source:    "discord",
		GuildID:   guildID,
		ChannelID: channelID,
	})
}

func failureLedgerContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(base, 5*time.Second)
}

func withFailureRecordError(failure, recordErr error) error {
	if recordErr == nil {
		return failure
	}
	return fmt.Errorf("%w (record failure ledger: %w)", failure, recordErr)
}

func (t *tailHandler) RecordTailFailure(failure discordclient.TailFailure) error {
	eventKind, messageEvent := normalizeTailMessageEventKind(failure.EventType)
	if !messageEvent {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(failure.EventType)), "MESSAGE_") {
			return errors.New("record tail message failure: unsupported event type")
		}
		return nil
	}
	failureKind, durableFailure, ok := tailMessageFailureSentinel(failure.Kind)
	if !ok {
		return errors.New("record tail message failure: unsupported failure kind")
	}
	if failure.MessageID == "" {
		return errors.New("record tail message failure: missing message id")
	}
	if t == nil || t.store == nil {
		return errors.New("record tail message failure: missing store")
	}

	ref := store.FailureRef{
		Operation:   tailMessageFailureOperation,
		Source:      "discord",
		GuildID:     failure.GuildID,
		ChannelID:   failure.ChannelID,
		MessageID:   failure.MessageID,
		RelatedKind: tailMessageFailureRelatedKind,
		RelatedID:   eventKind,
	}
	fallback := store.TailMessageFailureFallback{
		EventKind:   eventKind,
		FailureKind: failureKind,
		GuildID:     failure.GuildID,
		ChannelID:   failure.ChannelID,
		MessageID:   failure.MessageID,
	}

	var (
		diagnostics     store.FailurePersistenceDiagnostics
		ledgerElapsed   time.Duration
		fallbackElapsed time.Duration
		ledgerAttempted bool
		ledgerPersisted bool
		fallbackTried   bool
		fallbackSaved   bool
	)
	if !failure.ForceFallback {
		ledgerAttempted = true
		ledgerStartedAt := time.Now()
		ledgerCtx, cancel := context.WithTimeout(context.Background(), t.tailFailureLedgerTimeout())
		var ledgerErr error
		diagnostics, ledgerErr = t.store.RecordFailureWithMessageScopeTimed(ledgerCtx, ref, durableFailure)
		cancel()
		ledgerElapsed = time.Since(ledgerStartedAt)
		ledgerPersisted = ledgerErr == nil
	}
	if failure.ForceFallback || !ledgerPersisted {
		fallbackTried = true
		fallbackStartedAt := time.Now()
		fallbackErr := t.store.PersistTailMessageFailureFallback(fallback)
		fallbackElapsed = time.Since(fallbackStartedAt)
		fallbackSaved = fallbackErr == nil
	}
	t.logTailFailureDurability(
		failure,
		eventKind,
		failureKind,
		diagnostics,
		ledgerElapsed,
		fallbackElapsed,
		ledgerAttempted,
		ledgerPersisted,
		fallbackTried,
		fallbackSaved,
	)
	if ledgerPersisted || fallbackSaved {
		return nil
	}
	return errors.New("record tail message failure: no durable path succeeded")
}

func (t *tailHandler) tailFailureLedgerTimeout() time.Duration {
	if t != nil && t.failureLedgerTimeout > 0 {
		return t.failureLedgerTimeout
	}
	return tailMessageFailureLedgerTimeout
}

func (t *tailHandler) logTailFailureDurability(
	failure discordclient.TailFailure,
	eventKind string,
	failureKind string,
	diagnostics store.FailurePersistenceDiagnostics,
	ledgerElapsed time.Duration,
	fallbackElapsed time.Duration,
	ledgerAttempted bool,
	ledgerPersisted bool,
	fallbackAttempted bool,
	fallbackPersisted bool,
) {
	if t == nil || t.logger == nil {
		return
	}
	durablePath := "none"
	switch {
	case ledgerPersisted:
		durablePath = "ledger"
	case fallbackPersisted:
		durablePath = "fallback"
	}
	t.logger.LogAttrs(
		context.Background(),
		slog.LevelWarn,
		"tail message failure durability",
		slog.String("event_type", eventKind),
		slog.String("failure_kind", failureKind),
		slog.String("guild_id", failure.GuildID),
		slog.String("channel_id", failure.ChannelID),
		slog.String("message_id", failure.MessageID),
		slog.String("handler_stage", safeTailFailureStage(failure.HandlerStage)),
		slog.Duration("handler_stage_elapsed", failure.HandlerStageElapsed),
		slog.Duration("handler_elapsed", failure.HandlerElapsed),
		slog.String("join_outcome", safeTailFailureJoinOutcome(failure.JoinOutcome)),
		slog.Duration("join_elapsed", failure.JoinElapsed),
		slog.Bool("forced_fallback", failure.ForceFallback),
		slog.Bool("ledger_attempted", ledgerAttempted),
		slog.Bool("ledger_persisted", ledgerPersisted),
		slog.Duration("sql_pool_wait", diagnostics.PoolWait),
		slog.Duration("db_elapsed", diagnostics.DBElapsed),
		slog.Duration("ledger_elapsed", ledgerElapsed),
		slog.Time("ledger_deadline", diagnostics.ContextDeadline),
		slog.Int("sqlite_code", diagnostics.SQLiteCode),
		slog.Int("sqlite_category", diagnostics.SQLiteCategory),
		slog.Bool("fallback_attempted", fallbackAttempted),
		slog.Bool("fallback_persisted", fallbackPersisted),
		slog.Duration("fallback_elapsed", fallbackElapsed),
		slog.String("durable_path", durablePath),
	)
}

func safeTailFailureStage(stage discordclient.TailFailureStage) string {
	switch stage {
	case discordclient.TailFailureStageHandler,
		discordclient.TailFailureStageMessageUpdateRefetch,
		discordclient.TailFailureStageMessageBuild,
		discordclient.TailFailureStageCanonicalWrite,
		discordclient.TailFailureStageEventAppend,
		discordclient.TailFailureStageStateUpdate,
		discordclient.TailFailureStageCursorAdvance,
		discordclient.TailFailureStageCanonicalDelete,
		discordclient.TailFailureStageFailureResolution:
		return string(stage)
	default:
		return string(discordclient.TailFailureStageUnknown)
	}
}

func safeTailFailureJoinOutcome(outcome discordclient.TailFailureJoinOutcome) string {
	switch outcome {
	case discordclient.TailFailureJoinNotRequired,
		discordclient.TailFailureJoinJoined,
		discordclient.TailFailureJoinTimedOut:
		return string(outcome)
	default:
		return string(discordclient.TailFailureJoinNotRequired)
	}
}

func normalizeTailMessageEventKind(eventType string) (string, bool) {
	eventType = strings.ToLower(strings.TrimSpace(eventType))
	eventType = strings.TrimPrefix(eventType, "message_")
	switch eventType {
	case "create", "update", "delete":
		return eventType, true
	default:
		return "", false
	}
}

func tailMessageFailureSentinel(kind string) (string, error, bool) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	kind = strings.ReplaceAll(kind, "-", "_")
	switch kind {
	case "returned_error":
		return kind, errTailMessageHandlerReturned, true
	case "panic":
		return kind, errTailMessageHandlerPanic, true
	case "timeout":
		return kind, errTailMessageHandlerTimeout, true
	default:
		return "", nil, false
	}
}

func tailMessageFailureIdentity(guildID, channelID, messageID, eventKind string) store.FailureRef {
	return store.FailureRef{
		Operation:   tailMessageFailureOperation,
		Source:      "discord",
		GuildID:     guildID,
		ChannelID:   channelID,
		MessageID:   messageID,
		RelatedKind: tailMessageFailureRelatedKind,
		RelatedID:   eventKind,
	}
}

func (t *tailHandler) resolveMessageFailure(ctx context.Context, guildID, channelID, messageID, eventKind string) error {
	if t == nil || t.store == nil {
		return nil
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageFailureResolution)
	eventKind, ok := normalizeTailMessageEventKind(eventKind)
	if !ok {
		return errors.New("resolve tail message failure: unsupported event type")
	}
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	return t.store.ResolveFailureIdentity(
		ledgerCtx,
		tailMessageFailureIdentity(guildID, channelID, messageID, eventKind),
	)
}

func (s *Syncer) resolveTailMessageCreateFailuresForMessages(ctx context.Context, messages []*discordgo.Message, fallbackGuildID string) error {
	if s == nil || s.store == nil || len(messages) == 0 {
		return nil
	}
	type scope struct {
		guildID   string
		channelID string
	}
	messageIDs := map[scope][]string{}
	for _, message := range messages {
		if message == nil || message.ID == "" || message.ChannelID == "" {
			continue
		}
		guildID := message.GuildID
		if guildID == "" {
			guildID = fallbackGuildID
		}
		messageIDs[scope{guildID: guildID, channelID: message.ChannelID}] = append(
			messageIDs[scope{guildID: guildID, channelID: message.ChannelID}],
			message.ID,
		)
	}
	if len(messageIDs) == 0 {
		return nil
	}
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	for key, ids := range messageIDs {
		if err := s.store.ResolveMessageFailures(ledgerCtx, store.FailureRef{
			Operation:   tailMessageFailureOperation,
			Source:      "discord",
			GuildID:     key.guildID,
			ChannelID:   key.channelID,
			RelatedKind: tailMessageFailureRelatedKind,
			RelatedID:   "create",
		}, ids); err != nil {
			return err
		}
	}
	return nil
}

func tailMessageFailureRef(failure store.Failure) store.FailureRef {
	return store.FailureRef{
		Operation:   failure.Operation,
		Source:      failure.Source,
		GuildID:     failure.GuildID,
		ChannelID:   failure.ChannelID,
		MessageID:   failure.MessageID,
		RelatedKind: failure.RelatedKind,
		RelatedID:   failure.RelatedID,
	}
}

func validateTailMessageReplay(failure store.Failure, message *discordgo.Message) error {
	switch {
	case message == nil:
		return errors.New("exact message fetch returned no message")
	case message.ID == "":
		return errors.New("exact message fetch returned an empty message id")
	case message.ID != failure.MessageID:
		return errors.New("exact message fetch returned a different message id")
	case message.ChannelID == "":
		return errors.New("exact message fetch returned an empty channel id")
	case message.ChannelID != failure.ChannelID:
		return errors.New("exact message fetch returned a different channel id")
	case message.GuildID != "" && failure.GuildID != "" && message.GuildID != failure.GuildID:
		return errors.New("exact message fetch returned a different guild id")
	default:
		return nil
	}
}
