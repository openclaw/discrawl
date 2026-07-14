package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/store"
)

func TestTailFailureLedgerHelpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	var nilSyncer *Syncer
	require.NoError(t, nilSyncer.recordChannelFailure(ctx, "g", "c", errors.New("ignored")))
	require.NoError(t, nilSyncer.resolveChannelFailures(ctx, "g", "c"))
	var nilHandler *tailHandler
	require.NoError(t, nilHandler.resolveMessageFailure(ctx, "g", "c", "m", "create"))

	original := errors.New("original")
	require.ErrorIs(t, withFailureRecordError(original, nil), original)
	require.ErrorContains(t, withFailureRecordError(original, errors.New("ledger")), "record failure ledger: ledger")
}

func TestRecordTailFailurePersistsExactEventAwareIdentities(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	handler := &tailHandler{store: st}
	tests := []struct {
		eventType   string
		eventKind   string
		failureKind string
		messageID   string
		wantMessage string
	}{
		{
			eventType:   "MESSAGE_CREATE",
			eventKind:   "create",
			failureKind: "returned_error",
			messageID:   "m-create",
			wantMessage: errTailMessageHandlerReturned.Error(),
		},
		{
			eventType:   "MESSAGE_UPDATE",
			eventKind:   "update",
			failureKind: "panic",
			messageID:   "m-update",
			wantMessage: errTailMessageHandlerPanic.Error(),
		},
		{
			eventType:   "MESSAGE_DELETE",
			eventKind:   "delete",
			failureKind: "timeout",
			messageID:   "m-delete",
			wantMessage: errTailMessageHandlerTimeout.Error(),
		},
	}
	for _, tt := range tests {
		require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
			EventType: tt.eventType,
			Kind:      tt.failureKind,
			GuildID:   "g1",
			ChannelID: "c1",
			MessageID: tt.messageID,
			UserID:    "sensitive user field token=do-not-store",
		}))
	}

	report, err := st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 3)
	byEvent := make(map[string]store.Failure, len(report.Failures))
	for _, failure := range report.Failures {
		require.Equal(t, tailMessageFailureOperation, failure.Operation)
		require.Equal(t, "discord", failure.Source)
		require.Equal(t, "g1", failure.GuildID)
		require.Equal(t, "c1", failure.ChannelID)
		require.Equal(t, tailMessageFailureRelatedKind, failure.RelatedKind)
		require.NotContains(t, failure.ErrorMessage, "do-not-store")
		byEvent[failure.RelatedID] = failure
	}
	for _, tt := range tests {
		require.Equal(t, tt.messageID, byEvent[tt.eventKind].MessageID)
		require.Equal(t, tt.wantMessage, byEvent[tt.eventKind].ErrorMessage)
	}

	require.NoError(t, handler.resolveMessageFailure(ctx, "g1", "c1", "m-create", "create"))
	report, err = st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 2)
	require.ElementsMatch(t, []string{"update", "delete"}, []string{
		report.Failures[0].RelatedID,
		report.Failures[1].RelatedID,
	})
}

func TestRecordTailFailureRecoversCanonicalMessageScope(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, st.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			CreatedAt:         now,
			Content:           "test",
			NormalizedContent: "test",
			RawJSON:           `{}`,
		},
	}}))

	handler := &tailHandler{store: st}
	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_UPDATE",
		Kind:      "returned_error",
		MessageID: "m1",
	}))

	report, err := st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "g1", report.Failures[0].GuildID)
	require.Equal(t, "c1", report.Failures[0].ChannelID)
	require.Equal(t, tailMessageFailureRelatedKind, report.Failures[0].RelatedKind)
	require.Equal(t, "update", report.Failures[0].RelatedID)
}

func TestRecordTailFailurePoolStarvationFallsBack(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()
	st.DB().SetMaxOpenConns(1)

	held, err := st.DB().Conn(ctx)
	require.NoError(t, err)
	handler := &tailHandler{
		store:                st,
		failureLedgerTimeout: 20 * time.Millisecond,
	}
	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "timeout",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m1",
	}))
	require.NoError(t, held.Close())

	report, err := st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Empty(t, report.Failures)
	imported, err := st.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, imported)
	imported, err = st.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Zero(t, imported)

	report, err = st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "create", report.Failures[0].RelatedID)
	require.Equal(t, "timeout", report.Failures[0].ErrorClass)
}

func TestRecordTailFailureForcedFallbackSkipsLedger(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	handler := &tailHandler{store: st}
	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType:     "MESSAGE_DELETE",
		Kind:          "panic",
		GuildID:       "g1",
		ChannelID:     "c1",
		MessageID:     "m1",
		ForceFallback: true,
	}))

	report, err := st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Empty(t, report.Failures)
	imported, err := st.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, imported)
	report, err = st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "delete", report.Failures[0].RelatedID)
	require.Equal(t, "panic", report.Failures[0].ErrorClass)
}

func TestRecordTailFailureLogsOnlySafeStructuredDiagnostics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	out := &lockedBuffer{}
	handler := &tailHandler{store: st, logger: newTestLogger(out)}
	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType:           "MESSAGE_UPDATE",
		Kind:                "returned_error",
		GuildID:             "g1",
		ChannelID:           "c1",
		MessageID:           "m1",
		UserID:              "secret token=do-not-log",
		HandlerStage:        discordclient.TailFailureStageMessageUpdateRefetch,
		HandlerStageElapsed: 2 * time.Millisecond,
		HandlerElapsed:      5 * time.Millisecond,
		JoinElapsed:         time.Millisecond,
		JoinOutcome:         discordclient.TailFailureJoinJoined,
	}))

	logged := out.String()
	require.Contains(t, logged, `msg="tail message failure durability"`)
	require.Contains(t, logged, "event_type=update")
	require.Contains(t, logged, "failure_kind=returned_error")
	require.Contains(t, logged, "handler_stage=message_update_refetch")
	require.Contains(t, logged, "handler_stage_elapsed=2ms")
	require.Contains(t, logged, "handler_elapsed=5ms")
	require.Contains(t, logged, "join_outcome=joined")
	require.Contains(t, logged, "sql_pool_wait=")
	require.Contains(t, logged, "db_elapsed=")
	require.Contains(t, logged, "ledger_elapsed=")
	require.Contains(t, logged, "ledger_deadline=")
	require.Contains(t, logged, "fallback_elapsed=")
	require.Contains(t, logged, "sqlite_code=0")
	require.Contains(t, logged, "sqlite_category=0")
	require.Contains(t, logged, "durable_path=ledger")
	require.NotContains(t, logged, "do-not-log")
	require.NotContains(t, logged, "err=")
}

func TestSafeTailFailureStageAllowsOnlyDiagnosticVocabulary(t *testing.T) {
	t.Parallel()

	for _, stage := range []discordclient.TailFailureStage{
		discordclient.TailFailureStageHandler,
		discordclient.TailFailureStageMessageUpdateRefetch,
		discordclient.TailFailureStageMessageBuild,
		discordclient.TailFailureStageCanonicalWrite,
		discordclient.TailFailureStageEventAppend,
		discordclient.TailFailureStageStateUpdate,
		discordclient.TailFailureStageCursorAdvance,
		discordclient.TailFailureStageCanonicalDelete,
		discordclient.TailFailureStageFailureResolution,
	} {
		require.Equal(t, string(stage), safeTailFailureStage(stage))
	}
	require.Equal(
		t,
		string(discordclient.TailFailureStageUnknown),
		safeTailFailureStage("sensitive raw stage"),
	)
}

func TestValidateTailMessageReplayRequiresExactIdentity(t *testing.T) {
	t.Parallel()

	failure := store.Failure{
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m1",
	}
	tests := []struct {
		name    string
		message *discordgo.Message
		wantErr string
	}{
		{name: "missing message", wantErr: "no message"},
		{name: "empty message id", message: &discordgo.Message{}, wantErr: "empty message id"},
		{
			name:    "different message id",
			message: &discordgo.Message{ID: "other", ChannelID: "c1"},
			wantErr: "different message id",
		},
		{
			name:    "empty channel id",
			message: &discordgo.Message{ID: "m1"},
			wantErr: "empty channel id",
		},
		{
			name:    "different channel id",
			message: &discordgo.Message{ID: "m1", ChannelID: "other"},
			wantErr: "different channel id",
		},
		{
			name:    "different guild id",
			message: &discordgo.Message{ID: "m1", ChannelID: "c1", GuildID: "other"},
			wantErr: "different guild id",
		},
		{
			name:    "exact identity",
			message: &discordgo.Message{ID: "m1", ChannelID: "c1", GuildID: "g1"},
		},
		{
			name:    "missing fetched guild is allowed",
			message: &discordgo.Message{ID: "m1", ChannelID: "c1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTailMessageReplay(failure, tt.message)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestRecordTailFailureRejectsInvalidMessageMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()
	handler := &tailHandler{store: st}

	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "CHANNEL_UPDATE",
		Kind:      "returned_error",
	}))
	require.ErrorContains(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_REACTION",
		Kind:      "returned_error",
		MessageID: "m1",
	}), "unsupported event type")
	require.ErrorContains(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "unknown",
		MessageID: "m1",
	}), "unsupported failure kind")
	require.ErrorContains(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "panic",
	}), "missing message id")
	require.ErrorContains(t, (&tailHandler{}).RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "panic",
		MessageID: "m1",
	}), "missing store")
}
