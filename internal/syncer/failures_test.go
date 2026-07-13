package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

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
	require.NoError(t, nilHandler.recordMessageFailure(ctx, "g", "c", "m", errors.New("ignored")))
	require.NoError(t, nilHandler.resolveMessageFailures(ctx, "g", "c", "m"))

	handler := &tailHandler{store: st}
	require.NoError(t, handler.recordMessageFailure(ctx, "g1", "c1", "m1", errors.New("gateway write failed")))
	report, err := st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, tailMessageFailureOperation, report.Failures[0].Operation)
	require.Equal(t, "m1", report.Failures[0].MessageID)

	require.NoError(t, handler.resolveMessageFailures(ctx, "g1", "c1", "m1"))
	report, err = st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Empty(t, report.Failures)
	require.NoError(t, handler.recordMessageFailure(ctx, "g1", "c1", "m1", nil))

	original := errors.New("original")
	require.ErrorIs(t, withFailureRecordError(original, nil), original)
	require.ErrorContains(t, withFailureRecordError(original, errors.New("ledger")), "record failure ledger: ledger")
}

func TestTailHandlerRecordsTimedOutMessageBeforeRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	handler := &tailHandler{store: st}
	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_UPDATE",
		Kind:      "timeout",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m1",
		UserID:    "u1",
	}))

	report, err := st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, tailMessageFailureOperation, report.Failures[0].Operation)
	require.Equal(t, "discord", report.Failures[0].Source)
	require.Equal(t, "g1", report.Failures[0].GuildID)
	require.Equal(t, "c1", report.Failures[0].ChannelID)
	require.Equal(t, "m1", report.Failures[0].MessageID)
	require.Equal(t, "deadline_exceeded", report.Failures[0].ErrorClass)
	require.Equal(t, context.DeadlineExceeded.Error(), report.Failures[0].ErrorMessage)

	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "CHANNEL_UPDATE",
		Kind:      "timeout",
		ChannelID: "c1",
	}))
	require.ErrorContains(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "timeout",
		ChannelID: "c1",
	}), "missing message id")
	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "returned_error",
		ChannelID: "c1",
		MessageID: "ignored",
	}))
	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "unknown",
		ChannelID: "c1",
		MessageID: "ignored-unknown",
	}))
	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "CHANNEL_UPDATE",
		Kind:      "panic",
		ChannelID: "c1",
		MessageID: "accidental-message-id",
	}))
	report, err = st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)

	require.ErrorContains(t, (&tailHandler{}).RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "timeout",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "no-store",
	}), "missing store")
	require.ErrorContains(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "panic",
		ChannelID: "c1",
	}), "missing message id")
}

func TestTailHandlerRecordsPanickedMessageSanitized(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	const sensitivePanicText = "sensitive panic payload token=do-not-store"
	handler := &tailHandler{store: st}
	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "panic",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m1",
		UserID:    sensitivePanicText,
	}))

	report, err := st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	failure := report.Failures[0]
	require.Equal(t, tailMessageFailureOperation, failure.Operation)
	require.Equal(t, "discord", failure.Source)
	require.Equal(t, "g1", failure.GuildID)
	require.Equal(t, "c1", failure.ChannelID)
	require.Equal(t, "m1", failure.MessageID)
	require.Equal(t, "errors.errorString", failure.ErrorClass)
	require.Equal(t, errTailMessageHandlerPanic.Error(), failure.ErrorMessage)
	require.NotContains(t, failure.ErrorClass, sensitivePanicText)
	require.NotContains(t, failure.ErrorMessage, sensitivePanicText)
}

func TestTailHandlerRecoversMessageScopeFromStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		kind             string
		wantErrorClass   string
		wantErrorMessage string
	}{
		{
			name:             "timeout",
			kind:             "timeout",
			wantErrorClass:   "deadline_exceeded",
			wantErrorMessage: context.DeadlineExceeded.Error(),
		},
		{
			name:             "panic",
			kind:             "panic",
			wantErrorClass:   "errors.errorString",
			wantErrorMessage: errTailMessageHandlerPanic.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
			require.NoError(t, err)
			defer func() { _ = st.Close() }()

			now := time.Now().UTC().Format(time.RFC3339Nano)
			require.NoError(t, st.UpsertMessages(ctx, []store.MessageMutation{{
				Record: store.MessageRecord{
					ID:                "m2",
					GuildID:           "g2",
					ChannelID:         "c2",
					CreatedAt:         now,
					Content:           "test",
					NormalizedContent: "test",
					RawJSON:           `{}`,
				},
			}}))

			handler := &tailHandler{store: st}
			require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
				EventType: "MESSAGE_UPDATE",
				Kind:      tt.kind,
				MessageID: "m2",
			}))

			report, err := st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
			require.NoError(t, err)
			require.Len(t, report.Failures, 1)
			require.Equal(t, "g2", report.Failures[0].GuildID)
			require.Equal(t, "c2", report.Failures[0].ChannelID)
			require.Equal(t, "m2", report.Failures[0].MessageID)
			require.Equal(t, tt.wantErrorClass, report.Failures[0].ErrorClass)
			require.Equal(t, tt.wantErrorMessage, report.Failures[0].ErrorMessage)

			err = handler.RecordTailFailure(discordclient.TailFailure{
				EventType: "MESSAGE_CREATE",
				Kind:      tt.kind,
				MessageID: "unknown",
			})
			require.ErrorContains(t, err, "identity is incomplete")

			err = handler.RecordTailFailure(discordclient.TailFailure{
				EventType: "MESSAGE_UPDATE",
				Kind:      tt.kind,
				GuildID:   "wrong-guild",
				ChannelID: "c2",
				MessageID: "m2",
			})
			require.ErrorContains(t, err, "guild mismatch")

			err = handler.RecordTailFailure(discordclient.TailFailure{
				EventType: "MESSAGE_UPDATE",
				Kind:      tt.kind,
				GuildID:   "g2",
				ChannelID: "wrong-channel",
				MessageID: "m2",
			})
			require.ErrorContains(t, err, "channel mismatch")

			report, err = st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
			require.NoError(t, err)
			require.Len(t, report.Failures, 1)
			require.Equal(t, "g2", report.Failures[0].GuildID)
			require.Equal(t, "c2", report.Failures[0].ChannelID)
			require.Equal(t, "m2", report.Failures[0].MessageID)
			require.Equal(t, tt.wantErrorClass, report.Failures[0].ErrorClass)
			require.Equal(t, tt.wantErrorMessage, report.Failures[0].ErrorMessage)
			require.Zero(t, report.Failures[0].RetryCount)
		})
	}
}
