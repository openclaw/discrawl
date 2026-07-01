package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFailureLedgerRetriesResolvesReopensAndRedacts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	ref := FailureRef{Operation: "sync_messages", Source: "discord", GuildID: "g1", ChannelID: "c1"}
	failure := errors.New(`request failed: Bearer abc123 https://example.test/?token=secret {"authorization":"hidden"}`)
	require.NoError(t, s.RecordFailure(ctx, ref, failure))
	require.NoError(t, s.RecordFailure(ctx, ref, failure))

	report, err := s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedCount)
	require.Len(t, report.Failures, 1)
	require.Equal(t, 1, report.Failures[0].RetryCount)
	require.Contains(t, report.Failures[0].ErrorMessage, "[redacted]")
	require.NotContains(t, report.Failures[0].ErrorMessage, "abc123")
	require.NotContains(t, report.Failures[0].ErrorMessage, "secret")
	require.NotContains(t, report.Failures[0].ErrorMessage, "hidden")
	firstSeen := report.Failures[0].FirstSeenAt

	require.NoError(t, s.ResolveFailures(ctx, ref))
	report, err = s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Zero(t, report.UnresolvedCount)
	require.Empty(t, report.Failures)

	report, err = s.ListFailures(ctx, FailureListOptions{IncludeResolved: true}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.False(t, report.Failures[0].ResolvedAt.IsZero())

	require.NoError(t, s.RecordFailure(ctx, ref, errors.New("failed again")))
	report, err = s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, 2, report.Failures[0].RetryCount)
	require.Equal(t, firstSeen, report.Failures[0].FirstSeenAt)
	require.True(t, report.Failures[0].ResolvedAt.IsZero())
}

func TestAttachmentWriteErrorIncludesSafeRowContext(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	_, err = s.DB().ExecContext(ctx, `
		create trigger fail_attachment before insert on message_attachments
		begin select raise(abort, 'forced attachment failure'); end
	`)
	require.NoError(t, err)

	err = s.UpsertMessages(ctx, []MessageMutation{{
		Record: MessageRecord{
			ID: "m1", GuildID: "g1", ChannelID: "c1", AuthorID: "u1",
			CreatedAt: "2026-07-01T08:00:00Z", Content: "private body", NormalizedContent: "private body", RawJSON: `{}`,
		},
		Attachments: []AttachmentRecord{{
			AttachmentID: "a1", MessageID: "m1", GuildID: "g1", ChannelID: "c1", AuthorID: "u1",
			Filename: "trace.txt", ContentType: "text/plain", Size: 42, URL: "https://example.invalid/private",
		}},
	}})
	require.Error(t, err)
	require.ErrorContains(t, err, `attachment_id="a1"`)
	require.ErrorContains(t, err, `message_id="m1"`)
	require.ErrorContains(t, err, `guild_id="g1"`)
	require.ErrorContains(t, err, `channel_id="c1"`)
	require.ErrorContains(t, err, `author_id="u1"`)
	require.ErrorContains(t, err, `filename="trace.txt"`)
	require.ErrorContains(t, err, `content_type="text/plain"`)
	require.ErrorContains(t, err, `size=42`)
	require.NotContains(t, err.Error(), "private body")
	require.NotContains(t, err.Error(), "example.invalid")
	require.Equal(t, FailureRef{
		Operation: "write_attachment", GuildID: "g1", ChannelID: "c1", MessageID: "m1",
		RelatedKind: "attachment_id", RelatedID: "a1",
	}, FailureRefFromError(err))
}

func TestOpenMigratesSchemaV3FailureLedger(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `drop table failure_ledger`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `pragma user_version = 3`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	s, err = Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.RecordFailure(ctx, FailureRef{Operation: "sync", Source: "discord"}, errors.New("boom")))
	var version int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, storeSchemaVersion, version)
}
