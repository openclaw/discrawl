package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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
	failure := errors.New(`request failed: Bearer abc123 https://example.test/private?hm=signed-value api_key=api-value&access_token=access-value {"authorization":"hidden","refresh_token":"refresh-value","client_secret":"client-value"} X-API-Key: header-value Authorization: Basic basic-value`)
	require.NoError(t, s.RecordFailure(ctx, ref, failure))
	require.NoError(t, s.RecordFailure(ctx, ref, failure))

	report, err := s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedCount)
	require.Len(t, report.Failures, 1)
	require.Equal(t, 1, report.Failures[0].RetryCount)
	require.Contains(t, report.Failures[0].ErrorMessage, "[redacted]")
	require.NotContains(t, report.Failures[0].ErrorMessage, "abc123")
	require.NotContains(t, report.Failures[0].ErrorMessage, "example.test")
	require.NotContains(t, report.Failures[0].ErrorMessage, "signed-value")
	require.NotContains(t, report.Failures[0].ErrorMessage, "api-value")
	require.NotContains(t, report.Failures[0].ErrorMessage, "hidden")
	require.NotContains(t, report.Failures[0].ErrorMessage, "access-value")
	require.NotContains(t, report.Failures[0].ErrorMessage, "refresh-value")
	require.NotContains(t, report.Failures[0].ErrorMessage, "client-value")
	require.NotContains(t, report.Failures[0].ErrorMessage, "header-value")
	require.NotContains(t, report.Failures[0].ErrorMessage, "basic-value")
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

func TestFailureLedgerFiltersLimitsAndBulkResolution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	refs := []FailureRef{
		{Operation: "sync", Source: "discord", GuildID: "g1", ChannelID: "c1", MessageID: "m1"},
		{Operation: "sync", Source: "discord", GuildID: "g1", ChannelID: "c2", MessageID: "m2"},
		{Operation: "import", Source: "wiretap", GuildID: "g2", ChannelID: "c3", MessageID: "m3"},
	}
	for i, ref := range refs {
		require.NoError(t, s.RecordFailure(ctx, ref, fmt.Errorf("failure %d", i)))
	}

	report, err := s.ListFailures(ctx, FailureListOptions{Source: " discord ", GuildID: " g1 ", ChannelID: " c1 ", Limit: 1}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedCount)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "m1", report.Failures[0].MessageID)

	_, err = s.ListFailures(ctx, FailureListOptions{Limit: maxFailureLimit + 1}, time.Now())
	require.ErrorContains(t, err, "at most 1000")
	require.Error(t, s.ResolveMessageFailures(ctx, FailureRef{Operation: "sync"}, []string{"m1"}))
	require.NoError(t, s.ResolveMessageFailures(ctx, FailureRef{Operation: "sync", Source: "discord"}, []string{" ", "m1", "m1", "m2"}))

	report, err = s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedCount)
	require.Equal(t, "m3", report.Failures[0].MessageID)
	report, err = s.ListFailures(ctx, FailureListOptions{IncludeResolved: true}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 3)

	require.Error(t, s.RecordFailure(ctx, FailureRef{Operation: "sync"}, errors.New("missing source")))
	require.Error(t, s.ResolveFailures(ctx, FailureRef{Source: "discord"}))
	require.NoError(t, s.ResolveMessageFailures(ctx, FailureRef{Operation: "sync", Source: "discord"}, nil))
}

func TestFailureLedgerPrunesOldResolvedRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	oldRef := FailureRef{Operation: "sync", Source: "discord", MessageID: "old"}
	require.NoError(t, s.RecordFailure(ctx, oldRef, errors.New("old failure")))
	require.NoError(t, s.ResolveFailures(ctx, oldRef))
	oldResolvedAt := time.Now().Add(-resolvedFailureMaxAge - time.Hour).UTC().Format(timeLayout)
	_, err = s.DB().ExecContext(ctx, `update failure_ledger set resolved_at = ? where message_id = 'old'`, oldResolvedAt)
	require.NoError(t, err)

	require.NoError(t, s.RecordFailure(ctx, FailureRef{Operation: "sync", Source: "discord", MessageID: "new"}, errors.New("new failure")))
	var oldCount int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from failure_ledger where message_id = 'old'`).Scan(&oldCount))
	require.Zero(t, oldCount)
}

func TestResolveFailureIdentityDoesNotClearScopedFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	unscoped := FailureRef{Operation: "import_messages", Source: "wiretap"}
	scoped := FailureRef{Operation: "import_messages", Source: "wiretap", MessageID: "m1"}
	require.NoError(t, s.RecordFailure(ctx, unscoped, errors.New("transaction failed")))
	require.NoError(t, s.RecordFailure(ctx, scoped, errors.New("row failed")))
	require.NoError(t, s.ResolveFailureIdentity(ctx, unscoped))

	report, err := s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedCount)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "m1", report.Failures[0].MessageID)
}

func TestFailureHelpersHandleNilContextsAndBounds(t *testing.T) {
	t.Parallel()
	var rowErr *RowWriteError
	require.Equal(t, "row write failed", rowErr.Error())
	require.NoError(t, rowErr.Unwrap())
	require.Empty(t, FailureRefFromError(errors.New("plain")))
	require.Equal(t, "context_canceled", failureClass(context.Canceled))
	require.Equal(t, "deadline_exceeded", failureClass(context.DeadlineExceeded))
	require.Equal(t, "error", failureClass(nil))
	require.Empty(t, firstNonEmpty(" ", ""))

	wrapped := &RowWriteError{Ref: FailureRef{Operation: "write_message", Source: "wiretap"}, Err: errors.New("boom")}
	require.Equal(t, wrapped.Ref, FailureRefFromError(fmt.Errorf("outer: %w", wrapped)))
	require.ErrorContains(t, wrapped, "write_message failed")
	require.ErrorIs(t, wrapped, wrapped.Err)

	message := sanitizeFailureMessage(string([]byte{'a', 0xff, 'b'}) + " " + strings.Repeat("x", maxFailureMessage+10))
	require.NotContains(t, message, "\ufffd")
	require.Len(t, []rune(message), maxFailureMessage)

	original := errors.New("embedding failed")
	require.ErrorIs(t, withEmbeddingFailureRecord(original, nil), original)
	require.ErrorContains(t, withEmbeddingFailureRecord(original, errors.New("ledger failed")), "record failure ledger: ledger failed")
}

func TestFailureLedgerReportsClosedDatabaseErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	require.NoError(t, s.Close())
	ref := FailureRef{Operation: "sync", Source: "discord", MessageID: "m1"}

	require.ErrorContains(t, s.RecordFailure(ctx, ref, errors.New("boom")), "record discord/sync failure")
	require.ErrorContains(t, s.ResolveFailures(ctx, ref), "resolve discord/sync failures")
	require.ErrorContains(t, s.ResolveMessageFailures(ctx, ref, []string{"m1"}), "resolve discord/sync message failures")
	_, err = s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.ErrorContains(t, err, "count unresolved failures")
}
