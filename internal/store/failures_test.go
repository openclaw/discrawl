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

func TestRecordFailureWithMessageScope(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		CreatedAt:         "2026-07-13T00:00:00Z",
		Content:           "message",
		NormalizedContent: "message",
		RawJSON:           `{}`,
	}))

	failure := context.DeadlineExceeded
	require.NoError(t, s.RecordFailureWithMessageScope(ctx, FailureRef{
		Operation: "tail_message",
		Source:    "discord",
		MessageID: "m1",
	}, failure))
	report, err := s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "g1", report.Failures[0].GuildID)
	require.Equal(t, "c1", report.Failures[0].ChannelID)
	require.Equal(t, "m1", report.Failures[0].MessageID)

	require.ErrorContains(t, s.RecordFailureWithMessageScope(ctx, FailureRef{
		Operation: "tail_message",
		Source:    "discord",
		GuildID:   "wrong-guild",
		ChannelID: "c1",
		MessageID: "m1",
	}, failure), "guild mismatch")
	require.ErrorContains(t, s.RecordFailureWithMessageScope(ctx, FailureRef{
		Operation: "tail_message",
		Source:    "discord",
		GuildID:   "g1",
		ChannelID: "wrong-channel",
		MessageID: "m1",
	}, failure), "channel mismatch")

	require.NoError(t, s.RecordFailureWithMessageScope(ctx, FailureRef{
		Operation: "tail_message",
		Source:    "discord",
		GuildID:   "g2",
		ChannelID: "c2",
		MessageID: "new-message",
	}, failure))
	require.ErrorContains(t, s.RecordFailureWithMessageScope(ctx, FailureRef{
		Operation: "tail_message",
		Source:    "discord",
		MessageID: "missing-scope",
	}, failure), "identity is incomplete")

	report, err = s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 2)
}

func TestRecordFailureWithMessageScopeTimedDiagnostics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	deadline := time.Now().Add(time.Minute)
	timedCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	diagnostics, err := s.RecordFailureWithMessageScopeTimed(timedCtx, FailureRef{
		Operation: "tail_message",
		Source:    "discord",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m1",
	}, context.DeadlineExceeded)
	require.NoError(t, err)
	require.Equal(t, deadline, diagnostics.ContextDeadline)
	require.GreaterOrEqual(t, diagnostics.PoolWait, time.Duration(0))
	require.Greater(t, diagnostics.DBElapsed, time.Duration(0))
	require.Zero(t, diagnostics.SQLiteCode)
	require.Zero(t, diagnostics.SQLiteCategory)

	conn, err := s.DB().Conn(ctx)
	require.NoError(t, err)
	waitCtx, waitCancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer waitCancel()
	waitDeadline, ok := waitCtx.Deadline()
	require.True(t, ok)
	diagnostics, err = s.RecordFailureWithMessageScopeTimed(waitCtx, FailureRef{
		Operation: "tail_message",
		Source:    "discord",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m2",
	}, context.DeadlineExceeded)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, waitDeadline, diagnostics.ContextDeadline)
	require.GreaterOrEqual(t, diagnostics.PoolWait, 20*time.Millisecond)
	require.Zero(t, diagnostics.DBElapsed)
	require.Zero(t, diagnostics.SQLiteCode)
	require.NoError(t, conn.Close())
}

func TestRecordFailureWithMessageScopeTimedReportsExternalSQLiteBusy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	_, err = s.DB().ExecContext(ctx, `pragma busy_timeout = 25`)
	require.NoError(t, err)

	lockerDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer func() { _ = lockerDB.Close() }()
	lockerDB.SetMaxOpenConns(1)
	locker, err := lockerDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = locker.Close() }()
	_, err = locker.ExecContext(ctx, `begin immediate`)
	require.NoError(t, err)
	defer func() { _, _ = locker.ExecContext(context.Background(), `rollback`) }()

	timedCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	deadline, ok := timedCtx.Deadline()
	require.True(t, ok)
	diagnostics, err := s.RecordFailureWithMessageScopeTimed(timedCtx, FailureRef{
		Operation: "tail_message",
		Source:    "discord",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m1",
	}, context.DeadlineExceeded)
	require.Error(t, err)
	require.Equal(t, deadline, diagnostics.ContextDeadline)
	require.Greater(t, diagnostics.DBElapsed, time.Duration(0))
	require.Equal(t, 5, diagnostics.SQLiteCode)
	require.Equal(t, 5, diagnostics.SQLiteCategory)
}

func TestSQLiteFailureCodesUseNumericCategory(t *testing.T) {
	t.Parallel()
	code, category := sqliteFailureCodes(&codedFailureError{code: 6 | 2<<8})
	require.Equal(t, 518, code)
	require.Equal(t, 6, category)
	code, category = sqliteFailureCodes(errors.New("plain"))
	require.Zero(t, code)
	require.Zero(t, category)
}

type codedFailureError struct {
	code int
}

func (e *codedFailureError) Error() string {
	return "coded failure"
}

func (e *codedFailureError) Code() int {
	return e.code
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

func TestFailureReplayCandidatesAreBoundedAndLeastRecentlyAttempted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	refs := []FailureRef{
		{Operation: "tail_message", Source: "discord", GuildID: "g1", ChannelID: "c1", MessageID: "m1"},
		{Operation: "tail_message", Source: "discord", GuildID: "g1", ChannelID: "c1", MessageID: "m2"},
		{Operation: "tail_message", Source: "discord", GuildID: "g2", ChannelID: "c2", MessageID: "m3"},
		{Operation: "sync_messages", Source: "discord", GuildID: "g1", ChannelID: "c1", MessageID: "m4"},
	}
	for i, ref := range refs {
		require.NoError(t, s.RecordFailure(ctx, ref, fmt.Errorf("failure %d", i)))
	}
	_, err = s.DB().ExecContext(ctx, `
		update failure_ledger
		set last_seen_at = case message_id
			when 'm1' then '2026-07-13 00:00:03.000000000'
			when 'm2' then '2026-07-13 00:00:01.000000000'
			when 'm3' then '2026-07-13 00:00:00.000000000'
			else last_seen_at
		end
	`)
	require.NoError(t, err)

	candidates, err := s.ListFailureReplayCandidates(ctx, FailureRef{
		Operation: "tail_message",
		Source:    "discord",
	}, nil, 2)
	require.NoError(t, err)
	require.Len(t, candidates, 2)
	require.Equal(t, []string{"m3", "m2"}, []string{candidates[0].MessageID, candidates[1].MessageID})

	candidates, err = s.ListFailureReplayCandidates(ctx, FailureRef{
		Operation: "tail_message",
		Source:    "discord",
	}, []string{"g1"}, 1)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "m2", candidates[0].MessageID)

	_, err = s.ListFailureReplayCandidates(ctx, FailureRef{Operation: "tail_message"}, nil, 1)
	require.Error(t, err)
	_, err = s.ListFailureReplayCandidates(
		ctx,
		FailureRef{Operation: "tail_message", Source: "discord"},
		nil,
		maxFailureLimit+1,
	)
	require.ErrorContains(t, err, "at most")
}

func TestFailureReplayCandidatesCanMatchRelatedIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, ref := range []FailureRef{
		{
			Operation: "tail_message",
			Source:    "discord",
			GuildID:   "g1",
			ChannelID: "c1",
			MessageID: "legacy",
		},
		{
			Operation:   "tail_message",
			Source:      "discord",
			GuildID:     "g1",
			ChannelID:   "c1",
			MessageID:   "invalid",
			RelatedKind: "message_event",
			RelatedID:   "unknown",
		},
		{
			Operation:   "tail_message",
			Source:      "discord",
			GuildID:     "g1",
			ChannelID:   "c1",
			MessageID:   "event-aware-create",
			RelatedKind: "message_event",
			RelatedID:   "create",
		},
		{
			Operation:   "tail_message",
			Source:      "discord",
			GuildID:     "g1",
			ChannelID:   "c1",
			MessageID:   "event-aware-delete",
			RelatedKind: "message_event",
			RelatedID:   "delete",
		},
	} {
		require.NoError(t, s.RecordFailure(ctx, ref, errors.New(ref.MessageID)))
	}
	_, err = s.DB().ExecContext(ctx, `
		update failure_ledger
		set last_seen_at = case message_id
			when 'legacy' then '2026-07-13 00:00:01.000000000'
			when 'invalid' then '2026-07-13 00:00:02.000000000'
			when 'event-aware-create' then '2026-07-13 00:00:03.000000000'
			else '2026-07-13 00:00:04.000000000'
		end
	`)
	require.NoError(t, err)

	candidates, err := s.ListFailureReplayCandidates(
		ctx,
		FailureRef{Operation: "tail_message", Source: "discord"},
		[]string{"g1"},
		1,
	)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "legacy", candidates[0].MessageID)

	candidates, err = s.ListFailureReplayCandidatesMatchingRelatedIDs(
		ctx,
		FailureRef{
			Operation:   "tail_message",
			Source:      "discord",
			RelatedKind: "message_event",
		},
		[]string{"g1"},
		[]string{"create", "delete", "create", ""},
		10,
	)
	require.NoError(t, err)
	require.Len(t, candidates, 2)
	require.Equal(
		t,
		[]string{"event-aware-create", "event-aware-delete"},
		[]string{candidates[0].MessageID, candidates[1].MessageID},
	)

	_, err = s.ListFailureReplayCandidatesMatchingRelatedIDs(
		ctx,
		FailureRef{Operation: "tail_message", Source: "discord"},
		nil,
		nil,
		1,
	)
	require.ErrorContains(t, err, "at least one")
	_, err = s.ListFailureReplayCandidatesMatchingRelatedIDs(
		ctx,
		FailureRef{
			Operation: "tail_message",
			Source:    "discord",
			RelatedID: "create",
		},
		nil,
		[]string{"delete"},
		1,
	)
	require.ErrorContains(t, err, "cannot both")
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
