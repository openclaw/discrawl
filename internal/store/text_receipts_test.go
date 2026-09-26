package store

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataOnlyReceiptRemainsUnchangedAndIsNotAnIngestionFailure(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c", GuildID: "g", Kind: "text", RawJSON: `{}`}))
	_, err = s.DB().ExecContext(ctx, `update channels set collection_scope='allowed',scope_policy='p'`)
	require.NoError(t, err)
	raw := `{"id":"1","guild_id":"g","channel_id":"c","source":"discord_desktop","desktop_cache_note":"raw desktop cache payload intentionally not stored"}`
	m := MessageRecord{ID: "1", GuildID: "g", ChannelID: "c", Content: "retained content", NormalizedContent: "retained normalization", RawJSON: raw}
	require.NoError(t, s.UpsertMessage(ctx, m))
	var clock string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from messages where id='1'`).Scan(&clock))
	require.NoError(t, s.RecordFailure(ctx, FailureRef{Operation: "derive_text", Source: "local", GuildID: "g", ChannelID: "c", MessageID: "1"}, errors.New("stored provider identity/content mismatch")))
	bad := m
	bad.ID = "2"
	require.NoError(t, s.UpsertMessage(ctx, bad))
	require.NoError(t, s.RecordFailure(ctx, FailureRef{Operation: "derive_text", Source: "local", GuildID: "g", ChannelID: "c", MessageID: "2"}, errors.New("stored provider identity/content mismatch")))
	require.NoError(t, s.ReconcileTextReceiptFailures(ctx))
	var reason string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select resolution_reason from failure_ledger where message_id='1'`).Scan(&reason))
	require.Equal(t, "non_provider_receipt_retained", reason)
	p, err := s.RepairMessageTextBatch(ctx, "p", 10, true)
	require.NoError(t, err)
	require.True(t, p.Complete)
	require.Equal(t, 1, p.SkippedReceipt)
	require.Equal(t, 1, p.Rejected, "an identity mismatch must not be misclassified as expected coverage")
	var body, normalized, after, storedRaw string
	var version, n int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content,normalized_content,updated_at,raw_json,text_version from messages where id='1'`).Scan(&body, &normalized, &after, &storedRaw, &version))
	require.Equal(t, m.Content, body)
	require.Equal(t, m.NormalizedContent, normalized)
	require.Equal(t, clock, after)
	require.Equal(t, raw, storedRaw)
	require.Zero(t, version)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from embedding_jobs`).Scan(&n))
	require.Zero(t, n)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from failure_ledger where resolved_at is null`).Scan(&n))
	require.Equal(t, 1, n)
	for _, invalid := range []string{`{}`, `not-json`, `{"id":"1","guild_id":"g","channel_id":"c","source":"discord_desktop","content":"","desktop_cache_note":"raw desktop cache payload intentionally not stored"}`} {
		m.RawJSON = invalid
		require.False(t, nonProviderTextReceipt(m))
	}
	for _, invalid := range []string{strings.Replace(raw, `"id":"1"`, `"id":"wrong","id":"1"`, 1), strings.Replace(raw, `"source":`, `"future_body":"unexpected","source":`, 1), raw + raw} {
		m.RawJSON = invalid
		require.False(t, nonProviderTextReceipt(m))
	}
}

func TestReceiptReconciliationDoesNotStarveBehindUnrelatedFailures(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{ID: "1", GuildID: "g", ChannelID: "c", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{ID: "2", GuildID: "g", ChannelID: "c", RawJSON: `{"id":"2","guild_id":"g","channel_id":"c","source":"discord_desktop","desktop_cache_note":"raw desktop cache payload intentionally not stored"}`}))
	_, err = s.DB().ExecContext(ctx, `with recursive n(x) as (select 1 union all select x+1 from n where x<501) insert into failure_ledger(operation,source,guild_id,channel_id,message_id,related_id,error_class,error_message,first_seen_at,last_seen_at) select 'derive_text','local','g','c','1',cast(x as text),'fixture','invalid raw','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z' from n`)
	require.NoError(t, err)
	require.NoError(t, s.RecordFailure(ctx, FailureRef{Operation: "derive_text", Source: "local", GuildID: "g", ChannelID: "c", MessageID: "2"}, errors.New("fixture")))
	require.NoError(t, s.ReconcileTextReceiptFailures(ctx))
	var n int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from failure_ledger where resolved_at is null`).Scan(&n))
	require.Equal(t, 501, n)
	var reason string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select resolution_reason from failure_ledger where message_id='2'`).Scan(&reason))
	require.Equal(t, "non_provider_receipt_retained", reason)
}
