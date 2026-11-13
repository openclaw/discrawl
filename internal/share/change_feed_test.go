package share

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func changeFeedGeneration(t *testing.T, s *store.Store, table string) (int64, string) {
	t.Helper()
	status, err := s.ChangeFeedStatus(t.Context())
	require.NoError(t, err)
	require.NotNil(t, status)
	for _, entry := range status.Tables {
		if entry.Table == table {
			return entry.Generation, entry.Reason
		}
	}
	t.Fatalf("change feed table %s missing", table)
	return 0, ""
}

func TestSnapshotImportsKeepChangeFeedExact(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	_, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	// A merge writes rows; triggers record them without a new generation.
	_, changed, err := MergeIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)
	generation, reason := changeFeedGeneration(t, dst, "mention_events")
	require.Equal(t, int64(1), generation)
	require.Equal(t, "enabled", reason)
	var logged int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from change_log where table_name='mention_events' and op='i'`).Scan(&logged))
	require.Positive(t, logged)

	// A replacement restores whole tables, so readers reconcile once. The
	// triggers are suspended during the replacement: no per-row entries.
	var before int
	var beforeSequence int64
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select coalesce(max(seq),0) from change_log`).Scan(&beforeSequence))
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from change_log where table_name not in ('mention_event_history','mention_event_id_map','entity_evidence','entity_observations','sync_state')`).Scan(&before))
	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	var after int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from change_log where table_name not in ('mention_event_history','mention_event_id_map','entity_evidence','entity_observations','sync_state')`).Scan(&after))
	require.Equal(t, before, after)
	// Search rebuild and import receipts are real writes after replacement.
	// They must be journaled, while imported coverage rows require reconciliation.
	rows, err := dst.DB().QueryContext(ctx, `select json_extract(row_key,'$[0]') from change_log where table_name='sync_state' and seq>? order by seq`, beforeSequence)
	require.NoError(t, err)
	var scopes []string
	for rows.Next() {
		var scope string
		require.NoError(t, rows.Scan(&scope))
		scopes = append(scopes, scope)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.ElementsMatch(t, []string{
		"schema:message_fts_rowid_version", "schema:member_fts_rowid_version",
		LastImportSyncScope, LastImportManifestSyncScope, LastImportManifestJSONScope,
		LastCheckSyncScope, LastMergeSyncScope, LastMergeManifestSyncScope,
		LastMergeManifestJSONScope, PendingReplacementSyncScope,
	}, scopes)
	var historical int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from mention_event_history`).Scan(&historical))
	require.Positive(t, historical)
	for _, table := range []string{"mention_events", "message_attachments", "guilds", "channels", "members", "messages", "sync_state"} {
		generation, reason = changeFeedGeneration(t, dst, table)
		require.Equal(t, int64(2), generation, table)
		require.Equal(t, "snapshot_import", reason, table)
	}
	// The triggers are back: a later write is recorded.
	var triggers int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from sqlite_master where type='trigger' and name like 'change_feed_%'`).Scan(&triggers))
	require.Equal(t, 3*len(store.ChangeFeedTables()), triggers)
	generation, _ = changeFeedGeneration(t, dst, "message_embedding_history")
	require.Equal(t, int64(1), generation)
}
