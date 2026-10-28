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

	// A replacement restores whole tables, so readers reconcile once.
	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	for _, table := range []string{"mention_events", "message_attachments"} {
		generation, reason = changeFeedGeneration(t, dst, table)
		require.Equal(t, int64(2), generation, table)
		require.Equal(t, "snapshot_import", reason, table)
	}
	generation, _ = changeFeedGeneration(t, dst, "message_embedding_history")
	require.Equal(t, int64(1), generation)
}
