package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeSnapshotKeepsRowsAndBaselineAtOneHead(t *testing.T) {
	source, path := openChangeFeedStore(t)
	ctx := nativeTestContext(t)
	seedChangeFeedGuild(t, source)
	require.NoError(t, source.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m", "c1", "before", nil, 0)}))
	before, err := ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	var exported string
	wrote := false
	_, baseline, err := ExportNativeSnapshot(ctx, path, filepath.Join(t.TempDir(), "snapshot"), func(ctx context.Context, tx *sql.Tx, table string, row map[string]any) (bool, error) {
		if !wrote {
			wrote = true
			// A real independent WAL writer commits after the export's cursor read.
			require.NoError(t, source.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m", "c1", "after", nil, 0)}))
			require.NoError(t, source.SetSyncState(ctx, "channel:c1:history_complete", "new"))
		}
		if table == "messages" {
			exported = row["content"].(string)
		}
		if table == "sync_state" {
			require.NotEqual(t, "channel:c1:history_complete", row["scope"])
		}
		return true, nil
	})
	require.NoError(t, err)
	require.True(t, wrote)
	require.Equal(t, "before", exported)
	require.Equal(t, before, baseline)
	changes, err := CaptureNativeChanges(ctx, path, baseline, NativeCaptureLimits{MaxEvents: 100, MaxBytes: 1 << 20})
	require.NoError(t, err)
	require.Greater(t, changes.Through.Cursor.Sequence, baseline.Cursor.Sequence)
	require.Len(t, changes.Changes, 2)
}
