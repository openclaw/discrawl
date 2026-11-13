package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChangeFeedSyncStateReplaysCoverageChanges(t *testing.T) {
	t.Parallel()
	s, _ := openChangeFeedStore(t)
	ctx := t.Context()
	r := &feedReader{table: "sync_state"}
	r.capture(ctx, t, s.DB())
	scope := "channel:c:history_window:v1:2026-11-01T00:00:00Z"
	require.NoError(t, s.SetSyncState(ctx, scope, "coverage\x00first"))
	r.capture(ctx, t, s.DB())
	require.Equal(t, snapshotTable(ctx, t, s.DB(), "sync_state"), r.rows)
	require.Equal(t, 1, r.reconciles)
	require.Equal(t, 1, r.replayed)
	require.NoError(t, s.SetSyncState(ctx, scope, "coverage\x00updated"))
	r.capture(ctx, t, s.DB())
	require.Equal(t, snapshotTable(ctx, t, s.DB(), "sync_state"), r.rows)
	require.Equal(t, 2, r.replayed)
	// A rolled-back checkpoint must not create a visible feed entry.
	before := r.cursor.Seq
	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `update sync_state set cursor='uncommitted' where scope=?`, scope)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	r.capture(ctx, t, s.DB())
	require.Equal(t, before, r.cursor.Seq)
	require.NoError(t, s.DeleteSyncState(ctx, scope))
	r.capture(ctx, t, s.DB())
	require.Equal(t, snapshotTable(ctx, t, s.DB(), "sync_state"), r.rows)
	require.Equal(t, 3, r.replayed)
	require.Equal(t, 1, r.reconciles, "coverage updates must not force full snapshots")
}

func TestChangeFeedContractThreePreservesExistingGenerations(t *testing.T) {
	t.Parallel()
	s, file := openChangeFeedStore(t)
	ctx := t.Context()
	status, err := s.ChangeFeedStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, status.ContractVersions)
	var contract int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select contract_version from change_feed_tables where table_name='sync_state'`).Scan(&contract))
	require.Equal(t, 3, contract)
	var oldTables string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select tables_json from change_feed_contracts where contract_version=2`).Scan(&oldTables))
	require.NotContains(t, oldTables, "sync_state", "contract 2 must retain its table set")
	generations := map[string]string{}
	for _, table := range status.Tables {
		if table.Contract < 3 {
			generations[table.Table] = table.GenerationID
		}
	}
	// Recreate the exact missing metadata/triggers of a contract-2 archive.
	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, SuspendChangeFeedTx(ctx, tx, "sync_state"))
	_, err = tx.ExecContext(ctx, `delete from change_feed_tables where table_name='sync_state'; delete from change_feed_contracts where contract_version=3`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, s.Close())
	reopened, err := Open(ctx, file)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	status, err = reopened.ChangeFeedStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, status.ContractVersion)
	require.Equal(t, []int{1, 2, 3}, status.ContractVersions)
	for _, table := range status.Tables {
		if table.Table == "sync_state" {
			require.Equal(t, 3, table.Contract)
			require.Equal(t, "enabled", table.Reason)
			continue
		}
		require.Equal(t, generations[table.Table], table.GenerationID, table.Table)
	}
	require.NoError(t, reopened.SetSyncState(ctx, "channel:new:history_complete", "true"))
	require.Equal(t, [][2]string{{"i", `["channel:new:history_complete"]`}}, changeFeedEntries(t, reopened, "sync_state"))
}
