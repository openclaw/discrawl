package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInspectSQLiteIgnoresApplicationSchemaVersion(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	_, err = s.Exec(ctx, "pragma user_version = 999")
	require.NoError(t, err)
	require.NoError(t, s.Close())

	health, err := InspectSQLite(ctx, dbPath)
	require.NoError(t, err)
	require.Equal(t, "wal", health.JournalMode)
	require.Equal(t, 999, health.SchemaVersion)
	require.True(t, health.IntegrityOK)
}

func TestInspectSQLiteReportsOpenAndQueryErrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	_, err := InspectSQLite(ctx, filepath.Join(dir, "missing.db"))
	require.Error(t, err)

	invalidPath := filepath.Join(dir, "invalid.db")
	require.NoError(t, os.WriteFile(invalidPath, []byte("not sqlite"), 0o600))
	_, err = InspectSQLite(ctx, invalidPath)
	require.Error(t, err)
}
