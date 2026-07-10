package cli

import (
	"os"
	"path/filepath"
	goruntime "runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLockedResourceGuardRejectsWALAtThreshold(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("locked free-space guard is unsupported on Windows")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "discrawl.db")
	require.NoError(t, os.WriteFile(dbPath, nil, 0o600))
	require.NoError(t, os.WriteFile(dbPath+"-wal", []byte{1}, 0o600))
	t.Setenv(lockedMinFreeKiBEnv, "1")
	t.Setenv(lockedMaxWALBytesEnv, "1")

	err := lockedResourceGuard(dbPath)

	require.ErrorContains(t, err, "WAL at or above threshold")
}

func TestLockedResourceGuardRejectsSymlinkedWAL(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("locked free-space guard is unsupported on Windows")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "discrawl.db")
	target := filepath.Join(dir, "target")
	require.NoError(t, os.WriteFile(dbPath, nil, 0o600))
	require.NoError(t, os.WriteFile(target, nil, 0o600))
	require.NoError(t, os.Symlink(target, dbPath+"-wal"))
	t.Setenv(lockedMinFreeKiBEnv, "1")
	t.Setenv(lockedMaxWALBytesEnv, "4294967296")

	err := lockedResourceGuard(dbPath)

	require.ErrorContains(t, err, "WAL path is not a regular file")
}
