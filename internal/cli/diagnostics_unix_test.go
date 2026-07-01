//go:build unix

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsKeepsUnknownStateWhenLockProbeFails(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "missing.db")
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))
	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	require.NoError(t, os.WriteFile(lockPath, []byte("locked"), 0o600))
	writeSyncLockMetadata(t, lockPath, "wiretap", os.Getpid())
	require.NoError(t, os.Chmod(lockPath, 0))
	t.Cleanup(func() { _ = os.Chmod(lockPath, 0o600) })

	var out bytes.Buffer
	require.NoError(t, Run(context.Background(), []string{"--config", cfgPath, "diagnostics", "--json"}, &out, &bytes.Buffer{}))
	var report diagnosticsReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &report))
	require.Equal(t, "unknown", report.SyncLock.State)
	require.NotEmpty(t, report.SyncLock.Error)
	require.NotNil(t, report.SyncLock.Owner)
}
