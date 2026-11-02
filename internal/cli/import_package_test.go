package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/discordpackage"
	"github.com/stretchr/testify/require"
)

func TestImportPackageDryRunAndLocalImport(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "package")
	channel := filepath.Join(input, "messages", "900719925474099303")
	require.NoError(t, os.MkdirAll(channel, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(channel, "channel.json"), []byte(`{"id":"900719925474099303","type":1,"recipients":["900719925474099302"]}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(channel, "messages.json"), []byte(`[{"ID":"900719925474099304","Timestamp":"2026-01-01T00:00:00Z","Contents":"synthetic"}]`), 0o600))
	db := filepath.Join(dir, "archive", "personal.db")
	cfg := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("version = 1\ndb_path = %q\ncache_dir = %q\nlog_dir = %q\n", db, filepath.Join(dir, "cache"), filepath.Join(dir, "logs"))), 0o600))
	args := []string{"--config", cfg, "import-package", "--path", input, "--account-id", "900719925474099301", "--json"}
	var out bytes.Buffer
	require.NoError(t, Run(context.Background(), append(args, "--dry-run"), &out, &bytes.Buffer{}))
	var result discordpackage.Stats
	require.NoError(t, json.Unmarshal(out.Bytes(), &result))
	require.Equal(t, 1, result.Messages)
	require.Equal(t, "sent_only", result.Coverage)
	require.NoDirExists(t, filepath.Dir(db))
	require.NoDirExists(t, filepath.Join(dir, "cache"))
	require.NoDirExists(t, filepath.Join(dir, "logs"))
	out.Reset()
	require.NoError(t, Run(context.Background(), args, &out, &bytes.Buffer{}))
	require.NoError(t, json.Unmarshal(out.Bytes(), &result))
	require.Equal(t, 1, result.Inserted)
	out.Reset()
	require.NoError(t, Run(context.Background(), append(args, "--dry-run"), &out, &bytes.Buffer{}))
	require.FileExists(t, db)
}

func TestImportPackageHelpAndRequiredPath(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, Run(context.Background(), []string{"import-package", "--help"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "sent DMs")
	require.Contains(t, out.String(), "--account-id")
	err := Run(context.Background(), []string{"import-package"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Equal(t, 2, ExitCode(err))
}
