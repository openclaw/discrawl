package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/discorddesktop"
)

func dryRunFixture(t *testing.T) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	db := filepath.Join(dir, "archive", "archive.db")
	desktop := filepath.Join(dir, "desktop")
	require.NoError(t, os.MkdirAll(desktop, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(desktop, "payload.json"), []byte(`{"id":"111111111111111121","guild_id":"999999999999999996","type":0,"name":"synthetic"}
{"id":"333333333333333346","channel_id":"111111111111111121","content":"Sapphire preview","timestamp":"2026-09-01T12:00:00Z","author":{"id":"222222222222222232","username":"fixture"}}`), 0o600))
	cfg := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("version = 1\ndb_path = %q\ncache_dir = %q\nlog_dir = %q\n", db, filepath.Join(dir, "runtime-cache"), filepath.Join(dir, "runtime-logs"))), 0o600))
	return cfg, db, desktop
}

func TestWiretapDryRunDoesNotCreateArchive(t *testing.T) {
	for _, command := range []string{"wiretap", "tap", "cache-import"} {
		for _, stats := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stats=%t", command, stats), func(t *testing.T) {
				cfg, db, desktop := dryRunFixture(t)
				args := []string{"--config", cfg, "--json", command, "--dry-run", "--path", desktop}
				if stats {
					args = append(args, "--stats")
				}
				var out bytes.Buffer
				require.NoError(t, Run(context.Background(), args, &out, &bytes.Buffer{}))
				if stats {
					var result wiretapProgress
					require.NoError(t, json.Unmarshal(out.Bytes(), &result))
					require.Equal(t, 1, result.Import.Messages)
					require.Zero(t, result.Coverage.Totals.MessageCount)
					require.Empty(t, result.Coverage.Guilds)
				} else {
					var result discorddesktop.Stats
					require.NoError(t, json.Unmarshal(out.Bytes(), &result))
					require.Equal(t, 1, result.Messages)
					require.True(t, result.DryRun)
				}
				require.NoDirExists(t, filepath.Dir(db))
				require.NoDirExists(t, filepath.Join(filepath.Dir(cfg), "runtime-cache"))
				require.NoDirExists(t, filepath.Join(filepath.Dir(cfg), "runtime-logs"))
			})
		}
	}
}

func TestWiretapDryRunReadsExistingCoverageWithoutWriting(t *testing.T) {
	cfg, db, desktop := dryRunFixture(t)
	args := []string{"--config", cfg, "--json", "wiretap", "--path", desktop}
	require.NoError(t, Run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}))
	before, err := os.ReadFile(db)
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, Run(context.Background(), append(args, "--dry-run", "--stats"), &out, &bytes.Buffer{}))
	var result wiretapProgress
	require.NoError(t, json.Unmarshal(out.Bytes(), &result))
	require.Equal(t, 1, result.Coverage.Totals.MessageCount)
	after, err := os.ReadFile(db)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(before), sha256.Sum256(after))
}

func TestWiretapExplicitFalseDryRunStillImports(t *testing.T) {
	cfg, db, desktop := dryRunFixture(t)
	args := []string{"--config", cfg, "wiretap", "--path", desktop, "--dry-run", "--dry-run=false"}
	require.NoError(t, Run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}))
	require.FileExists(t, db)
}

func TestWiretapDryRunWithoutStatsDoesNotOpenArchive(t *testing.T) {
	cfg, db, desktop := dryRunFixture(t)
	require.NoError(t, os.MkdirAll(filepath.Dir(db), 0o755))
	original := []byte("not a SQLite database")
	require.NoError(t, os.WriteFile(db, original, 0o600))
	require.NoError(t, Run(context.Background(), []string{"--config", cfg, "wiretap", "--dry-run", "--path", desktop}, &bytes.Buffer{}, &bytes.Buffer{}))
	contents, err := os.ReadFile(db)
	require.NoError(t, err)
	require.Equal(t, original, contents)
}
