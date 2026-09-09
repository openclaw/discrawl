package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/share"
	"github.com/stretchr/testify/require"
)

func TestPublishProducerValidationBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(dir, "cache"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(dir, "state"))
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.Share.RepoPath = filepath.Join(dir, "share")
	cfg.Share.Remote = filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", cfg.Share.Remote)
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))
	src := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, src.Close())
	t.Setenv("DISCRAWL_PRODUCER_REPOSITORY", "private.invalid")
	t.Setenv("DISCRAWL_PRODUCER_REVISION", strings.Repeat("a", 40))
	t.Setenv("DISCRAWL_PRODUCER_RUN_ID", "123")
	t.Setenv("DISCRAWL_PRODUCER_RUN_ATTEMPT", "1")
	err := Run(t.Context(), []string{"--config", cfgPath, "publish", "--no-media"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private.invalid")
	require.NoDirExists(t, cfg.Share.RepoPath)
	t.Setenv("DISCRAWL_PRODUCER_REPOSITORY", "openclaw/discrawl")
	var out bytes.Buffer
	require.NoError(t, Run(t.Context(), []string{"--config", cfgPath, "--json", "publish", "--no-media"}, &out, &bytes.Buffer{}))
	var result map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &result))
	require.Equal(t, true, result["committed"], "committed still means a Git commit")
	require.Equal(t, false, result["pushed"])
	manifest, err := share.ReadManifest(cfg.Share.RepoPath)
	require.NoError(t, err)
	require.Equal(t, "producer.json", manifest.Files["producer"])
	receipt, err := os.ReadFile(filepath.Join(cfg.Share.RepoPath, "producer.json"))
	require.NoError(t, err)
	require.Contains(t, string(receipt), "openclaw/discrawl")
	require.NotContains(t, string(receipt), dir)
	require.NotContains(t, string(receipt), "private.invalid")
	// Daily report generation does not rewrite or mint a producer receipt.
	out.Reset()
	require.NoError(t, Run(t.Context(), []string{
		"--config", cfgPath, "report", "--published",
		"--readme", filepath.Join(cfg.Share.RepoPath, "README.md"),
	}, &out, &bytes.Buffer{}))
	after, err := os.ReadFile(filepath.Join(cfg.Share.RepoPath, "producer.json"))
	require.NoError(t, err)
	require.Equal(t, receipt, after)
}
