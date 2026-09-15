package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/headlinemetrics"
	"github.com/stretchr/testify/require"
)

func TestMetricsHelpAndIndependentStatus(t *testing.T) {
	for _, args := range [][]string{{"help", "metrics"}, {"help", "metrics", "collect"}, {"metrics"}, {"metrics", "collect", "--help"}, {"--json", "metrics", "status", "--help"}} {
		var out bytes.Buffer
		require.NoError(t, Run(t.Context(), args, &out, &bytes.Buffer{}))
		require.Contains(t, out.String(), "Usage: discrawl metrics")
	}
	var out bytes.Buffer
	require.NoError(t, Run(t.Context(), []string{"--help"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "metrics")
	root := t.TempDir()
	database := filepath.Join(root, "metrics.sqlite")
	s, err := headlinemetrics.Open(t.Context(), database, "discrawl")
	require.NoError(t, err)
	require.NoError(t, s.Close())
	config := filepath.Join(root, "metrics.json")
	b, err := json.Marshal(headlinemetrics.Config{Database: database, Targets: []headlinemetrics.Target{{Entity: "openclaw", Target: "clawd"}}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(config, b, 0o600))
	out.Reset()
	// The invalid archive config must never be opened or initialized by metrics.
	archiveConfig := filepath.Join(root, "absent-archive-config.toml")
	require.NoError(t, Run(t.Context(), []string{"--config", archiveConfig, "--json", "metrics", "status", "--config", config}, &out, &bytes.Buffer{}))
	require.JSONEq(t, `{"source":"discrawl","observations":0,"events":0,"sequence":0,"last_observed":null}`, out.String())
	require.NoFileExists(t, archiveConfig)
}
