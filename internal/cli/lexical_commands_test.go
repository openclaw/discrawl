package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/stretchr/testify/require"
)

func TestRunLexicalInstallReportsConfiguredHelpers(t *testing.T) {
	var stdout bytes.Buffer
	r := &runtime{
		ctx:    context.Background(),
		cfg:    config.Default(),
		stdout: &stdout,
	}
	r.cfg.Search.Lexical.Languages = []string{"ko", "zh"}
	r.cfg.Search.Lexical.ZhCommand = "/tmp/discrawl-zh"

	require.NoError(t, r.runLexical([]string{"install"}))
	require.Contains(t, stdout.String(), "discrawl-kiwi")
	require.Contains(t, stdout.String(), "/tmp/discrawl-zh")
	require.NotContains(t, stdout.String(), "python")
}

func TestRunLexicalInstallRequiresConfiguredLanguages(t *testing.T) {
	r := &runtime{
		ctx: context.Background(),
		cfg: config.Default(),
	}
	err := r.runLexical([]string{"install"})
	require.ErrorContains(t, err, "search.lexical.languages is empty")
}

func TestRunLexicalInstallReportsUsage(t *testing.T) {
	r := &runtime{
		ctx: context.Background(),
		cfg: config.Default(),
	}
	r.cfg.Search.Lexical.Languages = []string{"ko"}
	err := r.runLexical([]string{"unknown"})
	require.ErrorContains(t, err, "usage: discrawl lexical install")
}

func TestRunLexicalInstallJSONOutput(t *testing.T) {
	var stdout bytes.Buffer
	r := &runtime{
		ctx:    context.Background(),
		cfg:    config.Default(),
		stdout: &stdout,
		json:   true,
	}
	r.cfg.Search.Lexical.Languages = []string{"ko", "ar"}

	require.NoError(t, r.runLexical([]string{"install"}))
	require.JSONEq(t, `{
		"languages": ["ko", "ar"],
		"helpers": [
			{"language":"ko","runtime":"helper","command":"discrawl-kiwi"},
			{"language":"ar","runtime":"in-process"}
		]
	}`, stdout.String())
}

func TestLexicalHelp(t *testing.T) {
	var stdout bytes.Buffer
	require.NoError(t, Run(context.Background(), []string{"help", "lexical"}, &stdout, &bytes.Buffer{}))
	require.Contains(t, stdout.String(), "discrawl lexical install")
	require.Contains(t, stdout.String(), "discrawl-ja")
	require.NotContains(t, stdout.String(), "Python packages")
}

func TestRunDispatchesLexicalInstall(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(configPath, []byte(`
version = 1

[discord]
token_source = "env"
`), 0o600))
	t.Setenv("DISCORD_BOT_TOKEN", "dummy")

	err := Run(
		context.Background(),
		[]string{"--config", configPath, "lexical", "install"},
		&bytes.Buffer{},
		&bytes.Buffer{},
	)
	require.ErrorContains(t, err, "search.lexical.languages is empty")
}
