package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestRunLexicalInstallUsesConfiguredLanguages(t *testing.T) {
	var stdout bytes.Buffer
	r := &runtime{
		ctx:    context.Background(),
		cfg:    config.Default(),
		stdout: &stdout,
	}
	r.cfg.Search.Lexical.Languages = []string{"ko", "zh"}
	r.cfg.Search.Lexical.Python = "/tmp/tokenizers/bin/python"
	r.installLexical = func(
		_ context.Context,
		python string,
		languages []string,
	) (store.LexicalInstallResult, error) {
		require.Equal(t, "/tmp/tokenizers/bin/python", python)
		require.Equal(t, []string{"ko", "zh"}, languages)
		return store.LexicalInstallResult{
			Packages: []string{"jieba==0.42.1"},
		}, nil
	}

	require.NoError(t, r.runLexical([]string{"install"}))
	require.Contains(t, stdout.String(), "jieba==0.42.1")
}

func TestRunLexicalInstallRequiresConfiguredLanguages(t *testing.T) {
	r := &runtime{
		ctx: context.Background(),
		cfg: config.Default(),
		installLexical: func(context.Context, string, []string) (store.LexicalInstallResult, error) {
			return store.LexicalInstallResult{}, errors.New("must not run")
		},
	}

	err := r.runLexical([]string{"install"})
	require.ErrorContains(t, err, "search.lexical.languages is empty")
}

func TestRunLexicalInstallReportsInstallerError(t *testing.T) {
	r := &runtime{
		ctx: context.Background(),
		cfg: config.Default(),
		installLexical: func(context.Context, string, []string) (store.LexicalInstallResult, error) {
			return store.LexicalInstallResult{}, errors.New("pip unavailable")
		},
	}
	r.cfg.Search.Lexical.Languages = []string{"ko"}

	err := r.runLexical([]string{"install"})
	require.ErrorContains(t, err, "pip unavailable")
	err = r.runLexical([]string{"unknown"})
	require.ErrorContains(t, err, "usage: discrawl lexical install")
}

func TestRunLexicalInstallJSONOutput(t *testing.T) {
	var stdout bytes.Buffer
	r := &runtime{
		ctx:    context.Background(),
		cfg:    config.Default(),
		stdout: &stdout,
		json:   true,
		installLexical: func(context.Context, string, []string) (store.LexicalInstallResult, error) {
			return store.LexicalInstallResult{}, nil
		},
	}
	r.cfg.Search.Lexical.Languages = []string{"ko"}

	require.NoError(t, r.runLexical([]string{"install"}))
	require.JSONEq(t, `{
		"languages": ["ko"],
		"packages": [],
		"python": "python3",
		"kiwi": "discrawl-kiwi"
	}`, stdout.String())
}

func TestLexicalHelp(t *testing.T) {
	var stdout bytes.Buffer
	require.NoError(t, Run(context.Background(), []string{"help", "lexical"}, &stdout, &bytes.Buffer{}))
	require.Contains(t, stdout.String(), "discrawl lexical install")
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
