//go:build darwin || linux

package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func credentialFixture(t *testing.T, body string) string {
	t.Helper()
	// A root-owned shared /tmp is intentionally not a credential ancestor.
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	home, err = filepath.EvalSymlinks(home)
	require.NoError(t, err)
	dir, err := os.MkdirTemp(home, ".discrawl-credential-test-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	path := filepath.Join(dir, "fixture.key")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestFileCredentialsNeverConsultKeyringOrEnvironment(t *testing.T) {
	stubDiscordTokenKeyring(t, func(string, string) (string, error) {
		t.Fatal("file source consulted Keychain")
		return "", errors.New("unexpected keyring")
	})
	old := embeddingKeyringGet
	embeddingKeyringGet = func(string, string) (string, error) {
		t.Fatal("file source consulted Keychain")
		return "", errors.New("unexpected keyring")
	}
	t.Cleanup(func() { embeddingKeyringGet = old })
	t.Setenv(DefaultTokenEnv, "wrong-environment-token")
	t.Setenv("OPENAI_API_KEY", "wrong-environment-key")
	cfg := Default()
	cfg.Discord.TokenSource = "file"
	cfg.Discord.TokenFile = credentialFixture(t, " Bot discord-fixture \n")
	cfg.Search.Embeddings.APIKeySource = "file"
	cfg.Search.Embeddings.APIKeyFile = credentialFixture(t, " Bot embedding-fixture \n")
	token, err := ResolveDiscordToken(cfg)
	require.NoError(t, err)
	require.Equal(t, "discord-fixture", token.Token)
	require.Equal(t, "file", token.Source)
	key, err := ResolveEmbeddingFileAPIKey(cfg.Search.Embeddings)
	require.NoError(t, err)
	require.Equal(t, "Bot embedding-fixture", key)

	configPath := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, Write(configPath, cfg))
	loaded, err := Load(configPath)
	require.NoError(t, err)
	require.Equal(t, cfg.Discord.TokenFile, loaded.Discord.TokenFile)
	require.Equal(t, cfg.Search.Embeddings.APIKeyFile, loaded.Search.Embeddings.APIKeyFile)
	require.NoError(t, os.Remove(cfg.Discord.TokenFile))
	_, err = ResolveDiscordToken(cfg)
	require.Error(t, err) // A missing file never selects the valid environment.
	require.NotContains(t, err.Error(), "wrong-environment")
	cfg.Search.Embeddings.APIKeyFile = cfg.Discord.TokenFile
	_, err = ResolveEmbeddingFileAPIKey(cfg.Search.Embeddings)
	require.Error(t, err)
}

func TestProtectedCredentialFileRejectsUnsafeInputs(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string) string
	}{
		{"relative", func(_ *testing.T, _ string) string { return "fixture.key" }},
		{"missing", func(_ *testing.T, p string) string { return p + ".missing" }},
		{"mode", func(t *testing.T, p string) string { require.NoError(t, os.Chmod(p, 0o644)); return p }},
		{"parent", func(t *testing.T, p string) string { require.NoError(t, os.Chmod(filepath.Dir(p), 0o755)); return p }},
		{"hardlink", func(t *testing.T, p string) string { require.NoError(t, os.Link(p, p+".link")); return p }},
		{"symlink", func(t *testing.T, p string) string { require.NoError(t, os.Symlink(p, p+".link")); return p + ".link" }},
		{"ancestor_symlink", func(t *testing.T, p string) string {
			d := filepath.Dir(p)
			require.NoError(t, os.Mkdir(filepath.Join(d, "child"), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(d, "child", "key"), []byte("fixture"), 0o600))
			require.NoError(t, os.Symlink("child", filepath.Join(d, "alias")))
			return filepath.Join(d, "alias", "key")
		}},
		{"writable_ancestor", func(t *testing.T, p string) string {
			d := filepath.Dir(p)
			require.NoError(t, os.Mkdir(filepath.Join(d, "child"), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(d, "child", "key"), []byte("fixture"), 0o600))
			require.NoError(t, os.Chmod(d, 0o770))
			return filepath.Join(d, "child", "key")
		}},
		{"fifo", func(t *testing.T, p string) string {
			require.NoError(t, os.Remove(p))
			require.NoError(t, unix.Mkfifo(p, 0o600))
			return p
		}},
		{"directory", func(t *testing.T, p string) string {
			require.NoError(t, os.Remove(p))
			require.NoError(t, os.Mkdir(p, 0o700))
			return p
		}},
		{"oversize", func(t *testing.T, p string) string {
			require.NoError(t, os.WriteFile(p, []byte(strings.Repeat("x", maxCredentialFileBytes+1)), 0o600))
			return p
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := test.mutate(t, credentialFixture(t, "sensitive-fixture"))
			_, err := readProtectedCredential(path)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "sensitive-fixture")
			require.NotContains(t, err.Error(), path)
		})
	}
}

func TestFileCredentialsRejectEmptyAndWrongSelector(t *testing.T) {
	path := credentialFixture(t, " \n")
	cfg := Default()
	cfg.Discord.TokenSource, cfg.Discord.TokenFile = "file", path
	_, err := ResolveDiscordToken(cfg)
	require.EqualError(t, err, "discord credential file is empty")
	_, err = ResolveEmbeddingFileAPIKey(EmbeddingsConfig{APIKeySource: "file", APIKeyFile: path})
	require.EqualError(t, err, "embedding credential file is empty")
	_, err = ResolveEmbeddingFileAPIKey(EmbeddingsConfig{APIKeySource: "env", APIKeyFile: path})
	require.EqualError(t, err, "embedding file source is not selected")
}

func TestProtectedCredentialTraversesExecuteOnlyAncestor(t *testing.T) {
	root := filepath.Dir(credentialFixture(t, "unused"))
	ancestor := filepath.Join(root, "traverse")
	parent := filepath.Join(ancestor, "private")
	require.NoError(t, os.MkdirAll(parent, 0o700))
	path := filepath.Join(parent, "key")
	require.NoError(t, os.WriteFile(path, []byte("fixture"), 0o600))
	require.NoError(t, os.Chmod(ancestor, 0o111))
	t.Cleanup(func() { require.NoError(t, os.Chmod(ancestor, 0o700)) })
	value, err := readProtectedCredential(path)
	require.NoError(t, err)
	require.Equal(t, "fixture", value)
}
