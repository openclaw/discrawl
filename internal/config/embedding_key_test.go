package config

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmbeddingKeyringSelectionAndSafeFailures(t *testing.T) {
	previous := embeddingKeyringGet
	t.Cleanup(func() { embeddingKeyringGet = previous })
	calls := 0
	value, lookupErr := " Bot example ", error(nil)
	embeddingKeyringGet = func(service, account string) (string, error) {
		calls++
		require.Equal(t, "service", service)
		require.Equal(t, "account", account)
		return value, lookupErr
	}
	cfg := EmbeddingsConfig{APIKeySource: "keyring", APIKeyKeyringService: " service ", APIKeyKeyringAccount: " account "}
	key, err := ResolveEmbeddingKeyringAPIKey(cfg)
	require.NoError(t, err)
	require.Equal(t, "Bot example", key) // Embedding keys are not Discord tokens.
	value = "  "
	_, err = ResolveEmbeddingKeyringAPIKey(cfg)
	require.EqualError(t, err, "embedding keyring credential is empty")
	lookupErr = errors.New("locked: sensitive-provider-material")
	_, err = ResolveEmbeddingKeyringAPIKey(cfg)
	require.EqualError(t, err, "embedding keyring credential is unavailable")
	require.NotContains(t, err.Error(), "sensitive")
	cfg.APIKeyKeyringAccount = ""
	_, err = ResolveEmbeddingKeyringAPIKey(cfg)
	require.ErrorContains(t, err, "service and account must be configured")
	cfg.APIKeySource = "env"
	_, err = ResolveEmbeddingKeyringAPIKey(cfg)
	require.EqualError(t, err, "embedding keyring source is not selected")
	require.Equal(t, 3, calls)
}

func TestEmbeddingCredentialConfigRoundTripAndLegacyDefaults(t *testing.T) {
	cfg := Default()
	require.Empty(t, cfg.Search.Embeddings.APIKeySource)
	require.Equal(t, "OPENAI_API_KEY", cfg.Search.Embeddings.APIKeyEnv)
	cfg.Search.Embeddings.APIKeySource = " Keyring "
	cfg.Search.Embeddings.APIKeyKeyringService = " service "
	cfg.Search.Embeddings.APIKeyKeyringAccount = " account "
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, Write(path, cfg))
	loaded, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "keyring", loaded.Search.Embeddings.APIKeySource)
	require.Equal(t, "service", loaded.Search.Embeddings.APIKeyKeyringService)
	require.Equal(t, "account", loaded.Search.Embeddings.APIKeyKeyringAccount)
	require.Equal(t, cfg.Search.Embeddings.APIKeyEnv, loaded.Search.Embeddings.APIKeyEnv)
}
