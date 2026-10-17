//go:build darwin || linux

package cli

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zalando/go-keyring"
)

func TestFileEmbeddingProviderWorksWithUnavailableKeyring(t *testing.T) {
	keyring.MockInitWithError(errors.New("keychain must not be used"))
	t.Cleanup(keyring.MockInit)
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	home, err = filepath.EvalSymlinks(home)
	require.NoError(t, err)
	dir, err := os.MkdirTemp(home, ".discrawl-provider-test-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	path := filepath.Join(dir, "fixture.key")
	require.NoError(t, os.WriteFile(path, []byte("Bot file-fixture"), 0o600))
	t.Setenv("DISCRAWL_TEST_EMBED_KEY", "environment-is-not-selected")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer Bot file-fixture", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"model":"fixture","data":[{"index":0,"embedding":[1,2]}]}`)
	}))
	defer server.Close()
	cfg := config.EmbeddingsConfig{Provider: "openai_compatible", Model: "fixture", BaseURL: server.URL, APIKeySource: "file", APIKeyFile: path, APIKeyEnv: "DISCRAWL_TEST_EMBED_KEY"}
	provider, err := newEmbeddingProvider(cfg)
	require.NoError(t, err)
	_, err = provider.Embed(t.Context(), []string{"synthetic document"})
	require.NoError(t, err)
	// A fresh provider resolves the same selected source for query embeddings.
	queryProvider, err := newEmbeddingProvider(cfg)
	require.NoError(t, err)
	_, err = queryProvider.Embed(t.Context(), []string{"synthetic query"})
	require.NoError(t, err)
	require.Equal(t, "ok", checkEmbeddingProvider(t.Context(), cfg).Status)
	require.NoError(t, os.Remove(path))
	_, err = newEmbeddingProvider(cfg)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "file-fixture")
	require.Equal(t, "environment-is-not-selected", os.Getenv("DISCRAWL_TEST_EMBED_KEY"))
}
