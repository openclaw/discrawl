package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/embed"
	"github.com/openclaw/crawlkit/worker"
	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/openclaw/discrawl/internal/syncer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zalando/go-keyring"
)

func TestNativeEmbeddingCredentialsReachProviderWithoutEnvironmentExport(t *testing.T) {
	for _, source := range []string{"", "env", "keyring"} {
		t.Run(source, func(t *testing.T) {
			keyring.MockInit()
			t.Setenv("DISCRAWL_TEST_EMBED_KEY", "environment-value")
			want := "environment-value"
			if source == "keyring" {
				want = "Bot example"
				require.NoError(t, keyring.Set("test-embeddings", "test-account", want))
			} else {
				keyring.MockInitWithError(errors.New("keyring must not be accessed"))
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "Bearer "+want, r.Header.Get("Authorization"))
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, `{"model":"fixture","data":[{"index":0,"embedding":[1,2]}]}`)
			}))
			defer server.Close()
			cfg := config.EmbeddingsConfig{Provider: "openai_compatible", Model: "fixture", BaseURL: server.URL, APIKeyEnv: "DISCRAWL_TEST_EMBED_KEY", APIKeySource: source, APIKeyKeyringService: "test-embeddings", APIKeyKeyringAccount: "test-account"}
			p, err := newEmbeddingProvider(cfg)
			require.NoError(t, err)
			_, err = p.Embed(t.Context(), []string{"hello"})
			require.NoError(t, err)
			check := checkEmbeddingProvider(t.Context(), cfg)
			require.Equal(t, "ok", check.Status)
			require.True(t, check.Probed)
			require.NotContains(t, fmt.Sprint(check), want)
			require.Equal(t, "environment-value", os.Getenv("DISCRAWL_TEST_EMBED_KEY"))
		})
	}
}

func TestNativeEmbeddingCredentialFailuresAndRecovery(t *testing.T) {
	keyring.MockInit()
	t.Setenv("DISCRAWL_TEST_EMBED_KEY", "environment-must-not-be-fallback")
	cfg := config.EmbeddingsConfig{Provider: "openai", APIKeySource: "keyring", APIKeyEnv: "DISCRAWL_TEST_EMBED_KEY", APIKeyKeyringService: "test-embeddings", APIKeyKeyringAccount: "test-account"}
	_, err := newEmbeddingProvider(cfg)
	require.EqualError(t, err, "embedding keyring credential is unavailable")
	check := checkEmbeddingProvider(t.Context(), cfg)
	require.Equal(t, "warning", check.Status)
	require.NoError(t, keyring.Set("test-embeddings", "test-account", " "))
	_, err = newEmbeddingProvider(cfg)
	require.EqualError(t, err, "embedding keyring credential is empty")
	keyring.MockInitWithError(errors.New("locked: sensitive-material"))
	_, err = newEmbeddingProvider(cfg)
	require.EqualError(t, err, "embedding keyring credential is unavailable")
	keyring.MockInit()
	require.NoError(t, keyring.Set("test-embeddings", "test-account", "example"))
	check = checkEmbeddingProvider(t.Context(), cfg)
	require.Equal(t, "ok", check.Status)
	require.False(t, check.Probed) // Remote OpenAI checks do not send a paid probe.
	cfg.APIKeySource = "unsupported"
	_, err = newEmbeddingProvider(cfg)
	require.ErrorContains(t, err, "unsupported embedding api_key_source")
}

func TestNativeEmbeddingCredentialFreeProvider(t *testing.T) {
	keyring.MockInitWithError(errors.New("keyring must not be accessed"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Authorization"))
		_, _ = fmt.Fprint(w, `{"model":"fixture","data":[{"index":0,"embedding":[1,2]}]}`)
	}))
	defer server.Close()
	p, err := newEmbeddingProvider(config.EmbeddingsConfig{Provider: "openai_compatible", BaseURL: server.URL, Model: "fixture"})
	require.NoError(t, err)
	_, err = p.Embed(t.Context(), []string{"hello"})
	require.NoError(t, err)
}

func TestDeferredKeyringLookupDoesNotBlockCancellationOrMultiplyLookups(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var lookups atomic.Int32
	p := &deferredEmbeddingProvider{create: func() (embed.Provider, error) {
		lookups.Add(1)
		close(entered)
		<-release
		return liveCLIProvider(func(context.Context, []string) (embed.EmbeddingBatch, error) {
			return embed.EmbeddingBatch{Vectors: [][]float32{{1, 2}}}, nil
		}), nil
	}}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := p.Embed(ctx, []string{"one"}); done <- err }()
	<-entered
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	for range 3 {
		_, err := p.Embed(ctx, []string{"two"})
		require.ErrorIs(t, err, context.Canceled)
	}
	require.EqualValues(t, 1, lookups.Load())
	timed, stop := context.WithTimeout(t.Context(), time.Millisecond)
	defer stop()
	_, timeoutErr := p.Embed(timed, []string{"timed out"})
	var failure *worker.Failure
	require.ErrorAs(t, timeoutErr, &failure)
	require.True(t, failure.Pause) // A locked prompt must not exhaust job attempts.
	require.Equal(t, "embedding_provider_configuration", failure.Code)
	close(release)
	_, err := p.Embed(t.Context(), []string{"three"})
	require.NoError(t, err)
	require.EqualValues(t, 1, lookups.Load())
}

func TestDeferredCredentialLookupPanicIsSafe(t *testing.T) {
	p := &deferredEmbeddingProvider{create: func() (embed.Provider, error) { panic("sensitive-material") }}
	_, err := p.Embed(t.Context(), []string{"hello"})
	require.ErrorContains(t, err, "embedding_provider_configuration")
	require.NotContains(t, err.Error(), "sensitive")
}

func TestTailNativeKeyringRecoveryKeepsCaptureRunning(t *testing.T) {
	keyring.MockInit()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer example", r.Header.Get("Authorization"))
		_, _ = fmt.Fprint(w, `{"model":"fixture","data":[{"index":0,"embedding":[1,2]}]}`)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Second)
	defer cancel()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath, cfg.CacheDir, cfg.LogDir = filepath.Join(dir, "archive.db"), filepath.Join(dir, "cache"), filepath.Join(dir, "logs")
	cfg.Share.RepoPath, cfg.Share.AutoUpdate = filepath.Join(dir, "share"), false
	cfg.Search.Embeddings = config.EmbeddingsConfig{Enabled: true, Provider: "openai_compatible", Model: "fixture", BaseURL: server.URL, APIKeySource: "keyring", APIKeyKeyringService: "recovery-test", APIKeyKeyringAccount: "test-account", BatchSize: 1}
	path := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(path, cfg))
	fake := &fakeSyncService{callTailReady: true}
	rt := tailTestRuntime(ctx, path, fake)
	rt.newSyncer = func(_ syncer.Client, s *store.Store, _ *slog.Logger) syncService {
		return &liveCLISync{fakeSyncService: fake, live: func(ctx context.Context) error {
			require.NoError(t, s.UpsertMessageWithOptions(ctx, store.MessageRecord{ID: "100", GuildID: "g", ChannelID: "c", Content: "first", NormalizedContent: "first"}, store.WriteOptions{EnqueueEmbedding: true}))
			require.Eventually(t, func() bool {
				status, err := s.ReadEmbeddingWorkerStatus(ctx)
				return err == nil && status != nil && status.State == "paused"
			}, 5*time.Second, 20*time.Millisecond)
			// The lookup has finished. Restore the stub credential while the
			// production worker is paused, then allow its normal retry timer.
			require.NoError(t, keyring.Set("recovery-test", "test-account", "example"))
			require.NoError(t, s.UpsertMessageWithOptions(ctx, store.MessageRecord{ID: "101", GuildID: "g", ChannelID: "c", Content: "second", NormalizedContent: "second"}, store.WriteOptions{EnqueueEmbedding: true}))
			var count int
			require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages`).Scan(&count))
			require.Equal(t, 2, count)
			require.Eventually(t, func() bool {
				_ = s.DB().QueryRowContext(ctx, `select count(*) from message_embeddings`).Scan(&count)
				return count == 2
			}, 65*time.Second, 50*time.Millisecond)
			return nil
		}}
	}
	require.NoError(t, rt.dispatch([]string{"tail", "--embed-live", "--guild", "g"}))
	require.Equal(t, 1, fake.tailCalls)
}
