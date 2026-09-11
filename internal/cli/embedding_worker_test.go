package cli

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/embed"
	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/openclaw/discrawl/internal/syncer"
	"github.com/stretchr/testify/require"
)

type liveCLISync struct {
	*fakeSyncService
	live func(context.Context) error
}

func (s *liveCLISync) RunTail(ctx context.Context, g []string, d time.Duration) error {
	if err := s.fakeSyncService.RunTail(ctx, g, d); err != nil {
		return err
	}
	return s.live(ctx)
}

type liveCLIProvider func(context.Context, []string) (embed.EmbeddingBatch, error)

func (f liveCLIProvider) Embed(ctx context.Context, v []string) (embed.EmbeddingBatch, error) {
	return f(ctx, v)
}

func TestTailLiveEmbeddingsKeepsCaptureAndWriterOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "archive.db")
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.LogDir = filepath.Join(dir, "logs")
	cfg.Share.RepoPath = filepath.Join(dir, "share")
	cfg.Share.AutoUpdate = false
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai"
	cfg.Search.Embeddings.Model = "fixture"
	p := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(p, cfg))
	fake := &fakeSyncService{callTailReady: true}
	rt := tailTestRuntime(ctx, p, fake)
	var requests atomic.Int32
	rt.newEmbed = func(config.EmbeddingsConfig) (embed.Provider, error) {
		return liveCLIProvider(func(ctx context.Context, texts []string) (embed.EmbeddingBatch, error) {
			requests.Add(1)
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return embed.EmbeddingBatch{}, ctx.Err()
			}
			v := make([][]float32, len(texts))
			for i := range v {
				v[i] = []float32{1, 2}
			}
			return embed.EmbeddingBatch{Vectors: v}, nil
		}), nil
	}
	rt.newSyncer = func(_ syncer.Client, s *store.Store, _ *slog.Logger) syncService {
		return &liveCLISync{fakeSyncService: fake, live: func(ctx context.Context) error {
			require.True(t, fake.tailEmbeddings)
			lockPath, e := rt.syncLockPath()
			require.NoError(t, e)
			owner, ok := readSyncLockOwner(lockPath)
			require.True(t, ok)
			require.Equal(t, "tail", owner.Operation)
			for i := range 10 {
				start := time.Now()
				require.NoError(t, s.UpsertMessageWithOptions(ctx, store.MessageRecord{ID: strconv.Itoa(100 + i), GuildID: "g", ChannelID: "c", Content: "live", NormalizedContent: "live"}, store.WriteOptions{EnqueueEmbedding: true}))
				require.Less(t, time.Since(start), time.Second)
				time.Sleep(100 * time.Millisecond)
			}
			require.Eventually(t, func() bool {
				var n int
				_ = s.DB().QueryRowContext(t.Context(), `select count(*) from message_embeddings`).Scan(&n)
				return n == 10
			}, 5*time.Second, 20*time.Millisecond)
			require.True(t, rt.dbLockHeld)
			return nil
		}}
	}
	require.NoError(t, rt.dispatch([]string{"tail", "--embed-live", "--guild", "g"}))
	require.Positive(t, requests.Load())
	require.Equal(t, 1, fake.tailCalls)
}

func TestTailLiveFlagValidation(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		cfg := config.Default()
		root := t.TempDir()
		cfg.DBPath = filepath.Join(root, "db")
		cfg.CacheDir = filepath.Join(root, "cache")
		cfg.LogDir = filepath.Join(root, "logs")
		cfg.Share.RepoPath = filepath.Join(root, "share")
		cfg.Search.Embeddings.Enabled = enabled
		cfg.Share.AutoUpdate = false
		p := filepath.Join(t.TempDir(), "config.toml")
		require.NoError(t, config.Write(p, cfg))
		f := &fakeSyncService{}
		rt := tailTestRuntime(t.Context(), p, f)
		args := []string{"tail", "--embed-live"}
		if enabled {
			args = append(args, "--replay-failures-only")
		}
		require.Error(t, rt.dispatch(args))
		require.Zero(t, f.tailCalls)
	}
}

func TestDeferredEmbeddingProviderConfigurationFailure(t *testing.T) {
	var count int
	p := &deferredEmbeddingProvider{create: func() (embed.Provider, error) {
		count++
		if count == 1 {
			return nil, errors.New("secret credential")
		}
		return liveCLIProvider(func(context.Context, []string) (embed.EmbeddingBatch, error) {
			return embed.EmbeddingBatch{Vectors: [][]float32{{1}}}, nil
		}), nil
	}}
	_, err := p.Embed(t.Context(), []string{"a"})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
	_, err = p.Embed(t.Context(), []string{"a"})
	require.NoError(t, err)
	_, err = p.Embed(t.Context(), []string{"a"})
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestTailLiveMissingCredentialsDoesNotStopCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "archive.db")
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.LogDir = filepath.Join(dir, "logs")
	cfg.Share.RepoPath = filepath.Join(dir, "share")
	cfg.Share.AutoUpdate = false
	cfg.Search.Embeddings.Enabled = true
	p := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(p, cfg))
	fake := &fakeSyncService{}
	rt := tailTestRuntime(ctx, p, fake)
	rt.newEmbed = func(config.EmbeddingsConfig) (embed.Provider, error) { return nil, errors.New("missing secret") }
	rt.newSyncer = func(_ syncer.Client, s *store.Store, _ *slog.Logger) syncService {
		return &liveCLISync{fakeSyncService: fake, live: func(ctx context.Context) error {
			require.NoError(t, s.UpsertMessageWithOptions(ctx, store.MessageRecord{ID: "100", GuildID: "g", ChannelID: "c", Content: "first", NormalizedContent: "first"}, store.WriteOptions{EnqueueEmbedding: true}))
			require.Eventually(t, func() bool {
				status, e := s.ReadEmbeddingWorkerStatus(ctx)
				return e == nil && status != nil && status.State == "paused"
			}, 4*time.Second, 20*time.Millisecond)
			require.NoError(t, s.UpsertMessageWithOptions(ctx, store.MessageRecord{ID: "101", GuildID: "g", ChannelID: "c", Content: "second", NormalizedContent: "second"}, store.WriteOptions{EnqueueEmbedding: true}))
			var n int
			require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from messages`).Scan(&n))
			require.Equal(t, 2, n)
			return nil
		}}
	}
	require.NoError(t, rt.dispatch([]string{"tail", "--embed-live"}))
}
