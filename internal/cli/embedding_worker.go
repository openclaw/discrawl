package cli

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/openclaw/crawlkit/embed"
	"github.com/openclaw/crawlkit/worker"
	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
)

// Configuration/credential failures disable processing, never raw capture.
// Retrying construction allows temporarily unavailable credentials to recover.
type deferredEmbeddingProvider struct {
	mu       sync.Mutex
	provider embed.Provider
	create   func() (embed.Provider, error)
}

func (p *deferredEmbeddingProvider) Embed(ctx context.Context, inputs []string) (embed.EmbeddingBatch, error) {
	p.mu.Lock()
	if p.provider == nil {
		v, err := p.create()
		if err != nil || v == nil {
			p.mu.Unlock()
			return embed.EmbeddingBatch{}, &worker.Failure{Code: "embedding_provider_configuration", Pause: true, RetryAfter: time.Minute}
		}
		p.provider = v
	}
	provider := p.provider
	p.mu.Unlock()
	return provider.Embed(ctx, inputs)
}

func (r *runtime) runTailWithEmbeddingWorker(ctx context.Context, guilds []string, repair time.Duration) error {
	create := r.newEmbed
	if create == nil {
		create = func(c config.EmbeddingsConfig) (embed.Provider, error) {
			return embed.NewProvider(crawlkitEmbeddingConfig(c))
		}
	}
	provider := &deferredEmbeddingProvider{create: func() (embed.Provider, error) { return create(r.cfg.Search.Embeddings) }}
	w, err := r.store.NewEmbeddingWorker(ctx, provider, store.EmbeddingDrainOptions{Provider: r.cfg.Search.Embeddings.Provider, Model: r.cfg.Search.Embeddings.Model, InputVersion: store.EmbeddingInputVersion, MaxInputChars: r.cfg.Search.Embeddings.MaxInputChars})
	if err != nil {
		return err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(workerCtx) }()
	tailErr := r.syncer.RunTail(ctx, guilds, repair)
	cancel()
	return errors.Join(tailErr, <-done)
}
