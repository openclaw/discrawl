package cli

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/openclaw/crawlkit/embed"
	"github.com/openclaw/crawlkit/worker"
	"github.com/openclaw/discrawl/internal/store"
)

// Configuration/credential failures disable processing, never raw capture.
// Retrying construction allows temporarily unavailable credentials to recover.
type deferredEmbeddingProvider struct {
	mu       sync.Mutex
	provider embed.Provider
	create   func() (embed.Provider, error)
	pending  *embeddingProviderAttempt
}

type embeddingProviderAttempt struct {
	done     chan struct{}
	provider embed.Provider
	err      error
}

func (p *deferredEmbeddingProvider) Embed(ctx context.Context, inputs []string) (embed.EmbeddingBatch, error) {
	p.mu.Lock()
	if p.provider == nil {
		if p.pending == nil {
			attempt := &embeddingProviderAttempt{done: make(chan struct{})}
			p.pending = attempt
			// OS keyrings can wait for an interactive unlock. Share one lookup
			// across workers/retries; cancellation must not wait on the prompt.
			go func() {
				defer func() {
					if recover() != nil {
						attempt.err = errors.New("embedding provider initialization failed")
					}
					close(attempt.done)
				}()
				attempt.provider, attempt.err = p.create()
			}()
		}
		attempt := p.pending
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return embed.EmbeddingBatch{}, &worker.Failure{Code: "embedding_provider_configuration", Pause: true, RetryAfter: time.Minute}
			}
			return embed.EmbeddingBatch{}, ctx.Err()
		case <-attempt.done:
		}
		p.mu.Lock()
		if p.pending == attempt {
			p.pending = nil
		}
		if attempt.err != nil || attempt.provider == nil {
			p.mu.Unlock()
			return embed.EmbeddingBatch{}, &worker.Failure{Code: "embedding_provider_configuration", Pause: true, RetryAfter: time.Minute}
		}
		p.provider = attempt.provider
	}
	provider := p.provider
	p.mu.Unlock()
	return provider.Embed(ctx, inputs)
}

func (r *runtime) runTailWithEmbeddingWorker(ctx context.Context, guilds []string, repair time.Duration) error {
	create := r.newEmbed
	if create == nil {
		create = newEmbeddingProvider
	}
	provider := &deferredEmbeddingProvider{create: func() (embed.Provider, error) { return create(r.cfg.Search.Embeddings) }}
	w, err := r.store.NewEmbeddingWorker(ctx, provider, store.EmbeddingDrainOptions{Provider: r.cfg.Search.Embeddings.Provider, Model: r.cfg.Search.Embeddings.Model, InputVersion: store.EmbeddingInputVersion, MaxInputChars: r.cfg.Search.Embeddings.MaxInputChars, BatchSize: r.cfg.Search.Embeddings.BatchSize, RequestTimeout: mustDuration(r.cfg.Search.Embeddings.RequestTimeout)})
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
