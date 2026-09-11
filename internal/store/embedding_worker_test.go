package store

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/embed"
	"github.com/openclaw/crawlkit/worker"
	"github.com/stretchr/testify/require"
)

type liveTestProvider func(context.Context, []string) (embed.EmbeddingBatch, error)

func (f liveTestProvider) Embed(ctx context.Context, input []string) (embed.EmbeddingBatch, error) {
	return f(ctx, input)
}

func liveVectors(_ context.Context, input []string) (embed.EmbeddingBatch, error) {
	v := make([][]float32, len(input))
	for i := range v {
		v[i] = []float32{float32(len(input[i])), 1}
	}
	return embed.EmbeddingBatch{Vectors: v}, nil
}

func liveStore(t *testing.T) *Store {
	t.Helper()
	s, e := Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, e)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func liveOpts() EmbeddingDrainOptions {
	return EmbeddingDrainOptions{Provider: "openai", Model: "fixture", InputVersion: EmbeddingInputVersion}
}

func liveMessage(t *testing.T, s *Store, id, text string) {
	t.Helper()
	require.NoError(t, s.UpsertMessageWithOptions(t.Context(), MessageRecord{ID: id, GuildID: "g", ChannelID: "c", Content: text, NormalizedContent: text, CreatedAt: time.Now().UTC().Format(time.RFC3339)}, WriteOptions{EnqueueEmbedding: true}))
}

func makeLiveWorker(t *testing.T, s *Store, p embed.Provider) *EmbeddingWorker {
	t.Helper()
	w, e := s.NewEmbeddingWorker(t.Context(), p, liveOpts())
	require.NoError(t, e)
	t.Cleanup(func() { _ = w.queue.reader.Close() })
	return w
}

func startLive(t *testing.T, w *EmbeddingWorker) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			require.NoError(t, e)
		case <-time.After(5 * time.Second):
			t.Error("worker did not stop")
		}
	})
	return cancel
}

func claimLive(t *testing.T, q *embeddingWorkQueue, class worker.Class) worker.Job[string] {
	t.Helper()
	now := time.Now().UTC()
	jobs, e := q.Claim(t.Context(), worker.ClaimRequest{Kind: "embeddings", Class: class, Limit: 1, Now: now, LeaseUntil: now.Add(time.Minute)})
	require.NoError(t, e)
	require.Len(t, jobs, 1)
	return jobs[0]
}

func TestEmbeddingWorkerMigrationPreservesExistingArchive(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "old.db")
	db, e := sql.Open("sqlite", path)
	require.NoError(t, e)
	old := &Store{db: db, path: path}
	require.NoError(t, old.applyBaselineSchema(ctx))
	require.NoError(t, old.setSchemaVersion(ctx, 5))
	_, e = db.ExecContext(t.Context(), `insert into embedding_jobs(message_id,state,attempts,updated_at)values('old','pending',2,'2026-01-01')`)
	require.NoError(t, e)
	require.NoError(t, db.Close())
	s, e := Open(ctx, path)
	require.NoError(t, e)
	defer func() { _ = s.Close() }()
	version, e := s.schemaVersion(ctx)
	require.NoError(t, e)
	require.Equal(t, 6, version)
	var priority, revision, attempts int
	require.NoError(t, s.db.QueryRowContext(t.Context(), `select priority,revision,attempts from embedding_jobs where message_id='old'`).Scan(&priority, &revision, &attempts))
	require.Equal(t, 0, priority)
	require.Equal(t, 1, revision)
	require.Equal(t, 2, attempts)
	require.NoError(t, s.applyEmbeddingWorkerMigration(ctx))
}

func TestEmbeddingWorkerFencesEditsDeletesAndLeaseExpiry(t *testing.T) {
	s := liveStore(t)
	w := makeLiveWorker(t, s, liveTestProvider(liveVectors))
	q := w.queue
	ctx := t.Context()
	liveMessage(t, s, "101", "old")
	j := claimLive(t, q, worker.Fresh)
	result, e := q.process(ctx, []worker.Job[string]{j})
	require.NoError(t, e)
	liveMessage(t, s, "101", "new")
	ok, e := q.Complete(ctx, j, result[0], time.Now())
	require.NoError(t, e)
	require.False(t, ok)
	ok, e = q.Retry(ctx, j, worker.Retry{Now: time.Now(), At: time.Now(), Permanent: true})
	require.NoError(t, e)
	require.False(t, ok)
	ok, e = q.Release(ctx, j, time.Now())
	require.NoError(t, e)
	require.False(t, ok)
	current := claimLive(t, q, worker.Fresh)
	result, e = q.process(ctx, []worker.Job[string]{current})
	require.NoError(t, e)
	ok, e = q.Complete(ctx, current, result[0], time.Now().Add(2*time.Minute))
	require.NoError(t, e)
	require.False(t, ok)
	ok, e = q.Complete(ctx, current, result[0], time.Now())
	require.NoError(t, e)
	require.True(t, ok)
	ok, e = q.Complete(ctx, current, result[0], time.Now())
	require.NoError(t, e)
	require.False(t, ok)
	var count int
	require.NoError(t, s.db.QueryRowContext(t.Context(), `select count(*) from message_embeddings where message_id='101'`).Scan(&count))
	require.Equal(t, 1, count)
	liveMessage(t, s, "101", "third")
	require.NoError(t, s.db.QueryRowContext(t.Context(), `select count(*) from message_embeddings where message_id='101'`).Scan(&count))
	require.Zero(t, count)
	j = claimLive(t, q, worker.Fresh)
	result, e = q.process(ctx, []worker.Job[string]{j})
	require.NoError(t, e)
	require.NoError(t, s.MarkMessageDeletedWithoutEvent(ctx, "g", "c", "101"))
	ok, e = q.Complete(ctx, j, result[0], time.Now())
	require.NoError(t, e)
	require.False(t, ok)
	liveMessage(t, s, "101", "resurrected")
	ok, e = q.Complete(ctx, j, result[0], time.Now())
	require.NoError(t, e)
	require.False(t, ok)
}

func TestEmbeddingWorkerDetectsUnqueuedSourceChangesAndPrivacy(t *testing.T) {
	s := liveStore(t)
	w := makeLiveWorker(t, s, liveTestProvider(liveVectors))
	ctx := t.Context()
	q := w.queue
	liveMessage(t, s, "101", "old")
	j := claimLive(t, q, worker.Fresh)
	result, e := q.process(ctx, []worker.Job[string]{j})
	require.NoError(t, e)
	_, e = s.db.ExecContext(t.Context(), `update messages set normalized_content='external update' where id='101'`)
	require.NoError(t, e)
	ok, e := q.Complete(ctx, j, result[0], time.Now())
	require.NoError(t, e)
	require.False(t, ok)
	j = claimLive(t, q, worker.Fresh)
	result, e = q.process(ctx, []worker.Job[string]{j})
	require.NoError(t, e)
	_, e = s.db.ExecContext(t.Context(), `update messages set guild_id='@me' where id='101'`)
	require.NoError(t, e)
	ok, e = q.Complete(ctx, j, result[0], time.Now())
	require.NoError(t, e)
	require.False(t, ok)
	var count int
	require.NoError(t, s.db.QueryRowContext(t.Context(), `select count(*) from embedding_jobs`).Scan(&count))
	require.Zero(t, count)
	liveMessage(t, s, "102", " ")
	j = claimLive(t, q, worker.Fresh)
	result, e = q.process(ctx, []worker.Job[string]{j})
	require.NoError(t, e)
	require.True(t, result[0].empty)
	ok, e = q.Complete(ctx, j, result[0], time.Now())
	require.NoError(t, e)
	require.True(t, ok)
	liveMessage(t, s, "103", "next")
	j = claimLive(t, q, worker.Fresh)
	liveMessage(t, s, "103", "updated")
	result, e = q.process(ctx, []worker.Job[string]{j})
	require.NoError(t, e)
	require.False(t, result[0].valid)
}

func TestEmbeddingWorkerRestartRecoversAndFailuresAreDurable(t *testing.T) {
	s := liveStore(t)
	w := makeLiveWorker(t, s, liveTestProvider(liveVectors))
	q := w.queue
	ctx := t.Context()
	liveMessage(t, s, "101", "text")
	old := claimLive(t, q, worker.Fresh)
	// Simulate a dead owner, before starting any goroutine. The CLI's writer lock
	// guarantees this recovery never competes with another live tail process.
	restarted := makeLiveWorker(t, s, liveTestProvider(liveVectors))
	j := claimLive(t, restarted.queue, worker.Fresh)
	require.NotEqual(t, old.Token, j.Token)
	ok, e := q.Retry(ctx, old, worker.Retry{Now: time.Now(), At: time.Now()})
	require.NoError(t, e)
	require.False(t, ok)
	now := time.Now()
	ok, e = restarted.queue.Retry(ctx, j, worker.Retry{Now: now, At: now.Add(time.Hour), Code: "throttled"})
	require.NoError(t, e)
	require.True(t, ok)
	jobs, e := q.Claim(ctx, worker.ClaimRequest{Kind: "embeddings", Class: worker.Fresh, Limit: 10, Now: now, LeaseUntil: now.Add(time.Minute)})
	require.NoError(t, e)
	require.Empty(t, jobs)
	_, e = s.db.ExecContext(t.Context(), `update embedding_jobs set available_at='' where message_id='101'`)
	require.NoError(t, e)
	j = claimLive(t, q, worker.Fresh)
	ok, e = q.Retry(ctx, j, worker.Retry{Now: time.Now(), At: time.Now(), Code: "invalid_input", Permanent: true, CountAttempt: true})
	require.NoError(t, e)
	require.True(t, ok)
	var attempts int
	var state string
	require.NoError(t, s.db.QueryRowContext(t.Context(), `select attempts,state from embedding_jobs where message_id='101'`).Scan(&attempts, &state))
	require.Equal(t, 1, attempts)
	require.Equal(t, "failed", state)
	liveMessage(t, s, "101", "changed")
	j = claimLive(t, q, worker.Fresh)
	ok, e = q.Release(ctx, j, time.Now())
	require.NoError(t, e)
	require.True(t, ok)
	_, e = q.Claim(ctx, worker.ClaimRequest{Kind: "wrong"})
	require.Error(t, e)
}

func TestEmbeddingWorkerProviderOutageDoesNotBlockIngestion(t *testing.T) {
	s := liveStore(t)
	entered := make(chan struct{}, 2)
	w := makeLiveWorker(t, s, liveTestProvider(func(ctx context.Context, text []string) (embed.EmbeddingBatch, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return embed.EmbeddingBatch{}, ctx.Err()
	}))
	liveMessage(t, s, "101", "slow request")
	cancel := startLive(t, w)
	<-entered
	start := time.Now()
	liveMessage(t, s, "102", "still collecting")
	require.Less(t, time.Since(start), time.Second)
	cancel()
	require.Eventually(t, func() bool { return w.Status().State == "stopped" }, 3*time.Second, time.Millisecond)
	var status *EmbeddingWorkerStatus
	require.Eventually(t, func() bool {
		var err error
		status, err = s.ReadEmbeddingWorkerStatus(t.Context())
		return err == nil && status != nil && status.State == "stopped"
	}, 3*time.Second, time.Millisecond)
	require.Equal(t, 2, status.Pending)
}

func TestEmbeddingWorkerFreshnessUnderCatchup(t *testing.T) {
	s := liveStore(t)
	for i := range 256 {
		liveMessage(t, s, strconv.Itoa(1000+i), "history")
	}
	_, e := s.db.ExecContext(t.Context(), `update embedding_jobs set priority=0`)
	require.NoError(t, e)
	w := makeLiveWorker(t, s, liveTestProvider(func(ctx context.Context, text []string) (embed.EmbeddingBatch, error) {
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return embed.EmbeddingBatch{}, ctx.Err()
		}
		return liveVectors(ctx, text)
	}))
	startLive(t, w)
	var mu sync.Mutex
	latencies := map[string]time.Time{}
	for i := range 40 {
		id := strconv.Itoa(2000 + i)
		start := time.Now()
		liveMessage(t, s, id, "fresh content")
		require.Less(t, time.Since(start), time.Second)
		mu.Lock()
		latencies[id] = start
		mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	require.Eventually(t, func() bool {
		var n int
		_ = s.db.QueryRowContext(t.Context(), `select count(*) from message_embeddings where message_id>='2000'`).Scan(&n)
		return n == 40
	}, 10*time.Second, 20*time.Millisecond)
	var ages []time.Duration
	rows, e := s.db.QueryContext(t.Context(), `select message_id,embedded_at from message_embeddings where message_id>='2000'`)
	require.NoError(t, e)
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, stamp string
		require.NoError(t, rows.Scan(&id, &stamp))
		at, e := time.Parse(timeLayout, stamp)
		require.NoError(t, e)
		ages = append(ages, at.Sub(latencies[id]))
	}
	require.NoError(t, rows.Err())

	slices.Sort(ages)
	p95 := ages[int(float64(len(ages)-1)*.95)]
	t.Logf("freshness p95=%s; 10 messages/second; provider=200ms; catch-up=256", p95)
	require.Less(t, p95, 10*time.Second)
}

func TestEmbeddingWorkerFailureClassification(t *testing.T) {
	classify := func(err error) *worker.Failure {
		t.Helper()
		f, ok := errors.AsType[*worker.Failure](err)
		require.True(t, ok)
		return f
	}
	for _, code := range []int{400, 401, 403, 429, 500} {
		f := classify(embeddingWorkerFailure(&embed.HTTPError{StatusCode: code, Header: http.Header{"Retry-After": []string{"12"}}}))
		require.Equal(t, 12*time.Second, f.RetryAfter)
		require.Equal(t, code != 400, f.Pause)
	}
	f := classify(embeddingWorkerFailure(&embed.HTTPError{StatusCode: 429, Header: http.Header{"Retry-After": []string{time.Now().Add(time.Minute).UTC().Format(http.TimeFormat)}}}))
	require.Positive(t, f.RetryAfter)
	f = classify(embeddingWorkerFailure(&embed.HTTPError{StatusCode: 503}))
	require.Equal(t, 5*time.Second, f.RetryAfter)
	require.True(t, classify(embeddingWorkerFailure(errors.New("private provider body"))).Pause)
	original := &worker.Failure{Code: "configuration", Pause: true}
	require.Same(t, original, embeddingWorkerFailure(original))
	s := liveStore(t)
	_, e := s.NewEmbeddingWorker(t.Context(), nil, liveOpts())
	require.Error(t, e)
	for _, provider := range []liveTestProvider{func(context.Context, []string) (embed.EmbeddingBatch, error) {
		return embed.EmbeddingBatch{}, errors.New("private")
	}, func(context.Context, []string) (embed.EmbeddingBatch, error) { return embed.EmbeddingBatch{}, nil }} {
		w := makeLiveWorker(t, s, provider)
		liveMessage(t, s, "101", "input")
		j := claimLive(t, w.queue, worker.Fresh)
		_, e = w.queue.process(t.Context(), []worker.Job[string]{j})
		require.Error(t, e)
	}
}

func TestEmbeddingWorkerCatchupPromotesLiveEdits(t *testing.T) {
	s := liveStore(t)
	ctx := t.Context()
	msg := MessageRecord{ID: "101", GuildID: "g", ChannelID: "c", Content: "history", NormalizedContent: "history"}
	require.NoError(t, s.UpsertMessageWithOptions(ctx, msg, WriteOptions{EnqueueEmbedding: true, EmbeddingCatchUp: true}))
	var priority, revision int
	require.NoError(t, s.db.QueryRowContext(t.Context(), `select priority,revision from embedding_jobs where message_id='101'`).Scan(&priority, &revision))
	require.Zero(t, priority)
	liveMessage(t, s, "101", "live edit")
	var next int
	require.NoError(t, s.db.QueryRowContext(t.Context(), `select priority,revision from embedding_jobs where message_id='101'`).Scan(&priority, &next))
	require.Equal(t, 1, priority)
	require.Greater(t, next, revision)
}

func TestEmbeddingWorkerProviderChangeResetsAttempts(t *testing.T) {
	s := liveStore(t)
	w := makeLiveWorker(t, s, liveTestProvider(liveVectors))
	liveMessage(t, s, "101", "input")
	_, err := s.db.ExecContext(t.Context(), `update embedding_jobs set attempts=2,provider='old',model='old',input_version='old' where message_id='101'`)
	require.NoError(t, err)
	job := claimLive(t, w.queue, worker.Fresh)
	require.Zero(t, job.Attempts)
	var attempts int
	require.NoError(t, s.db.QueryRowContext(t.Context(), `select attempts from embedding_jobs where message_id='101'`).Scan(&attempts))
	require.Zero(t, attempts)
}
