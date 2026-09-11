package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/embed"
	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/crawlkit/worker"
)

func (s *Store) applyEmbeddingWorkerMigration(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	rows, err := tx.QueryContext(ctx, `pragma table_info(embedding_jobs)`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	present := map[string]bool{}
	for rows.Next() {
		var id, nn, pk int
		var name, typ string
		var def sql.NullString
		if err = rows.Scan(&id, &name, &typ, &nn, &def, &pk); err != nil {
			return err
		}
		present[name] = true
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	for _, column := range []struct{ name, definition string }{
		{"revision", "integer not null default 1"},
		{"lease_token", "text not null default ''"},
		{"lease_until", "text not null default ''"},
		{"available_at", "text not null default ''"},
		{"priority", "integer not null default 0"},
		{"enqueued_at", "text not null default ''"},
	} {
		if !present[column.name] {
			if _, err = tx.ExecContext(ctx, `alter table embedding_jobs add column `+column.name+` `+column.definition); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `create index if not exists idx_embedding_worker_ready on embedding_jobs(priority,available_at,updated_at,message_id) where state='pending'`); err != nil {
		return err
	}
	// Older pending work is catch-up. No archive scan or vector regeneration is required.
	if _, err = tx.ExecContext(ctx, `update embedding_jobs set locked_at=null where state='pending'`); err != nil {
		return err
	}
	return tx.Commit()
}

// EmbeddingWorker uses the tail owner's existing writer connection for short
// transactions, and a separate read-only connection for content preparation.
type EmbeddingWorker struct {
	queue  *embeddingWorkQueue
	runner *worker.Runner[string, embeddingWorkResult]
}
type embeddingWorkQueue struct {
	store         *Store
	reader        *crawlstore.Store
	opts          EmbeddingDrainOptions
	embedProvider embed.Provider
}
type embeddingWorkResult struct {
	input      string
	valid      bool
	blob       []byte
	dimensions int
	empty      bool
}

type EmbeddingWorkerStatus struct {
	FailedJobs int `json:"failed_jobs"`
	worker.Status
	UpdatedAt       time.Time `json:"updated_at"`
	Pending         int       `json:"pending"`
	OldestPendingAt string    `json:"oldest_pending_at,omitempty"`
}

func (s *Store) NewEmbeddingWorker(ctx context.Context, provider embed.Provider, opts EmbeddingDrainOptions) (*EmbeddingWorker, error) {
	if provider == nil {
		return nil, errors.New("embedding worker requires a provider")
	}
	reader, err := crawlstore.OpenReadOnly(ctx, s.path)
	if err != nil {
		return nil, err
	}
	opts = normalizeEmbeddingDrainOptions(opts)
	// Startup happens under the CLI's exclusive writer lock, so no previous tail
	// owner is alive. Recover its interrupted claims immediately, without waiting
	// the full lease interval. This method must not be used by competing processes.
	if _, err = s.db.ExecContext(ctx, `update embedding_jobs set lease_token='',lease_until='',locked_at=null where state='pending' and (lease_token!='' or locked_at is not null)`); err != nil {
		_ = reader.Close()
		return nil, err
	}
	q := &embeddingWorkQueue{store: s, reader: reader, opts: opts, embedProvider: provider}
	requestTimeout := opts.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = embed.DefaultRequestTimeout
	}
	r, err := worker.New[string, embeddingWorkResult](q, q.process, worker.Options{Kind: "embeddings", BatchSize: min(opts.BatchSize, 64), Concurrency: 2, TaskTimeout: requestTimeout + 30*time.Second})
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	return &EmbeddingWorker{queue: q, runner: r}, nil
}

func (w *EmbeddingWorker) Run(ctx context.Context) error {
	defer func() { _ = w.queue.reader.Close() }()
	w.queue.store.setEmbeddingWake(w.runner.Notify)
	defer w.queue.store.setEmbeddingWake(nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.persistStatus(ctx)
			}
		}
	}()
	err := w.runner.Run(ctx)
	<-done
	final, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.persistStatus(final)
	return err
}
func (w *EmbeddingWorker) Status() worker.Status { return w.runner.Status() }
func (w *EmbeddingWorker) persistStatus(ctx context.Context) {
	call, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	body, err := json.Marshal(EmbeddingWorkerStatus{Status: w.runner.Status(), UpdatedAt: time.Now().UTC()})
	if err != nil {
		return
	}
	_, _ = w.queue.store.db.ExecContext(call, `insert into sync_state(scope,cursor,updated_at)values('worker:embeddings',?,?) on conflict(scope)do update set cursor=excluded.cursor,updated_at=excluded.updated_at`, string(body), time.Now().UTC().Format(timeLayout))
}

func (s *Store) ReadEmbeddingWorkerStatus(ctx context.Context) (*EmbeddingWorkerStatus, error) {
	var body string
	err := s.db.QueryRowContext(ctx, `select cursor from sync_state where scope='worker:embeddings'`).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var status EmbeddingWorkerStatus
	if err = json.Unmarshal([]byte(body), &status); err != nil {
		return nil, err
	}
	if time.Since(status.UpdatedAt) > 10*time.Second && status.State != "stopped" {
		status.State = "stale"
	}
	err = s.db.QueryRowContext(ctx, `select count(*),coalesce(min(coalesce(nullif(j.enqueued_at,''),j.updated_at)),'') from embedding_jobs j join messages m on m.id=j.message_id where j.state='pending' and m.deleted_at is null and m.guild_id!='@me'`).Scan(&status.Pending, &status.OldestPendingAt)
	if err != nil {
		return nil, err
	}
	err = s.db.QueryRowContext(ctx, `select count(*) from embedding_jobs where state='failed'`).Scan(&status.FailedJobs)
	return &status, err
}

func (q *embeddingWorkQueue) Claim(ctx context.Context, req worker.ClaimRequest) ([]worker.Job[string], error) {
	if req.Kind != "embeddings" {
		return nil, errors.New("unsupported worker kind")
	}
	priority := 1
	if req.Class == worker.CatchUp {
		priority = 0
	}
	tx, err := q.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	now := req.Now.UTC().Format(timeLayout)
	rows, err := tx.QueryContext(ctx, `select message_id,revision,attempts,provider,model,input_version from embedding_jobs where state='pending' and priority=? and available_at<=? and lease_until<=? and exists(select 1 from messages m where m.id=embedding_jobs.message_id and m.deleted_at is null and m.guild_id!='@me') order by available_at,updated_at,message_id limit ?`, priority, now, now, req.Limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var jobs []worker.Job[string]
	for rows.Next() {
		var j worker.Job[string]
		var revision int64
		var provider, model, inputVersion string
		if err = rows.Scan(&j.Key, &revision, &j.Attempts, &provider, &model, &inputVersion); err != nil {
			return nil, err
		}
		if provider != q.opts.Provider || model != q.opts.Model || inputVersion != q.opts.InputVersion {
			j.Attempts = 0
		}
		j.Revision = strconv.FormatInt(revision, 10)
		j.Token = rand.Text()
		j.Payload = j.Key
		jobs = append(jobs, j)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	for _, j := range jobs {
		if _, err = tx.ExecContext(ctx, `update embedding_jobs set lease_token=?,lease_until=?,locked_at=?,provider=?,model=?,input_version=?,attempts=? where message_id=? and revision=?`, j.Token, req.LeaseUntil.UTC().Format(timeLayout), now, q.opts.Provider, q.opts.Model, q.opts.InputVersion, j.Attempts, j.Key, j.Revision); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (q *embeddingWorkQueue) process(ctx context.Context, jobs []worker.Job[string]) ([]embeddingWorkResult, error) {
	results := make([]embeddingWorkResult, len(jobs))
	var inputs []string
	var indexes []int
	for i, j := range jobs {
		var text string
		err := q.reader.DB().QueryRowContext(ctx, `select m.normalized_content from messages m join embedding_jobs j on j.message_id=m.id where m.id=? and m.deleted_at is null and m.guild_id!='@me' and j.revision=? and j.lease_token=?`, j.Key, j.Revision, j.Token).Scan(&text)
		if errors.Is(err, sql.ErrNoRows) {
			results[i].empty = true
			continue
		}
		if err != nil {
			return nil, &worker.Failure{Code: "embedding_read_failed", Pause: true}
		}
		results[i].input = capRunes(text, q.opts.MaxInputChars)
		results[i].valid = true
		if strings.TrimSpace(text) == "" {
			results[i].empty = true
			continue
		}
		inputs = append(inputs, capRunes(text, q.opts.MaxInputChars))
		indexes = append(indexes, i)
	}
	if len(inputs) == 0 {
		return results, nil
	}
	batch, err := q.embedProvider.Embed(ctx, inputs)
	if err != nil {
		return nil, embeddingWorkerFailure(err)
	}
	dimensions, err := validateEmbeddingBatch(batch, len(inputs))
	if err != nil {
		return nil, &worker.Failure{Code: "invalid_embedding_response", Permanent: true}
	}
	for k, i := range indexes {
		blob, err := EncodeEmbeddingVector(batch.Vectors[k])
		if err != nil {
			return nil, &worker.Failure{Code: "invalid_embedding_vector", Permanent: true}
		}
		results[i].blob = blob
		results[i].dimensions = dimensions
	}
	return results, nil
}

func embeddingWorkerFailure(err error) error {
	if classified, ok := errors.AsType[*worker.Failure](err); ok {
		return classified
	}
	if h, ok := errors.AsType[*embed.HTTPError](err); ok {
		f := &worker.Failure{Code: "embedding_http_" + strconv.Itoa(h.StatusCode)}
		f.Pause = h.StatusCode == http.StatusUnauthorized || h.StatusCode == http.StatusForbidden || h.StatusCode == http.StatusTooManyRequests || h.StatusCode >= 500
		f.Permanent = !f.Pause
		if seconds, e := strconv.Atoi(h.Header.Get("Retry-After")); e == nil && seconds > 0 {
			f.RetryAfter = time.Duration(seconds) * time.Second
		} else if at, e := http.ParseTime(h.Header.Get("Retry-After")); e == nil {
			f.RetryAfter = time.Until(at)
		}
		if f.Pause && f.RetryAfter <= 0 {
			f.RetryAfter = 5 * time.Second
		}
		return f
	}
	return &worker.Failure{Code: "embedding_provider_unavailable", Pause: true, RetryAfter: 5 * time.Second}
}

func (q *embeddingWorkQueue) Complete(ctx context.Context, j worker.Job[string], r embeddingWorkResult, now time.Time) (bool, error) {
	tx, err := q.store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	n, err := tx.ExecContext(ctx, `update embedding_jobs set state='done',attempts=0,last_error='',locked_at=null,lease_token='',lease_until='',available_at='',updated_at=? where message_id=? and revision=? and lease_token=? and lease_until>? and state='pending'`, now.UTC().Format(timeLayout), j.Key, j.Revision, j.Token, now.UTC().Format(timeLayout))
	if err != nil {
		return false, err
	}
	affected, err := n.RowsAffected()
	if err != nil || affected == 0 {
		return false, err
	}
	var current string
	err = tx.QueryRowContext(ctx, `select normalized_content from messages where id=? and deleted_at is null and guild_id!='@me'`, j.Key).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err = tx.ExecContext(ctx, `delete from embedding_jobs where message_id=?`, j.Key); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `delete from message_embeddings where message_id=?`, j.Key); err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if !r.valid || capRunes(current, q.opts.MaxInputChars) != r.input {
		if _, err = tx.ExecContext(ctx, `update embedding_jobs set state='pending',revision=revision+1,priority=1,enqueued_at=? where message_id=?`, now.UTC().Format(timeLayout), j.Key); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `delete from message_embeddings where message_id=?`, j.Key); err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	if r.empty {
		_, err = tx.ExecContext(ctx, `delete from message_embeddings where message_id=?`, j.Key)
	} else {
		_, err = tx.ExecContext(ctx, `insert into message_embeddings(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at)values(?,?,?,?,?,?,?) on conflict(message_id,provider,model,input_version)do update set dimensions=excluded.dimensions,embedding_blob=excluded.embedding_blob,embedded_at=excluded.embedded_at`, j.Key, q.opts.Provider, q.opts.Model, q.opts.InputVersion, r.dimensions, r.blob, now.UTC().Format(timeLayout))
	}
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (q *embeddingWorkQueue) Retry(ctx context.Context, j worker.Job[string], r worker.Retry) (bool, error) {
	state := "pending"
	if r.Permanent {
		state = "failed"
	}
	inc := 0
	if r.CountAttempt {
		inc = 1
	}
	result, err := q.store.db.ExecContext(ctx, `update embedding_jobs set state=?,attempts=attempts+?,last_error=?,locked_at=null,lease_token='',lease_until='',available_at=?,updated_at=? where message_id=? and revision=? and lease_token=? and lease_until>? and state='pending'`, state, inc, r.Code, r.At.UTC().Format(timeLayout), r.Now.UTC().Format(timeLayout), j.Key, j.Revision, j.Token, r.Now.UTC().Format(timeLayout))
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

func (q *embeddingWorkQueue) Release(ctx context.Context, j worker.Job[string], now time.Time) (bool, error) {
	result, err := q.store.db.ExecContext(ctx, `update embedding_jobs set locked_at=null,lease_token='',lease_until='' where message_id=? and revision=? and lease_token=? and lease_until>? and state='pending'`, j.Key, j.Revision, j.Token, now.UTC().Format(timeLayout))
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

func (s *Store) setEmbeddingWake(wake func()) {
	s.embeddingWakeMu.Lock()
	defer s.embeddingWakeMu.Unlock()
	s.embeddingWake = wake
}

func (s *Store) notifyEmbeddingWork() {
	s.embeddingWakeMu.RLock()
	wake := s.embeddingWake
	s.embeddingWakeMu.RUnlock()
	if wake != nil {
		wake()
	}
}
