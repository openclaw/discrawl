package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/embed"
	"github.com/openclaw/crawlkit/vector"
	"github.com/openclaw/discrawl/internal/store/storedb"
)

const (
	EmbeddingInputVersion = "message_normalized_v1"
	defaultEmbedLimit     = 1000
	maxEmbeddingAttempts  = 3
	maxStoredErrorChars   = 500
	embeddingLockTimeout  = 15 * time.Minute
)

type EmbeddingDrainOptions struct {
	Provider      string
	Model         string
	InputVersion  string
	Limit         int
	BatchSize     int
	MaxInputChars int
	Now           func() time.Time
}

type EmbeddingDrainStats struct {
	Processed        int    `json:"processed"`
	Succeeded        int    `json:"succeeded"`
	Failed           int    `json:"failed"`
	Skipped          int    `json:"skipped"`
	Requeued         int    `json:"requeued,omitempty"`
	RemainingBacklog int    `json:"remaining_backlog"`
	Provider         string `json:"provider"`
	Model            string `json:"model"`
	InputVersion     string `json:"input_version"`
	RateLimited      bool   `json:"rate_limited,omitempty"`
}

type embeddingJob struct {
	MessageID         string
	NormalizedContent string
	Attempts          int
	Provider          string
	Model             string
	InputVersion      string
}

func DefaultEmbedLimit() int {
	return defaultEmbedLimit
}

func (s *Store) DrainEmbeddingJobs(ctx context.Context, provider embed.Provider, opts EmbeddingDrainOptions) (EmbeddingDrainStats, error) {
	opts = normalizeEmbeddingDrainOptions(opts)
	stats := EmbeddingDrainStats{
		Provider:     opts.Provider,
		Model:        opts.Model,
		InputVersion: opts.InputVersion,
	}
	if provider == nil {
		return stats, errors.New("embedding provider is nil")
	}
	now := opts.Now()
	staleBefore := now.Add(-embeddingLockTimeout).Format(timeLayout)
	jobs, err := s.pendingEmbeddingJobs(ctx, opts.Limit, staleBefore)
	if err != nil {
		return stats, err
	}
	var batch []embeddingJob
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		rateLimited, err := s.processEmbeddingBatch(ctx, provider, opts, batch, &stats)
		batch = batch[:0]
		if err != nil {
			return err
		}
		if rateLimited {
			stats.RateLimited = true
		}
		return nil
	}
	for _, job := range jobs {
		if !sameEmbeddingIdentity(job, opts) {
			resetAttempts := !emptyEmbeddingIdentity(job)
			if err := s.resetEmbeddingJobIdentity(ctx, job.MessageID, opts, resetAttempts); err != nil {
				return stats, err
			}
			job.Provider = opts.Provider
			job.Model = opts.Model
			job.InputVersion = opts.InputVersion
			if resetAttempts {
				job.Attempts = 0
			}
		}
		if strings.TrimSpace(job.NormalizedContent) == "" {
			if err := s.markEmbeddingJobsDone(ctx, opts, []embeddingJob{job}); err != nil {
				return stats, err
			}
			if err := s.ResolveMessageFailures(ctx, FailureRef{Operation: "embed_message", Source: "embeddings"}, []string{job.MessageID}); err != nil {
				return stats, err
			}
			stats.Processed++
			stats.Skipped++
			continue
		}
		batch = append(batch, job)
		if len(batch) >= opts.BatchSize {
			if err := flush(); err != nil {
				return stats, err
			}
			if stats.RateLimited {
				break
			}
		}
	}
	if !stats.RateLimited {
		if err := flush(); err != nil {
			return stats, err
		}
	}
	stats.RemainingBacklog, err = s.EmbeddingBacklog(ctx)
	if err != nil {
		return stats, err
	}
	return stats, nil
}

func normalizeEmbeddingDrainOptions(opts EmbeddingDrainOptions) EmbeddingDrainOptions {
	opts.Provider = strings.ToLower(strings.TrimSpace(opts.Provider))
	opts.Model = strings.TrimSpace(opts.Model)
	opts.InputVersion = strings.TrimSpace(opts.InputVersion)
	if opts.InputVersion == "" {
		opts.InputVersion = EmbeddingInputVersion
	}
	if opts.Limit <= 0 {
		opts.Limit = defaultEmbedLimit
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = embed.DefaultBatchSize
	}
	if opts.BatchSize > opts.Limit {
		opts.BatchSize = opts.Limit
	}
	if opts.MaxInputChars <= 0 {
		opts.MaxInputChars = embed.DefaultMaxInputChars
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	return opts
}

func sameEmbeddingIdentity(job embeddingJob, opts EmbeddingDrainOptions) bool {
	return job.Provider == opts.Provider && job.Model == opts.Model && job.InputVersion == opts.InputVersion
}

func emptyEmbeddingIdentity(job embeddingJob) bool {
	return job.Provider == "" && job.Model == "" && job.InputVersion == ""
}

func (s *Store) pendingEmbeddingJobs(ctx context.Context, limit int, staleBefore string) ([]embeddingJob, error) {
	rows, err := s.q.ListPendingEmbeddingJobs(ctx, storedb.ListPendingEmbeddingJobsParams{
		LockedAt: nullString(staleBefore),
		Limit:    int64(limit),
	})
	if err != nil {
		return nil, err
	}
	jobs := make([]embeddingJob, 0, len(rows))
	for _, row := range rows {
		jobs = append(jobs, embeddingJob{
			MessageID:         row.MessageID,
			NormalizedContent: row.NormalizedContent,
			Attempts:          int(row.Attempts),
			Provider:          row.Provider,
			Model:             row.Model,
			InputVersion:      row.InputVersion,
		})
	}
	return jobs, nil
}

func (s *Store) resetEmbeddingJobIdentity(ctx context.Context, messageID string, opts EmbeddingDrainOptions, resetAttempts bool) error {
	arg := storedb.ResetEmbeddingJobIdentityParams{
		Provider:     opts.Provider,
		Model:        opts.Model,
		InputVersion: opts.InputVersion,
		UpdatedAt:    opts.Now().Format(timeLayout),
		MessageID:    messageID,
	}
	if resetAttempts {
		return s.q.ResetEmbeddingJobIdentityAndAttempts(ctx, storedb.ResetEmbeddingJobIdentityAndAttemptsParams(arg))
	}
	return s.q.ResetEmbeddingJobIdentity(ctx, arg)
}

func (s *Store) processEmbeddingBatch(ctx context.Context, provider embed.Provider, opts EmbeddingDrainOptions, jobs []embeddingJob, stats *EmbeddingDrainStats) (bool, error) {
	now := opts.Now()
	lockedAt := now.Format(timeLayout)
	staleBefore := now.Add(-embeddingLockTimeout).Format(timeLayout)
	claimed, err := s.lockEmbeddingJobs(ctx, jobs, lockedAt, staleBefore)
	if err != nil {
		return false, err
	}
	if len(claimed) == 0 {
		return false, nil
	}
	jobs = claimed
	inputs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		inputs = append(inputs, capRunes(job.NormalizedContent, opts.MaxInputChars))
	}
	batch, err := provider.Embed(ctx, inputs)
	if err != nil {
		if embed.IsRateLimitError(err) {
			if markErr := s.markEmbeddingJobsRateLimited(ctx, opts, jobs, err); markErr != nil {
				return false, markErr
			}
			if recordErr := s.recordEmbeddingFailures(ctx, opts, jobs, err); recordErr != nil {
				return false, recordErr
			}
			stats.Requeued += len(jobs)
			return true, nil
		}
		if markErr := s.markEmbeddingJobsFailed(ctx, opts, jobs, err); markErr != nil {
			return false, markErr
		}
		if recordErr := s.recordEmbeddingFailures(ctx, opts, jobs, err); recordErr != nil {
			return false, recordErr
		}
		stats.Processed += len(jobs)
		stats.Failed += len(jobs)
		return embed.IsRateLimitError(err), nil
	}
	dimensions, err := validateEmbeddingBatch(batch, len(jobs))
	if err != nil {
		if markErr := s.markEmbeddingJobsFailed(ctx, opts, jobs, err); markErr != nil {
			return false, markErr
		}
		if recordErr := s.recordEmbeddingFailures(ctx, opts, jobs, err); recordErr != nil {
			return false, recordErr
		}
		stats.Processed += len(jobs)
		stats.Failed += len(jobs)
		return false, nil
	}
	if err := s.storeEmbeddingBatch(ctx, opts, jobs, batch.Vectors, dimensions); err != nil {
		return false, withEmbeddingFailureRecord(err, s.recordEmbeddingFailures(ctx, opts, jobs, err))
	}
	messageIDs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		messageIDs = append(messageIDs, job.MessageID)
	}
	if err := s.ResolveMessageFailures(ctx, embeddingFailureRef(opts, ""), messageIDs); err != nil {
		return false, err
	}
	stats.Processed += len(jobs)
	stats.Succeeded += len(jobs)
	return false, nil
}

func (s *Store) recordEmbeddingFailures(ctx context.Context, opts EmbeddingDrainOptions, jobs []embeddingJob, failure error) error {
	for _, job := range jobs {
		if err := s.RecordFailure(ctx, embeddingFailureRef(opts, job.MessageID), failure); err != nil {
			return err
		}
	}
	return nil
}

func embeddingFailureRef(opts EmbeddingDrainOptions, messageID string) FailureRef {
	return FailureRef{
		Operation:   "embed_message",
		Source:      "embeddings",
		MessageID:   messageID,
		RelatedKind: "embedding_identity",
		RelatedID:   strings.Join([]string{opts.Provider, opts.Model, opts.InputVersion}, "/"),
	}
}

func withEmbeddingFailureRecord(failure, recordErr error) error {
	if recordErr == nil {
		return failure
	}
	return fmt.Errorf("%w (record failure ledger: %w)", failure, recordErr)
}

func (s *Store) lockEmbeddingJobs(ctx context.Context, jobs []embeddingJob, lockedAt, staleBefore string) ([]embeddingJob, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	claimed := make([]embeddingJob, 0, len(jobs))
	for _, job := range jobs {
		rows, err := qtx.LockEmbeddingJob(ctx, storedb.LockEmbeddingJobParams{
			LockedAt:    nullString(lockedAt),
			UpdatedAt:   lockedAt,
			MessageID:   job.MessageID,
			StaleBefore: nullString(staleBefore),
		})
		if err != nil {
			return nil, err
		}
		if rows == 1 {
			claimed = append(claimed, job)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func validateEmbeddingBatch(batch embed.EmbeddingBatch, expected int) (int, error) {
	if len(batch.Vectors) != expected {
		return 0, fmt.Errorf("embedding provider returned %d vectors for %d inputs", len(batch.Vectors), expected)
	}
	dimensions := batch.Dimensions
	for _, vector := range batch.Vectors {
		if len(vector) == 0 {
			return 0, errors.New("embedding provider returned an empty vector")
		}
		if dimensions == 0 {
			dimensions = len(vector)
			continue
		}
		if len(vector) != dimensions {
			return 0, fmt.Errorf("embedding provider dimensions mismatch: got %d want %d", len(vector), dimensions)
		}
	}
	return dimensions, nil
}

func (s *Store) storeEmbeddingBatch(ctx context.Context, opts EmbeddingDrainOptions, jobs []embeddingJob, vectors [][]float32, dimensions int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	embeddedAt := opts.Now().Format(timeLayout)
	for i, job := range jobs {
		blob, err := EncodeEmbeddingVector(vectors[i])
		if err != nil {
			return err
		}
		if err := qtx.UpsertMessageEmbedding(ctx, storedb.UpsertMessageEmbeddingParams{
			MessageID:     job.MessageID,
			Provider:      opts.Provider,
			Model:         opts.Model,
			InputVersion:  opts.InputVersion,
			Dimensions:    int64(dimensions),
			EmbeddingBlob: blob,
			EmbeddedAt:    embeddedAt,
		}); err != nil {
			return err
		}
		if err := qtx.MarkEmbeddingJobDone(ctx, storedb.MarkEmbeddingJobDoneParams{
			Provider:     opts.Provider,
			Model:        opts.Model,
			InputVersion: opts.InputVersion,
			UpdatedAt:    embeddedAt,
			MessageID:    job.MessageID,
		}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) markEmbeddingJobsDone(ctx context.Context, opts EmbeddingDrainOptions, jobs []embeddingJob) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	now := opts.Now().Format(timeLayout)
	for _, job := range jobs {
		if err := qtx.DeleteMessageEmbeddingsByMessage(ctx, job.MessageID); err != nil {
			return err
		}
		if err := qtx.MarkEmptyEmbeddingJobDone(ctx, storedb.MarkEmptyEmbeddingJobDoneParams{
			Provider:     opts.Provider,
			Model:        opts.Model,
			InputVersion: opts.InputVersion,
			UpdatedAt:    now,
			MessageID:    job.MessageID,
		}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) markEmbeddingJobsRateLimited(ctx context.Context, opts EmbeddingDrainOptions, jobs []embeddingJob, cause error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	now := opts.Now().Format(timeLayout)
	lastError := trimStoredError(cause)
	for _, job := range jobs {
		if err := qtx.MarkEmbeddingJobRateLimited(ctx, storedb.MarkEmbeddingJobRateLimitedParams{
			Provider:     opts.Provider,
			Model:        opts.Model,
			InputVersion: opts.InputVersion,
			LastError:    lastError,
			UpdatedAt:    now,
			MessageID:    job.MessageID,
		}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) markEmbeddingJobsFailed(ctx context.Context, opts EmbeddingDrainOptions, jobs []embeddingJob, cause error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	now := opts.Now().Format(timeLayout)
	lastError := trimStoredError(cause)
	for _, job := range jobs {
		attempts := job.Attempts + 1
		state := "pending"
		if attempts >= maxEmbeddingAttempts {
			state = "failed"
		}
		if err := qtx.MarkEmbeddingJobFailed(ctx, storedb.MarkEmbeddingJobFailedParams{
			State:        state,
			Attempts:     int64(attempts),
			Provider:     opts.Provider,
			Model:        opts.Model,
			InputVersion: opts.InputVersion,
			LastError:    lastError,
			UpdatedAt:    now,
			MessageID:    job.MessageID,
		}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func trimStoredError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	runes := []rune(msg)
	if len(runes) > maxStoredErrorChars {
		msg = string(runes[:maxStoredErrorChars])
	}
	return msg
}

func capRunes(value string, maxChars int) string {
	if maxChars <= 0 {
		return value
	}
	runes := []rune(value)
	if len(runes) <= maxChars {
		return value
	}
	return string(runes[:maxChars])
}

func EncodeEmbeddingVector(values []float32) ([]byte, error) {
	blob, err := vector.EncodeFloat32(values)
	if err != nil {
		return nil, fmt.Errorf("encode embedding vector: %w", err)
	}
	return blob, nil
}

func DecodeEmbeddingVector(blob []byte) ([]float32, error) {
	if len(blob)%4 != 0 {
		return nil, fmt.Errorf("embedding blob length %d is not a float32 multiple", len(blob))
	}
	values, err := vector.DecodeFloat32(blob)
	if err != nil {
		return nil, fmt.Errorf("decode embedding vector: %w", err)
	}
	return values, nil
}

func (s *Store) EmbeddingBacklog(ctx context.Context) (int, error) {
	count, err := s.q.CountEmbeddingBacklog(ctx)
	return int(count), err
}

func (s *Store) RequeueAllEmbeddingJobs(ctx context.Context, opts EmbeddingDrainOptions) (int, error) {
	opts = normalizeEmbeddingDrainOptions(opts)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	now := opts.Now().Format(timeLayout)
	if err := qtx.InsertMissingEmbeddingJobs(ctx, storedb.InsertMissingEmbeddingJobsParams{
		Provider:     opts.Provider,
		Model:        opts.Model,
		InputVersion: opts.InputVersion,
		UpdatedAt:    now,
	}); err != nil {
		return 0, err
	}
	affected, err := qtx.RequeueAllEmbeddingJobs(ctx, storedb.RequeueAllEmbeddingJobsParams{
		Provider:     opts.Provider,
		Model:        opts.Model,
		InputVersion: opts.InputVersion,
		UpdatedAt:    now,
	})
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int(affected), nil
}
