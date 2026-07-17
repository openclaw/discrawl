package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

const (
	defaultFailureLimit   = 100
	maxFailureLimit       = 1000
	maxFailureMessage     = 2048
	resolvedFailureMaxAge = 90 * 24 * time.Hour
)

var (
	failureURLPattern    = regexp.MustCompile(`(?i)\bhttps?://[^\s,;]+`)
	failureBearerPattern = regexp.MustCompile(`(?i)(bearer\s+)[^\s,;]+`)
	failureHeaderPattern = regexp.MustCompile(`(?i)(\b[a-z0-9_.-]*(?:token|key|secret|authorization)[a-z0-9_.-]*\s*:\s*)(?:(?:bearer|basic)\s+)?[^\s,;]+`)
	failureSecretPattern = regexp.MustCompile(`(?i)([?&]?[a-z0-9_.-]*(?:token|key|secret|authorization)[a-z0-9_.-]*=)[^&\s,;]+`)
	failureJSONPattern   = regexp.MustCompile(`(?i)("[a-z0-9_.-]*(?:token|key|secret|authorization)[a-z0-9_.-]*"\s*:\s*")[^"]+`)
)

type FailureRef struct {
	Operation   string
	Source      string
	GuildID     string
	ChannelID   string
	MessageID   string
	RelatedKind string
	RelatedID   string
}

type Failure struct {
	FailureID    int64     `json:"failure_id"`
	Operation    string    `json:"operation"`
	Source       string    `json:"source"`
	GuildID      string    `json:"guild_id,omitempty"`
	ChannelID    string    `json:"channel_id,omitempty"`
	MessageID    string    `json:"message_id,omitempty"`
	RelatedKind  string    `json:"related_kind,omitempty"`
	RelatedID    string    `json:"related_id,omitempty"`
	ErrorClass   string    `json:"error_class"`
	ErrorMessage string    `json:"error_message"`
	FirstSeenAt  time.Time `json:"first_seen_at"`
	LastSeenAt   time.Time `json:"last_seen_at"`
	RetryCount   int       `json:"retry_count"`
	ResolvedAt   time.Time `json:"resolved_at,omitzero"`
}

type FailureListOptions struct {
	IncludeResolved bool
	Source          string
	GuildID         string
	ChannelID       string
	Limit           int
}

type FailureReport struct {
	GeneratedAt     time.Time `json:"generated_at"`
	UnresolvedCount int       `json:"unresolved_count"`
	Failures        []Failure `json:"failures"`
}

// FailurePersistenceDiagnostics contains timing and SQLite result codes without
// retaining the database error text.
type FailurePersistenceDiagnostics struct {
	PoolWait        time.Duration
	DBElapsed       time.Duration
	ContextDeadline time.Time
	SQLiteCode      int
	SQLiteCategory  int
}

// RowWriteError carries identifiers without retaining message content, URLs, or raw payloads.
type RowWriteError struct {
	Ref         FailureRef
	AuthorID    string
	Filename    string
	ContentType string
	Size        int64
	Err         error
}

func (e *RowWriteError) Error() string {
	if e == nil {
		return "row write failed"
	}
	return fmt.Sprintf(
		"%s failed: guild_id=%q channel_id=%q message_id=%q %s=%q author_id=%q filename=%q content_type=%q size=%d: %v",
		firstNonEmpty(e.Ref.Operation, "row write"), e.Ref.GuildID, e.Ref.ChannelID, e.Ref.MessageID,
		firstNonEmpty(e.Ref.RelatedKind, "related_id"), e.Ref.RelatedID, e.AuthorID, e.Filename, e.ContentType, e.Size, e.Err,
	)
}

func (e *RowWriteError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func FailureRefFromError(err error) FailureRef {
	var rowErr *RowWriteError
	if errors.As(err, &rowErr) && rowErr != nil {
		return rowErr.Ref
	}
	return FailureRef{}
}

func (s *Store) RecordFailure(ctx context.Context, ref FailureRef, failure error) error {
	if failure == nil {
		return s.ResolveFailures(ctx, ref)
	}
	now := time.Now().UTC()
	if err := recordFailure(ctx, s.db, ref, failure, now); err != nil {
		return err
	}
	return s.pruneResolvedFailures(ctx, now)
}

// RecordFailureWithMessageScope atomically validates an existing message's scope
// and records the failure under either that scope or the complete supplied scope.
func (s *Store) RecordFailureWithMessageScope(
	ctx context.Context,
	ref FailureRef,
	failure error,
) error {
	_, err := s.RecordFailureWithMessageScopeTimed(ctx, ref, failure)
	return err
}

// RecordFailureWithMessageScopeTimed is RecordFailureWithMessageScope with
// pool, transaction, deadline, and numeric SQLite diagnostics for safe logging.
func (s *Store) RecordFailureWithMessageScopeTimed(
	ctx context.Context,
	ref FailureRef,
	failure error,
) (diagnostics FailurePersistenceDiagnostics, returnErr error) {
	if deadline, ok := ctx.Deadline(); ok {
		diagnostics.ContextDeadline = deadline
	}
	defer func() {
		diagnostics.SQLiteCode, diagnostics.SQLiteCategory = sqliteFailureCodes(returnErr)
	}()
	if failure == nil {
		return diagnostics, errors.New("message-scoped failure is required")
	}
	ref = normalizeFailureRef(mergeFailureRef(ref, FailureRefFromError(failure)))
	if ref.Operation == "" || ref.Source == "" {
		return diagnostics, errors.New("failure operation and source are required")
	}
	if ref.MessageID == "" {
		return diagnostics, errors.New("message id is required")
	}

	poolStartedAt := time.Now()
	conn, err := s.db.Conn(ctx)
	diagnostics.PoolWait = time.Since(poolStartedAt)
	if err != nil {
		return diagnostics, fmt.Errorf("acquire message-scoped failure connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	dbStartedAt := time.Now()
	defer func() {
		diagnostics.DBElapsed = time.Since(dbStartedAt)
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return diagnostics, fmt.Errorf("begin message-scoped failure transaction: %w", err)
	}
	defer rollback(tx)

	var storedGuildID string
	var storedChannelID string
	err = tx.QueryRowContext(
		ctx,
		`select guild_id, channel_id from messages where id = ?`,
		ref.MessageID,
	).Scan(&storedGuildID, &storedChannelID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return diagnostics, fmt.Errorf("look up message failure scope: %w", err)
	default:
		if ref.ChannelID == "" {
			ref.ChannelID = storedChannelID
		} else if storedChannelID != "" && ref.ChannelID != storedChannelID {
			return diagnostics, fmt.Errorf(
				"message channel mismatch: event=%s stored=%s",
				ref.ChannelID,
				storedChannelID,
			)
		}
		if ref.GuildID == "" {
			ref.GuildID = storedGuildID
		} else if storedGuildID != "" && ref.GuildID != storedGuildID {
			return diagnostics, fmt.Errorf(
				"message guild mismatch: event=%s stored=%s",
				ref.GuildID,
				storedGuildID,
			)
		}
	}
	if ref.GuildID == "" || ref.ChannelID == "" {
		return diagnostics, fmt.Errorf(
			"message identity is incomplete: guild_id=%q channel_id=%q message_id=%q",
			ref.GuildID,
			ref.ChannelID,
			ref.MessageID,
		)
	}

	now := time.Now().UTC()
	if err := recordFailure(ctx, tx, ref, failure, now); err != nil {
		return diagnostics, err
	}
	if err := pruneResolvedFailures(ctx, tx, now); err != nil {
		return diagnostics, err
	}
	if err := tx.Commit(); err != nil {
		return diagnostics, fmt.Errorf("commit message-scoped failure transaction: %w", err)
	}
	return diagnostics, nil
}

type failureExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type sqliteErrorCoder interface {
	Code() int
}

func sqliteFailureCodes(err error) (code, category int) {
	var coder sqliteErrorCoder
	if err == nil || !errors.As(err, &coder) {
		return 0, 0
	}
	code = coder.Code()
	return code, code & 0xff
}

func recordFailure(
	ctx context.Context,
	execer failureExecer,
	ref FailureRef,
	failure error,
	now time.Time,
) error {
	ref = normalizeFailureRef(mergeFailureRef(ref, FailureRefFromError(failure)))
	if ref.Operation == "" || ref.Source == "" {
		return errors.New("failure operation and source are required")
	}
	if _, err := execer.ExecContext(ctx, `
		insert into failure_ledger(
			operation, source, guild_id, channel_id, message_id, related_kind, related_id,
			error_class, error_message, first_seen_at, last_seen_at, retry_count, resolved_at
		) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, null)
		on conflict(operation, source, guild_id, channel_id, message_id, related_kind, related_id)
		do update set
			error_class = excluded.error_class,
			error_message = excluded.error_message,
			last_seen_at = excluded.last_seen_at,
			retry_count = failure_ledger.retry_count + 1,
			resolved_at = null
	`, ref.Operation, ref.Source, ref.GuildID, ref.ChannelID, ref.MessageID, ref.RelatedKind, ref.RelatedID,
		failureClass(failure), sanitizeFailureMessage(failure.Error()), now.Format(timeLayout), now.Format(timeLayout)); err != nil {
		return fmt.Errorf("record %s/%s failure: %w", ref.Source, ref.Operation, err)
	}
	return nil
}

// ResolveFailures resolves every unresolved row matching the non-empty identifiers in ref.
func (s *Store) ResolveFailures(ctx context.Context, ref FailureRef) error {
	ref = normalizeFailureRef(ref)
	if ref.Operation == "" || ref.Source == "" {
		return errors.New("failure operation and source are required")
	}
	now := time.Now().UTC()
	clauses := []string{"resolved_at is null", "operation = ?", "source = ?"}
	args := []any{now.Format(timeLayout), ref.Operation, ref.Source}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"guild_id", ref.GuildID},
		{"channel_id", ref.ChannelID},
		{"message_id", ref.MessageID},
		{"related_kind", ref.RelatedKind},
		{"related_id", ref.RelatedID},
	} {
		if field.value != "" {
			clauses = append(clauses, field.name+" = ?")
			args = append(args, field.value)
		}
	}
	query := `update failure_ledger set resolved_at = ? where ` + strings.Join(clauses, " and ")
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("resolve %s/%s failures: %w", ref.Source, ref.Operation, err)
	}
	return s.pruneResolvedFailures(ctx, now)
}

// ResolveFailureIdentity resolves only the exact failure identity, including empty scope fields.
func (s *Store) ResolveFailureIdentity(ctx context.Context, ref FailureRef) error {
	ref = normalizeFailureRef(ref)
	if ref.Operation == "" || ref.Source == "" {
		return errors.New("failure operation and source are required")
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		update failure_ledger set resolved_at = ?
		where resolved_at is null and operation = ? and source = ?
		  and guild_id = ? and channel_id = ? and message_id = ?
		  and related_kind = ? and related_id = ?
	`, now.Format(timeLayout), ref.Operation, ref.Source, ref.GuildID, ref.ChannelID, ref.MessageID, ref.RelatedKind, ref.RelatedID); err != nil {
		return fmt.Errorf("resolve exact %s/%s failure: %w", ref.Source, ref.Operation, err)
	}
	return s.pruneResolvedFailures(ctx, now)
}

func (s *Store) ResolveMessageFailures(ctx context.Context, ref FailureRef, messageIDs []string) error {
	ref = normalizeFailureRef(ref)
	if ref.Operation == "" || ref.Source == "" {
		return errors.New("failure operation and source are required")
	}
	seen := map[string]struct{}{}
	ids := make([]string, 0, len(messageIDs))
	for _, id := range messageIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil
	}
	now := time.Now().UTC()
	clauses := []string{"resolved_at is null", "operation = ?", "source = ?"}
	baseArgs := []any{now.Format(timeLayout), ref.Operation, ref.Source}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"guild_id", ref.GuildID},
		{"channel_id", ref.ChannelID},
		{"related_kind", ref.RelatedKind},
		{"related_id", ref.RelatedID},
	} {
		if field.value != "" {
			clauses = append(clauses, field.name+" = ?")
			baseArgs = append(baseArgs, field.value)
		}
	}
	for start := 0; start < len(ids); start += 500 {
		batch := ids[start:min(start+500, len(ids))]
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(batch)), ",")
		query := `update failure_ledger set resolved_at = ? where ` + strings.Join(clauses, " and ") + ` and message_id in (` + placeholders + `)`
		args := append([]any(nil), baseArgs...)
		for _, id := range batch {
			args = append(args, id)
		}
		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("resolve %s/%s message failures: %w", ref.Source, ref.Operation, err)
		}
	}
	return s.pruneResolvedFailures(ctx, now)
}

func (s *Store) ListFailures(ctx context.Context, opts FailureListOptions, generatedAt time.Time) (FailureReport, error) {
	opts.Source = strings.TrimSpace(opts.Source)
	opts.GuildID = strings.TrimSpace(opts.GuildID)
	opts.ChannelID = strings.TrimSpace(opts.ChannelID)
	if opts.Limit <= 0 {
		opts.Limit = defaultFailureLimit
	}
	if opts.Limit > maxFailureLimit {
		return FailureReport{}, fmt.Errorf("failure limit must be at most %d", maxFailureLimit)
	}
	where, args := failureWhere(opts)
	report := FailureReport{GeneratedAt: generatedAt.UTC(), Failures: []Failure{}}
	countOpts := opts
	countOpts.IncludeResolved = false
	countWhere, countArgs := failureWhere(countOpts)
	if err := s.db.QueryRowContext(ctx, `select count(*) from failure_ledger `+countWhere, countArgs...).Scan(&report.UnresolvedCount); err != nil {
		return FailureReport{}, fmt.Errorf("count unresolved failures: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `
		select failure_id, operation, source, guild_id, channel_id, message_id,
		       related_kind, related_id, error_class, error_message,
		       first_seen_at, last_seen_at, retry_count, coalesce(resolved_at, '')
		from failure_ledger
		`+where+`
		order by (resolved_at is null) desc, last_seen_at desc, failure_id desc
		limit ?`, append(args, opts.Limit)...)
	if err != nil {
		return FailureReport{}, fmt.Errorf("list failures: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var row Failure
		var firstSeen, lastSeen, resolved string
		if err := rows.Scan(
			&row.FailureID, &row.Operation, &row.Source, &row.GuildID, &row.ChannelID, &row.MessageID,
			&row.RelatedKind, &row.RelatedID, &row.ErrorClass, &row.ErrorMessage,
			&firstSeen, &lastSeen, &row.RetryCount, &resolved,
		); err != nil {
			return FailureReport{}, fmt.Errorf("scan failure: %w", err)
		}
		row.FirstSeenAt = parseTime(firstSeen)
		row.LastSeenAt = parseTime(lastSeen)
		row.ResolvedAt = parseTime(resolved)
		report.Failures = append(report.Failures, row)
	}
	if err := rows.Err(); err != nil {
		return FailureReport{}, fmt.Errorf("list failures: %w", err)
	}
	return report, nil
}

// ListFailureReplayCandidates returns unresolved failures in least-recently-attempted order.
func (s *Store) ListFailureReplayCandidates(
	ctx context.Context,
	ref FailureRef,
	guildIDs []string,
	limit int,
) ([]Failure, error) {
	return s.listFailureReplayCandidates(ctx, ref, guildIDs, nil, limit)
}

// ListFailureReplayCandidatesMatchingRelatedIDs returns unresolved failures
// whose related ID matches one of the allowed values.
func (s *Store) ListFailureReplayCandidatesMatchingRelatedIDs(
	ctx context.Context,
	ref FailureRef,
	guildIDs []string,
	relatedIDs []string,
	limit int,
) ([]Failure, error) {
	if strings.TrimSpace(ref.RelatedID) != "" {
		return nil, errors.New("failure related ID and related ID list cannot both be set")
	}
	normalizedIDs := make([]string, 0, len(relatedIDs))
	seen := make(map[string]struct{}, len(relatedIDs))
	for _, relatedID := range relatedIDs {
		relatedID = strings.TrimSpace(relatedID)
		if relatedID == "" {
			continue
		}
		if _, ok := seen[relatedID]; ok {
			continue
		}
		seen[relatedID] = struct{}{}
		normalizedIDs = append(normalizedIDs, relatedID)
	}
	if len(normalizedIDs) == 0 {
		return nil, errors.New("at least one failure related ID is required")
	}
	return s.listFailureReplayCandidates(ctx, ref, guildIDs, normalizedIDs, limit)
}

func (s *Store) listFailureReplayCandidates(
	ctx context.Context,
	ref FailureRef,
	guildIDs []string,
	relatedIDs []string,
	limit int,
) ([]Failure, error) {
	ref = normalizeFailureRef(ref)
	if ref.Operation == "" || ref.Source == "" {
		return nil, errors.New("failure operation and source are required")
	}
	if limit <= 0 {
		limit = defaultFailureLimit
	}
	if limit > maxFailureLimit {
		return nil, fmt.Errorf("failure replay limit must be at most %d", maxFailureLimit)
	}
	clauses := []string{"resolved_at is null", "operation = ?", "source = ?"}
	args := []any{ref.Operation, ref.Source}
	if len(guildIDs) > 0 {
		clauses = append(clauses, "guild_id in ("+placeholders(len(guildIDs))+")")
		for _, guildID := range guildIDs {
			args = append(args, guildID)
		}
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"guild_id", ref.GuildID},
		{"channel_id", ref.ChannelID},
		{"message_id", ref.MessageID},
		{"related_kind", ref.RelatedKind},
		{"related_id", ref.RelatedID},
	} {
		if field.value != "" {
			clauses = append(clauses, field.name+" = ?")
			args = append(args, field.value)
		}
	}
	if len(relatedIDs) > 0 {
		clauses = append(clauses, "related_id in ("+placeholders(len(relatedIDs))+")")
		for _, relatedID := range relatedIDs {
			args = append(args, relatedID)
		}
	}
	rows, err := s.db.QueryContext(ctx, `
		select failure_id, operation, source, guild_id, channel_id, message_id,
		       related_kind, related_id, error_class, error_message,
		       first_seen_at, last_seen_at, retry_count, coalesce(resolved_at, '')
		from failure_ledger
		where `+strings.Join(clauses, " and ")+`
		order by last_seen_at asc, failure_id asc
		limit ?`, append(args, limit)...)
	if err != nil {
		return nil, fmt.Errorf("list failure replay candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	failures := []Failure{}
	for rows.Next() {
		var row Failure
		var firstSeen, lastSeen, resolved string
		if err := rows.Scan(
			&row.FailureID, &row.Operation, &row.Source, &row.GuildID, &row.ChannelID, &row.MessageID,
			&row.RelatedKind, &row.RelatedID, &row.ErrorClass, &row.ErrorMessage,
			&firstSeen, &lastSeen, &row.RetryCount, &resolved,
		); err != nil {
			return nil, fmt.Errorf("scan failure replay candidate: %w", err)
		}
		row.FirstSeenAt = parseTime(firstSeen)
		row.LastSeenAt = parseTime(lastSeen)
		row.ResolvedAt = parseTime(resolved)
		failures = append(failures, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list failure replay candidates: %w", err)
	}
	return failures, nil
}

func (s *Store) pruneResolvedFailures(ctx context.Context, now time.Time) error {
	return pruneResolvedFailures(ctx, s.db, now)
}

func pruneResolvedFailures(ctx context.Context, execer failureExecer, now time.Time) error {
	cutoff := now.Add(-resolvedFailureMaxAge).Format(timeLayout)
	if _, err := execer.ExecContext(ctx, `delete from failure_ledger where resolved_at is not null and resolved_at < ?`, cutoff); err != nil {
		return fmt.Errorf("prune resolved failures: %w", err)
	}
	return nil
}

func failureWhere(opts FailureListOptions) (string, []any) {
	clauses := []string{"1 = 1"}
	args := []any{}
	if !opts.IncludeResolved {
		clauses = append(clauses, "resolved_at is null")
	}
	if opts.Source != "" {
		clauses = append(clauses, "source = ?")
		args = append(args, opts.Source)
	}
	if opts.GuildID != "" {
		clauses = append(clauses, "guild_id = ?")
		args = append(args, opts.GuildID)
	}
	if opts.ChannelID != "" {
		clauses = append(clauses, "channel_id = ?")
		args = append(args, opts.ChannelID)
	}
	return "where " + strings.Join(clauses, " and "), args
}

func mergeFailureRef(base, contextual FailureRef) FailureRef {
	if base.Operation == "" {
		base.Operation = contextual.Operation
	}
	if base.Source == "" {
		base.Source = contextual.Source
	}
	if base.GuildID == "" {
		base.GuildID = contextual.GuildID
	}
	if base.ChannelID == "" {
		base.ChannelID = contextual.ChannelID
	}
	if base.MessageID == "" {
		base.MessageID = contextual.MessageID
	}
	if base.RelatedKind == "" {
		base.RelatedKind = contextual.RelatedKind
	}
	if base.RelatedID == "" {
		base.RelatedID = contextual.RelatedID
	}
	return base
}

func normalizeFailureRef(ref FailureRef) FailureRef {
	ref.Operation = strings.TrimSpace(ref.Operation)
	ref.Source = strings.TrimSpace(ref.Source)
	ref.GuildID = strings.TrimSpace(ref.GuildID)
	ref.ChannelID = strings.TrimSpace(ref.ChannelID)
	ref.MessageID = strings.TrimSpace(ref.MessageID)
	ref.RelatedKind = strings.TrimSpace(ref.RelatedKind)
	ref.RelatedID = strings.TrimSpace(ref.RelatedID)
	return ref
}

func failureClass(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	}
	root := err
	for {
		unwrapped := errors.Unwrap(root)
		if unwrapped == nil {
			break
		}
		root = unwrapped
	}
	typ := reflect.TypeOf(root)
	if typ == nil {
		return "error"
	}
	name := strings.TrimPrefix(typ.String(), "*")
	if strings.Contains(strings.ToLower(name), "sqlite") {
		return "sqlite"
	}
	return name
}

func sanitizeFailureMessage(message string) string {
	message = strings.ToValidUTF8(message, "")
	message = strings.Join(strings.Fields(message), " ")
	message = failureURLPattern.ReplaceAllString(message, "[redacted-url]")
	message = failureHeaderPattern.ReplaceAllString(message, `${1}[redacted]`)
	message = failureBearerPattern.ReplaceAllString(message, `${1}[redacted]`)
	message = failureSecretPattern.ReplaceAllString(message, `${1}[redacted]`)
	message = failureJSONPattern.ReplaceAllString(message, `${1}[redacted]`)
	runes := []rune(message)
	if len(runes) > maxFailureMessage {
		message = string(runes[:maxFailureMessage])
	}
	return message
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
