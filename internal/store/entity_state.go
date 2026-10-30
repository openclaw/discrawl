package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"math"
	"slices"
	"time"
)

// Check log and metric series.
//
// Content rows (guilds, channels, members, messages, message_attachments)
// change only when their content changes; updated_at then means "content last
// changed". Every accepted observation, changed or not, is recorded in
// entity_checks instead, and live counters go to entity_metrics as a
// value-change (run-length) series. Both tables are local archive state and
// are additive: older binaries ignore them, so the schema version is not
// bumped and a rollback keeps working.
//
// entity_checks holds the latest check per entity and kind:
//
//	table_name    content table of the checked row
//	row_key       JSON array of the row's primary-key values (change_log encoding)
//	check_kind    'observed' (a provider observation of the row) or
//	              'attachment_text' (an attachment text extraction attempt)
//	checked_at    time of the latest check (UTC, fixed nanosecond layout)
//	succeeded_at  latest successful check; for 'attachment_text' the latest
//	              successful extraction, for 'observed' equal to checked_at
//	outcome       'changed' or 'unchanged' for 'observed'; the text status for
//	              'attachment_text'
//
// entity_metrics holds one row per value change:
//
//	entity_type   'guild' or 'channel'
//	entity_id     guild or channel id
//	metric        numeric provider field name (member_count, message_count, ...)
//	observed_at   time the value was first observed
//	value         exact decimal text
const (
	checkObserved       = "observed"
	checkAttachmentText = "attachment_text"
)

func ensureEntityStateTables(ctx context.Context, db changeFeedDB) error {
	for _, stmt := range []string{
		`create table if not exists entity_evidence (
 evidence_hash text primary key, payload_json text not null
 ) without rowid`,
		`create table if not exists entity_observations (
 observation_id integer primary key autoincrement, table_name text not null,
 row_key text not null, observed_at text not null, evidence_hash text not null references entity_evidence(evidence_hash)
 )`,
		`create index if not exists idx_entity_observations_owner on entity_observations(table_name,row_key,observed_at)`,
		`create table if not exists entity_check_history (
 check_id integer primary key autoincrement, table_name text not null, row_key text not null,
 check_kind text not null, checked_at text not null, succeeded_at text, outcome text not null
 )`,
		`create table if not exists mention_event_id_map (
 source_event_id integer not null, identity_key text not null, local_event_id integer not null,
 primary key(source_event_id,identity_key)
 ) without rowid`,
		`create index if not exists idx_mention_event_id_map_local on mention_event_id_map(local_event_id)`,
		`create table if not exists mention_event_history (
 event_id integer primary key, payload_json text not null
 )`,
		`create table if not exists entity_checks (
			table_name text not null,
			row_key text not null,
			check_kind text not null,
			checked_at text not null,
			succeeded_at text,
			outcome text not null default '',
			primary key (table_name, row_key, check_kind)
		) without rowid`,
		`create index if not exists idx_entity_checks_checked on entity_checks(checked_at)`,
		`create table if not exists entity_metrics (
			entity_type text not null,
			entity_id text not null,
			metric text not null,
			observed_at text not null,
			value text not null,
			primary key (entity_type, entity_id, metric, observed_at)
		)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) applyEntityStateMigration(ctx context.Context) error {
	var present int
	if err := s.db.QueryRowContext(ctx, `select count(*) from sqlite_master where type in ('table','index') and name in ('entity_checks','entity_metrics','idx_entity_checks_checked','entity_evidence','entity_observations','entity_check_history','mention_event_history','idx_entity_observations_owner','mention_event_id_map','idx_mention_event_id_map_local')`).Scan(&present); err != nil {
		return err
	}
	if present == 10 {
		// No DDL on routine opens, so PRAGMA schema_version stays stable.
		return nil
	}
	// Install the additive schema atomically and pay one durable commit,
	// rather than a full-sync commit for each table/index on every new archive.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := ensureEntityStateTables(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// recordCheckTx records one check of a content row. rowKey values are the
// primary-key values in key order.
func recordCheckTx(ctx context.Context, tx *sql.Tx, table, kind, checkedAt, succeededAt, outcome string, rowKey ...any) error {
	if checkedAt == "" {
		checkedAt = time.Now().UTC().Format(timeLayout)
	}
	if _, err := tx.ExecContext(ctx, `insert into entity_check_history(table_name,row_key,check_kind,checked_at,succeeded_at,outcome)
 values(?,json_array(`+placeholders(len(rowKey))+`),?,?,?,?)`,
		append(append([]any{table}, rowKey...), kind, checkedAt, nullString(succeededAt), outcome)...); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `insert into entity_checks(table_name,row_key,check_kind,checked_at,succeeded_at,outcome)
		values(?,json_array(`+placeholders(len(rowKey))+`),?,?,?,?)
		on conflict(table_name,row_key,check_kind) do update set
			checked_at=max(entity_checks.checked_at,excluded.checked_at),
			succeeded_at=case when excluded.succeeded_at is null then entity_checks.succeeded_at
				when entity_checks.succeeded_at is null then excluded.succeeded_at
				else max(entity_checks.succeeded_at,excluded.succeeded_at) end,
			outcome=case when excluded.checked_at>=entity_checks.checked_at then excluded.outcome else entity_checks.outcome end`,
		append(append([]any{table}, rowKey...), kind, checkedAt, nullString(succeededAt), outcome)...)
	return err
}

func recordObservedTx(ctx context.Context, tx *sql.Tx, table string, changed bool, rowKey ...any) error {
	now := time.Now().UTC().Format(timeLayout)
	outcome := "unchanged"
	if changed {
		outcome = "changed"
	}
	return recordCheckTx(ctx, tx, table, checkObserved, now, now, outcome, rowKey...)
}

// lastObservedTx returns the later of a row's updated_at and its latest
// observed check. Freshness guards ("a write after this observation started
// is newer") use it, because an unchanged observation no longer advances
// updated_at. ok is false when the row does not exist.
func lastObservedTx(ctx context.Context, tx *sql.Tx, table, where string, rowKey ...any) (time.Time, bool, error) {
	var updated string
	err := tx.QueryRowContext(ctx, `select updated_at from `+quoteIdent(table)+` where `+where, rowKey...).Scan(&updated)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	last := parseTime(updated)
	var checked sql.NullString
	err = tx.QueryRowContext(ctx, `select checked_at from entity_checks where table_name=? and row_key=json_array(`+placeholders(len(rowKey))+`) and check_kind=?`,
		append(append([]any{table}, rowKey...), checkObserved)...).Scan(&checked)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, err
	}
	if t := parseTime(checked.String); t.After(last) {
		last = t
	}
	return last, true, nil
}

// recordMetricsTx appends a metric value only when it differs from the
// latest recorded value of that metric.
func recordMetricsTx(ctx context.Context, tx *sql.Tx, entityType, entityID string, metrics map[string]string) error {
	if len(metrics) == 0 || entityID == "" {
		return nil
	}
	now := time.Now().UTC().Format(timeLayout)
	for _, metric := range slices.Sorted(maps.Keys(metrics)) {
		value := metrics[metric]
		var latest string
		err := tx.QueryRowContext(ctx, `select value from entity_metrics where entity_type=? and entity_id=? and metric=? order by observed_at desc limit 1`, entityType, entityID, metric).Scan(&latest)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && latest == value {
			continue
		}
		if _, err := tx.ExecContext(ctx, `insert into entity_metrics(entity_type,entity_id,metric,observed_at,value) values(?,?,?,?,?)
			on conflict(entity_type,entity_id,metric,observed_at) do update set value=excluded.value`, entityType, entityID, metric, now, value); err != nil {
			return err
		}
	}
	return nil
}

// MetricValue is one entity_metrics row.
type MetricValue struct {
	EntityType string
	EntityID   string
	Metric     string
	ObservedAt string
	Value      string
}

// EntityMetrics returns the metric series of one entity in observation order.
func (s *Store) EntityMetrics(ctx context.Context, entityType, entityID string) ([]MetricValue, error) {
	rows, err := s.db.QueryContext(ctx, `select entity_type,entity_id,metric,observed_at,value from entity_metrics where entity_type=? and entity_id=? order by metric,observed_at`, entityType, entityID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []MetricValue
	for rows.Next() {
		var m MetricValue
		if err := rows.Scan(&m.EntityType, &m.EntityID, &m.Metric, &m.ObservedAt, &m.Value); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// SeedEntityMetrics is the cutover backfill: it records the live counters
// still embedded in stored guild and channel raw_json as initial metric
// values, without touching any content row. It is idempotent and writes only
// metrics that have no recorded value yet. It is not run automatically.
func (s *Store) SeedEntityMetrics(ctx context.Context) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	type source struct {
		entityType, id, kind, raw string
		entity                    entityKind
	}
	var sources []source
	for _, q := range []struct {
		sql    string
		typ    string
		entity entityKind
	}{
		{`select id,'',raw_json from guilds where deleted_at is null`, "guild", entityGuild},
		{`select id,kind,raw_json from channels where deleted_at is null`, "channel", entityChannel},
	} {
		rows, err := tx.QueryContext(ctx, q.sql)
		if err != nil {
			return 0, err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			src := source{entityType: q.typ, entity: q.entity}
			if err := rows.Scan(&src.id, &src.kind, &src.raw); err != nil {
				return 0, err
			}
			sources = append(sources, src)
		}
		if err := rows.Err(); err != nil {
			return 0, err
		}
		if err := rows.Close(); err != nil {
			return 0, err
		}
	}
	written := 0
	for _, src := range sources {
		_, metrics := canonicalizeRaw(src.entity, src.raw, "", src.kind)
		for metric := range metrics {
			var exists int
			if err := tx.QueryRowContext(ctx, `select count(*) from entity_metrics where entity_type=? and entity_id=? and metric=?`, src.entityType, src.id, metric).Scan(&exists); err != nil {
				return 0, err
			}
			if exists > 0 {
				delete(metrics, metric)
			}
		}
		if err := recordMetricsTx(ctx, tx, src.entityType, src.id, metrics); err != nil {
			return 0, err
		}
		written += len(metrics)
	}
	return written, tx.Commit()
}

// recordEvidenceTx preserves exact input fields before canonicalization or
// freshness rejection. Repeated observations share immutable evidence bytes;
// every receipt retains its own sequence and check time, including A-B-A.
func recordEvidenceTx(ctx context.Context, tx *sql.Tx, table string, value any, rowKey ...any) error {
	// WriteOptions are local execution policy, not source evidence. Keep the
	// request start as a narrow check rather than duplicating the entire
	// unchanged message body for each repair's different start time.
	if mutation, ok := value.(MessageMutation); ok {
		if started := mutation.Options.ObservationStartedAt; !started.IsZero() {
			if err := recordCheckTx(ctx, tx, table, "observation_started", started.UTC().Format(timeLayout), "", "requested", rowKey...); err != nil {
				return err
			}
		}
		mutation.Options = WriteOptions{}
		value = mutation
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	if _, err := tx.ExecContext(ctx, `insert into entity_evidence(evidence_hash,payload_json) values(?,?) on conflict do nothing`, hash, string(body)); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `insert into entity_observations(table_name,row_key,observed_at,evidence_hash) values(?,json_array(`+placeholders(len(rowKey))+`),?,?)`, append(append([]any{table}, rowKey...), time.Now().UTC().Format(timeLayout), hash)...)
	return err
}

// retainMentionIDTx keeps the original exact row addressable after removal,
// duplicate reconciliation, or an in-place metadata update. It never rewrites
// a historical ID. Subsequent observations remain in entity_evidence.
func retainMentionIDTx(ctx context.Context, tx *sql.Tx, id int64) error {
	_, err := tx.ExecContext(ctx, `insert into mention_event_history(event_id,payload_json)
 select event_id,json_object('event_id',event_id,'message_id',message_id,'guild_id',guild_id,'channel_id',channel_id,'author_id',author_id,'target_type',target_type,'target_id',target_id,'target_name',target_name,'event_at',event_at)
 from mention_events where event_id=? on conflict do nothing`, id)
	return err
}

// retainLegacyRawTx preserves the pre-upgrade raw representation before a
// canonical replacement. Its evidence is a legacy receipt, not a fabricated
// original check timestamp. Existing history and IDs are untouched.
func retainLegacyRawTx(ctx context.Context, tx *sql.Tx, table, raw string, rowKey ...any) error {
	if raw == "" {
		return nil
	}
	var count int
	if err := tx.QueryRowContext(ctx, `select count(*) from entity_observations where table_name=? and row_key=json_array(`+placeholders(len(rowKey))+`) and observed_at='legacy'`, append([]any{table}, rowKey...)...).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	where := "id=?"
	switch table {
	case "guilds", "channels", "messages":
	case "members":
		where = "guild_id=? and user_id=?"
	case "message_attachments":
		where = "attachment_id=?"
	default:
		return errors.New("unsupported legacy evidence owner")
	}
	var updatedAt string
	if err := tx.QueryRowContext(ctx, "select updated_at from "+quoteIdent(table)+" where "+where, rowKey...).Scan(&updatedAt); err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		RawJSON    string
		Provenance string
		UpdatedAt  string
	}{raw, "legacy_stored", updatedAt})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	if _, err = tx.ExecContext(ctx, `insert into entity_evidence(evidence_hash,payload_json) values(?,?) on conflict do nothing`, hash, string(body)); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `insert into entity_observations(table_name,row_key,observed_at,evidence_hash) values(?,json_array(`+placeholders(len(rowKey))+`),'legacy',?)`, append(append([]any{table}, rowKey...), hash)...)
	return err
}

// retainLegacyAttachmentTx keeps all pre-upgrade attachment fields, not only
// extracted text, before an in-place replacement or removal.
func retainLegacyAttachmentTx(ctx context.Context, tx *sql.Tx, id string) error {
	var body string
	err := tx.QueryRowContext(ctx, `select json_object(
 'attachment_id',attachment_id,'message_id',message_id,'guild_id',guild_id,'channel_id',channel_id,
 'author_id',author_id,'filename',filename,'content_type',content_type,'size',size,'url',url,'proxy_url',proxy_url,
 'text_content',text_content,'media_path',media_path,'content_sha256',content_sha256,'content_size',content_size,
 'fetched_at',fetched_at,'fetch_status',fetch_status,'fetch_error',fetch_error,'updated_at',updated_at,
 'text_status',text_status,'text_error',text_error,'text_attempted_at',text_attempted_at,'text_succeeded_at',text_succeeded_at)
 from message_attachments where attachment_id=?`, id).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return retainLegacyRawTx(ctx, tx, "message_attachments", body, id)
}

// RetainMentionIdentitiesTx protects existing IDs before exact snapshot
// replacement. Imported explicit IDs pass MapImportedMentionIdentityTx;
// omitted IDs continue above SQLite's existing AUTOINCREMENT high-water mark.
func RetainMentionIdentitiesTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `insert into mention_event_history(event_id,payload_json)
 select event_id,json_object('event_id',event_id,'message_id',message_id,'guild_id',guild_id,'channel_id',channel_id,'author_id',author_id,'target_type',target_type,'target_id',target_id,'target_name',target_name,'event_at',event_at)
 from mention_events where true on conflict do nothing`)
	return err
}

// MapImportedMentionIdentityTx preserves source aliases without reusing a local
// historical ID for another fact. This allows ordinary snapshots to coexist
// with local DM IDs and supports revisiting older snapshot revisions.
func MapImportedMentionIdentityTx(ctx context.Context, tx *sql.Tx, row map[string]any) error {
	var sourceID int64
	if err := tx.QueryRowContext(ctx, `select ?`, row["event_id"]).Scan(&sourceID); err != nil {
		return err
	}
	var identity string
	if err := tx.QueryRowContext(ctx, `select json_array(cast(? as text),cast(? as text),cast(? as text))`, row["message_id"], row["target_type"], row["target_id"]).Scan(&identity); err != nil {
		return err
	}
	var localID int64
	err := tx.QueryRowContext(ctx, `select local_event_id from mention_event_id_map where source_event_id=? and identity_key=?`, sourceID, identity).Scan(&localID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		var previous string
		err = tx.QueryRowContext(ctx, `select json_array(cast(json_extract(payload_json,'$.message_id') as text),cast(json_extract(payload_json,'$.target_type') as text),cast(json_extract(payload_json,'$.target_id') as text)) from mention_event_history where event_id=?`, sourceID).Scan(&previous)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var high int64
		if err := tx.QueryRowContext(ctx, `select max(v) from (
   select coalesce(max(event_id),0) v from mention_events union all
   select coalesce(max(event_id),0) from mention_event_history union all
   select coalesce(max(local_event_id),0) from mention_event_id_map union all
   select coalesce(max(seq),0) from sqlite_sequence where name='mention_events')`).Scan(&high); err != nil {
			return err
		}
		localID = sourceID
		if previous != identity && sourceID <= high {
			if high == math.MaxInt64 {
				return errors.New("mention identity sequence exhausted")
			}
			localID = high + 1
		}
		if _, err := tx.ExecContext(ctx, `insert into mention_event_id_map(source_event_id,identity_key,local_event_id) values(?,?,?)`, sourceID, identity, localID); err != nil {
			return err
		}
	}
	original := maps.Clone(row)
	row["event_id"] = localID
	body, err := json.Marshal(row)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `insert into mention_event_history(event_id,payload_json) values(?,?) on conflict do nothing`, localID, string(body)); err != nil {
		return err
	}
	return recordEvidenceTx(ctx, tx, "mention_events", struct {
		SourceEventID int64
		Row           map[string]any
	}{sourceID, original}, localID)
}
