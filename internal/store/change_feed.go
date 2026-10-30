package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// The source change feed lets readers copy only changed rows of selected
// tables. See docs/guides/change-feed.md for the reader contract. Triggers
// write one change_log row per changed native row in the writer's own
// transaction, so every writer (including older binaries, share imports and
// manual repairs) is covered without application bookkeeping.
const (
	// ChangeFeedContractVersion is the baseline contract recorded in
	// change_feed_meta. It stays 1 so contract-1 readers keep their cursors;
	// later contracts are served next to it (see changeFeedContracts).
	ChangeFeedContractVersion = 1
	// ChangeFeedLatestContractVersion is the newest contract this binary
	// serves. Contract 2 adds the content tables and entity_metrics with the
	// same entry semantics; see docs/guides/change-feed.md.
	ChangeFeedLatestContractVersion = 2
	// ChangeFeedRetention bounds change_log growth. Entries younger than this
	// are never compacted; a reader whose cursor falls below the compaction
	// floor must reconcile once.
	ChangeFeedRetention = 7 * 24 * time.Hour

	changeFeedCompactionBatch = 5000
)

// changeEntryDigestSQL identifies one change_log entry for cursor validation.
const changeEntryDigestSQL = `json_array(table_name,op,row_key,recorded_at_ms)`

const (
	ChangeOpInsert = "i"
	ChangeOpUpdate = "u"
	ChangeOpDelete = "d"
)

var changeFeedV1Tables = []string{
	"message_attachments",
	"mention_events",
	"message_embedding_history",
	"attachment_text_history",
}

// Tables added by contract 2. Each table's change_feed_tables row records the
// contract that introduced it, so contract-1 tables keep their generations
// and a contract-1 binary still finds its own tables current.
var changeFeedV2Tables = []string{
	"guilds",
	"channels",
	"members",
	"messages",
	"entity_metrics",
	"entity_evidence",
	"entity_observations",
	"entity_check_history",
	"mention_event_history",
}

var changeFeedTables = append(slices.Clone(changeFeedV1Tables), changeFeedV2Tables...)

// changeFeedContracts lists every contract this binary serves and its tables.
var changeFeedContracts = map[int][]string{
	1: changeFeedV1Tables,
	2: changeFeedTables,
}

// ChangeFeedTables returns the tables covered by the change feed (all
// contracts).
func ChangeFeedTables() []string {
	return slices.Clone(changeFeedTables)
}

func changeFeedTableContract(table string) int {
	if slices.Contains(changeFeedV1Tables, table) {
		return 1
	}
	return 2
}

func changeFeedContractsJSON(version int) string {
	body, err := json.Marshal(changeFeedContracts[version])
	if err != nil {
		panic(err)
	} // []string contains no unsupported JSON values.
	return string(body)
}

type ChangeFeedTableStatus struct {
	Table         string `json:"table"`
	Contract      int    `json:"contract_version"`
	Generation    int64  `json:"generation"`
	GenerationID  string `json:"generation_id"`
	GenerationSeq int64  `json:"generation_seq"`
	Reason        string `json:"reason"`
	UpdatedAt     string `json:"updated_at"`
}

type ChangeFeedStatus struct {
	ContractVersion  int                     `json:"contract_version"`
	ContractVersions []int                   `json:"contract_versions"`
	HeadSeq          int64                   `json:"head_seq"`
	CompactedThrough int64                   `json:"compacted_through"`
	CompactedAt      string                  `json:"compacted_at,omitempty"`
	RetentionSeconds int64                   `json:"retention_seconds"`
	Tables           []ChangeFeedTableStatus `json:"tables"`
}

type changeFeedDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type changeFeedColumn struct {
	name string
	pk   int
}

// ensureChangeFeed installs or repairs the feed after all migrations. It
// issues no DDL when the installed triggers already match the current table
// definitions, so routine opens never change PRAGMA schema_version.
func (s *Store) ensureChangeFeed(ctx context.Context) error {
	ok, err := changeFeedCurrent(ctx, s.db)
	if err != nil || ok {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := ensureChangeFeedTx(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func changeFeedCurrent(ctx context.Context, db changeFeedDB) (bool, error) {
	var present int
	if err := db.QueryRowContext(ctx, `select count(*) from sqlite_master where type='table' and name in ('change_log','change_feed_tables','change_feed_meta')`).Scan(&present); err != nil {
		return false, err
	}
	if present != 3 {
		return false, nil
	}
	var version sql.NullInt64
	err := db.QueryRowContext(ctx, `select contract_version from change_feed_meta where singleton=1`).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if version.Int64 != ChangeFeedContractVersion {
		return false, nil
	}
	if ok, err := changeFeedContractsCurrent(ctx, db); err != nil || !ok {
		return false, err
	}
	for _, table := range changeFeedTables {
		ok, err := changeFeedTableCurrent(ctx, db, table)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// changeFeedContractsCurrent reports whether change_feed_contracts lists
// exactly the contracts this binary serves. A newer binary's extra contract
// rows are left alone by older binaries, which never read this table.
func changeFeedContractsCurrent(ctx context.Context, db changeFeedDB) (bool, error) {
	var present int
	if err := db.QueryRowContext(ctx, `select count(*) from sqlite_master where type='table' and name='change_feed_contracts'`).Scan(&present); err != nil {
		return false, err
	}
	if present == 0 {
		return false, nil
	}
	for version := range changeFeedContracts {
		var tables string
		err := db.QueryRowContext(ctx, `select tables_json from change_feed_contracts where contract_version=?`, version).Scan(&tables)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if tables != changeFeedContractsJSON(version) {
			return false, nil
		}
	}
	return true, nil
}

func changeFeedTableCurrent(ctx context.Context, db changeFeedDB, table string) (bool, error) {
	var stored sql.NullString
	var contract sql.NullInt64
	err := db.QueryRowContext(ctx, `select schema_sha256,contract_version from change_feed_tables where table_name=?`, table).Scan(&stored, &contract)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	schemaSHA, triggers, err := changeFeedExpected(ctx, db, table)
	if err != nil {
		return false, err
	}
	if stored.String != schemaSHA || contract.Int64 != int64(changeFeedTableContract(table)) {
		return false, nil
	}
	for name, want := range triggers {
		var got sql.NullString
		err := db.QueryRowContext(ctx, `select sql from sqlite_master where type='trigger' and name=? and tbl_name=?`, name, table).Scan(&got)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if got.String != want {
			return false, nil
		}
	}
	return true, nil
}

func ensureChangeFeedTx(ctx context.Context, tx *sql.Tx) error {
	// A missing log, state or meta table means entries or the compaction floor
	// may be lost, so every table must start a new generation.
	var present int
	if err := tx.QueryRowContext(ctx, `select count(*) from sqlite_master where type='table' and name in ('change_log','change_feed_tables','change_feed_meta')`).Scan(&present); err != nil {
		return err
	}
	partial := present != 3
	if !partial {
		var meta int
		if err := tx.QueryRowContext(ctx, `select count(*) from change_feed_meta where singleton=1`).Scan(&meta); err != nil {
			return err
		}
		partial = meta == 0
	}
	for _, stmt := range []string{
		`create table if not exists change_log (
			seq integer primary key autoincrement,
			table_name text not null,
			op text not null,
			row_key text not null,
			recorded_at_ms integer not null
		)`,
		`create table if not exists change_feed_tables (
			table_name text primary key,
			generation integer not null,
			generation_id text not null,
			generation_seq integer not null,
			reason text not null,
			schema_sha256 text not null,
			contract_version integer not null,
			updated_at text not null
		)`,
		`create table if not exists change_feed_meta (
			singleton integer primary key check (singleton = 1),
			contract_version integer not null,
			compacted_through integer not null default 0,
			compacted_entry text,
			compacted_at text,
			retention_seconds integer not null
		)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("create change feed: %w", err)
		}
	}
	// A recreated log has no AUTOINCREMENT high-water mark. Never allocate a
	// sequence at or below one a reader may already hold.
	if err := restoreChangeLogAllocator(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `insert into change_feed_meta(singleton,contract_version,compacted_through,retention_seconds)
		values(1,?,0,?) on conflict(singleton) do update set contract_version=excluded.contract_version,retention_seconds=excluded.retention_seconds`,
		ChangeFeedContractVersion, int64(ChangeFeedRetention/time.Second)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `create table if not exists change_feed_contracts (
			contract_version integer primary key,
			tables_json text not null,
			updated_at text not null
		)`); err != nil {
		return fmt.Errorf("create change feed: %w", err)
	}
	for _, version := range slices.Sorted(maps.Keys(changeFeedContracts)) {
		if _, err := tx.ExecContext(ctx, `insert into change_feed_contracts(contract_version,tables_json,updated_at) values(?,?,?)
			on conflict(contract_version) do update set tables_json=excluded.tables_json,updated_at=excluded.updated_at
			where change_feed_contracts.tables_json<>excluded.tables_json`,
			version, changeFeedContractsJSON(version), time.Now().UTC().Format(timeLayout)); err != nil {
			return err
		}
	}
	if partial {
		// Every generation restarts below. Make the current head the floor so
		// it is a valid empty cursor boundary, and drop entries no reader of
		// the new generations can use.
		head, err := changeFeedHead(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `delete from change_log where seq<=?`, head); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `update change_feed_meta set compacted_through=?,compacted_entry=null,compacted_at=? where singleton=1`, head, time.Now().UTC().Format(timeLayout)); err != nil {
			return err
		}
	}
	for _, table := range changeFeedTables {
		current, err := changeFeedTableCurrent(ctx, tx, table)
		if err != nil {
			return err
		}
		if current && !partial {
			continue
		}
		reason, err := changeFeedRepairReason(ctx, tx, table)
		if err != nil {
			return err
		}
		if partial && reason != "enabled" {
			reason = "feed_restored"
		}
		if err := installChangeFeedTriggers(ctx, tx, table); err != nil {
			return err
		}
		if err := bumpChangeFeedGeneration(ctx, tx, table, reason); err != nil {
			return err
		}
	}
	return nil
}

func restoreChangeLogAllocator(ctx context.Context, tx *sql.Tx) error {
	var floor int64
	if err := tx.QueryRowContext(ctx, `select max(
		coalesce((select max(compacted_through) from change_feed_meta),0),
		coalesce((select max(generation_seq) from change_feed_tables),0),
		coalesce((select max(seq) from change_log),0))`).Scan(&floor); err != nil {
		return err
	}
	if floor == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `insert into sqlite_sequence(name,seq) select 'change_log',0 where not exists(select 1 from sqlite_sequence where name='change_log')`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `update sqlite_sequence set seq=? where name='change_log' and seq<?`, floor, floor)
	return err
}

func changeFeedRepairReason(ctx context.Context, tx *sql.Tx, table string) (string, error) {
	var stored sql.NullString
	var contract sql.NullInt64
	err := tx.QueryRowContext(ctx, `select schema_sha256,contract_version from change_feed_tables where table_name=?`, table).Scan(&stored, &contract)
	if errors.Is(err, sql.ErrNoRows) {
		return "enabled", nil
	}
	if err != nil {
		return "", err
	}
	schemaSHA, _, err := changeFeedExpected(ctx, tx, table)
	if err != nil {
		return "", err
	}
	switch {
	case contract.Int64 != int64(changeFeedTableContract(table)):
		return "contract_upgraded", nil
	case stored.String != schemaSHA:
		return "schema_changed", nil
	default:
		return "triggers_restored", nil
	}
}

func installChangeFeedTriggers(ctx context.Context, tx *sql.Tx, table string) error {
	_, triggers, err := changeFeedExpected(ctx, tx, table)
	if err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(triggers)) {
		if _, err := tx.ExecContext(ctx, `drop trigger if exists `+quoteIdent(name)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, triggers[name]); err != nil {
			return fmt.Errorf("install change feed trigger %s: %w", name, err)
		}
	}
	return nil
}

// BumpChangeFeedGenerationTx starts a new generation for tables whose history
// was rewritten in a way readers should reconcile instead of replaying, for
// example a snapshot replacement. It is a no-op before the feed is installed.
func BumpChangeFeedGenerationTx(ctx context.Context, tx *sql.Tx, reason string, tables ...string) error {
	var present int
	if err := tx.QueryRowContext(ctx, `select count(*) from sqlite_master where type='table' and name='change_feed_tables'`).Scan(&present); err != nil {
		return err
	}
	if present == 0 {
		return nil
	}
	for _, table := range tables {
		if !slices.Contains(changeFeedTables, table) {
			return fmt.Errorf("change feed does not track %q", table)
		}
		// Reinstall the triggers, so a generation that follows
		// SuspendChangeFeedTx records later writes again.
		if err := installChangeFeedTriggers(ctx, tx, table); err != nil {
			return err
		}
		if err := bumpChangeFeedGeneration(ctx, tx, table, reason); err != nil {
			return err
		}
	}
	return nil
}

// SuspendChangeFeedTx drops the feed triggers of tables whose contents the
// caller is about to replace wholesale, so a replacement does not log a delete
// and an insert for every row. The caller must call
// BumpChangeFeedGenerationTx for the same tables in the same transaction; a
// rollback restores the triggers. It is a no-op before the feed is installed.
func SuspendChangeFeedTx(ctx context.Context, tx *sql.Tx, tables ...string) error {
	var present int
	if err := tx.QueryRowContext(ctx, `select count(*) from sqlite_master where type='table' and name='change_feed_tables'`).Scan(&present); err != nil {
		return err
	}
	if present == 0 {
		return nil
	}
	for _, table := range tables {
		if !slices.Contains(changeFeedTables, table) {
			return fmt.Errorf("change feed does not track %q", table)
		}
		for _, suffix := range []string{"ai", "au", "ad"} {
			if _, err := tx.ExecContext(ctx, `drop trigger if exists `+quoteIdent("change_feed_"+table+"_"+suffix)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ChangeFeedReplacedTables returns the tracked tables among tables.
func ChangeFeedReplacedTables(tables []string) []string {
	var out []string
	for _, table := range tables {
		if slices.Contains(changeFeedTables, table) {
			out = append(out, table)
		}
	}
	return out
}

func bumpChangeFeedGeneration(ctx context.Context, tx *sql.Tx, table, reason string) error {
	schemaSHA, _, err := changeFeedExpected(ctx, tx, table)
	if err != nil {
		return err
	}
	head, err := changeFeedHead(ctx, tx)
	if err != nil {
		return err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `insert into change_feed_tables(table_name,generation,generation_id,generation_seq,reason,schema_sha256,contract_version,updated_at)
		values(?,1,?,?,?,?,?,?)
		on conflict(table_name) do update set generation=change_feed_tables.generation+1,generation_id=excluded.generation_id,
			generation_seq=excluded.generation_seq,reason=excluded.reason,schema_sha256=excluded.schema_sha256,
			contract_version=excluded.contract_version,updated_at=excluded.updated_at`,
		table, hex.EncodeToString(id[:]), head, reason, schemaSHA, changeFeedTableContract(table), time.Now().UTC().Format(timeLayout))
	return err
}

// changeFeedHead is the highest sequence ever allocated, including entries
// already compacted. AUTOINCREMENT guarantees sequences are never reused.
func changeFeedHead(ctx context.Context, db changeFeedDB) (int64, error) {
	var head int64
	err := db.QueryRowContext(ctx, `select max(
		coalesce((select seq from sqlite_sequence where name='change_log'),0),
		coalesce((select max(seq) from change_log),0),
		coalesce((select compacted_through from change_feed_meta where singleton=1),0))`).Scan(&head)
	return head, err
}

func changeFeedExpected(ctx context.Context, db changeFeedDB, table string) (string, map[string]string, error) {
	var ddl sql.NullString
	err := db.QueryRowContext(ctx, `select sql from sqlite_master where type='table' and name=?`, table).Scan(&ddl)
	if err != nil {
		return "", nil, fmt.Errorf("inspect change feed table %s: %w", table, err)
	}
	columns, err := changeFeedColumns(ctx, db, table)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256([]byte(ddl.String))
	return hex.EncodeToString(sum[:]), changeFeedTriggerSQL(table, columns), nil
}

func changeFeedColumns(ctx context.Context, db changeFeedDB, table string) ([]changeFeedColumn, error) {
	rows, err := db.QueryContext(ctx, `select name,pk from pragma_table_info(?) order by cid`, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var columns []changeFeedColumn
	for rows.Next() {
		var c changeFeedColumn
		if err := rows.Scan(&c.name, &c.pk); err != nil {
			return nil, err
		}
		columns = append(columns, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("change feed table %s has no columns", table)
	}
	return columns, nil
}

func changeFeedTriggerSQL(table string, columns []changeFeedColumn) map[string]string {
	keys := make([]changeFeedColumn, 0, len(columns))
	for _, c := range columns {
		if c.pk > 0 {
			keys = append(keys, c)
		}
	}
	slices.SortFunc(keys, func(a, b changeFeedColumn) int { return a.pk - b.pk })
	keyExpr := func(alias string) string {
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			ref := alias + "." + quoteIdent(k.name)
			// A blob key must never abort the writer's statement.
			parts = append(parts, "case typeof("+ref+") when 'blob' then json_object('blob_hex',hex("+ref+")) else "+ref+" end")
		}
		return "json_array(" + strings.Join(parts, ",") + ")"
	}
	// Values of different storage classes never compare equal, and column
	// affinity normalizes numerics, so IS NOT also detects storage changes.
	diff := make([]string, 0, len(columns))
	for _, c := range columns {
		diff = append(diff, "old."+quoteIdent(c.name)+" is not new."+quoteIdent(c.name))
	}
	keyChanged := make([]string, 0, len(keys))
	for _, k := range keys {
		keyChanged = append(keyChanged, "old."+quoteIdent(k.name)+" is not new."+quoteIdent(k.name))
	}
	const now = `cast(round((julianday('now')-2440587.5)*86400000.0) as integer)`
	entry := func(op, alias string) string {
		return "insert into change_log(table_name,op,row_key,recorded_at_ms) values('" + table + "','" + op + "'," + keyExpr(alias) + "," + now + ");"
	}
	updateWhen := " when " + strings.Join(diff, " or ")
	name := func(suffix string) string { return "change_feed_" + table + "_" + suffix }
	return map[string]string{
		name("ai"): "CREATE TRIGGER " + quoteIdent(name("ai")) + " after insert on " + quoteIdent(table) + " begin " + entry(ChangeOpInsert, "new") + " end",
		name("au"): "CREATE TRIGGER " + quoteIdent(name("au")) + " after update on " + quoteIdent(table) + updateWhen + " begin " +
			"insert into change_log(table_name,op,row_key,recorded_at_ms) select '" + table + "','" + ChangeOpDelete + "'," + keyExpr("old") + "," + now +
			" where " + strings.Join(keyChanged, " or ") + "; " + entry(ChangeOpUpdate, "new") + " end",
		name("ad"): "CREATE TRIGGER " + quoteIdent(name("ad")) + " after delete on " + quoteIdent(table) + " begin " + entry(ChangeOpDelete, "old") + " end",
	}
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// CompactChangeFeed deletes the oldest entries recorded before now-retention,
// in sequence order and in bounded transactions. It only ever removes a
// sequence prefix and records the new floor in the same transaction; it stops
// at the first entry inside the retention window, even if a later entry has
// an older wall-clock stamp.
func (s *Store) CompactChangeFeed(ctx context.Context, now time.Time, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, errors.New("change feed retention must be positive")
	}
	cutoff := now.Add(-retention).UnixMilli()
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		deleted, done, err := s.compactChangeFeedBatch(ctx, cutoff, now)
		total += deleted
		if err != nil || done {
			return total, err
		}
	}
}

func (s *Store) compactChangeFeedBatch(ctx context.Context, cutoff int64, now time.Time) (int64, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer rollback(tx)
	var floor int64
	if err := tx.QueryRowContext(ctx, `select compacted_through from change_feed_meta where singleton=1`).Scan(&floor); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, true, nil
		}
		return 0, false, err
	}
	through, seen, stopped, err := compactableChangePrefix(ctx, tx, floor, cutoff)
	if err != nil {
		return 0, false, err
	}
	if through == floor {
		return 0, true, nil
	}
	// Keep the identity of the last compacted entry, so a reader whose cursor
	// sits exactly on the floor can still prove continuity after a restore.
	var entry string
	if err := tx.QueryRowContext(ctx, `select `+changeEntryDigestSQL+` from change_log where seq=?`, through).Scan(&entry); err != nil {
		return 0, false, err
	}
	result, err := tx.ExecContext(ctx, `delete from change_log where seq>? and seq<=?`, floor, through)
	if err != nil {
		return 0, false, err
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	if _, err := tx.ExecContext(ctx, `update change_feed_meta set compacted_through=?,compacted_entry=?,compacted_at=? where singleton=1`, through, entry, now.UTC().Format(timeLayout)); err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return deleted, stopped || seen < changeFeedCompactionBatch, nil
}

// compactableChangePrefix returns the last sequence of the prefix above floor
// recorded before cutoff, scanning at most one batch.
func compactableChangePrefix(ctx context.Context, tx *sql.Tx, floor, cutoff int64) (through int64, seen int, stopped bool, err error) {
	rows, err := tx.QueryContext(ctx, `select seq,recorded_at_ms from change_log where seq>? order by seq limit ?`, floor, changeFeedCompactionBatch)
	if err != nil {
		return 0, 0, false, err
	}
	defer func() { _ = rows.Close() }()
	through = floor
	for rows.Next() {
		var seq, at int64
		if err := rows.Scan(&seq, &at); err != nil {
			return 0, 0, false, err
		}
		seen++
		if at >= cutoff {
			return through, seen, true, nil
		}
		through = seq
	}
	return through, seen, false, rows.Err()
}

// ChangeFeedStatus reports generations and sequence bounds without scanning
// the log. It returns nil when the feed is not installed.
func (s *Store) ChangeFeedStatus(ctx context.Context) (*ChangeFeedStatus, error) {
	var present int
	if err := s.db.QueryRowContext(ctx, `select count(*) from sqlite_master where type='table' and name in ('change_log','change_feed_tables','change_feed_meta')`).Scan(&present); err != nil {
		return nil, err
	}
	if present != 3 {
		return nil, nil
	}
	status := &ChangeFeedStatus{}
	var compactedAt sql.NullString
	if err := s.db.QueryRowContext(ctx, `select contract_version,compacted_through,compacted_at,retention_seconds from change_feed_meta where singleton=1`).
		Scan(&status.ContractVersion, &status.CompactedThrough, &compactedAt, &status.RetentionSeconds); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	status.CompactedAt = compactedAt.String
	head, err := changeFeedHead(ctx, s.db)
	if err != nil {
		return nil, err
	}
	status.HeadSeq = head
	status.ContractVersions = []int{ChangeFeedContractVersion}
	if ok, err := changeFeedContractsCurrent(ctx, s.db); err != nil {
		return nil, err
	} else if ok {
		status.ContractVersions = slices.Sorted(maps.Keys(changeFeedContracts))
	}
	rows, err := s.db.QueryContext(ctx, `select table_name,contract_version,generation,generation_id,generation_seq,reason,updated_at from change_feed_tables order by table_name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var t ChangeFeedTableStatus
		if err := rows.Scan(&t.Table, &t.Contract, &t.Generation, &t.GenerationID, &t.GenerationSeq, &t.Reason, &t.UpdatedAt); err != nil {
			return nil, err
		}
		status.Tables = append(status.Tables, t)
	}
	return status, rows.Err()
}
