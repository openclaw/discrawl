package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"
)

var (
	ErrNativeBaseline = errors.New("native capture requires a matching retained baseline")
	ErrNativeLimit    = errors.New("native capture exceeds its bound; cursor not advanced")
	ErrNativeData     = errors.New("native capture found uninterpretable data")
)

var nativeCaptureTables = []string{"messages", "guilds", "channels", "members", "sync_state"}

type NativeTableGeneration struct {
	Table, GenerationID, SchemaSHA256 string
}

type NativeCursor struct {
	Sequence    int64
	Entry       string
	Generations []NativeTableGeneration
}

// NativeBaseline is local state, never a service payload or permission grant.
// SourceID identifies the physical file, not an archive's public identity.
type NativeBaseline struct {
	SourceID string
	Cursor   NativeCursor
}

type NativeCaptureLimits struct {
	// MaxEvents bounds journal entries plus channel dependency rows.
	MaxEvents int
	MaxBytes  int64
}

// NativeValue preserves SQLite storage classes. Text may contain NUL bytes;
// integers stay int64 until an egress owner encodes them as decimal strings.
type NativeValue struct {
	Kind    string
	Text    string
	Integer int64
}

type NativeChange struct {
	Table, Operation string
	Sequence         int64
	Key              []NativeValue
	Columns          []string
	Row              []NativeValue
}

// NativeCapture contains private, unfiltered native rows. A separate owner
// must apply the qualified source scope and delivery profile before egress.
type NativeCapture struct {
	From, Through NativeBaseline
	Changes       []NativeChange
	EventsRead    int
	// CoverageGuilds resolves changed channel checkpoint scopes in this same
	// snapshot. Empty values mean the channel is absent, not globally readable.
	CoverageGuilds map[string]string
	// ChannelCoverage contains the complete current checkpoint set for each
	// changed channel, including absent channels. Missing entries are unknown.
	ChannelCoverage map[string]NativeChannelCoverage
}

type NativeChannelCoverage struct {
	Present     bool
	GuildID     string
	Checkpoints []NativeChange
}

// ObserveNativeBaseline records a source position without scanning its rows.
// This is not seed qualification: the caller must separately prove that the
// retained seed describes this same source snapshot before registering it.
func ObserveNativeBaseline(ctx context.Context, path string) (NativeBaseline, error) {
	s, identity, err := openNativeSource(ctx, path)
	if err != nil {
		return NativeBaseline{}, err
	}
	defer s.Close()
	tx, err := s.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return NativeBaseline{}, err
	}
	defer tx.Rollback()
	cursor, _, err := readNativeCursor(ctx, tx, nil)
	if err != nil {
		return NativeBaseline{}, err
	}
	if err := finishNativeSource(ctx, tx, path, identity); err != nil {
		return NativeBaseline{}, err
	}
	return NativeBaseline{SourceID: identity, Cursor: cursor}, nil
}

// CaptureNativeChanges materializes the complete delta to one read-snapshot
// head. The journal stores keys, not old row versions: returning a prefix of
// its keys with current rows could silently pair old cursors with newer data.
// Overflow therefore returns no capture. Only a durable delivery owner may
// split a successful capture into parts and advance its acknowledged cursor.
func CaptureNativeChanges(ctx context.Context, path string, baseline NativeBaseline, limits NativeCaptureLimits) (NativeCapture, error) {
	if _, ok := ctx.Deadline(); !ok {
		return NativeCapture{}, errors.New("native capture requires a context deadline")
	}
	if limits.MaxEvents <= 0 || limits.MaxBytes <= 0 {
		return NativeCapture{}, ErrNativeLimit
	}
	s, identity, err := openNativeSource(ctx, path)
	if err != nil {
		return NativeCapture{}, err
	}
	defer s.Close()
	if identity != baseline.SourceID {
		return NativeCapture{}, ErrNativeBaseline
	}
	tx, err := s.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return NativeCapture{}, err
	}
	defer tx.Rollback()
	captured, err := captureNativeTx(ctx, tx, baseline, limits)
	if err != nil {
		return NativeCapture{}, err
	}
	if err := finishNativeSource(ctx, tx, path, identity); err != nil {
		return NativeCapture{}, err
	}
	return captured, nil
}

func openNativeSource(ctx context.Context, path string) (*Store, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	identity, err := nativeSourceIdentity(path)
	if err != nil {
		return nil, "", err
	}
	s, err := OpenReadOnly(ctx, path)
	if err != nil {
		return nil, "", err
	}
	return s, identity, nil
}

// The source owner must keep the pathname bound to its file during capture.
// Persistent replacement is detected; a transient swap away and back is not
// supported. Ordinary concurrent SQLite writers are supported.
func finishNativeSource(ctx context.Context, tx *sql.Tx, path, identity string) error {
	after, err := nativeSourceIdentity(path)
	if err != nil || after != identity {
		return ErrNativeBaseline
	}
	return tx.Commit()
}

func nativeSourceIdentity(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", ErrNativeBaseline
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", ErrNativeBaseline
	}
	identity, ok := nativePhysicalIdentity(info)
	if !ok {
		return "", ErrNativeBaseline
	}
	return identity, nil
}

func readNativeCursor(ctx context.Context, tx *sql.Tx, budget *int64) (NativeCursor, int64, error) {
	var cursor NativeCursor
	var floor int64
	var contract int
	if err := tx.QueryRowContext(ctx, `select contract_version,compacted_through from change_feed_meta where singleton=1`).Scan(&contract, &floor); err != nil {
		return cursor, 0, fmt.Errorf("%w: missing journal metadata", ErrNativeBaseline)
	}
	if contract != ChangeFeedContractVersion {
		return cursor, 0, ErrNativeBaseline
	}
	var registry string
	if err := tx.QueryRowContext(ctx, `select tables_json from change_feed_contracts where contract_version=3`).Scan(&registry); err != nil || registry != changeFeedContractsJSON(3) {
		return cursor, 0, fmt.Errorf("%w: coverage contract is not installed", ErrNativeBaseline)
	}
	for _, table := range nativeCaptureTables {
		ok, err := changeFeedTableCurrent(ctx, tx, table)
		if err != nil || !ok {
			return NativeCursor{}, 0, fmt.Errorf("%w: journal for %s is not current", ErrNativeBaseline, table)
		}
		generation := NativeTableGeneration{Table: table}
		if err := tx.QueryRowContext(ctx, `select generation_id,schema_sha256 from change_feed_tables where table_name=?`, table).Scan(&generation.GenerationID, &generation.SchemaSHA256); err != nil {
			return NativeCursor{}, 0, err
		}
		cursor.Generations = append(cursor.Generations, generation)
	}
	var err error
	cursor.Sequence, err = changeFeedHead(ctx, tx)
	if err != nil {
		return NativeCursor{}, 0, err
	}
	if floor < 0 || floor > cursor.Sequence {
		return NativeCursor{}, 0, ErrNativeBaseline
	}
	cursor.Entry, err = nativeCursorEntry(ctx, tx, cursor.Sequence, floor, budget)
	return cursor, floor, err
}

func nativeCursorEntry(ctx context.Context, tx *sql.Tx, sequence, floor int64, budget *int64) (string, error) {
	limit := int64(math.MaxInt64)
	if budget != nil {
		limit = *budget
	}
	// A global cursor can name an unselected table's large key. Bound it in
	// SQLite before returning the string to the application, just like row data.
	var entry sql.NullString
	err := tx.QueryRowContext(ctx, `select case when length(cast(entry as blob))<=? then entry end
		from (select `+changeEntryDigestSQL+` as entry from change_log where seq=?)`, limit, sequence).Scan(&entry)
	if errors.Is(err, sql.ErrNoRows) && sequence == floor {
		err = tx.QueryRowContext(ctx, `select case when length(cast(coalesce(compacted_entry,'') as blob))<=?
			then coalesce(compacted_entry,'') end from change_feed_meta where singleton=1`, limit).Scan(&entry)
	}
	if err != nil {
		return "", ErrNativeBaseline
	}
	if !entry.Valid {
		return "", ErrNativeLimit
	}
	if budget != nil {
		if err := nativeTake(budget, int64(len(entry.String))); err != nil {
			return "", err
		}
	}
	return entry.String, nil
}

func captureNativeTx(ctx context.Context, tx *sql.Tx, baseline NativeBaseline, limits NativeCaptureLimits) (NativeCapture, error) {
	budget := limits.MaxBytes
	through, floor, err := readNativeCursor(ctx, tx, &budget)
	if err != nil {
		return NativeCapture{}, err
	}
	from := baseline.Cursor
	if from.Sequence < floor || from.Sequence > through.Sequence || !slices.Equal(from.Generations, through.Generations) {
		return NativeCapture{}, ErrNativeBaseline
	}
	entry, err := nativeCursorEntry(ctx, tx, from.Sequence, floor, &budget)
	if errors.Is(err, ErrNativeLimit) {
		return NativeCapture{}, err
	}
	if err != nil || entry != from.Entry {
		return NativeCapture{}, ErrNativeBaseline
	}
	changes, events, err := nativeChangedKeys(ctx, tx, from.Sequence, through.Sequence, limits.MaxEvents, &budget)
	if err != nil {
		return NativeCapture{}, err
	}
	for i := range changes {
		if err := readNativeChangedRow(ctx, tx, &changes[i], &budget); err != nil {
			return NativeCapture{}, err
		}
	}
	coverage, err := nativeCoverageGuilds(ctx, tx, changes, &budget)
	if err != nil {
		return NativeCapture{}, err
	}
	dependencies, err := nativeChannelCoverage(ctx, tx, changes, limits.MaxEvents-events, &budget)
	if err != nil {
		return NativeCapture{}, err
	}
	return NativeCapture{From: baseline, Through: NativeBaseline{SourceID: baseline.SourceID, Cursor: through}, Changes: changes, EventsRead: events, CoverageGuilds: coverage, ChannelCoverage: dependencies}, nil
}

func nativeCoverageGuilds(ctx context.Context, tx *sql.Tx, changes []NativeChange, budget *int64) (map[string]string, error) {
	coverage := map[string]string{}
	for _, change := range changes {
		if change.Table != "sync_state" || change.Operation != "upsert" {
			continue
		}
		scope := change.Key[0].Text
		rest, ok := strings.CutPrefix(scope, "channel:")
		if !ok {
			continue
		}
		channel, suffix, ok := strings.Cut(rest, ":")
		if !ok || channel == "" || suffix == "" {
			return nil, ErrNativeData
		}
		var guild sql.NullString
		err := tx.QueryRowContext(ctx, `select case when length(cast(guild_id as blob))<=? then guild_id end from channels where id=?`, *budget, channel).Scan(&guild)
		if errors.Is(err, sql.ErrNoRows) {
			coverage[scope] = ""
			continue
		}
		if err != nil {
			return nil, err
		}
		if !guild.Valid {
			return nil, ErrNativeLimit
		}
		if !utf8.ValidString(guild.String) {
			return nil, ErrNativeData
		}
		if err := nativeTake(budget, int64(len(guild.String))); err != nil {
			return nil, err
		}
		coverage[scope] = guild.String
	}
	return coverage, nil
}

func nativeChangedKeys(ctx context.Context, tx *sql.Tx, from, through int64, limit int, budget *int64) ([]NativeChange, int, error) {
	// Bound each key before SQLite materializes it into an application string.
	rows, err := tx.QueryContext(ctx, `select seq,table_name,op,
		case when length(cast(row_key as blob))<=? then row_key end from change_log
		where seq>? and seq<=? and table_name in ('messages','guilds','channels','members','sync_state') order by seq`, *budget, from, through)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	latest := map[string]NativeChange{}
	events := 0
	for rows.Next() {
		if events == limit {
			return nil, 0, ErrNativeLimit
		}
		events++
		var change NativeChange
		var boundedKey sql.NullString
		if err := rows.Scan(&change.Sequence, &change.Table, &change.Operation, &boundedKey); err != nil {
			return nil, 0, err
		}
		if !boundedKey.Valid {
			return nil, 0, ErrNativeLimit
		}
		key := boundedKey.String
		if err := nativeTake(budget, int64(len(key)+len(change.Table))); err != nil {
			return nil, 0, err
		}
		var keys []string
		if !utf8.ValidString(key) || json.Unmarshal([]byte(key), &keys) != nil || len(keys) == 0 {
			return nil, 0, ErrNativeData
		}
		for _, value := range keys {
			if value == "" || !utf8.ValidString(value) {
				return nil, 0, ErrNativeData
			}
			change.Key = append(change.Key, NativeValue{Kind: "text", Text: value})
		}
		if change.Operation != ChangeOpInsert && change.Operation != ChangeOpUpdate && change.Operation != ChangeOpDelete {
			return nil, 0, ErrNativeData
		}
		latest[change.Table+"\x00"+key] = change
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	changes := make([]NativeChange, 0, len(latest))
	for _, change := range latest {
		changes = append(changes, change)
	}
	slices.SortFunc(changes, func(a, b NativeChange) int {
		if a.Sequence < b.Sequence {
			return -1
		}
		if a.Sequence > b.Sequence {
			return 1
		}
		return 0
	})
	return changes, events, nil
}

func readNativeChangedRow(ctx context.Context, tx *sql.Tx, change *NativeChange, budget *int64) error {
	columns, err := changeFeedColumns(ctx, tx, change.Table)
	if err != nil {
		return err
	}
	var names, sizes, keys []string
	var args []any
	for _, column := range columns {
		name := quoteIdent(column.name)
		names = append(names, name)
		sizes = append(sizes, "coalesce(length(cast("+name+" as blob)),0)")
		change.Columns = append(change.Columns, column.name)
		if column.pk > 0 {
			if column.pk > len(change.Key) {
				return ErrNativeData
			}
			keys = append(keys, name+" is ?")
			args = append(args, change.Key[column.pk-1].Text)
		}
	}
	if len(keys) != len(change.Key) {
		return ErrNativeData
	}
	where := " from " + quoteIdent(change.Table) + " where " + strings.Join(keys, " and ")
	var size int64
	err = tx.QueryRowContext(ctx, "select "+strings.Join(sizes, "+")+where, args...).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) && change.Operation == ChangeOpDelete {
		change.Operation = "remove"
		change.Columns = nil
		return nil
	}
	if err != nil || change.Operation == ChangeOpDelete {
		return ErrNativeData
	}
	if err := nativeTake(budget, size); err != nil {
		return err
	}
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	if err := tx.QueryRowContext(ctx, "select "+strings.Join(names, ",")+where, args...).Scan(pointers...); err != nil {
		return err
	}
	for _, value := range values {
		switch value := value.(type) {
		case nil:
			change.Row = append(change.Row, NativeValue{Kind: "null"})
		case string:
			if !utf8.ValidString(value) {
				return ErrNativeData
			}
			change.Row = append(change.Row, NativeValue{Kind: "text", Text: value})
		case int64:
			change.Row = append(change.Row, NativeValue{Kind: "integer", Integer: value})
		default:
			return ErrNativeData
		}
	}
	change.Operation = "upsert"
	return nil
}

func nativeTake(budget *int64, size int64) error {
	if size < 0 || size > *budget {
		return ErrNativeLimit
	}
	*budget -= size
	return nil
}
