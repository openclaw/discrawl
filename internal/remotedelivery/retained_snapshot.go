package remotedelivery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/openclaw/crawlkit/snapshot"
	crawlstore "github.com/openclaw/crawlkit/store"
)

const (
	RetainedSnapshotFormat = "discrawl-retained-snapshot/v1"
	retainedMaxRowBytes    = 4 << 20
)

// RetainedSnapshot is custody of selected current rows, not a native journal
// baseline or evidence of fresh collection. It cannot initialize an outbox.
type RetainedSnapshot struct {
	Format           string `json:"format"`
	Scope            Scope  `json:"scope"`
	ScopeSHA256      string `json:"scope_sha256"`
	SourceFileSHA256 string `json:"source_file_sha256"`
	ManifestSHA256   string `json:"manifest_sha256"`
	Rows             int64  `json:"rows"`
	RowJSONBytes     int64  `json:"row_json_bytes"`
}

type RetainedSnapshotLimits struct {
	MaxSourceBytes int64
	MaxRows        int64
	MaxBytes       int64
}

// PrepareRetainedSnapshot reads an already stopped source through immutable
// SQLite. No migrations, journal, ownership spool, credentials or provider calls
// are used. A failed output remains unqualified: preparation.json is written last.
func PrepareRetainedSnapshot(ctx context.Context, source, root, expectedSHA string, scope Scope, limits RetainedSnapshotLimits) (RetainedSnapshot, string, error) {
	var result RetainedSnapshot
	if _, ok := ctx.Deadline(); !ok {
		return result, "", errors.New("retained export requires a deadline")
	}
	if limits.MaxSourceBytes <= 0 || limits.MaxRows <= 0 || limits.MaxBytes <= 0 {
		return result, "", errors.New("retained export requires positive limits")
	}
	hash, err := hex.DecodeString(expectedSHA)
	if err != nil || len(hash) != sha256.Size || hex.EncodeToString(hash) != expectedSHA {
		return result, "", errors.New("invalid source SHA256")
	}
	scope, err = canonicalScope(scope)
	if err != nil {
		return result, "", err
	}
	if err = literalOutboxPath(source); err != nil {
		return result, "", err
	}
	if err = literalOutboxPath(root); err != nil {
		return result, "", err
	}
	before, err := retainedSourceState(source, limits.MaxSourceBytes)
	if err != nil {
		return result, "", err
	}
	if err = retainedSourceHash(ctx, source, expectedSHA, limits.MaxSourceBytes); err != nil {
		return result, "", err
	}
	if err = os.Mkdir(root, 0o700); err != nil {
		return result, "", err
	}
	// URL escaping preserves literal ?, # and spaces in source filenames.
	uri := (&url.URL{Scheme: "file", Path: source, RawQuery: "immutable=1"}).String()
	db, err := crawlstore.OpenReadOnly(ctx, uri)
	if err != nil {
		return result, "", err
	}
	defer func() { _ = db.Close() }()
	tx, err := db.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, "", err
	}
	defer func() { _ = tx.Rollback() }()
	tables := []string{"messages", "guilds", "channels", "members", "sync_state"}
	for _, table := range tables {
		if err = validateRetainedTable(ctx, tx, table); err != nil {
			return result, "", err
		}
	}
	var rows, bytes int64
	filter := func(ctx context.Context, tx *sql.Tx, table string, row map[string]any) (bool, error) {
		_, included, err := scopedSeedRow(ctx, tx, table, row, scope)
		if err != nil || !included {
			return false, err
		}
		body, err := json.Marshal(row)
		if err != nil {
			return false, err
		}
		if len(body) > retainedMaxRowBytes {
			return false, errors.New("retained encoded row too large")
		}
		if rows >= limits.MaxRows || int64(len(body)) > limits.MaxBytes-bytes {
			return false, errors.New("retained export limit exceeded")
		}
		rows++
		bytes += int64(len(body))
		return true, nil
	}
	manifest, err := snapshot.Export(ctx, snapshot.ExportOptions{ReadTx: tx, RootDir: filepath.Join(root, "snapshot"), Tables: tables, FilterTx: filter, MaxShardBytes: retainedMaxRowBytes})
	if err != nil {
		return result, "", err
	}
	for _, table := range manifest.Tables {
		if !sameSeedColumns(table.Name, table.Columns) || (table.Name == "guilds" && table.Rows != len(scope.GuildIDs)) {
			return result, "", ErrProjection
		}
	}
	if err = tx.Commit(); err != nil {
		return result, "", err
	}
	if err = db.Close(); err != nil {
		return result, "", err
	}
	if err = retainedSourceHash(ctx, source, expectedSHA, limits.MaxSourceBytes); err != nil {
		return result, "", err
	}
	after, err := retainedSourceState(source, limits.MaxSourceBytes)
	if err != nil || !sameRetainedSource(before, after) {
		return result, "", errors.New("retained source changed")
	}
	body, err := os.ReadFile(filepath.Join(root, "snapshot", snapshot.ManifestName))
	if err != nil {
		return result, "", err
	}
	digest := sha256.Sum256(body)
	scopeSHA, err := ScopeSHA256(scope)
	if err != nil {
		return result, "", err
	}
	result = RetainedSnapshot{Format: RetainedSnapshotFormat, Scope: scope, ScopeSHA256: scopeSHA, SourceFileSHA256: expectedSHA, ManifestSHA256: hex.EncodeToString(digest[:]), Rows: rows, RowJSONBytes: bytes}
	body, err = json.Marshal(result)
	if err != nil {
		return RetainedSnapshot{}, "", err
	}
	body = append(body, '\n')
	if err = writeSeedFile(filepath.Join(root, "preparation.json"), body); err != nil {
		return RetainedSnapshot{}, "", err
	}
	digest = sha256.Sum256(body)
	return result, hex.EncodeToString(digest[:]), nil
}

// Validate empty tables too, before the exporter materializes any rows. SQL
// byte lengths bound large TEXT/BLOB values before they enter Go memory.
func validateRetainedTable(ctx context.Context, tx *sql.Tx, table string) error {
	names, err := retainedTableColumns(ctx, tx, table)
	if err != nil {
		return err
	}
	if !sameSeedColumns(table, names) {
		return ErrProjection
	}
	if err = validateSeedStorage(ctx, tx, table); err != nil {
		return err
	}
	var sizes []string
	for _, name := range names {
		sizes = append(sizes, "coalesce(length(CAST(\""+name+"\" AS BLOB)),0)")
	}
	var found int
	err = tx.QueryRowContext(ctx, "SELECT 1 FROM \""+table+"\" WHERE "+strings.Join(sizes, "+")+">? LIMIT 1", retainedMaxRowBytes).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("retained source row too large")
}

func retainedTableColumns(ctx context.Context, tx *sql.Tx, table string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, "PRAGMA table_info(\""+table+"\")")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var cid, notnull, pk int
		var name, kind string
		var defaultValue any
		if err = rows.Scan(&cid, &name, &kind, &notnull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

type retainedFileState struct{ Info os.FileInfo }

func retainedSourceState(path string, maxBytes int64) ([]retainedFileState, error) {
	var state []retainedFileState
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		info, err := os.Lstat(path + suffix)
		if errors.Is(err, os.ErrNotExist) && suffix != "" {
			state = append(state, retainedFileState{})
			continue
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || (suffix == "" && info.Size() > maxBytes) || ((suffix == "-wal" || suffix == "-journal") && info.Size() != 0) {
			return nil, errors.New("retained source must be a stopped regular database")
		}
		state = append(state, retainedFileState{Info: info})
	}
	return state, nil
}

func sameRetainedSource(a, b []retainedFileState) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i].Info, b[i].Info
		if x == nil || y == nil {
			if x != nil || y != nil {
				return false
			}
			continue
		}
		if !os.SameFile(x, y) || x.Size() != y.Size() || x.Mode() != y.Mode() || !x.ModTime().Equal(y.ModTime()) {
			return false
		}
	}
	return true
}

func retainedSourceHash(ctx context.Context, path, expected string, maxBytes int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	buf := make([]byte, 128<<10)
	var total int64
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		n, readErr := f.Read(buf)
		total += int64(n)
		if total > maxBytes {
			return errors.New("retained source exceeds byte limit")
		}
		if n > 0 {
			_, _ = h.Write(buf[:n])
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if hex.EncodeToString(h.Sum(nil)) != expected {
		return errors.New("retained source SHA256 mismatch")
	}
	return nil
}
