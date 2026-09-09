//go:build ignore

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/openclaw/crawlkit/snapshot"
	"github.com/openclaw/discrawl/internal/share"
	"modernc.org/sqlite"
)

const (
	maxSourceBytes = int64(10 << 30)
	reserveBytes   = int64(8 << 30)
	maxTextBytes   = int64(256 << 20)
	maxCellBytes   = int64(1 << 20)
	expectedCells  = int64(15264)
	expectedFixes  = int64(46)
	expectedDrop   = int64(67)
)

var inputNames = []string{"discrawl.db", "discrawl.db-shm", "discrawl.db-wal"}

const attachmentDDL = `CREATE TABLE message_attachments (
	attachment_id text primary key, message_id text not null, guild_id text not null,
	channel_id text not null, author_id text, filename text not null, content_type text,
	size integer not null default 0, url text, proxy_url text, text_content text not null default '',
	media_path text, content_sha256 text, content_size integer not null default 0,
	fetched_at text, fetch_status text not null default '', fetch_error text not null default '',
	updated_at text not null)`

var attachmentColumns = []string{
	"attachment_id", "message_id", "guild_id", "channel_id", "author_id", "filename",
	"content_type", "size", "url", "proxy_url", "text_content", "media_path",
	"content_sha256", "content_size", "fetched_at", "fetch_status", "fetch_error", "updated_at",
}

type repairResult struct {
	SchemaVersion     int              `json:"schema_version"`
	Scope             string           `json:"scope"`
	Complete          bool             `json:"complete"`
	Stage             string           `json:"stage"`
	Error             string           `json:"error"`
	ChangedCells      int64            `json:"changed_cells"`
	RemovedBytes      int64            `json:"removed_bytes"`
	Tails             [3]int64         `json:"tails"`
	OriginalUnchanged bool             `json:"original_unchanged"`
	ExportRows        map[string]int64 `json:"export_rows"`
}

func main() {
	syscall.Umask(0077)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, deadline := context.WithTimeout(ctx, 45*time.Minute)
	defer deadline()
	result := runRepair(ctx, os.Args[1:])
	if json.NewEncoder(os.Stdout).Encode(result) != nil || !result.Complete {
		os.Exit(1)
	}
}

type originalFile struct {
	info os.FileInfo
	hash [32]byte
}

func runRepair(ctx context.Context, args []string) (out repairResult) {
	out = repairResult{SchemaVersion: 1, Scope: "restored_cache.message_attachments.text_content",
		Stage: "arguments", Error: "arguments", ExportRows: map[string]int64{}}
	defer func() {
		if recover() != nil {
			out.Complete, out.Error = false, "internal"
		}
	}()
	if len(args) != 2 {
		return out
	}
	source, scratch := args[0], args[1]
	if !filepath.IsAbs(source) || !filepath.IsAbs(scratch) || filepath.Base(source) != ".discrawl-ci" {
		return out
	}
	for _, directory := range []string{source, scratch} {
		info, err := os.Lstat(directory)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return out
		}
	}
	relative, err := filepath.Rel(source, scratch)
	if err != nil || relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))) {
		return out
	}
	sourceInfo, _ := os.Stat(source)
	scratchInfo, _ := os.Stat(scratch)
	if sourceInfo.Sys().(*syscall.Stat_t).Dev != scratchInfo.Sys().(*syscall.Stat_t).Dev {
		return out
	}
	out.Stage, out.Error = "originals", "input_files"
	originals, total, err := inspectOriginals(source)
	if err != nil {
		return out
	}
	if err = capacity(scratch, total); err != nil {
		out.Error = "capacity"
		return out
	}
	for name, stamp := range originals {
		stamp.hash, err = hashOriginal(ctx, source, name, stamp.info)
		if err != nil {
			return out
		}
		originals[name] = stamp
	}
	originalPath := source
	defer func() {
		checkCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		out.OriginalUnchanged = originalsMatch(checkCtx, originalPath, originals)
		if !out.OriginalUnchanged {
			out.Complete, out.Error = false, "original_changed"
		}
	}()
	work, err := os.MkdirTemp(scratch, "repair-work-")
	if err != nil {
		out.Error = "copy"
		return out
	}
	defer os.RemoveAll(work)
	if err = os.Chmod(work, 0700); err != nil {
		out.Error = "copy"
		return out
	}
	out.Stage, out.Error = "copy", "copy"
	for _, name := range inputNames {
		stamp, exists := originals[name]
		if !exists {
			continue
		}
		if err = copyOriginal(ctx, source, work, name, stamp); err != nil {
			return out
		}
	}
	workDB := filepath.Join(work, inputNames[0])
	out.Stage, out.Error = "repair", "sqlite"
	db, logical, err := prepareWorkingDatabase(ctx, workDB, scratch, capacity)
	if err != nil {
		out.Error = fixedError(err)
		return out
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return out
	}
	expectedHash, err := repairTransaction(ctx, conn, nil)
	closeErr := conn.Close()
	if err != nil || closeErr != nil {
		out.Error = fixedError(err)
		return out
	}
	if err = db.Close(); err != nil {
		return out
	}
	out.ChangedCells, out.RemovedBytes, out.Tails = expectedFixes, expectedDrop, [3]int64{25, 21, 0}
	out.Stage, out.Error = "reopen", "verification"
	db, err = openDatabase(workDB, true)
	if err != nil {
		return out
	}
	defer db.Close()
	if err = verifyRepaired(ctx, db, expectedHash); err != nil {
		return out
	}
	sourceCounts, err := snapshotCounts(ctx, db)
	if err != nil {
		return out
	}
	stage, err := os.MkdirTemp(scratch, "repair-ready-")
	if err != nil {
		return out
	}
	defer os.RemoveAll(stage)
	if err = os.Chmod(stage, 0700); err != nil {
		return out
	}
	out.Stage, out.Error = "standalone", "backup"
	standalone := filepath.Join(stage, inputNames[0])
	if err = backupDatabase(ctx, db, standalone, scratch); err != nil {
		return out
	}
	if err = db.Close(); err != nil {
		return out
	}
	ready, err := openDatabase(standalone, true)
	if err != nil {
		return out
	}
	defer ready.Close()
	if err = verifyRepaired(ctx, ready, expectedHash); err != nil {
		out.Error = "verification"
		return out
	}
	out.Stage, out.Error = "export", "strict_export"
	exportDir, err := os.MkdirTemp(scratch, "repair-export-")
	if err != nil {
		return out
	}
	defer os.RemoveAll(exportDir)
	if err = os.Chmod(exportDir, 0700); err != nil {
		return out
	}
	out.ExportRows, err = strictExport(ctx, ready, exportDir, scratch, logical, sourceCounts)
	if err != nil {
		return out
	}
	if err = ready.Close(); err != nil {
		return out
	}
	entries, err := os.ReadDir(stage)
	if err != nil || len(entries) != 1 || entries[0].Name() != inputNames[0] {
		out.Error = "standalone_files"
		return out
	}
	if info, err := os.Lstat(standalone); err != nil || !info.Mode().IsRegular() ||
		info.Size() <= 0 || info.Size() > maxSourceBytes || info.Sys().(*syscall.Stat_t).Nlink != 1 {
		out.Error = "standalone_files"
		return out
	}
	if err = os.Chmod(standalone, 0600); err != nil || !originalsMatch(ctx, source, originals) {
		out.Error = "original_changed"
		return out
	}
	out.Stage, out.Error = "stage", "staging"
	kept := filepath.Join(scratch, "originals")
	if _, err = os.Lstat(kept); !errors.Is(err, os.ErrNotExist) {
		return out
	}
	if err = os.Rename(source, kept); err != nil {
		return out
	}
	originalPath = kept
	if err = os.Rename(stage, source); err != nil {
		if os.Rename(kept, source) == nil {
			originalPath = source
		}
		return out
	}
	out.Stage, out.Error, out.Complete = "cache_ready", "none", true
	return out
}

func fixedError(err error) string {
	for _, value := range []string{"schema", "aggregate", "cas", "transaction", "verification", "capacity", "time_limit"} {
		if err != nil && err.Error() == value {
			return value
		}
	}
	return "sqlite"
}

func inspectOriginals(directory string) (map[string]originalFile, int64, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, 0, errors.New("input_files")
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return nil, 0, errors.New("input_files")
	}
	entries, readErr := dir.ReadDir(4)
	closeErr := dir.Close()
	if (readErr != nil && !errors.Is(readErr, io.EOF)) || closeErr != nil || len(entries) > 3 {
		return nil, 0, errors.New("input_files")
	}
	files, total := map[string]originalFile{}, int64(0)
	for _, entry := range entries {
		name := entry.Name()
		if name != inputNames[0] && name != inputNames[1] && name != inputNames[2] {
			return nil, 0, errors.New("input_files")
		}
		info, err := root.Lstat(name)
		if err != nil || !info.Mode().IsRegular() || info.Sys().(*syscall.Stat_t).Nlink != 1 ||
			info.Size() < 0 || info.Size() > maxSourceBytes-total {
			return nil, 0, errors.New("input_files")
		}
		total += info.Size()
		files[name] = originalFile{info: info}
	}
	if file, ok := files[inputNames[0]]; !ok || file.info.Size() == 0 {
		return nil, 0, errors.New("input_files")
	}
	return files, total, nil
}

func openOriginal(directory, name string, expected os.FileInfo) (*os.File, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !os.SameFile(info, expected) || !info.Mode().IsRegular() ||
		info.Size() != expected.Size() || info.Sys().(*syscall.Stat_t).Nlink != 1 {
		file.Close()
		return nil, errors.New("input_files")
	}
	return file, nil
}

type boundedReader struct {
	ctx context.Context
	r   io.Reader
}

func (r boundedReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func hashOriginal(ctx context.Context, directory, name string, info os.FileInfo) ([32]byte, error) {
	file, err := openOriginal(directory, name, info)
	if err != nil {
		return [32]byte{}, err
	}
	defer file.Close()
	digest := sha256.New()
	n, err := io.Copy(digest, boundedReader{ctx, io.LimitReader(file, info.Size()+1)})
	if err != nil || n != info.Size() {
		return [32]byte{}, errors.New("input_files")
	}
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func originalsMatch(ctx context.Context, directory string, before map[string]originalFile) bool {
	after, _, err := inspectOriginals(directory)
	if err != nil || len(after) != len(before) {
		return false
	}
	for name, stamp := range before {
		current, exists := after[name]
		if !exists || !os.SameFile(stamp.info, current.info) || stamp.info.Mode() != current.info.Mode() {
			return false
		}
		digest, err := hashOriginal(ctx, directory, name, stamp.info)
		if err != nil || digest != stamp.hash {
			return false
		}
	}
	return true
}

func capacity(directory string, additional int64) error {
	var space syscall.Statfs_t
	if additional < 0 || syscall.Statfs(directory, &space) != nil || space.Bsize <= 0 {
		return errors.New("capacity")
	}
	free := uint64(space.Bsize) * space.Bavail
	if free < uint64(additional+reserveBytes) {
		return errors.New("capacity")
	}
	return nil
}

func copyOriginal(ctx context.Context, source, destination, name string, stamp originalFile) error {
	input, err := openOriginal(source, name, stamp.info)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(filepath.Join(destination, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer output.Close()
	digest := sha256.New()
	buffer := make([]byte, 1<<20)
	left := stamp.info.Size()
	for left > 0 {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = capacity(destination, int64(len(buffer))); err != nil {
			return err
		}
		n, readErr := input.Read(buffer[:min(int64(len(buffer)), left)])
		if n > 0 {
			if _, err = output.Write(buffer[:n]); err != nil {
				return err
			}
			digest.Write(buffer[:n])
			left -= int64(n)
		}
		if readErr != nil && !(errors.Is(readErr, io.EOF) && left == 0) {
			return readErr
		}
		if n == 0 && left > 0 {
			return io.ErrUnexpectedEOF
		}
	}
	if !bytes.Equal(digest.Sum(nil), stamp.hash[:]) {
		return errors.New("input_files")
	}
	if err = output.Sync(); err != nil {
		return err
	}
	return output.Close()
}

func openDatabase(file string, readOnly bool) (*sql.DB, error) {
	u := &url.URL{Scheme: "file", Path: file}
	query := url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(0)", "trusted_schema(OFF)"}}
	if readOnly {
		query.Set("mode", "ro")
		query.Add("_pragma", "query_only(ON)")
	}
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err == nil {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	return db, err
}

func logicalBytes(ctx context.Context, db *sql.DB) (int64, error) {
	var pages, size int64
	if db.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages) != nil ||
		db.QueryRowContext(ctx, "PRAGMA page_size").Scan(&size) != nil ||
		pages <= 0 || size <= 0 || pages > maxSourceBytes/size {
		return 0, errors.New("capacity")
	}
	return pages * size, nil
}

func prepareWorkingDatabase(ctx context.Context, file, scratch string, admit func(string, int64) error) (*sql.DB, int64, error) {
	// Admit a full measured checkpoint before opening a writer: its close can
	// also checkpoint a WAL, including on an early error path.
	inspection, err := openDatabase(file, true)
	if err != nil {
		return nil, 0, errors.New("sqlite")
	}
	logical, err := logicalBytes(ctx, inspection)
	closeErr := inspection.Close()
	if err != nil {
		return nil, 0, err
	}
	if closeErr != nil {
		return nil, 0, errors.New("sqlite")
	}
	if err = admit(scratch, logical); err != nil {
		return nil, 0, errors.New("capacity")
	}
	db, err := openDatabase(file, false)
	if err != nil {
		return nil, 0, errors.New("sqlite")
	}
	var journal string
	if err = db.QueryRowContext(ctx, "PRAGMA journal_mode=DELETE").Scan(&journal); err != nil || journal != "delete" {
		db.Close()
		return nil, 0, errors.New("sqlite")
	}
	// Recheck resident space after consolidation for rollback and backup.
	if err = admit(scratch, 2*logical); err != nil {
		db.Close()
		return nil, 0, errors.New("capacity")
	}
	return db, logical, nil
}

type candidate struct {
	rowid int64
	old   []byte
	tail  int
}

func incompleteTail(value []byte) int {
	if len(value) != 8192 || utf8.Valid(value) {
		return 0
	}
	for count := 1; count <= 3; count++ {
		prefix, tail := value[:len(value)-count], value[len(value)-count:]
		if utf8.Valid(prefix) && !utf8.FullRune(tail) {
			return count
		}
	}
	return 0
}

func validateSchema(ctx context.Context, conn *sql.Conn) error {
	var encoding, ddl, kind string
	var triggers int
	if conn.QueryRowContext(ctx, "PRAGMA encoding").Scan(&encoding) != nil || encoding != "UTF-8" ||
		conn.QueryRowContext(ctx, "SELECT type, sql FROM main.sqlite_schema WHERE name='message_attachments'").Scan(&kind, &ddl) != nil ||
		kind != "table" || conn.QueryRowContext(ctx, "SELECT count(*) FROM main.sqlite_schema WHERE type='trigger' AND tbl_name='message_attachments'").Scan(&triggers) != nil || triggers != 0 {
		return errors.New("schema")
	}
	if normalizedDDL(ddl) != normalizedDDL(attachmentDDL) {
		return errors.New("schema")
	}
	rows, err := conn.QueryContext(ctx, "PRAGMA main.index_list(message_attachments)")
	if err != nil {
		return errors.New("schema")
	}
	var names []string
	for rows.Next() {
		var seq, unique, partial int
		var name, origin string
		if rows.Scan(&seq, &name, &unique, &origin, &partial) != nil || partial != 0 {
			rows.Close()
			return errors.New("schema")
		}
		names = append(names, name)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil || closeErr != nil {
		return errors.New("schema")
	}
	for _, name := range names {
		index, err := conn.QueryContext(ctx, "SELECT cid, name, key FROM pragma_index_xinfo(?)", name)
		if err != nil {
			return errors.New("schema")
		}
		for index.Next() {
			var cid, key int
			var column sql.NullString
			if index.Scan(&cid, &column, &key) != nil || cid < -1 || column.String == "text_content" {
				index.Close()
				return errors.New("schema")
			}
		}
		err = index.Err()
		closeErr = index.Close()
		if err != nil || closeErr != nil {
			return errors.New("schema")
		}
	}
	return nil
}

func normalizedDDL(value string) string {
	value = strings.TrimSuffix(strings.TrimSpace(value), ";")
	var normalized strings.Builder
	quoted := false
	for index := 0; index < len(value); index++ {
		char := value[index]
		if char == '\'' {
			normalized.WriteByte(char)
			if quoted && index+1 < len(value) && value[index+1] == '\'' {
				index++
				normalized.WriteByte('\'')
			} else {
				quoted = !quoted
			}
		} else if quoted {
			normalized.WriteByte(char)
		} else if !strings.ContainsRune(" \t\r\n", rune(char)) {
			if char >= 'A' && char <= 'Z' {
				char += 'a' - 'A'
			}
			normalized.WriteByte(char)
		}
	}
	return normalized.String()
}

func frame(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	digest.Write(length[:])
	digest.Write(value)
}

func attachmentSnapshot(ctx context.Context, conn *sql.Conn, before bool) ([32]byte, []candidate, error) {
	var count, byteCount, largest int64
	if conn.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(length(CAST(text_content AS BLOB))),0),
		coalesce(max(length(CAST(text_content AS BLOB))),0) FROM main.message_attachments`).Scan(&count, &byteCount, &largest) != nil ||
		count != expectedCells || byteCount > maxTextBytes || largest > maxCellBytes {
		return [32]byte{}, nil, errors.New("aggregate")
	}
	parts := []string{"rowid"}
	for _, column := range attachmentColumns {
		parts = append(parts, `typeof("`+column+`")`, `CAST("`+column+`" AS BLOB)`)
	}
	rows, err := conn.QueryContext(ctx, "SELECT "+strings.Join(parts, ",")+" FROM main.message_attachments ORDER BY rowid")
	if err != nil {
		return [32]byte{}, nil, err
	}
	defer rows.Close()
	digest := sha256.New()
	var candidates []candidate
	var valid, seen int64
	var tails [3]int64
	for rows.Next() {
		var rowid int64
		types := make([]string, len(attachmentColumns))
		values := make([][]byte, len(attachmentColumns))
		dest := []any{&rowid}
		for i := range types {
			dest = append(dest, &types[i], &values[i])
		}
		if rows.Scan(dest...) != nil || types[10] != "text" || int64(len(values[10])) > maxCellBytes {
			return [32]byte{}, nil, errors.New("aggregate")
		}
		value := values[10]
		if utf8.Valid(value) {
			valid++
		} else {
			tail := incompleteTail(value)
			if !before || tail == 0 || len(candidates) >= int(expectedFixes) {
				return [32]byte{}, nil, errors.New("aggregate")
			}
			candidates = append(candidates, candidate{rowid, bytes.Clone(value), tail})
			tails[tail-1]++
			values[10] = value[:len(value)-tail]
		}
		var id [8]byte
		binary.LittleEndian.PutUint64(id[:], uint64(rowid))
		frame(digest, id[:])
		for i := range types {
			frame(digest, []byte(types[i]))
			frame(digest, values[i])
		}
		seen++
	}
	if rows.Err() != nil || seen != expectedCells ||
		(before && (valid != 15218 || tails != [3]int64{25, 21, 0})) || (!before && valid != expectedCells) {
		return [32]byte{}, nil, errors.New("aggregate")
	}
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result, candidates, nil
}

func repairTransaction(ctx context.Context, conn *sql.Conn, hook func(*sql.Conn, int) error) ([32]byte, error) {
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return [32]byte{}, errors.New("transaction")
	}
	defer conn.ExecContext(context.Background(), "ROLLBACK")
	if err := validateSchema(ctx, conn); err != nil {
		return [32]byte{}, err
	}
	expected, candidates, err := attachmentSnapshot(ctx, conn, true)
	if err != nil || len(candidates) != int(expectedFixes) {
		return [32]byte{}, errors.New("aggregate")
	}
	var start int64
	if conn.QueryRowContext(ctx, "SELECT total_changes()").Scan(&start) != nil {
		return [32]byte{}, errors.New("transaction")
	}
	var removed int64
	for index, cell := range candidates {
		if hook != nil {
			if err = hook(conn, index); err != nil {
				return [32]byte{}, errors.New("transaction")
			}
		}
		prefix := cell.old[:len(cell.old)-cell.tail]
		if !utf8.Valid(prefix) {
			return [32]byte{}, errors.New("aggregate")
		}
		result, err := conn.ExecContext(ctx, `UPDATE main.message_attachments
			SET text_content = CAST(?1 AS TEXT)
			WHERE rowid = ?2 AND typeof(text_content) = 'text' AND CAST(text_content AS BLOB) = ?3`,
			prefix, cell.rowid, cell.old)
		if err != nil {
			return [32]byte{}, errors.New("cas")
		}
		n, err := result.RowsAffected()
		if err != nil || n != 1 {
			return [32]byte{}, errors.New("cas")
		}
		removed += int64(cell.tail)
	}
	var end int64
	actual, _, err := attachmentSnapshot(ctx, conn, false)
	if err != nil || actual != expected || removed != expectedDrop ||
		conn.QueryRowContext(ctx, "SELECT total_changes()").Scan(&end) != nil || end-start != expectedFixes {
		return [32]byte{}, errors.New("verification")
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		return [32]byte{}, errors.New("transaction")
	}
	return expected, nil
}

func verifyRepaired(ctx context.Context, db *sql.DB, expected [32]byte) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return err
	}
	count := 0
	for rows.Next() {
		var value string
		if rows.Scan(&value) != nil || value != "ok" {
			rows.Close()
			return errors.New("verification")
		}
		count++
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil || closeErr != nil || count != 1 || validateSchema(ctx, conn) != nil {
		return errors.New("verification")
	}
	actual, _, err := attachmentSnapshot(ctx, conn, false)
	if err != nil || actual != expected {
		return errors.New("verification")
	}
	return nil
}

func backupDatabase(ctx context.Context, db *sql.DB, destination, scratch string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		return errors.New("backup")
	}
	return conn.Raw(func(driver any) (resultErr error) {
		backup, err := driver.(interface {
			NewBackup(string) (*sqlite.Backup, error)
		}).NewBackup(destination)
		if err != nil {
			return err
		}
		defer func() {
			if err := backup.Finish(); resultErr == nil {
				resultErr = err
			}
		}()
		for {
			if err = ctx.Err(); err != nil {
				return err
			}
			if err = capacity(scratch, 8<<20); err != nil {
				return err
			}
			more, err := backup.Step(256)
			if err != nil {
				return err
			}
			if info, err := os.Stat(destination); err != nil || info.Size() > maxSourceBytes {
				return errors.New("capacity")
			}
			if !more {
				return nil
			}
		}
	})
}

type rowCounter interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func snapshotCounts(ctx context.Context, reader rowCounter) (map[string]int64, error) {
	counts := make(map[string]int64, len(share.SnapshotTables))
	for _, table := range share.SnapshotTables {
		var count int64
		if reader.QueryRowContext(ctx, `SELECT count(*) FROM "`+table+`"`).Scan(&count) != nil || count < 0 {
			return nil, errors.New("strict_export")
		}
		counts[table] = count
	}
	return counts, nil
}

func strictExport(ctx context.Context, db *sql.DB, directory, scratch string, logical int64, sourceCounts map[string]int64) (map[string]int64, error) {
	if capacity(scratch, 40<<20) != nil {
		return nil, errors.New("capacity")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var exceeded atomic.Bool
	done := make(chan struct{})
	stopped := make(chan struct{})
	defer func() {
		close(done)
		<-stopped
	}()
	// Only metadata is monitored; the existing exporter owns all row encoding.
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if capacity(scratch, 40<<20) != nil || exportBytes(directory) > 2*logical+(40<<20) {
					exceeded.Store(true)
					cancel()
					return
				}
			}
		}
	}()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	counts, err := snapshotCounts(ctx, tx)
	if err != nil || !reflect.DeepEqual(counts, sourceCounts) {
		return nil, errors.New("strict_export")
	}
	manifest, err := snapshot.Export(ctx, snapshot.ExportOptions{ReadTx: tx, RootDir: directory,
		Tables: share.SnapshotTables, MaxShardBytes: 40 << 20})
	if err != nil || exceeded.Load() || capacity(scratch, 0) != nil ||
		exportBytes(directory) > 2*logical+(40<<20) || len(manifest.Tables) != 8 {
		return nil, errors.New("strict_export")
	}
	if err = tx.Commit(); err != nil {
		return nil, errors.New("strict_export")
	}
	disk, err := snapshot.ReadManifest(directory)
	if err != nil || !reflect.DeepEqual(disk, manifest) {
		return nil, errors.New("strict_export")
	}
	seen := map[string]bool{}
	for _, table := range manifest.Tables {
		expected, exists := counts[table.Name]
		if !exists || seen[table.Name] || int64(table.Rows) != expected {
			return nil, errors.New("strict_export")
		}
		seen[table.Name] = true
	}
	return counts, nil
}

func exportBytes(directory string) int64 {
	var size int64
	entries := 0
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		entries++
		if err != nil || entries > 100000 || entry.Type()&os.ModeSymlink != 0 {
			return errors.New("capacity")
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil || !info.Mode().IsRegular() || info.Size() < 0 {
				return errors.New("capacity")
			}
			size += info.Size()
			if size > 2*maxSourceBytes+(40<<20) {
				return errors.New("capacity")
			}
		}
		return nil
	})
	if err != nil {
		return 2*maxSourceBytes + (40 << 20) + 1
	}
	return size
}
