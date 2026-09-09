//go:build ignore

// This temporary Linux-only diagnostic never opens the restored files in SQLite.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	maxRestoredBytes = int64(10 << 30)
	maxCombinedBytes = int64(20 << 30)
)

var inputNames = [...]string{"discrawl.db", "discrawl.db-shm", "discrawl.db-wal"}

type bounds struct {
	duration time.Duration
	rows     int64
	bytes    int64
	cell     int64
	files    int64
	combined int64
}

func approvedBounds() bounds {
	return bounds{240 * time.Second, 1_000_000, 256 << 20, 1 << 20, maxRestoredBytes, maxCombinedBytes}
}

type buckets struct {
	Below int64 `json:"below_8192"`
	Equal int64 `json:"equal_8192"`
	Above int64 `json:"above_8192"`
}

func (b *buckets) add(n int64) {
	switch {
	case n < 8192:
		b.Below++
	case n == 8192:
		b.Equal++
	default:
		b.Above++
	}
}

type storageCounts struct {
	Text    int64 `json:"text"`
	Blob    int64 `json:"blob"`
	Integer int64 `json:"integer"`
	Real    int64 `json:"real"`
	Null    int64 `json:"null"`
}

type aggregate struct {
	SchemaVersion     int           `json:"schema_version"`
	Scope             string        `json:"scope"`
	Complete          bool          `json:"complete"`
	AbortReason       string        `json:"abort_reason"`
	OriginalUnchanged bool          `json:"original_unchanged"`
	TotalCells        int64         `json:"total_cells"`
	Storage           storageCounts `json:"storage_classes"`
	Lengths           buckets       `json:"byte_lengths"`
	ValidText         int64         `json:"valid_text"`
	InvalidText       int64         `json:"invalid_text"`
	Incomplete1       buckets       `json:"incomplete_tail_1"`
	Incomplete2       buckets       `json:"incomplete_tail_2"`
	Incomplete3       buckets       `json:"incomplete_tail_3"`
	OtherMalformed    int64         `json:"other_malformed_text"`
	StrictCandidates  int64         `json:"strict_candidates"`
}

func emptyAggregate() aggregate {
	return aggregate{SchemaVersion: 1, Scope: "restored_cache.message_attachments.text_content", AbortReason: "none"}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result := execute(ctx, os.Args[1:], approvedBounds())
	if json.NewEncoder(os.Stdout).Encode(result) != nil || !result.Complete {
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string, limit bounds) (out aggregate) {
	out = emptyAggregate()
	defer func() {
		if recover() != nil {
			out = emptyAggregate()
			out.AbortReason = "internal"
		}
	}()
	if len(args) != 2 || limit.duration <= 0 || limit.duration > 240*time.Second || limit.rows <= 0 || limit.rows > 1_000_000 ||
		limit.bytes <= 0 || limit.bytes > 256<<20 || limit.cell <= 0 || limit.cell > 1<<20 ||
		limit.files <= 0 || limit.files > maxRestoredBytes || limit.combined <= 0 || limit.combined > maxCombinedBytes {
		out.AbortReason = "arguments"
		return out
	}
	ctx, cancel := context.WithTimeout(ctx, limit.duration)
	defer cancel()
	return diagnose(ctx, args[0], args[1], limit)
}

type fileStamp struct {
	info os.FileInfo
	hash [sha256.Size]byte
}

type fileSet map[string]fileStamp

func diagnose(ctx context.Context, source, scratch string, limit bounds) (out aggregate) {
	out = emptyAggregate()
	abort := func(reason string) aggregate {
		out.Complete = false
		if ctx.Err() != nil {
			reason = "time_limit"
		}
		out.AbortReason = reason
		return out
	}
	sourceInfo, err := os.Lstat(source)
	if err != nil || !sourceInfo.IsDir() || sourceInfo.Mode()&os.ModeSymlink != 0 {
		return abort("input_files")
	}
	sourcePath, err := filepath.EvalSymlinks(source)
	if err != nil {
		return abort("input_files")
	}
	scratchPath, err := filepath.EvalSymlinks(scratch)
	if err != nil {
		return abort("arguments")
	}
	sourcePath, err = filepath.Abs(sourcePath)
	if err != nil {
		return abort("arguments")
	}
	scratchPath, err = filepath.Abs(scratchPath)
	if err != nil {
		return abort("arguments")
	}
	relative, err := filepath.Rel(sourcePath, scratchPath)
	if err != nil || relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))) {
		return abort("arguments")
	}
	root, err := os.OpenRoot(sourcePath)
	if err != nil {
		return abort("input_files")
	}
	defer root.Close()
	openedInfo, err := root.Stat(".")
	if err != nil || !os.SameFile(sourceInfo, openedInfo) {
		return abort("input_files")
	}
	original, total, reason := inspectFiles(root, limit.files)
	if reason != "" {
		return abort(reason)
	}
	if total > limit.combined/2 {
		return abort("file_budget")
	}
	for name, stamp := range original {
		stamp.hash, err = hashFile(ctx, root, name, stamp.info)
		if err != nil {
			return abort("input_files")
		}
		original[name] = stamp
	}
	work, err := os.MkdirTemp(scratchPath, "discrawl-diagnostic-")
	if err != nil {
		return abort("copy")
	}
	defer os.RemoveAll(work)
	if err = os.Chmod(work, 0700); err != nil {
		return abort("copy")
	}
	workRoot, err := os.OpenRoot(work)
	if err != nil {
		return abort("copy")
	}
	defer workRoot.Close()
	for name, stamp := range original {
		if err = copyFile(ctx, root, workRoot, name, stamp); err != nil {
			return abort("copy")
		}
	}
	// The complete available DB/WAL/SHM set is copied before SQLite opens anything.
	reason = scanCopy(ctx, filepath.Join(work, inputNames[0]), limit, &out)
	_, workBytes, workReason := inspectFiles(workRoot, limit.files)
	if workReason != "" || total > limit.combined-workBytes {
		reason = "file_budget"
	}
	out.OriginalUnchanged = unchanged(ctx, root, original, limit.files)
	currentInfo, statErr := os.Lstat(source)
	out.OriginalUnchanged = out.OriginalUnchanged && statErr == nil && os.SameFile(sourceInfo, currentInfo)
	if !out.OriginalUnchanged {
		return abort("original_changed")
	}
	if reason != "" {
		return abort(reason)
	}
	if ctx.Err() != nil {
		return abort("time_limit")
	}
	out.Complete = true
	return out
}

func inspectFiles(root *os.Root, budget int64) (fileSet, int64, string) {
	directory, err := root.Open(".")
	if err != nil {
		return nil, 0, "input_files"
	}
	entries, readErr := directory.ReadDir(4)
	closeErr := directory.Close()
	if (readErr != nil && !errors.Is(readErr, io.EOF)) || closeErr != nil || len(entries) > len(inputNames) {
		return nil, 0, "input_files"
	}
	result := make(fileSet)
	var total int64
	for _, entry := range entries {
		name := entry.Name()
		if name != inputNames[0] && name != inputNames[1] && name != inputNames[2] {
			return nil, 0, "input_files"
		}
		info, err := root.Lstat(name)
		if err != nil || !info.Mode().IsRegular() || info.Size() < 0 {
			return nil, 0, "input_files"
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return nil, 0, "input_files"
		}
		if info.Size() > budget-total {
			return nil, 0, "file_budget"
		}
		total += info.Size()
		result[name] = fileStamp{info: info}
	}
	if main, ok := result[inputNames[0]]; !ok || main.info.Size() == 0 {
		return nil, 0, "input_files"
	}
	return result, total, ""
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func openOriginal(root *os.Root, name string, expected os.FileInfo) (*os.File, error) {
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	current, err := f.Stat()
	if err != nil || !os.SameFile(current, expected) || !current.Mode().IsRegular() || current.Size() != expected.Size() {
		f.Close()
		return nil, errors.New("input_files")
	}
	stat, ok := current.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		f.Close()
		return nil, errors.New("input_files")
	}
	return f, nil
}

func hashFile(ctx context.Context, root *os.Root, name string, info os.FileInfo) ([sha256.Size]byte, error) {
	var result [sha256.Size]byte
	f, err := openOriginal(root, name, info)
	if err != nil {
		return result, err
	}
	h := sha256.New()
	n, readErr := io.Copy(h, contextReader{ctx, io.LimitReader(f, info.Size()+1)})
	closeErr := f.Close()
	if readErr != nil || closeErr != nil || n != info.Size() {
		return result, errors.New("input_files")
	}
	copy(result[:], h.Sum(nil))
	return result, nil
}

func copyFile(ctx context.Context, source, destination *os.Root, name string, stamp fileStamp) error {
	input, err := openOriginal(source, name, stamp.info)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := destination.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(output, h), contextReader{ctx, io.LimitReader(input, stamp.info.Size()+1)})
	modeErr := output.Chmod(0600)
	closeErr := output.Close()
	var copiedHash [sha256.Size]byte
	copy(copiedHash[:], h.Sum(nil))
	if copyErr != nil || modeErr != nil || closeErr != nil || n != stamp.info.Size() || copiedHash != stamp.hash {
		return errors.New("copy")
	}
	return nil
}

func unchanged(ctx context.Context, root *os.Root, before fileSet, budget int64) bool {
	after, _, reason := inspectFiles(root, budget)
	if reason != "" || len(after) != len(before) {
		return false
	}
	for name, stamp := range before {
		current, exists := after[name]
		if !exists || !os.SameFile(current.info, stamp.info) || current.info.Mode() != stamp.info.Mode() {
			return false
		}
		digest, err := hashFile(ctx, root, name, stamp.info)
		if err != nil || digest != stamp.hash {
			return false
		}
	}
	return true
}

func scanCopy(ctx context.Context, path string, limit bounds, out *aggregate) (reason string) {
	address := url.URL{Scheme: "file", Path: path}
	address.RawQuery = "mode=ro"
	db, err := sql.Open("sqlite", address.String())
	if err != nil {
		return "sqlite"
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer func() {
		if db.Close() != nil && reason == "" {
			reason = "sqlite"
		}
	}()
	conn, err := db.Conn(ctx)
	if err != nil {
		return "sqlite"
	}
	defer func() {
		if conn.Close() != nil && reason == "" {
			reason = "sqlite"
		}
	}()
	if _, err = sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_LENGTH, 2<<20); err != nil {
		return "sqlite"
	}
	for _, statement := range []string{"PRAGMA query_only=ON", "PRAGMA trusted_schema=OFF"} {
		if _, err = conn.ExecContext(ctx, statement); err != nil {
			return "sqlite"
		}
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "sqlite"
	}
	defer tx.Rollback()
	var encoding string
	if err = tx.QueryRowContext(ctx, "PRAGMA encoding").Scan(&encoding); err != nil {
		return "sqlite"
	}
	if encoding != "UTF-8" {
		return "encoding"
	}
	var kind, declared string
	var hidden int
	if err = tx.QueryRowContext(ctx, "SELECT type FROM pragma_table_list WHERE schema='main' AND name='message_attachments'").Scan(&kind); err != nil || kind != "table" {
		return "schema"
	}
	if err = tx.QueryRowContext(ctx, "SELECT type, hidden FROM pragma_table_xinfo('message_attachments') WHERE name='text_content'").Scan(&declared, &hidden); err != nil || !strings.EqualFold(strings.TrimSpace(declared), "TEXT") || hidden != 0 {
		return "schema"
	}
	// Finish the entire type/length budget preflight before materializing any cell.
	textBytes, reason := preflightCells(ctx, tx, limit, out)
	if reason != "" {
		return reason
	}
	rows, err := tx.QueryContext(ctx, `SELECT length(CAST(text_content AS BLOB)),
		CASE WHEN length(CAST(text_content AS BLOB)) <= ? THEN CAST(text_content AS BLOB) ELSE NULL END
		FROM main.message_attachments WHERE typeof(text_content)='text' LIMIT ?`, limit.cell, limit.rows+1)
	if err != nil {
		return "sqlite"
	}
	var count, used int64
	for rows.Next() {
		var length int64
		var body []byte
		if err = rows.Scan(&length, &body); err != nil {
			rows.Close()
			return "sqlite"
		}
		if ctx.Err() != nil || length < 0 || length > limit.cell || int64(len(body)) != length || count >= out.Storage.Text || length > textBytes-used {
			rows.Close()
			return "incomplete"
		}
		count++
		used += length
		classify(body, out)
	}
	readErr := rows.Err()
	closeErr := rows.Close()
	if readErr != nil || closeErr != nil {
		return "sqlite"
	}
	if count != out.Storage.Text || used != textBytes {
		return "incomplete"
	}
	if err = tx.Commit(); err != nil {
		return "sqlite"
	}
	return ""
}

func preflightCells(ctx context.Context, tx *sql.Tx, limit bounds, out *aggregate) (int64, string) {
	rows, err := tx.QueryContext(ctx, `SELECT typeof(text_content),
		CASE WHEN text_content IS NULL THEN 0 ELSE length(CAST(text_content AS BLOB)) END
		FROM main.message_attachments LIMIT ?`, limit.rows+1)
	if err != nil {
		return 0, "sqlite"
	}
	var used, textBytes int64
	for rows.Next() {
		var kind string
		var length int64
		if err = rows.Scan(&kind, &length); err != nil {
			rows.Close()
			return 0, "sqlite"
		}
		reason := ""
		switch {
		case ctx.Err() != nil:
			reason = "time_limit"
		case out.TotalCells >= limit.rows:
			reason = "row_limit"
		case length < 0:
			reason = "sqlite"
		case length > limit.cell:
			reason = "cell_limit"
		case length > limit.bytes-used:
			reason = "byte_limit"
		}
		if reason != "" {
			rows.Close()
			return 0, reason
		}
		switch kind {
		case "text":
			out.Storage.Text++
			textBytes += length
		case "blob":
			out.Storage.Blob++
		case "integer":
			out.Storage.Integer++
		case "real":
			out.Storage.Real++
		case "null":
			out.Storage.Null++
		default:
			rows.Close()
			return 0, "schema"
		}
		used += length
		out.TotalCells++
		out.Lengths.add(length)
	}
	readErr := rows.Err()
	closeErr := rows.Close()
	if readErr != nil || closeErr != nil {
		return 0, "sqlite"
	}
	return textBytes, ""
}

func classify(body []byte, out *aggregate) {
	for offset := 0; offset < len(body); {
		r, size := utf8.DecodeRune(body[offset:])
		if r != utf8.RuneError || size != 1 {
			offset += size
			continue
		}
		out.InvalidText++
		tail := body[offset:]
		if len(tail) <= 3 && !utf8.FullRune(tail) {
			switch len(tail) {
			case 1:
				out.Incomplete1.add(int64(len(body)))
			case 2:
				out.Incomplete2.add(int64(len(body)))
			case 3:
				out.Incomplete3.add(int64(len(body)))
			}
			if len(body) == 8192 {
				out.StrictCandidates++
			}
		} else {
			out.OtherMalformed++
		}
		return
	}
	out.ValidText++
}
