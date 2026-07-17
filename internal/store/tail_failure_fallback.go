package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	tailMessageFailureFallbackVersion    = 1
	tailMessageFailureFallbackMaxSize    = 4096
	tailMessageFailureFallbackDirSuffix  = ".tail-message-failures"
	tailMessageFailureFallbackTempPrefix = ".tmp-"
	tailMessageFailureFallbackExtension  = ".json"
	tailMessageFailureOccurrenceBytes    = 16
	tailMessageFailureNameAttempts       = 16
	tailMessageFailureReceiptPrefix      = "tail-message-failure-fallback:"

	tailMessageFailureOperation = "tail_message"
	tailMessageFailureSource    = "discord"
	tailMessageFailureRelated   = "message_event"
)

const importTailMessageFailureSQL = `
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
`

var errTailMessageFailureFallbackDirAbsent = errors.New("tail message failure fallback directory is absent")

// TailMessageFailureFallback is the content-free identity written when the
// failure ledger cannot be reached from a tail failure path.
type TailMessageFailureFallback struct {
	EventKind   string
	FailureKind string
	GuildID     string
	ChannelID   string
	MessageID   string
}

type tailMessageFailureFallbackRecord struct {
	Version     int    `json:"version"`
	EventKind   string `json:"event_kind"`
	FailureKind string `json:"failure_kind"`
	GuildID     string `json:"guild_id"`
	ChannelID   string `json:"channel_id"`
	MessageID   string `json:"message_id"`
}

type committedTailMessageFailure struct {
	name   string
	record tailMessageFailureFallbackRecord
}

type tailMessageFailureFallbackDir struct {
	root *os.Root
	file *os.File
}

type tailMessageFailureFallbackHooks struct {
	afterOpen func(string) error
	remove    func(*os.Root, string) error
	syncDir   func(*os.File) error
}

func (s *Store) PersistTailMessageFailureFallback(fallback TailMessageFailureFallback) error {
	return s.persistTailMessageFailureFallback(fallback, tailMessageFailureFallbackHooks{})
}

func (s *Store) persistTailMessageFailureFallback(
	fallback TailMessageFailureFallback,
	hooks tailMessageFailureFallbackHooks,
) error {
	record, err := normalizeTailMessageFailureFallback(fallback)
	if err != nil {
		return err
	}
	body, err := json.Marshal(record)
	if err != nil {
		return errors.New("marshal tail message failure fallback")
	}
	if len(body) > tailMessageFailureFallbackMaxSize {
		return errors.New("tail message failure fallback exceeds size limit")
	}

	dirPath, err := s.tailMessageFailureFallbackDir()
	if err != nil {
		return err
	}
	dir, err := openTailMessageFailureFallbackDir(dirPath, true)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	if err := hooks.runAfterOpen(dirPath); err != nil {
		return err
	}

	temp, tempName, err := createTailMessageFailureTemp(dir)
	if err != nil {
		return fmt.Errorf("create tail message failure fallback temp file: %w", err)
	}
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = hooks.removeFile(dir.root, tempName)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("secure tail message failure fallback temp file: %w", err)
	}
	if err := secureTailFailureFallbackTempFile(dir.file, tempName); err != nil {
		_ = temp.Close()
		return err
	}
	tempInfo, err := temp.Stat()
	if err != nil {
		_ = temp.Close()
		return fmt.Errorf("inspect tail message failure fallback temp file: %w", err)
	}
	if err := validateTailFailureFallbackFile(temp, tempInfo); err != nil {
		_ = temp.Close()
		return err
	}
	written, err := temp.Write(body)
	if err != nil {
		_ = temp.Close()
		return fmt.Errorf("write tail message failure fallback temp file: %w", err)
	}
	if written != len(body) {
		_ = temp.Close()
		return errors.New("write tail message failure fallback temp file: short write")
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync tail message failure fallback temp file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close tail message failure fallback temp file: %w", err)
	}

	digest := sha256.Sum256(body)
	digestHex := hex.EncodeToString(digest[:])
	for range tailMessageFailureNameAttempts {
		occurrenceID, err := newTailMessageFailureOccurrenceID()
		if err != nil {
			return err
		}
		finalName := occurrenceID + "-" + digestHex + tailMessageFailureFallbackExtension
		if err := renameTailFailureNoReplace(dir.root, dir.file, tempName, finalName); err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return fmt.Errorf("commit tail message failure fallback: %w", err)
		}
		removeTemp = false
		if err := hooks.syncDirectory(dir.file); err != nil {
			return fmt.Errorf("sync tail message failure fallback directory: %w", err)
		}
		return nil
	}
	return errors.New("commit tail message failure fallback: exhausted unique filenames")
}

// ImportTailMessageFailureFallbacks validates every committed fallback before
// importing any of them into the failure ledger.
func (s *Store) ImportTailMessageFailureFallbacks(ctx context.Context) (int, error) {
	return s.importTailMessageFailureFallbacks(ctx, tailMessageFailureFallbackHooks{})
}

func (s *Store) importTailMessageFailureFallbacks(
	ctx context.Context,
	hooks tailMessageFailureFallbackHooks,
) (int, error) {
	dirPath, err := s.tailMessageFailureFallbackDir()
	if err != nil {
		return 0, err
	}
	dir, err := openTailMessageFailureFallbackDir(dirPath, false)
	if errors.Is(err, errTailMessageFailureFallbackDirAbsent) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = dir.Close() }()
	if err := hooks.runAfterOpen(dirPath); err != nil {
		return 0, err
	}

	entries, err := dir.file.ReadDir(-1)
	if err != nil {
		return 0, fmt.Errorf("list tail message failure fallbacks: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	committed := make([]committedTailMessageFailure, 0, len(entries))
	tempNames := make([]string, 0)
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, tailMessageFailureFallbackTempPrefix) {
			tempNames = append(tempNames, name)
			continue
		}
		record, _, err := readCommittedTailMessageFailure(dir, name)
		if err != nil {
			return 0, fmt.Errorf("validate committed tail message failure fallback: %w", err)
		}
		committed = append(committed, committedTailMessageFailure{name: name, record: record})
	}
	if err := cleanupTailMessageFailureTemps(dir, tempNames, hooks); err != nil {
		return 0, err
	}
	if len(committed) == 0 {
		return 0, nil
	}

	imported, err := s.importTailMessageFailureFallbackRecords(ctx, committed, time.Now().UTC())
	if err != nil {
		return 0, err
	}

	var removeErr error
	removed := false
	for _, item := range committed {
		err := hooks.removeFile(dir.root, item.name)
		switch {
		case err == nil:
			removed = true
		case errors.Is(err, os.ErrNotExist):
		case removeErr == nil:
			removeErr = fmt.Errorf("remove imported tail message failure fallback: %w", err)
		}
	}
	if removed {
		if err := hooks.syncDirectory(dir.file); err != nil && removeErr == nil {
			removeErr = fmt.Errorf("sync imported tail message failure fallback removals: %w", err)
		}
	}
	if removeErr != nil {
		return imported, removeErr
	}
	return imported, nil
}

func (s *Store) importTailMessageFailureFallbackRecords(
	ctx context.Context,
	committed []committedTailMessageFailure,
	now time.Time,
) (int, error) {
	if len(committed) == 0 {
		return 0, nil
	}
	for _, item := range committed {
		if err := validateCommittedTailMessageFailure(item.record); err != nil {
			return 0, err
		}
		if _, ok := tailMessageFailureReceiptScope(item.name); !ok {
			return 0, errors.New("invalid committed tail message failure fallback filename")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tail message failure fallback import: %w", err)
	}
	defer rollback(tx)
	imported := 0
	storedAt := now.UTC().Format(timeLayout)
	for _, item := range committed {
		receiptScope, _ := tailMessageFailureReceiptScope(item.name)
		// Keep receipts after artifact cleanup so crash retries and concurrent
		// importers that already observed the file cannot apply it again.
		result, err := tx.ExecContext(ctx, `
			insert into sync_state(scope, cursor, updated_at)
			values(?, '', ?)
			on conflict(scope) do nothing
		`, receiptScope, storedAt)
		if err != nil {
			return 0, fmt.Errorf("record tail message failure fallback import receipt: %w", err)
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("inspect tail message failure fallback import receipt: %w", err)
		}
		if rowsAffected == 0 {
			continue
		}
		if err := importTailMessageFailureFallbackRecord(ctx, tx, item.record, now); err != nil {
			return 0, err
		}
		imported++
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit tail message failure fallback import: %w", err)
	}
	return imported, nil
}

func importTailMessageFailureFallbackRecord(
	ctx context.Context,
	tx *sql.Tx,
	record tailMessageFailureFallbackRecord,
	now time.Time,
) error {
	errorClass, errorMessage, ok := tailMessageFailureSentinel(record.FailureKind)
	if !ok {
		return errors.New("invalid tail message failure kind")
	}
	storedAt := now.UTC().Format(timeLayout)
	if _, err := tx.ExecContext(
		ctx,
		importTailMessageFailureSQL,
		tailMessageFailureOperation,
		tailMessageFailureSource,
		record.GuildID,
		record.ChannelID,
		record.MessageID,
		tailMessageFailureRelated,
		record.EventKind,
		errorClass,
		errorMessage,
		storedAt,
		storedAt,
	); err != nil {
		return fmt.Errorf("import tail message failure fallback: %w", err)
	}
	return nil
}

func normalizeTailMessageFailureFallback(
	fallback TailMessageFailureFallback,
) (tailMessageFailureFallbackRecord, error) {
	eventKind, ok := normalizeTailMessageEventKind(fallback.EventKind)
	if !ok {
		return tailMessageFailureFallbackRecord{}, errors.New("tail message event kind must be create, update, or delete")
	}
	failureKind, ok := normalizeTailMessageFailureKind(fallback.FailureKind)
	if !ok {
		return tailMessageFailureFallbackRecord{}, errors.New("tail message failure kind must be returned_error, panic, or timeout")
	}
	record := tailMessageFailureFallbackRecord{
		Version:     tailMessageFailureFallbackVersion,
		EventKind:   eventKind,
		FailureKind: failureKind,
		GuildID:     strings.TrimSpace(fallback.GuildID),
		ChannelID:   strings.TrimSpace(fallback.ChannelID),
		MessageID:   strings.TrimSpace(fallback.MessageID),
	}
	if record.GuildID == "" || record.ChannelID == "" || record.MessageID == "" {
		return tailMessageFailureFallbackRecord{}, errors.New("tail message failure fallback requires guild, channel, and message ids")
	}
	body, err := json.Marshal(record)
	if err != nil || len(body) > tailMessageFailureFallbackMaxSize {
		return tailMessageFailureFallbackRecord{}, errors.New("tail message failure fallback exceeds size limit")
	}
	return record, nil
}

func normalizeTailMessageEventKind(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimPrefix(value, "message_")
	switch value {
	case "create", "update", "delete":
		return value, true
	default:
		return "", false
	}
}

func normalizeTailMessageFailureKind(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "-", "_")
	switch value {
	case "returned_error", "panic", "timeout":
		return value, true
	default:
		return "", false
	}
}

func tailMessageFailureSentinel(kind string) (errorClass, errorMessage string, ok bool) {
	switch kind {
	case "returned_error":
		return "returned_error", "tail message handler returned an error", true
	case "panic":
		return "panic", "tail message handler panicked", true
	case "timeout":
		return "timeout", "tail message handler timed out", true
	default:
		return "", "", false
	}
}

func (s *Store) tailMessageFailureFallbackDir() (string, error) {
	if s == nil {
		return "", errors.New("tail message failure fallback store is required")
	}
	path := strings.TrimSpace(s.path)
	if path == "" || path == ":memory:" {
		return "", errors.New("tail message failure fallback requires a filesystem database")
	}
	if strings.HasPrefix(path, "file:") {
		parsed, err := url.Parse(path)
		if err != nil || parsed.Scheme != "file" || (parsed.Host != "" && parsed.Host != "localhost") {
			return "", errors.New("tail message failure fallback database path is invalid")
		}
		path = normalizeTailFailureFileURLPath(parsed.Path)
		if path == "" {
			path = parsed.Opaque
		}
		if path == "" || path == ":memory:" {
			return "", errors.New("tail message failure fallback requires a filesystem database")
		}
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve tail message failure fallback database path: %w", err)
	}
	return absolute + tailMessageFailureFallbackDirSuffix, nil
}

func openTailMessageFailureFallbackDir(path string, create bool) (*tailMessageFailureFallbackDir, error) {
	info, err := os.Lstat(path)
	created := false
	if errors.Is(err, os.ErrNotExist) && !create {
		return nil, errTailMessageFailureFallbackDirAbsent
	}
	if errors.Is(err, os.ErrNotExist) && create {
		if err := createTailFailureFallbackDir(path); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create tail message failure fallback directory: %w", err)
		} else if err == nil {
			created = true
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return nil, fmt.Errorf("inspect tail message failure fallback directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("tail message failure fallback directory must not be a symlink")
	}
	if !info.IsDir() {
		return nil, errors.New("tail message failure fallback path must be a directory")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, fmt.Errorf("open tail message failure fallback directory: %w", err)
	}
	openedInfo, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("verify tail message failure fallback directory: %w", err)
	}
	currentInfo, err := os.Lstat(path)
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("reinspect tail message failure fallback directory: %w", err)
	}
	if currentInfo.Mode()&os.ModeSymlink != 0 {
		_ = root.Close()
		return nil, errors.New("tail message failure fallback directory must not be a symlink")
	}
	if !openedInfo.IsDir() ||
		!os.SameFile(info, openedInfo) ||
		!os.SameFile(openedInfo, currentInfo) {
		_ = root.Close()
		return nil, errors.New("tail message failure fallback directory changed during verification")
	}
	dirFile, err := root.Open(".")
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("open verified tail message failure fallback directory: %w", err)
	}
	dirInfo, err := dirFile.Stat()
	if err != nil {
		_ = dirFile.Close()
		_ = root.Close()
		return nil, fmt.Errorf("verify opened tail message failure fallback directory: %w", err)
	}
	if !dirInfo.IsDir() || !os.SameFile(openedInfo, dirInfo) {
		_ = dirFile.Close()
		_ = root.Close()
		return nil, errors.New("tail message failure fallback directory handle changed during verification")
	}
	if err := validateTailFailureFallbackDir(dirFile, dirInfo); err != nil {
		_ = dirFile.Close()
		_ = root.Close()
		return nil, err
	}
	if created {
		parent, err := os.Open(filepath.Dir(path))
		if err != nil {
			_ = dirFile.Close()
			_ = root.Close()
			return nil, fmt.Errorf("open tail message failure fallback parent directory: %w", err)
		}
		if err := syncTailFailureDirectory(parent); err != nil {
			_ = parent.Close()
			_ = dirFile.Close()
			_ = root.Close()
			return nil, fmt.Errorf("sync tail message failure fallback parent directory: %w", err)
		}
		if err := parent.Close(); err != nil {
			_ = dirFile.Close()
			_ = root.Close()
			return nil, fmt.Errorf("close tail message failure fallback parent directory: %w", err)
		}
	}
	return &tailMessageFailureFallbackDir{root: root, file: dirFile}, nil
}

func readCommittedTailMessageFailure(
	dir *tailMessageFailureFallbackDir,
	name string,
) (tailMessageFailureFallbackRecord, []byte, error) {
	_, digestHex, ok := tailMessageFailureNameParts(name)
	if !ok {
		return tailMessageFailureFallbackRecord{}, nil, errors.New("invalid committed tail message failure fallback filename")
	}
	info, err := dir.root.Lstat(name)
	if err != nil {
		return tailMessageFailureFallbackRecord{}, nil, fmt.Errorf("inspect committed tail message failure fallback: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return tailMessageFailureFallbackRecord{}, nil, errors.New("committed tail message failure fallback must not be a symlink")
	}
	if !info.Mode().IsRegular() {
		return tailMessageFailureFallbackRecord{}, nil, errors.New("committed tail message failure fallback must be a regular file")
	}
	if info.Size() <= 0 || info.Size() > tailMessageFailureFallbackMaxSize {
		return tailMessageFailureFallbackRecord{}, nil, errors.New("committed tail message failure fallback size is invalid")
	}

	file, err := dir.root.Open(name)
	if err != nil {
		return tailMessageFailureFallbackRecord{}, nil, fmt.Errorf("open committed tail message failure fallback: %w", err)
	}
	defer func() { _ = file.Close() }()
	openedInfo, err := file.Stat()
	if err != nil {
		return tailMessageFailureFallbackRecord{}, nil, fmt.Errorf("verify committed tail message failure fallback: %w", err)
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		return tailMessageFailureFallbackRecord{}, nil, errors.New("committed tail message failure fallback changed during verification")
	}
	if err := validateTailFailureFallbackFile(file, openedInfo); err != nil {
		return tailMessageFailureFallbackRecord{}, nil, err
	}
	body, err := io.ReadAll(io.LimitReader(file, tailMessageFailureFallbackMaxSize+1))
	if err != nil {
		return tailMessageFailureFallbackRecord{}, nil, fmt.Errorf("read committed tail message failure fallback: %w", err)
	}
	if len(body) == 0 || len(body) > tailMessageFailureFallbackMaxSize {
		return tailMessageFailureFallbackRecord{}, nil, errors.New("committed tail message failure fallback size is invalid")
	}
	actualDigest := sha256.Sum256(body)
	if hex.EncodeToString(actualDigest[:]) != digestHex {
		return tailMessageFailureFallbackRecord{}, nil, errors.New("committed tail message failure fallback digest does not match filename")
	}

	var record tailMessageFailureFallbackRecord
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return tailMessageFailureFallbackRecord{}, nil, errors.New("committed tail message failure fallback JSON is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return tailMessageFailureFallbackRecord{}, nil, errors.New("committed tail message failure fallback has trailing data")
	}
	if err := validateCommittedTailMessageFailure(record); err != nil {
		return tailMessageFailureFallbackRecord{}, nil, err
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(canonical, body) {
		return tailMessageFailureFallbackRecord{}, nil, errors.New("committed tail message failure fallback JSON is not canonical")
	}
	return record, body, nil
}

func validateCommittedTailMessageFailure(record tailMessageFailureFallbackRecord) error {
	if record.Version != tailMessageFailureFallbackVersion {
		return errors.New("committed tail message failure fallback version is unsupported")
	}
	eventKind, ok := normalizeTailMessageEventKind(record.EventKind)
	if !ok || eventKind != record.EventKind {
		return errors.New("committed tail message failure fallback event kind is invalid")
	}
	failureKind, ok := normalizeTailMessageFailureKind(record.FailureKind)
	if !ok || failureKind != record.FailureKind {
		return errors.New("committed tail message failure fallback failure kind is invalid")
	}
	if record.GuildID == "" || strings.TrimSpace(record.GuildID) != record.GuildID ||
		record.ChannelID == "" || strings.TrimSpace(record.ChannelID) != record.ChannelID ||
		record.MessageID == "" || strings.TrimSpace(record.MessageID) != record.MessageID {
		return errors.New("committed tail message failure fallback identity is invalid")
	}
	return nil
}

func tailMessageFailureNameParts(name string) (artifactID, digest string, ok bool) {
	if !strings.HasSuffix(name, tailMessageFailureFallbackExtension) {
		return "", "", false
	}
	artifactID = strings.TrimSuffix(name, tailMessageFailureFallbackExtension)
	parts := strings.Split(artifactID, "-")
	switch len(parts) {
	case 1:
		digest = parts[0]
	case 2:
		if !validLowerHex(parts[0], tailMessageFailureOccurrenceBytes) {
			return "", "", false
		}
		digest = parts[1]
	default:
		return "", "", false
	}
	if !validLowerHex(digest, sha256.Size) {
		return "", "", false
	}
	return artifactID, digest, true
}

func tailMessageFailureReceiptScope(name string) (string, bool) {
	artifactID, _, ok := tailMessageFailureNameParts(name)
	if !ok {
		return "", false
	}
	return tailMessageFailureReceiptPrefix + artifactID, true
}

func validLowerHex(value string, byteLength int) bool {
	if len(value) != byteLength*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == byteLength
}

func cleanupTailMessageFailureTemps(
	dir *tailMessageFailureFallbackDir,
	names []string,
	hooks tailMessageFailureFallbackHooks,
) error {
	removed := false
	for _, name := range names {
		info, err := dir.root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect tail message failure fallback temp file: %w", err)
		}
		if info.IsDir() {
			return errors.New("tail message failure fallback temp entry must not be a directory")
		}
		if err := hooks.removeFile(dir.root, name); err != nil {
			return fmt.Errorf("remove tail message failure fallback temp file: %w", err)
		}
		removed = true
	}
	if removed {
		if err := hooks.syncDirectory(dir.file); err != nil {
			return fmt.Errorf("sync tail message failure fallback temp cleanup: %w", err)
		}
	}
	return nil
}

func createTailMessageFailureTemp(dir *tailMessageFailureFallbackDir) (*os.File, string, error) {
	for range tailMessageFailureNameAttempts {
		token, err := newTailMessageFailureOccurrenceID()
		if err != nil {
			return nil, "", err
		}
		name := tailMessageFailureFallbackTempPrefix + token
		file, err := dir.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return file, name, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", errors.New("exhausted unique temp filenames")
}

func newTailMessageFailureOccurrenceID() (string, error) {
	var occurrence [tailMessageFailureOccurrenceBytes]byte
	if _, err := io.ReadFull(rand.Reader, occurrence[:]); err != nil {
		return "", fmt.Errorf("generate tail message failure fallback occurrence id: %w", err)
	}
	return hex.EncodeToString(occurrence[:]), nil
}

func linkTailFailureNoReplaceRoot(root *os.Root, oldName, newName string) error {
	if err := root.Link(oldName, newName); err != nil {
		return err
	}
	// The no-replace link is the commit point. A stale temp name is safe for the
	// importer to clean later and must not turn a committed occurrence into an
	// ambiguous persistence error.
	_ = root.Remove(oldName)
	return nil
}

func (d *tailMessageFailureFallbackDir) Close() error {
	if d == nil {
		return nil
	}
	var errs []error
	if d.file != nil {
		errs = append(errs, d.file.Close())
	}
	if d.root != nil {
		errs = append(errs, d.root.Close())
	}
	return errors.Join(errs...)
}

func (h tailMessageFailureFallbackHooks) runAfterOpen(path string) error {
	if h.afterOpen == nil {
		return nil
	}
	return h.afterOpen(path)
}

func (h tailMessageFailureFallbackHooks) removeFile(root *os.Root, name string) error {
	if h.remove != nil {
		return h.remove(root, name)
	}
	return root.Remove(name)
}

func (h tailMessageFailureFallbackHooks) syncDirectory(dir *os.File) error {
	if h.syncDir != nil {
		return h.syncDir(dir)
	}
	return syncTailFailureDirectory(dir)
}
