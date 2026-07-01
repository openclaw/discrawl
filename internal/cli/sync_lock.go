package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/config"
)

func (r *runtime) withSyncLock(fn func() error) error {
	return r.withSyncLockOperation("writer", fn)
}

func (r *runtime) withSyncLockOperation(operation string, fn func() error) error {
	if r.dbLockHeld {
		return fn()
	}
	lockPath, err := r.syncLockPath()
	if err != nil {
		return err
	}
	started := r.nowUTC()
	token := newSyncLockToken()
	release, err := acquireSyncLockWithMetadata(r.ctx, lockPath, syncLockMetadataBody(operation, "locked", started, r.nowUTC(), token))
	if err != nil {
		return err
	}
	return r.runWithHeldSyncLock(lockPath, release, operation, started, token, fn)
}

func (r *runtime) withMessagesSyncLock(fn func() error) error {
	if r.dbLockHeld {
		return fn()
	}
	lockPath, err := r.syncLockPath()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		started := r.nowUTC()
		token := newSyncLockToken()
		release, locked, err := tryAcquireSyncLockWithMetadata(lockPath, syncLockMetadataBody("messages-sync", "locked", started, r.nowUTC(), token))
		if err != nil {
			return err
		}
		if locked {
			return r.runWithHeldSyncLock(lockPath, release, "messages-sync", started, token, fn)
		}
		// This check runs only after a failed nonblocking lock attempt, so stale
		// metadata or PID reuse cannot identify an active tail owner by itself.
		if r.activeTailOwnsSyncLock(lockPath) {
			started = r.nowUTC()
			token = newSyncLockToken()
			release, locked, err = tryAcquireSyncLockWithMetadata(lockPath, syncLockMetadataBody("messages-sync", "locked", started, r.nowUTC(), token))
			if err != nil {
				return err
			}
			if locked {
				return r.runWithHeldSyncLock(lockPath, release, "messages-sync", started, token, fn)
			}
			if r.activeTailOwnsSyncLock(lockPath) {
				return usageErr(errors.New("tail already owns live sync; omit --sync while tail is running"))
			}
		}
		select {
		case <-r.ctx.Done():
			return syncLockErr(r.ctx, lockPath)
		case <-ticker.C:
		}
	}
}

func (r *runtime) tryWithSyncLock(fn func() error) (bool, error) {
	if r.dbLockHeld {
		return true, fn()
	}
	lockPath, err := r.syncLockPath()
	if err != nil {
		return false, err
	}
	started := r.nowUTC()
	token := newSyncLockToken()
	release, locked, err := tryAcquireSyncLockWithMetadata(lockPath, syncLockMetadataBody("writer", "locked", started, r.nowUTC(), token))
	if err != nil || !locked {
		return locked, err
	}
	return true, r.runWithHeldSyncLock(lockPath, release, "writer", started, token, fn)
}

func (r *runtime) runWithHeldSyncLock(lockPath string, release func() error, operation string, started time.Time, token string, fn func() error) error {
	if strings.TrimSpace(operation) == "tail" && token != "" {
		var err error
		r.lockTokenFree, err = acquireSyncLockWithMetadata(r.ctx, syncLockTokenPath(lockPath, token), syncLockMetadataBody("tail-token", "locked", started, r.nowUTC(), token))
		if err != nil {
			_ = release()
			return err
		}
	}
	r.dbLockHeld = true
	r.lockStarted = started
	r.lockOperation = strings.TrimSpace(operation)
	r.lockToken = token
	defer func() {
		r.dbLockHeld = false
		r.lockStarted = time.Time{}
		r.lockOperation = ""
		cleanupToken := r.lockToken
		r.lockToken = ""
		if r.lockTokenFree != nil {
			_ = r.lockTokenFree()
			r.lockTokenFree = nil
			_ = os.Remove(syncLockTokenPath(lockPath, cleanupToken))
			_ = os.Remove(syncLockMetadataPath(syncLockTokenPath(lockPath, cleanupToken)))
		}
		_ = release()
	}()
	return fn()
}

func (r *runtime) activateTailSyncLock() error {
	if !r.dbLockHeld {
		return nil
	}
	lockPath, err := r.syncLockPath()
	if err != nil {
		return err
	}
	token := r.lockToken
	if token == "" {
		token = newSyncLockToken()
		r.lockToken = token
	}
	if r.lockTokenFree == nil {
		release, err := acquireSyncLockWithMetadata(r.ctx, syncLockTokenPath(lockPath, token), syncLockMetadataBody("tail-token", "locked", r.lockStarted, r.nowUTC(), token))
		if err != nil {
			return err
		}
		r.lockTokenFree = release
	}
	r.lockOperation = "tail"
	started := r.lockStarted
	if started.IsZero() {
		started = r.nowUTC()
		r.lockStarted = started
	}
	return writeSyncLockMetadataSidecar(lockPath, []byte(syncLockMetadataBody("tail", "live", started, r.nowUTC(), token)))
}

func (r *runtime) setSyncLockPhase(phase string) {
	if !r.dbLockHeld {
		return
	}
	path, err := r.syncLockPath()
	if err != nil {
		return
	}
	started := r.lockStarted
	if started.IsZero() {
		started = r.nowUTC()
	}
	body := syncLockMetadataBody(r.lockOperation, phase, started, r.nowUTC(), r.lockToken)
	_ = writeSyncLockMetadataSidecar(path, []byte(body))
}

func (r *runtime) syncLockPath() (string, error) {
	dbPath, err := config.ExpandPath(r.cfg.DBPath)
	if err != nil {
		return "", configErr(err)
	}
	return filepath.Join(filepath.Dir(dbPath), ".discrawl-sync.lock"), nil
}

type syncLockOwner struct {
	PID       int
	Operation string
	Phase     string
	StartedAt string
	UpdatedAt string
	Token     string
}

func (r *runtime) activeTailOwnsSyncLock(path string) bool {
	owner, ok := readSyncLockOwner(path)
	if !ok || owner.Operation != "tail" || owner.PID <= 0 || !validSyncLockToken(owner.Token) {
		return false
	}
	if !syncLockPIDAlive(owner.PID) {
		return false
	}
	select {
	case <-r.ctx.Done():
		return false
	case <-time.After(20 * time.Millisecond):
	}
	current, ok := readSyncLockOwner(path)
	return ok &&
		current.PID == owner.PID &&
		current.Operation == owner.Operation &&
		current.Token == owner.Token &&
		validSyncLockToken(current.Token) &&
		syncLockPIDAlive(current.PID) &&
		syncLockTokenHeld(path, current.Token)
}

func readSyncLockOwner(path string) (syncLockOwner, bool) {
	if owner, ok := readSyncLockOwnerFile(syncLockMetadataPath(path)); ok {
		return owner, true
	}
	return readSyncLockOwnerFile(path)
}

func readSyncLockOwnerFile(path string) (syncLockOwner, bool) {
	body, err := os.ReadFile(path)
	if err != nil {
		return syncLockOwner{}, false
	}
	fields := map[string]string{}
	for line := range strings.SplitSeq(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return syncLockOwner{}, false
		}
		fields[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	pidRaw := fields["pid"]
	if pidRaw == "" {
		return syncLockOwner{}, false
	}
	pid, err := strconv.Atoi(pidRaw)
	if err != nil {
		return syncLockOwner{}, false
	}
	return syncLockOwner{
		PID:       pid,
		Operation: fields["operation"],
		Phase:     fields["phase"],
		StartedAt: fields["started_at"],
		UpdatedAt: fields["updated_at"],
		Token:     fields["token"],
	}, true
}

func writeSyncLockMetadataRecord(file *os.File, path string, metadata []byte) error {
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	if err := file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.Write(metadata); err != nil {
		return err
	}
	return writeSyncLockMetadataSidecar(path, metadata)
}

func writeSyncLockMetadataFiles(path string, metadata []byte) error {
	if err := os.WriteFile(path, metadata, 0o600); err != nil {
		return err
	}
	return writeSyncLockMetadataSidecar(path, metadata)
}

func writeSyncLockMetadataSidecar(path string, metadata []byte) error {
	return os.WriteFile(syncLockMetadataPath(path), metadata, 0o600)
}

func syncLockMetadataPath(lockPath string) string {
	return lockPath + ".meta"
}

func syncLockMetadataBody(operation, phase string, started, updated time.Time, token string) string {
	return fmt.Sprintf("pid=%d\noperation=%s\ntoken=%s\nstarted_at=%s\nupdated_at=%s\nphase=%s\n",
		os.Getpid(),
		strings.TrimSpace(operation),
		token,
		started.Format(time.RFC3339Nano),
		updated.Format(time.RFC3339Nano),
		phase,
	)
}

func newSyncLockToken() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%016x%016x", os.Getpid(), time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}

func syncLockTokenPath(lockPath, token string) string {
	return lockPath + "." + token + ".owner"
}

func validSyncLockToken(token string) bool {
	if len(token) != 32 {
		return false
	}
	for _, ch := range token {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func syncLockTokenHeld(lockPath, token string) bool {
	if !validSyncLockToken(token) {
		return false
	}
	tokenPath := syncLockTokenPath(lockPath, token)
	if _, err := os.Stat(tokenPath); err != nil {
		return false
	}
	release, locked, err := tryAcquireSyncLock(tokenPath)
	if err != nil {
		return false
	}
	if locked {
		_ = release()
		return false
	}
	return true
}

func syncLockErr(ctx context.Context, path string) error {
	if ctx.Err() != nil {
		if body, err := readSyncLockMetadata(path); err == nil {
			details := strings.TrimSpace(string(body))
			if details != "" {
				return fmt.Errorf("wait for sync lock %s (%s): %w", path, strings.ReplaceAll(details, "\n", ", "), ctx.Err())
			}
		}
		return fmt.Errorf("wait for sync lock %s: %w", path, ctx.Err())
	}
	return nil
}

func readSyncLockMetadata(path string) ([]byte, error) {
	body, err := os.ReadFile(syncLockMetadataPath(path))
	if err == nil {
		return body, nil
	}
	return os.ReadFile(path)
}
