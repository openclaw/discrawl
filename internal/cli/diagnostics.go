package cli

import (
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
)

type diagnosticsReport struct {
	Status                    string               `json:"status"`
	Database                  diagnosticsDatabase  `json:"database"`
	SyncLock                  diagnosticsSyncLock  `json:"sync_lock"`
	Freshness                 diagnosticsFreshness `json:"freshness"`
	SafeForReadOnlyInspection bool                 `json:"safe_for_read_only_inspection"`
	Warnings                  []string             `json:"warnings,omitempty"`
}

type diagnosticsDatabase struct {
	Path          string          `json:"path"`
	Exists        bool            `json:"exists"`
	Bytes         int64           `json:"bytes"`
	JournalMode   string          `json:"journal_mode,omitempty"`
	SchemaVersion int             `json:"schema_version"`
	Integrity     string          `json:"integrity"`
	OpenError     string          `json:"open_error,omitempty"`
	WAL           diagnosticsFile `json:"wal"`
}

type diagnosticsFile struct {
	Path   string `json:"path"`
	Exists bool   `json:"exists"`
	Bytes  int64  `json:"bytes"`
}

type diagnosticsSyncLock struct {
	Path         string                    `json:"path"`
	MetadataPath string                    `json:"metadata_path"`
	Held         bool                      `json:"held"`
	State        string                    `json:"state"`
	Detection    string                    `json:"detection"`
	Owner        *diagnosticsSyncLockOwner `json:"owner,omitempty"`
	Error        string                    `json:"error,omitempty"`
}

type diagnosticsSyncLockOwner struct {
	PID       int    `json:"pid"`
	Alive     bool   `json:"alive"`
	Operation string `json:"operation,omitempty"`
	Phase     string `json:"phase,omitempty"`
	StartedAt string `json:"started_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type diagnosticsFreshness struct {
	LastSyncAt      string `json:"last_sync_at,omitempty"`
	LastTailEventAt string `json:"last_tail_event_at,omitempty"`
	Error           string `json:"error,omitempty"`
}

func (r *runtime) runDiagnostics(args []string) error {
	fs := flag.NewFlagSet("diagnostics", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("diagnostics takes no arguments"))
	}
	if *jsonOut {
		r.json = true
	}

	dbPath, err := config.ExpandPath(r.cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	lockPath := filepath.Join(filepath.Dir(dbPath), ".discrawl-sync.lock")
	report := diagnosticsReport{
		Status: "warning",
		Database: diagnosticsDatabase{
			Path:      dbPath,
			Integrity: "not_checked",
			WAL:       statDiagnosticsFile(dbPath + "-wal"),
		},
		SyncLock: diagnosticsSyncLock{
			Path:         lockPath,
			MetadataPath: syncLockMetadataPath(lockPath),
			State:        "unknown",
			Detection:    "file_lock",
		},
	}
	dbFile := statDiagnosticsFile(dbPath)
	report.Database.Exists = dbFile.Exists
	report.Database.Bytes = dbFile.Bytes

	held, known, lockErr := syncLockState(lockPath)
	report.SyncLock.Held = held
	if !known {
		report.SyncLock.Detection = "unsupported"
	}
	switch {
	case lockErr != nil:
		report.SyncLock.Error = lockErr.Error()
		report.Warnings = append(report.Warnings, "sync lock state could not be determined")
	case !known:
		report.Warnings = append(report.Warnings, "sync lock state detection is unsupported on this platform")
	case held:
		report.SyncLock.State = "active_writer"
	default:
		report.SyncLock.State = "unlocked"
	}
	if owner, ok := readSyncLockOwner(lockPath); ok {
		report.SyncLock.Owner = &diagnosticsSyncLockOwner{
			PID:       owner.PID,
			Alive:     syncLockPIDAlive(owner.PID),
			Operation: owner.Operation,
			Phase:     owner.Phase,
			StartedAt: owner.StartedAt,
			UpdatedAt: owner.UpdatedAt,
		}
		if lockErr == nil && known && !held {
			report.SyncLock.State = "stale_metadata"
		}
	}

	if !report.Database.Exists {
		report.Database.Integrity = "not_available"
		report.Warnings = append(report.Warnings, "database does not exist")
		return r.print(report)
	}

	health, inspectErr := store.InspectSQLite(r.ctx, dbPath)
	if inspectErr != nil {
		report.Database.Integrity = "unavailable"
		report.Database.OpenError = inspectErr.Error()
		report.Warnings = append(report.Warnings, "database could not be opened read-only")
		return r.print(report)
	}
	report.Database.JournalMode = strings.ToLower(strings.TrimSpace(health.JournalMode))
	report.Database.SchemaVersion = health.SchemaVersion
	if health.IntegrityOK {
		report.Database.Integrity = "ok"
	} else {
		report.Database.Integrity = "failed"
		report.Warnings = append(report.Warnings, "SQLite quick_check reported integrity errors")
	}
	db, openErr := store.OpenReadOnly(r.ctx, dbPath)
	if openErr != nil {
		report.Freshness.Error = openErr.Error()
		report.Warnings = append(report.Warnings, "archive freshness could not be read with this Discrawl version")
	} else {
		defer func() { _ = db.Close() }()
		if status, statusErr := db.Status(r.ctx, dbPath, r.cfg.EffectiveDefaultGuildID()); statusErr != nil {
			report.Freshness.Error = statusErr.Error()
			report.Warnings = append(report.Warnings, "archive freshness could not be read")
		} else {
			if !status.LastSyncAt.IsZero() {
				report.Freshness.LastSyncAt = status.LastSyncAt.UTC().Format(time.RFC3339)
			}
			if !status.LastTailEventAt.IsZero() {
				report.Freshness.LastTailEventAt = status.LastTailEventAt.UTC().Format(time.RFC3339)
			}
		}
	}
	report.SafeForReadOnlyInspection = report.Database.Integrity == "ok"
	if report.SafeForReadOnlyInspection && len(report.Warnings) == 0 {
		report.Status = "ok"
	}
	return r.print(report)
}

func statDiagnosticsFile(path string) diagnosticsFile {
	out := diagnosticsFile{Path: path}
	info, err := os.Stat(path)
	if err != nil {
		return out
	}
	out.Exists = true
	out.Bytes = info.Size()
	return out
}
