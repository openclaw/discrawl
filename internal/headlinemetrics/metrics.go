// Package headlinemetrics stores public aggregate observations separately from
// Discord's message/member archive. It does not load archive configuration.
package headlinemetrics

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/store"
)

type Target struct {
	Entity string `json:"entity"`
	Target string `json:"target"`
}

type Config struct {
	Database  string   `json:"database"`
	Targets   []Target `json:"targets"`
	CookieJar string   `json:"cookieJar,omitempty"`
	TokenEnv  string   `json:"tokenEnv,omitempty"`
}

type Row struct {
	Type       string   `json:"type"`
	ID         string   `json:"id,omitempty"`
	Entity     string   `json:"entity"`
	Target     string   `json:"target"`
	Metric     string   `json:"metric,omitempty"`
	Kind       string   `json:"kind"`
	TS         string   `json:"ts"`
	Value      *float64 `json:"value"`
	ObservedAt string   `json:"observed_at"`
	Provenance string   `json:"provenance"`
	Label      string   `json:"label,omitempty"`
	URL        string   `json:"url,omitempty"`
}

type Collector func(context.Context, Config, string) ([]Row, error)

func Counter(t Target, metric string, value *float64, ts, basis string) Row {
	return Row{Type: "metric", Entity: t.Entity, Target: t.Target, Metric: metric, Kind: "counter", TS: ts, Value: value, ObservedAt: ts, Provenance: basis}
}

const schema = `
CREATE TABLE IF NOT EXISTS metric_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS metric_observations(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,entity TEXT NOT NULL,target TEXT NOT NULL,metric TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('counter','daily')),ts TEXT NOT NULL,value REAL,observed_at TEXT NOT NULL,provenance TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS metric_series ON metric_observations(target,metric,ts,sequence);
CREATE TABLE IF NOT EXISTS metric_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,entity TEXT NOT NULL,target TEXT NOT NULL,kind TEXT NOT NULL,ts TEXT NOT NULL,label TEXT NOT NULL,url TEXT NOT NULL,observed_at TEXT NOT NULL,provenance TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS metric_runs(sequence INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,status TEXT NOT NULL,rows_written INTEGER NOT NULL);
`

func checkOwner(ctx context.Context, db *sql.DB, owner string) error {
	var identity, version string
	err := db.QueryRowContext(ctx, `SELECT
		(SELECT value FROM metric_meta WHERE key='owner'),
		(SELECT value FROM metric_meta WHERE key='version')`).Scan(&identity, &version)
	if err != nil || identity != owner || version != "1" {
		return errors.New("refusing database: expected this metrics collector's owner and version 1")
	}
	return nil
}

func openReadOnly(ctx context.Context, path, owner string) (*store.Store, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("metrics database path must be absolute")
	}
	s, err := store.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, err
	}
	if err := checkOwner(ctx, s.DB(), owner); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func Open(ctx context.Context, path, owner string) (*store.Store, error) {
	if !filepath.IsAbs(path) || strings.TrimSpace(owner) == "" {
		return nil, errors.New("metrics database path must be absolute and owner must be set")
	}
	_, err := os.Lstat(path)
	if err == nil {
		// Validate before write access, including journal or permission changes.
		read, err := openReadOnly(ctx, path, owner)
		if err != nil {
			return nil, err
		}
		if err := read.Close(); err != nil {
			return nil, err
		}
		return store.Open(ctx, store.Options{Path: path, MaxOpenConns: 1, MaxIdleConns: 1})
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return initialize(ctx, path, owner)
}

func initialize(ctx context.Context, path, owner string) (*store.Store, error) {
	// Publish a fully initialized database without replacing an existing path.
	// Concurrent creators must re-check the winning file's identity.
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".metrics-init-*")
	if err != nil {
		return nil, err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	if err := f.Close(); err != nil {
		return nil, err
	}
	s, err := store.Open(ctx, store.Options{Path: tmp, MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		return nil, err
	}
	err = s.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, schema); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO metric_meta VALUES('owner',?),('version','1')", owner)
		return err
	})
	closeErr := s.Close() // Checkpoint the private WAL before linking its database.
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err := os.Link(tmp, path); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	return Open(ctx, path, owner)
}

func Validate(r Row) error {
	if r.Type != "metric" && r.Type != "event" {
		return errors.New("invalid observation type")
	}
	if strings.TrimSpace(r.Entity) == "" || strings.TrimSpace(r.Target) == "" || len(r.Entity) > 200 || len(r.Target) > 300 || strings.TrimSpace(r.Provenance) == "" || strings.TrimSpace(r.Kind) == "" {
		return errors.New("invalid observation identity")
	}
	for _, v := range []string{r.TS, r.ObservedAt} {
		if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
			return errors.New("invalid observation time")
		}
	}
	if r.Type == "metric" && (strings.TrimSpace(r.Metric) == "" || (r.Kind != "counter" && r.Kind != "daily") || r.Value != nil && (math.IsNaN(*r.Value) || math.IsInf(*r.Value, 0) || *r.Value < 0)) {
		return errors.New("invalid metric")
	}
	return nil
}

func Write(ctx context.Context, s *store.Store, rows []Row) (int, error) {
	written := 0
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		for _, r := range rows {
			if err := Validate(r); err != nil {
				return err
			}
			if r.ID == "" {
				b, err := json.Marshal(r)
				if err != nil {
					return err
				}
				h := sha256.Sum256(b)
				r.ID = hex.EncodeToString(h[:])
			}
			// Only IDs deduplicate. Retain repeated values, decreases, unknowns,
			// and revised daily rows; consumers select the latest daily sequence.
			var result sql.Result
			var err error
			if r.Type == "metric" {
				result, err = tx.ExecContext(ctx, "INSERT INTO metric_observations(id,entity,target,metric,kind,ts,value,observed_at,provenance) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING", r.ID, r.Entity, r.Target, r.Metric, r.Kind, r.TS, r.Value, r.ObservedAt, r.Provenance)
			} else {
				result, err = tx.ExecContext(ctx, "INSERT INTO metric_events(id,entity,target,kind,ts,label,url,observed_at,provenance) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING", r.ID, r.Entity, r.Target, r.Kind, r.TS, r.Label, r.URL, r.ObservedAt, r.Provenance)
			}
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return err
			}
			written += int(n)
		}
		return nil
	})
	if err != nil {
		return 0, err // The entire batch rolled back.
	}
	return written, nil
}

const Usage = `Usage: discrawl metrics <collect|import|status> --config /absolute/metrics.json

Collect public invite approximate members and online presence into a separate
metrics database. Online presence is not a count of active posters.
Import reads scoped NDJSON history from stdin. Status is read-only.
No Discord token, archive configuration, or message/member archive is used.
Successful commands write JSON; partial collection writes NULLs and exits nonzero.
`

func loadConfig(path string) (Config, error) {
	var c Config
	raw, err := os.ReadFile(path)
	if err != nil {
		return c, errors.New("metrics config unavailable")
	}
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, errors.New("invalid metrics config")
	}
	if d.Decode(new(any)) != io.EOF || !filepath.IsAbs(c.Database) || len(c.Targets) == 0 {
		return c, errors.New("metrics config requires an absolute database path and targets")
	}
	seen := map[string]bool{}
	for _, t := range c.Targets {
		if strings.TrimSpace(t.Entity) == "" || len(t.Entity) > 200 || !inviteCode.MatchString(t.Target) || seen[t.Target] {
			return c, errors.New("metrics targets require an entity and unique Discord invite code")
		}
		seen[t.Target] = true
	}
	return c, nil
}

func Run(ctx context.Context, args []string, owner string, collect Collector, in io.Reader, out, errout io.Writer) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		_, err := io.WriteString(out, Usage)
		return err
	}
	command := args[0]
	if command != "collect" && command != "import" && command != "status" {
		return errors.New("unknown metrics command")
	}
	flags := flag.NewFlagSet("metrics "+command, flag.ContinueOnError)
	flags.SetOutput(errout)
	flags.Usage = func() { _, _ = io.WriteString(out, Usage) }
	configPath := flags.String("config", "", "metrics JSON configuration (separate from archive config)")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *configPath == "" || flags.NArg() != 0 {
		return errors.New("provide --config METRICS_CONFIG with no positional arguments")
	}
	c, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	if command == "status" {
		return status(ctx, c, owner, out)
	}
	s, err := Open(ctx, c.Database, owner)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	allowed := map[string]string{}
	for _, t := range c.Targets {
		allowed[t.Target] = t.Entity
	}
	if command == "import" {
		written, err := importRows(ctx, s, allowed, in)
		if outputErr := json.NewEncoder(out).Encode(map[string]any{"source": owner, "command": command, "rows_written": written, "ok": err == nil}); outputErr != nil {
			return outputErr
		}
		return err
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	rows, collectionErr := collect(ctx, c, ts)
	for _, r := range rows {
		entity, ok := allowed[r.Target]
		if !ok || entity != r.Entity {
			return errors.New("collector returned invalid target")
		}
	}
	// Persist partial observations after request cancellation, with a bounded
	// final write that cannot reach the message/member archive.
	writeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	written, err := Write(writeCtx, s, rows)
	if err != nil {
		return err
	}
	runStatus := "ok"
	if collectionErr != nil {
		runStatus = "partial"
	}
	if _, err := s.DB().ExecContext(writeCtx, "INSERT INTO metric_runs(ts,status,rows_written) VALUES(?,?,?)", ts, runStatus, written); err != nil {
		return err
	}
	if err := json.NewEncoder(out).Encode(map[string]any{"source": owner, "command": command, "rows_written": written, "ok": collectionErr == nil}); err != nil {
		return err
	}
	if collectionErr != nil {
		return errors.New("one or more metric reads failed; successful values and NULL observations were retained")
	}
	return nil
}

func importRows(ctx context.Context, s *store.Store, allowed map[string]string, in io.Reader) (int, error) {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 65536), 4*1024*1024)
	batch := make([]Row, 0, 500)
	written, line := 0, 0
	for scanner.Scan() {
		line++
		var row Row
		d := json.NewDecoder(strings.NewReader(scanner.Text()))
		d.DisallowUnknownFields()
		if err := d.Decode(&row); err != nil || d.Decode(new(any)) != io.EOF {
			return written, fmt.Errorf("invalid import JSON at line %d", line)
		}
		entity, ok := allowed[row.Target]
		if !ok || entity != row.Entity || strings.TrimSpace(row.ID) == "" {
			return written, fmt.Errorf("invalid import identity or scope at line %d", line)
		}
		if err := Validate(row); err != nil {
			return written, fmt.Errorf("import line %d: %w", line, err)
		}
		batch = append(batch, row)
		if len(batch) == 500 {
			n, err := Write(ctx, s, batch)
			if err != nil {
				return written, err
			}
			written += n
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return written, fmt.Errorf("read import after line %d: %w", line, err)
	}
	n, err := Write(ctx, s, batch)
	return written + n, err
}

func status(ctx context.Context, c Config, owner string, out io.Writer) error {
	s, err := openReadOnly(ctx, c.Database, owner)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	var observations, events, sequence int64
	if err := s.DB().QueryRowContext(ctx, "SELECT count(*),coalesce(max(sequence),0) FROM metric_observations").Scan(&observations, &sequence); err != nil {
		return err
	}
	if err := s.DB().QueryRowContext(ctx, "SELECT count(*) FROM metric_events").Scan(&events); err != nil {
		return err
	}
	var last *string
	err = s.DB().QueryRowContext(ctx, "SELECT observed_at FROM metric_observations ORDER BY julianday(observed_at) DESC,sequence DESC LIMIT 1").Scan(&last)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return json.NewEncoder(out).Encode(map[string]any{"source": owner, "observations": observations, "events": events, "sequence": sequence, "last_observed": last})
}
