package headlinemetrics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/store"
	"github.com/stretchr/testify/require"
)

func newMetrics(t *testing.T) *store.Store {
	t.Helper()
	s, err := Open(t.Context(), filepath.Join(t.TempDir(), "nested", "metrics.sqlite"), "discrawl")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func testConfig(t *testing.T, database string) string {
	t.Helper()
	b, err := json.Marshal(Config{Database: database, Targets: []Target{{"sample-alpha", "example-alpha"}, {"sample-beta", "example-beta"}}})
	require.NoError(t, err)
	p := filepath.Join(t.TempDir(), "metrics.json")
	require.NoError(t, os.WriteFile(p, b, 0o600))
	return p
}

func TestOpenPreservesUnownedAndNewerDatabases(t *testing.T) {
	for _, kind := range []string{"archive", "other-owner", "newer-version", "empty"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "existing.sqlite")
			switch kind {
			case "empty":
				require.NoError(t, os.WriteFile(path, nil, 0o600))
			case "archive":
				s, err := store.Open(t.Context(), store.Options{Path: path, Schema: "CREATE TABLE messages(id TEXT PRIMARY KEY,content TEXT); INSERT INTO messages VALUES('saved','preserve this archive');"})
				require.NoError(t, err)
				require.NoError(t, s.Close())
			default:
				s, err := Open(t.Context(), path, "discrawl")
				require.NoError(t, err)
				query := "UPDATE metric_meta SET value='another-collector' WHERE key='owner'"
				if kind == "newer-version" {
					query = "UPDATE metric_meta SET value='2' WHERE key='version'"
				}
				_, err = s.DB().ExecContext(t.Context(), query)
				require.NoError(t, err)
				require.NoError(t, s.Close())
			}
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			_, err = Open(t.Context(), path, "discrawl")
			require.Error(t, err)
			info, err := os.Stat(path)
			require.NoError(t, err)
			for _, suffix := range []string{" ", "\t"} {
				_, err = Open(t.Context(), path+suffix, "discrawl")
				require.Error(t, err)
				for _, command := range []string{"import", "collect", "status"} {
					err = Run(t.Context(), []string{command, "--config", testConfig(t, path+suffix)}, "discrawl", nil, strings.NewReader(""), io.Discard, io.Discard)
					require.Error(t, err)
				}
				require.NoFileExists(t, path+suffix)
			}
			afterInfo, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, info.Mode(), afterInfo.Mode())
			_, err = openReadOnly(t.Context(), path, "discrawl")
			require.Error(t, err)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestConcurrentInitializationHasOneOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.sqlite")
	var wg sync.WaitGroup
	results := make(chan string, 2)
	start := make(chan struct{})
	for _, owner := range []string{"discrawl", "another-collector"} {
		wg.Go(func() {
			<-start
			s, err := Open(t.Context(), path, owner)
			if err == nil {
				results <- owner
				_ = s.Close()
			}
		})
	}
	close(start)
	wg.Wait()
	close(results)
	owners := []string{}
	for owner := range results {
		owners = append(owners, owner)
	}
	require.Len(t, owners, 1)
	s, err := openReadOnly(t.Context(), path, owners[0])
	require.NoError(t, err)
	require.NoError(t, s.Close())
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".metrics-init-*"))
	require.NoError(t, err)
	require.Empty(t, leftovers)
}

func TestOpenRejectsDanglingDatabaseSymlink(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "metrics.sqlite")
	destination := filepath.Join(root, "missing-archive.sqlite")
	if err := os.Symlink(destination, path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	_, err := Open(t.Context(), path, "discrawl")
	require.Error(t, err)
	require.NoFileExists(t, destination)
	leftovers, err := filepath.Glob(filepath.Join(root, ".metrics-init-*"))
	require.NoError(t, err)
	require.Empty(t, leftovers)
}

func TestConcurrentWritesUseNativeSQLiteLock(t *testing.T) {
	first := newMetrics(t)
	second, err := Open(t.Context(), first.Path(), "discrawl")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	tx, err := first.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.ExecContext(t.Context(), "INSERT INTO metric_meta(key,value) VALUES('test-writer','held')")
	require.NoError(t, err)
	row := Counter(Target{"sample-alpha", "example-alpha"}, "members", new(12.0), sampleTime, "fixture")
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	written, err := Write(ctx, second, []Row{row})
	require.Error(t, err)
	require.Zero(t, written)
	var count int
	require.NoError(t, second.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM metric_observations").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, tx.Rollback())
	written, err = Write(t.Context(), second, []Row{row})
	require.NoError(t, err)
	require.Equal(t, 1, written)
}

func TestWritePreservesHistoryAndRollsBackBadBatches(t *testing.T) {
	s := newMetrics(t)
	rows := []Row{}
	for i, value := range []*float64{new(20.0), new(0.0), new(19.0), nil} {
		r := Counter(Target{"sample-alpha", "example-alpha"}, "members", value, sampleTime, "historical-source")
		r.ID = fmt.Sprintf("counter-%d", i)
		rows = append(rows, r)
	}
	for i, value := range []float64{10, 9, 9} {
		r := Counter(Target{"sample-alpha", "example-alpha"}, "members", new(value), "2026-09-14T00:00:00Z", "native-history")
		r.ID, r.Kind, r.ObservedAt = fmt.Sprintf("daily-%d", i), "daily", sampleTime
		rows = append(rows, r)
	}
	rows = append(rows, Row{Type: "event", ID: "event-1", Entity: "sample-beta", Target: "example-beta", Kind: "release", TS: sampleTime, ObservedAt: sampleTime, Provenance: "historical-source", Label: "release", URL: "https://example.test/release"})
	written, err := Write(t.Context(), s, rows)
	require.NoError(t, err)
	require.Equal(t, len(rows), written)
	written, err = Write(t.Context(), s, rows)
	require.NoError(t, err)
	require.Zero(t, written)
	var count int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM metric_observations").Scan(&count))
	require.Equal(t, 7, count)
	var nulls int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM metric_observations WHERE value IS NULL").Scan(&nulls))
	require.Equal(t, 1, nulls)
	var latest float64
	require.NoError(t, s.DB().QueryRowContext(t.Context(), "SELECT value FROM metric_observations WHERE kind='daily' ORDER BY sequence DESC LIMIT 1").Scan(&latest))
	require.InDelta(t, 9, latest, 0)
	good := Counter(Target{"sample-alpha", "example-alpha"}, "online", new(3.0), sampleTime, "native")
	bad := good
	bad.Value = new(-1.0)
	written, err = Write(t.Context(), s, []Row{good, bad})
	require.Error(t, err)
	require.Zero(t, written)
	written, err = Write(t.Context(), s, []Row{good})
	require.NoError(t, err)
	require.Equal(t, 1, written)
	written, err = Write(t.Context(), s, []Row{good})
	require.NoError(t, err)
	require.Zero(t, written)
}

func TestValidationRejectsInvalidObservations(t *testing.T) {
	good := Counter(Target{"sample-alpha", "example-alpha"}, "members", new(1.0), sampleTime, "native")
	for _, change := range []func(*Row){
		func(r *Row) { r.Type = "other" }, func(r *Row) { r.Entity = " " },
		func(r *Row) { r.TS = "yesterday" }, func(r *Row) { r.ObservedAt = "unknown" },
		func(r *Row) { r.Metric = "" }, func(r *Row) { r.Kind = "gauge" },
		func(r *Row) { r.Value = new(math.NaN()) }, func(r *Row) { r.Value = new(math.Inf(1)) },
		func(r *Row) { r.Type = "event"; r.Kind = "" },
	} {
		row := good
		change(&row)
		require.Error(t, Validate(row))
	}
}

func TestRunPartialCollectionAndReadOnlyStatus(t *testing.T) {
	database := filepath.Join(t.TempDir(), "metrics.sqlite")
	config := testConfig(t, database)
	collect := func(_ context.Context, c Config, ts string) ([]Row, error) {
		return []Row{Counter(c.Targets[0], "members", new(42.0), ts, "discord_invite_approximate"), Counter(c.Targets[1], "members", nil, ts, "discord_invite_approximate")}, errors.New("fixture unavailable")
	}
	var out bytes.Buffer
	err := Run(t.Context(), []string{"collect", "--config", config}, "discrawl", collect, nil, &out, io.Discard)
	require.ErrorContains(t, err, "NULL observations were retained")
	require.JSONEq(t, `{"source":"discrawl","command":"collect","rows_written":2,"ok":false}`, out.String())
	s, err := openReadOnly(t.Context(), database, "discrawl")
	require.NoError(t, err)
	var status string
	require.NoError(t, s.DB().QueryRowContext(t.Context(), "SELECT status FROM metric_runs").Scan(&status))
	require.Equal(t, "partial", status)
	require.NoError(t, s.Close())
	before, err := os.ReadFile(database)
	require.NoError(t, err)
	out.Reset()
	require.NoError(t, Run(t.Context(), []string{"status", "--config", config}, "discrawl", nil, nil, &out, io.Discard))
	var state map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &state))
	require.InDelta(t, 2, state["observations"], 0)
	require.IsType(t, "", state["last_observed"])
	after, err := os.ReadFile(database)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestStatusOrdersImportedObservationInstants(t *testing.T) {
	for _, tc := range []struct {
		name  string
		times []string
		want  *string
	}{
		{name: "empty"},
		{
			name:  "sub-millisecond",
			times: []string{"2026-09-25T00:00:00.000900Z", "2026-09-25T00:00:00.000800Z"},
			want:  new("2026-09-25T00:00:00.000900Z"),
		},
		{
			name:  "nanosecond",
			times: []string{"2026-09-25T00:00:00.000000002Z", "2026-09-25T00:00:00.000000001Z"},
			want:  new("2026-09-25T00:00:00.000000002Z"),
		},
		{
			name:  "offsets",
			times: []string{"2026-09-25T01:00:00+02:00", "2026-09-24T23:30:00Z", "2026-09-24T18:00:00-05:00"},
			want:  new("2026-09-24T23:30:00Z"),
		},
		{
			name:  "equal-instant-later-sequence",
			times: []string{"2026-09-25T00:00:00.1Z", "2026-09-25T01:00:00.100000000+01:00"},
			want:  new("2026-09-25T01:00:00.100000000+01:00"),
		},
		{
			name:  "fractional-width",
			times: []string{"2026-09-25T00:00:00.1Z", "2026-09-25T00:00:00Z"},
			want:  new("2026-09-25T00:00:00.1Z"),
		},
		{
			name:  "outside-unix-nanosecond-range",
			times: []string{"2500-01-01T00:00:00Z", "1600-01-01T00:00:00Z"},
			want:  new("2500-01-01T00:00:00Z"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := filepath.Join(t.TempDir(), "metrics.sqlite")
			config := testConfig(t, database)
			var input bytes.Buffer
			for i, observed := range tc.times {
				row := Counter(Target{"sample-alpha", "example-alpha"}, "members", nil, sampleTime, "history")
				row.ID = fmt.Sprintf("history-%d", i)
				row.ObservedAt = observed
				require.NoError(t, json.NewEncoder(&input).Encode(row))
			}
			require.NoError(t, Run(t.Context(), []string{"import", "--config", config}, "discrawl", nil, &input, io.Discard, io.Discard))
			before, err := os.ReadFile(database)
			require.NoError(t, err)
			var out bytes.Buffer
			require.NoError(t, Run(t.Context(), []string{"status", "--config", config}, "discrawl", nil, nil, &out, io.Discard))
			var state struct {
				Observations int     `json:"observations"`
				Sequence     int     `json:"sequence"`
				LastObserved *string `json:"last_observed"`
			}
			require.NoError(t, json.Unmarshal(out.Bytes(), &state))
			require.Equal(t, len(tc.times), state.Observations)
			require.Equal(t, len(tc.times), state.Sequence)
			if tc.want == nil {
				require.Nil(t, state.LastObserved)
			} else {
				require.NotNil(t, state.LastObserved)
				require.Equal(t, *tc.want, *state.LastObserved)
			}
			after, err := os.ReadFile(database)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestStatusRejectsMalformedStoredObservationTime(t *testing.T) {
	s := newMetrics(t)
	row := Counter(Target{"sample-alpha", "example-alpha"}, "members", nil, sampleTime, "history")
	_, err := Write(t.Context(), s, []Row{row})
	require.NoError(t, err)
	_, err = s.DB().ExecContext(t.Context(), "UPDATE metric_observations SET observed_at='invalid'")
	require.NoError(t, err)
	var out bytes.Buffer
	err = Run(t.Context(), []string{"status", "--config", testConfig(t, s.Path())}, "discrawl", nil, nil, &out, io.Discard)
	require.ErrorContains(t, err, "invalid stored observation time")
	require.Empty(t, out.String())
}

func TestImportCanResumeCommittedBatchesAndPreserveNulls(t *testing.T) {
	database := filepath.Join(t.TempDir(), "metrics.sqlite")
	config := testConfig(t, database)
	var input bytes.Buffer
	enc := json.NewEncoder(&input)
	for i := range 501 {
		r := Counter(Target{"sample-alpha", "example-alpha"}, "members", nil, sampleTime, "history")
		r.ID = fmt.Sprintf("history-%d", i)
		require.NoError(t, enc.Encode(r))
	}
	var out bytes.Buffer
	err := Run(t.Context(), []string{"import", "--config", config}, "discrawl", nil, strings.NewReader(input.String()+"broken\n"), &out, io.Discard)
	require.ErrorContains(t, err, "line 502")
	require.JSONEq(t, `{"source":"discrawl","command":"import","rows_written":500,"ok":false}`, out.String())
	out.Reset()
	require.NoError(t, Run(t.Context(), []string{"import", "--config", config}, "discrawl", nil, &input, &out, io.Discard))
	require.JSONEq(t, `{"source":"discrawl","command":"import","rows_written":1,"ok":true}`, out.String())
	s, err := openReadOnly(t.Context(), database, "discrawl")
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	var count int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM metric_observations WHERE value IS NULL").Scan(&count))
	require.Equal(t, 501, count)
}

func TestImportRejectsScopeMissingIDAndUnknownFields(t *testing.T) {
	for _, mutation := range []func(map[string]any){
		func(r map[string]any) { r["target"] = "another-invite" },
		func(r map[string]any) { r["entity"] = "another-entity" },
		func(r map[string]any) { delete(r, "id") },
		func(r map[string]any) { r["unexpected"] = true },
		func(r map[string]any) { r["ts"] = "invalid" },
	} {
		r := map[string]any{"type": "metric", "id": "one", "entity": "sample-alpha", "target": "example-alpha", "metric": "members", "kind": "counter", "ts": sampleTime, "observed_at": sampleTime, "provenance": "history", "value": nil}
		mutation(r)
		b, err := json.Marshal(r)
		require.NoError(t, err)
		config := testConfig(t, filepath.Join(t.TempDir(), "metrics.sqlite"))
		err = Run(t.Context(), []string{"import", "--config", config}, "discrawl", nil, bytes.NewReader(b), io.Discard, io.Discard)
		require.Error(t, err)
	}
}

func TestConfigHelpAndMissingDatabase(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"-h"}, {"--help"}, {"collect", "--help"}} {
		var out bytes.Buffer
		require.NoError(t, Run(t.Context(), args, "discrawl", nil, nil, &out, io.Discard))
		require.Contains(t, out.String(), "Online presence is not a count of active posters")
	}
	for _, args := range [][]string{{"bad"}, {"collect"}, {"collect", "--bad"}, {"collect", "--config", "missing"}, {"status", "--config", "x", "extra"}} {
		require.Error(t, Run(t.Context(), args, "discrawl", nil, nil, io.Discard, io.Discard))
	}
	path := filepath.Join(t.TempDir(), "missing.sqlite")
	config := testConfig(t, path)
	require.Error(t, Run(t.Context(), []string{"status", "--config", config}, "discrawl", nil, nil, io.Discard, io.Discard))
	require.NoFileExists(t, path)
	for _, raw := range []string{
		`{}`, `{"database":"relative","targets":[{"entity":"x","target":"example-alpha"}]}`,
		`{"database":"/tmp/x","targets":[{"entity":"x","target":"../bad"}]}`,
		`{"database":"/tmp/x","targets":[{"entity":"x","target":"example-alpha"},{"entity":"y","target":"example-alpha"}]}`,
		`{"unexpected":true}`, `{} {}`, `invalid`,
	} {
		require.NoError(t, os.WriteFile(config, []byte(raw), 0o600))
		_, err := loadConfig(config)
		require.Error(t, err)
	}
}

func TestCanceledInitializationCanRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.sqlite")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := Open(ctx, path, "discrawl")
	require.ErrorIs(t, err, context.Canceled)
	require.NoFileExists(t, path)
	s, err := Open(t.Context(), path, "discrawl")
	require.NoError(t, err)
	require.NoError(t, checkOwner(t.Context(), s.DB(), "discrawl"))
	require.NoError(t, s.Close())
}

func TestCollectionRunFailureRollsBackObservations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.sqlite")
	s, err := Open(t.Context(), path, "discrawl")
	require.NoError(t, err)
	_, err = s.DB().ExecContext(t.Context(), `CREATE TRIGGER reject_run BEFORE INSERT ON metric_runs BEGIN SELECT RAISE(ABORT, 'run rejected'); END`)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	collect := func(_ context.Context, c Config, ts string) ([]Row, error) {
		return []Row{Counter(c.Targets[0], "members", new(42.0), ts, "fixture")}, nil
	}
	err = Run(t.Context(), []string{"collect", "--config", testConfig(t, path)}, "discrawl", collect, nil, io.Discard, io.Discard)
	require.ErrorContains(t, err, "run rejected")
	s, err = openReadOnly(t.Context(), path, "discrawl")
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	var observations, runs int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM metric_observations), (SELECT count(*) FROM metric_runs)`).Scan(&observations, &runs))
	require.Zero(t, observations)
	require.Zero(t, runs)
}
