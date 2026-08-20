package cli

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
)

func TestUnavailableMarkerSummary(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	require.Empty(t, unavailableMarkerSummary(nil, now))

	markers := []store.SyncStateEntry{
		{Scope: "channel:a:unavailable", UpdatedAt: now.Add(-91 * 24 * time.Hour)},
		{Scope: "channel:b:unavailable", UpdatedAt: now.Add(-30 * 24 * time.Hour)},
		{Scope: "channel:c:unavailable", UpdatedAt: now.Add(-24 * time.Hour)},
	}
	summary := unavailableMarkerSummary(markers, now)
	require.Equal(t, "3 channels marked unavailable, 1 excluded from backfill, 2 past the 7d window, oldest 91d", summary)

	// An unparsable timestamp must not crash or claim to be the oldest.
	summary = unavailableMarkerSummary([]store.SyncStateEntry{{Scope: "channel:x:unavailable"}}, now)
	require.Equal(t, "1 channels marked unavailable, 0 excluded from backfill, 1 past the 7d window", summary)
}

func TestDoctorReportsStaleUnavailableMarkers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	// Written in the on-disk format SetSyncState produces, backdated so the
	// report has something stale to describe.
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	for scope, age := range map[string]time.Duration{
		"channel:a:unavailable":                91 * 24 * time.Hour,
		"channel:b:unavailable":                24 * time.Hour,
		"channel:c:thread_catalog_unavailable": 24 * time.Hour,
	} {
		_, execErr := db.ExecContext(ctx, `insert into sync_state(scope, cursor, updated_at) values(?, ?, ?)`,
			scope, "missing_access", time.Now().UTC().Add(-age).Format("2006-01-02T15:04:05.000000000Z07:00"))
		require.NoError(t, execErr)
	}
	require.NoError(t, db.Close())
	require.NoError(t, s.Close())

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		now:        func() time.Time { return time.Now().UTC() },
	}
	require.NoError(t, rt.runDoctor(nil))
	// The thread-catalog marker must not be counted.
	require.Contains(t, out.String(), "stale_sync_markers=2 channels marked unavailable, 1 excluded from backfill, 1 past the 7d window, oldest 91d")
}

func TestDoctorOmitsMarkerLineWhenNoneExist(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	var out bytes.Buffer
	rt := &runtime{ctx: ctx, configPath: cfgPath, stdout: &out, stderr: &bytes.Buffer{}, logger: discardLogger()}
	require.NoError(t, rt.runDoctor(nil))
	require.NotContains(t, out.String(), "stale_sync_markers")
	require.Contains(t, out.String(), "database=ok")
}
