package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ageSyncStateMarker writes a marker with a backdated updated_at in the exact
// on-disk format SetSyncState produces: RFC3339 with a 'T' separator, nine
// fractional digits, and a trailing 'Z'. Production rows look like
// 2026-05-20T03:09:04.033368707Z, so the TTL comparison must hold for that
// shape rather than for a synthetic 'YYYY-MM-DD HH:MM:SS' value.
func ageSyncStateMarker(ctx context.Context, t *testing.T, s *Store, scope, cursor string, age time.Duration) string {
	t.Helper()
	stamp := time.Now().UTC().Add(-age).Format(timeLayout)
	_, err := s.db.ExecContext(ctx, `
insert into sync_state(scope, cursor, updated_at)
values(?, ?, ?)
on conflict(scope) do update set cursor = excluded.cursor, updated_at = excluded.updated_at
`, scope, cursor, stamp)
	require.NoError(t, err)
	return stamp
}

func TestUnavailableMarkerExpiresFromIncompleteListing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, id := range []string{"stale", "fresh", "clean"} {
		require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: id, GuildID: "g1", Kind: "text", Name: id, RawJSON: `{}`}))
	}

	staleStamp := ageSyncStateMarker(ctx, t, s, "channel:stale:unavailable", "missing_access", 30*24*time.Hour)
	freshStamp := ageSyncStateMarker(ctx, t, s, "channel:fresh:unavailable", "missing_access", 24*time.Hour)

	// Guard the format itself: nanosecond precision, 'T', trailing 'Z'.
	require.Len(t, staleStamp, 30, "stale marker must use the production timestamp shape: %s", staleStamp)
	require.Len(t, freshStamp, 30, "fresh marker must use the production timestamp shape: %s", freshStamp)
	require.Contains(t, staleStamp, "T")
	require.True(t, staleStamp[len(staleStamp)-1] == 'Z')

	ids, err := s.IncompleteMessageChannelIDs(ctx, "")
	require.NoError(t, err)
	require.Contains(t, ids, "stale", "a 30-day-old marker must expire and let the channel back into backfill")
	require.NotContains(t, ids, "fresh", "a 1-day-old marker must still exclude the channel")
	require.Contains(t, ids, "clean")

	byGuild, err := s.IncompleteMessageChannelIDs(ctx, "g1")
	require.NoError(t, err)
	require.Equal(t, ids, byGuild, "the per-guild query must apply the same window")

	// A marker written by the normal path is fresh, so it excludes immediately.
	require.NoError(t, s.SetSyncState(ctx, "channel:clean:unavailable", "missing_access"))
	ids, err = s.IncompleteMessageChannelIDs(ctx, "g1")
	require.NoError(t, err)
	require.NotContains(t, ids, "clean")
}

func TestFreshUnavailableChannelIDsWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	ageSyncStateMarker(ctx, t, s, "channel:stale:unavailable", "missing_access", 30*24*time.Hour)
	ageSyncStateMarker(ctx, t, s, "channel:fresh:unavailable", "missing_access", 24*time.Hour)
	// Just inside and just outside the window.
	ageSyncStateMarker(ctx, t, s, "channel:edge-in:unavailable", "missing_access", 7*24*time.Hour-time.Hour)
	ageSyncStateMarker(ctx, t, s, "channel:edge-out:unavailable", "missing_access", 7*24*time.Hour+time.Hour)
	// A thread-catalog marker must not be mistaken for a message marker: it
	// ends in '_unavailable', not ':unavailable'.
	ageSyncStateMarker(ctx, t, s, "channel:catalog:thread_catalog_unavailable", "missing_access", time.Hour)

	fresh, err := s.FreshUnavailableChannelIDs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"fresh", "edge-in"}, fresh)

	// Re-marking an expired channel refreshes the window.
	require.NoError(t, s.SetSyncState(ctx, "channel:stale:unavailable", "missing_access"))
	fresh, err = s.FreshUnavailableChannelIDs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"fresh", "edge-in", "stale"}, fresh)

	// Clearing removes it entirely.
	require.NoError(t, s.DeleteSyncState(ctx, "channel:stale:unavailable"))
	fresh, err = s.FreshUnavailableChannelIDs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"fresh", "edge-in"}, fresh)
}

func TestSyncStateBySuffix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	empty, err := s.SyncStateBySuffix(ctx, "")
	require.NoError(t, err)
	require.Empty(t, empty)

	ageSyncStateMarker(ctx, t, s, "channel:old:unavailable", "missing_access", 90*24*time.Hour)
	ageSyncStateMarker(ctx, t, s, "channel:new:unavailable", "unknown_channel", time.Hour)
	ageSyncStateMarker(ctx, t, s, "channel:catalog:thread_catalog_unavailable", "missing_access", time.Hour)

	entries, err := s.SyncStateBySuffix(ctx, ":unavailable")
	require.NoError(t, err)
	require.Len(t, entries, 2, "thread_catalog_unavailable must not match the ':unavailable' suffix")
	// Oldest first.
	require.Equal(t, "channel:old:unavailable", entries[0].Scope)
	require.Equal(t, "channel:new:unavailable", entries[1].Scope)
	require.WithinDuration(t, time.Now().UTC().Add(-90*24*time.Hour), entries[0].UpdatedAt, time.Minute)
	require.False(t, entries[1].UpdatedAt.IsZero())
}
