package share

import (
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

// A channel Discord no longer serves (Unknown Channel or Missing Access marker)
// cannot have its stored public permissions verified: public-only snapshots
// withhold it and its threads until a successful crawl clears the marker,
// while the full (non-public-only) archive is unchanged.
func TestPublicOnlyWithholdsUnavailableChannelsAndDescendantsUntilRecovered(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g", RawJSON: `{"roles":[{"id":"g","permissions":"1024"}]}`}))
	for _, c := range []store.ChannelRecord{
		{ID: "gone", GuildID: "g", Kind: "text", RawJSON: `{"permission_overwrites":[]}`},
		{ID: "gone-thread", GuildID: "g", ParentID: "gone", ThreadParentID: "gone", Kind: "thread_public", RawJSON: `{}`},
		{ID: "denied", GuildID: "g", Kind: "text", RawJSON: `{"permission_overwrites":[]}`},
		{ID: "ok", GuildID: "g", Kind: "text", RawJSON: `{"permission_overwrites":[]}`},
		{ID: "ok-thread", GuildID: "g", ParentID: "ok", ThreadParentID: "ok", Kind: "thread_public", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertChannel(ctx, c))
	}
	_, err = s.DB().ExecContext(ctx, `update channels set collection_scope='allowed'`)
	require.NoError(t, err)
	require.NoError(t, s.SetSyncState(ctx, "channel:gone:unavailable", "unknown_channel"))
	require.NoError(t, s.SetSyncState(ctx, "channel:denied:unavailable", "missing_access"))
	// A thread-catalog marker concerns archived-thread listing, not message access.
	require.NoError(t, s.SetSyncState(ctx, "channel:ok:thread_catalog_unavailable", "missing_access"))

	f, err := newSnapshotFilter(ctx, s.DB(), FilterOptions{PublicOnly: true})
	require.NoError(t, err)
	for id, want := range map[string]bool{"gone": false, "gone-thread": false, "denied": false, "ok": true, "ok-thread": true} {
		require.Equal(t, want, f.allowChannelID(id), id)
	}
	require.False(t, f.allowSyncState("channel:gone:unavailable"))

	full, err := newSnapshotFilter(ctx, s.DB(), FilterOptions{ExcludeChannelIDs: []string{"unrelated"}})
	require.NoError(t, err)
	for _, id := range []string{"gone", "gone-thread", "denied", "ok"} {
		require.True(t, full.allowChannelID(id), id)
	}

	// Recovery: a successful crawl deletes the marker and re-admits the channel.
	require.NoError(t, s.DeleteSyncState(ctx, "channel:gone:unavailable"))
	f, err = newSnapshotFilter(ctx, s.DB(), FilterOptions{PublicOnly: true})
	require.NoError(t, err)
	require.True(t, f.allowChannelID("gone"))
	require.True(t, f.allowChannelID("gone-thread"))
}
