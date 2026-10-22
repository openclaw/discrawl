package discorddesktop

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestCachedImportPreservesChannelTombstonesAndSkipsTheirMessages(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "gone", GuildID: "g1", Kind: "text", Name: "gone", RawJSON: `{}`}))
	require.NoError(t, s.MarkChannelDeleted(ctx, "g1", "gone", "discord-gateway"))

	snap := newSnapshot()
	snap.guilds["g1"] = store.GuildRecord{ID: "g1", Name: "guild", RawJSON: `{}`}
	snap.channels["gone"] = store.ChannelRecord{ID: "gone", GuildID: "g1", Kind: "text", Name: "cached-old-name", RawJSON: `{"id":"gone"}`}
	snap.channels["live"] = store.ChannelRecord{ID: "live", GuildID: "g1", Kind: "text", Name: "live", RawJSON: `{"id":"live"}`}
	snap.messages["m-gone"] = store.MessageMutation{Record: store.MessageRecord{ID: "m-gone", GuildID: "g1", ChannelID: "gone", Content: "cached", RawJSON: `{}`}}
	snap.messages["m-live"] = store.MessageMutation{Record: store.MessageRecord{ID: "m-live", GuildID: "g1", ChannelID: "live", Content: "cached", RawJSON: `{}`}}
	skipped, err := writeSnapshot(ctx, s, snap, false)
	require.NoError(t, err)
	require.Equal(t, unresolvedMessages{"m-gone": "gone"}, skipped)

	var deleted sql.NullString
	var name string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select deleted_at,name from channels where id='gone'`).Scan(&deleted, &name))
	require.True(t, deleted.Valid, "cached metadata must not clear a recorded deletion")
	require.Equal(t, "gone", name, "the tombstoned channel keeps its retained metadata")
	var count int
	var id string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*),min(id) from messages`).Scan(&count, &id))
	require.Equal(t, 1, count, "only the admitted channel's cached message is imported")
	require.Equal(t, "m-live", id)
	report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Zero(t, report.UnresolvedCount, "a tombstoned channel is a policy outcome, not an import failure")
}

func TestCachedImportKeepsAuthoritativeAncestryAndCountsPolicySkips(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "forum", GuildID: "g1", Kind: "forum", Name: "forum", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "thread", GuildID: "g1", ParentID: "forum", Kind: "thread_public", Name: "thread", RawJSON: `{}`}))
	require.NoError(t, s.MarkChannelDeleted(ctx, "g1", "forum", "discord-gateway"))

	snap := newSnapshot()
	snap.guilds["g1"] = store.GuildRecord{ID: "g1", Name: "guild", RawJSON: `{}`}
	// Desktop cache metadata for the thread lacks its parent.
	snap.channels["thread"] = store.ChannelRecord{ID: "thread", GuildID: "g1", Kind: "text", Name: "thread-renamed", RawJSON: `{"id":"thread"}`}
	snap.messages["m1"] = store.MessageMutation{Record: store.MessageRecord{ID: "m1", GuildID: "g1", ChannelID: "thread", Content: "cached", RawJSON: `{}`}}
	totals := newScanTotals()
	stats := &Stats{}
	require.Empty(t, finalizeSnapshot(snap, map[string]store.ChannelRecord{}, totals, stats, true))
	require.Equal(t, 1, stats.Messages)
	require.NoError(t, commitSnapshot(ctx, s, Options{}, scanState{previous: map[string]fileFingerprint{}, current: map[string]fileFingerprint{}}, nil, snap, true, totals, stats))

	var parent, kind, name string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select coalesce(parent_id,''),kind,name from channels where id='thread'`).Scan(&parent, &kind, &name))
	require.Equal(t, "forum", parent, "incomplete cache metadata cannot sever authoritative ancestry")
	require.Equal(t, "thread_public", kind)
	require.Equal(t, "thread-renamed", name, "cache metadata still refreshes names")
	var stored int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages`).Scan(&stored))
	require.Zero(t, stored, "a message beneath a tombstoned ancestor is not imported")
	require.Equal(t, 0, stats.Messages)
	require.Equal(t, 0, stats.GuildMessages)
	require.Equal(t, 1, stats.SkippedMessages)
	require.Equal(t, 1, stats.SkippedChannels)
}
