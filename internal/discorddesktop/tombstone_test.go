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
	require.NoError(t, writeSnapshot(ctx, s, snap, false))

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
