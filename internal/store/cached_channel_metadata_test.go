package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A Desktop cache import writes sparse channel records (id, guild, kind,
// name). It must not erase thread metadata that bot sync stored.
func TestCachedChannelKeepsStoredThreadMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{
		ID: "t1", GuildID: "g1", ParentID: "f1", ThreadParentID: "f1", Kind: "thread_public", Name: "release-thread",
		Topic: "release notes", IsArchived: true, IsLocked: true, ArchiveTimestamp: "2026-05-01T00:00:00Z",
		RawJSON: `{"id":"t1","type":11,"parent_id":"f1","applied_tags":["777"],"thread_metadata":{"archived":true,"locked":true,"archive_timestamp":"2026-05-01T00:00:00.000Z"}}`,
	}))
	require.NoError(t, s.UpsertCachedChannel(ctx, ChannelRecord{
		ID: "t1", GuildID: "g1", Kind: "thread_public", Name: "release-thread",
		RawJSON: `{"id":"t1","guild_id":"g1","name":"release-thread","type":11,"source":"discord_desktop"}`,
	}))

	rows, err := s.Channels(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	row := rows[0]
	require.Equal(t, "f1", row.ParentID)
	require.Equal(t, "f1", row.ThreadParentID)
	require.Equal(t, "release notes", row.Topic)
	require.True(t, row.IsArchived)
	require.True(t, row.IsLocked)
	require.False(t, row.ArchiveTimestamp.IsZero())
	_, raw, err := s.ReadOnlyQuery(ctx, "select json_extract(raw_json, '$.applied_tags[0]'), json_extract(raw_json, '$.thread_metadata.archived') from channels where id = 't1'")
	require.NoError(t, err)
	require.Equal(t, []string{"777", "1"}, raw[0])
}

// A channel known only from the Desktop cache keeps refreshing from it.
func TestCachedChannelRefreshesDesktopOwnedPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	for _, name := range []string{"old-name", "new-name"} {
		require.NoError(t, s.UpsertCachedChannel(ctx, ChannelRecord{
			ID: "c1", GuildID: "g1", Kind: "text", Name: name,
			RawJSON: `{"id":"c1","guild_id":"g1","name":"` + name + `","type":0,"source":"discord_desktop"}`,
		}))
	}
	_, rows, err := s.ReadOnlyQuery(ctx, "select name, json_extract(raw_json, '$.name'), json_extract(raw_json, '$.source') from channels where id = 'c1'")
	require.NoError(t, err)
	require.Equal(t, []string{"new-name", "new-name", "discord_desktop"}, rows[0])
}
