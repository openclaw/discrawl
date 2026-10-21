package syncer

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestWindowedFullSyncDoesNotStarveLiveChannelOnIncompleteBacklog(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	cutoff := time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	for _, id := range []string{"old", "live"} {
		require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: id, GuildID: "g1", Kind: "text", Name: id, RawJSON: `{}`}))
	}
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID: "200", GuildID: "g1", ChannelID: "live", AuthorID: "u1",
		Content: "existing", NormalizedContent: "existing",
		CreatedAt: cutoff.Add(-time.Hour).Format(time.RFC3339Nano), RawJSON: `{}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("live"), "1"))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("live"), "200"))

	client := &fakeClient{
		guilds:    []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Guild"}},
		channels: map[string][]*discordgo.Channel{"g1": {
			{ID: "old", GuildID: "g1", Name: "old", Type: discordgo.ChannelTypeGuildText, LastMessageID: "100"},
			{ID: "live", GuildID: "g1", Name: "live", Type: discordgo.ChannelTypeGuildText, LastMessageID: "300"},
		}},
		messages: map[string][]*discordgo.Message{
			"old": {{
				ID: "100", GuildID: "g1", ChannelID: "old", Content: "old message",
				Timestamp: cutoff.Add(-time.Hour), Author: &discordgo.User{ID: "u1", Username: "fixture"},
			}},
			"live": {
				{
					ID: "300", GuildID: "g1", ChannelID: "live", Content: "newest message",
					Timestamp: cutoff.Add(2 * time.Hour), Author: &discordgo.User{ID: "u1", Username: "fixture"},
				},
				{
					ID: "250", GuildID: "g1", ChannelID: "live", Content: "new message",
					Timestamp: cutoff.Add(time.Hour), Author: &discordgo.User{ID: "u1", Username: "fixture"},
				},
			},
		},
	}

	svc := New(client, s, nil)
	_, err = svc.Sync(ctx, SyncOptions{Full: true, Since: cutoff, Concurrency: 1})
	require.NoError(t, err)

	_, rows, err := s.ReadOnlyQuery(ctx, "select id from messages where channel_id = 'live' order by id")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"200"}, {"250"}, {"300"}}, rows, "windowed full sync must store new messages from the live channel despite the incomplete backlog")
}
