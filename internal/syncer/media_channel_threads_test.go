package syncer

import (
	"context"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

// Discord media channels (type 16) contain only threads, like forums.
func mediaChannelTestClient() *fakeClient {
	now := time.Now().UTC()
	client := newGuildTestClient()
	client.channels = map[string][]*discordgo.Channel{
		"g1": {{ID: "m1", GuildID: "g1", Name: "clips", Type: discordgo.ChannelTypeGuildMedia}},
	}
	client.activeThreads = map[string][]*discordgo.Channel{
		"m1": {{
			ID: "t1", GuildID: "g1", ParentID: "m1", Name: "active-post",
			Type: discordgo.ChannelTypeGuildPublicThread, LastMessageID: "10",
		}},
	}
	client.publicArchived = map[string][]*discordgo.Channel{
		"m1": {{
			ID: "t2", GuildID: "g1", ParentID: "m1", Name: "archived-post",
			Type: discordgo.ChannelTypeGuildPublicThread, LastMessageID: "20",
			ThreadMetadata: &discordgo.ThreadMetadata{Archived: true, ArchiveTimestamp: now.Add(-time.Hour)},
		}},
	}
	client.messages = map[string][]*discordgo.Message{
		"t1": {{
			ID: "10", GuildID: "g1", ChannelID: "t1", Content: "active media post",
			Timestamp: now, Author: &discordgo.User{ID: "u1", Username: "user"},
		}},
		"t2": {{
			ID: "20", GuildID: "g1", ChannelID: "t2", Content: "archived media post",
			Timestamp: now.Add(-2 * time.Hour), Author: &discordgo.User{ID: "u1", Username: "user"},
		}},
	}
	return client
}

func requireMediaPostsArchived(t *testing.T, ctx context.Context, s *store.Store) {
	t.Helper()
	results, err := s.SearchMessages(ctx, store.SearchOptions{Query: "media post"})
	require.NoError(t, err)
	channels := make([]string, 0, len(results))
	for _, result := range results {
		channels = append(channels, result.ChannelID)
	}
	require.ElementsMatch(t, []string{"t1", "t2"}, channels)
}

func TestSyncArchivesMediaChannelPosts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()
	client := mediaChannelTestClient()

	stats, err := New(client, s, nil).Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Threads)
	require.Equal(t, 2, stats.Messages)
	requireMediaPostsArchived(t, ctx, s)
}

func TestSyncChannelSubsetExpandsRequestedMediaChannelPosts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()
	client := mediaChannelTestClient()

	stats, err := New(client, s, nil).Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}, ChannelIDs: []string{"m1"}})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Threads)
	require.Equal(t, 2, stats.Messages)
	requireMediaPostsArchived(t, ctx, s)
}
