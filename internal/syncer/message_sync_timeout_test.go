package syncer

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestSyncDefersSlowChannelAfterChannelTimeout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "slow", Type: discordgo.ChannelTypeGuildText}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "100",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "hello",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
		messageDelay: 100 * time.Millisecond,
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1, MessageChannelTimeout: 20 * time.Millisecond})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Messages)

	latest, err := s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Empty(t, latest)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Empty(t, complete)
}
