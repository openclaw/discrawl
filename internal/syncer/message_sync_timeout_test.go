package syncer

import (
	"context"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestSyncDefersSlowChannelAfterChannelTimeout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	client := newGuildTestClient()
	client.channels = map[string][]*discordgo.Channel{
		"g1": {{ID: "c1", GuildID: "g1", Name: "slow", Type: discordgo.ChannelTypeGuildText}},
	}
	client.messages = map[string][]*discordgo.Message{
		"c1": {{
			ID:        "100",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "hello",
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		}},
	}
	client.messageDelay = 100 * time.Millisecond

	svc := New(client, s, nil)
	svc.messageChannelTimeout = 20 * time.Millisecond
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Messages)

	latest, err := s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Empty(t, latest)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Empty(t, complete)
}
