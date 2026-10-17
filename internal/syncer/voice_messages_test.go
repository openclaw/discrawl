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

func TestVoiceAndStageMessagesUseHistorySync(t *testing.T) {
	t.Parallel()
	for _, channelType := range []discordgo.ChannelType{discordgo.ChannelTypeGuildVoice, discordgo.ChannelTypeGuildStageVoice} {
		t.Run(channelKind(&discordgo.Channel{Type: channelType}), func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close()) }()

			channel := &discordgo.Channel{ID: "c1", GuildID: "g1", Type: channelType, LastMessageID: "10"}
			require.NoError(t, s.UpsertChannel(ctx, toChannelRecord(channel, `{}`)))
			client := &fakeClient{messages: map[string][]*discordgo.Message{
				"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1"}, Content: "voice chat"}},
			}}
			svc := New(client, s, nil)
			count, err := svc.syncMessageChannels(ctx, "g1", []*discordgo.Channel{channel}, SyncOptions{Full: true})
			require.NoError(t, err)
			require.Equal(t, 1, count)
			latest, err := s.GetSyncState(ctx, channelLatestScope("c1"))
			require.NoError(t, err)
			require.Equal(t, "10", latest)
			complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
			require.NoError(t, err)
			require.Equal(t, "1", complete)

			// Targeted and resumed work reconstructs channels from the archive.
			rows, err := s.Channels(ctx, "g1")
			require.NoError(t, err)
			require.Len(t, rows, 1)
			restored := channelFromRow(rows[0])
			require.Equal(t, channelType, restored.Type)
			require.True(t, IsMessageChannel(restored))
			require.False(t, isThreadParent(restored))
		})
	}
}

func TestNumericChannelKindsRetainNativeIdentity(t *testing.T) {
	for _, kind := range []discordgo.ChannelType{discordgo.ChannelTypeGuildStageVoice, discordgo.ChannelTypeGuildMedia, 99} {
		channel := &discordgo.Channel{Type: kind}
		restored := channelFromRow(store.ChannelRow{Kind: channelKind(channel)})
		require.Equal(t, kind, restored.Type)
		require.Equal(t, kind == discordgo.ChannelTypeGuildStageVoice, IsMessageChannel(restored))
	}
	for _, kind := range []string{"unknown", "type_", "type_-1", "type_+13", "type_013", "type_99999999999999999999999"} {
		require.False(t, IsMessageChannel(channelFromRow(store.ChannelRow{Kind: kind})), kind)
	}
}
