package syncer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestSyncIndexesForwardedMessageSnapshotText(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	// Discord's forward shape: empty content, FORWARD reference, snapshot text.
	var forward discordgo.Message
	require.NoError(t, json.Unmarshal([]byte(`{
		"id": "20", "guild_id": "g1", "channel_id": "c1", "type": 0, "content": "",
		"timestamp": "2026-08-19T12:00:00.000000+00:00", "author": {"id": "u1", "username": "user"},
		"message_reference": {"type": 1, "message_id": "10", "channel_id": "c0", "guild_id": "g1"},
		"message_snapshots": [{"message": {"type": 0, "content": "quarterly roadmap draft", "timestamp": "2026-08-19T11:00:00.000000+00:00", "flags": 0}}]
	}`), &forward))
	client := newGuildTestClient()
	client.channels = map[string][]*discordgo.Channel{
		"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText, LastMessageID: "20"}},
	}
	client.messages = map[string][]*discordgo.Message{"c1": {&forward}}

	_, err := New(client, s, nil).Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	results, err := s.SearchMessages(ctx, store.SearchOptions{Query: "roadmap"})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "20", results[0].MessageID)
}
