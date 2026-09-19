package discord

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestCanReadHistoryUsesThreadParentPermissions(t *testing.T) {
	for _, allowed := range []bool{true, false} {
		t.Run(fmt.Sprint(allowed), func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v10/users/@me", writeJSON(map[string]any{"id": "bot"}))
			required := int64(discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory)
			mux.HandleFunc("/api/v10/guilds/g1", writeJSON(map[string]any{"id": "g1", "owner_id": "owner", "roles": []map[string]any{{"id": "g1", "permissions": fmt.Sprint(required)}}}))
			mux.HandleFunc("/api/v10/guilds/g1/members/bot", writeJSON(map[string]any{"user": map[string]any{"id": "bot"}, "roles": []string{}}))
			deny := int64(0)
			if !allowed {
				deny = discordgo.PermissionReadMessageHistory
			}
			mux.HandleFunc("/api/v10/channels/parent", writeJSON(map[string]any{"id": "parent", "guild_id": "g1", "type": 0, "permission_overwrites": []map[string]any{{"id": "g1", "type": 0, "allow": "0", "deny": fmt.Sprint(deny)}}}))
			server := httptest.NewServer(mux)
			defer server.Close()
			restore := patchDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()
			client, err := New("fixture-token")
			require.NoError(t, err)
			got, err := client.CanReadHistory(context.Background(), &discordgo.Channel{ID: "thread", GuildID: "g1", ParentID: "parent", Type: discordgo.ChannelTypeGuildPublicThread})
			require.NoError(t, err)
			require.Equal(t, allowed, got)
		})
	}
}
