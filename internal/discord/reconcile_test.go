package discord

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestCanReadHistoryUsesThreadParentPermissions(t *testing.T) {
	for _, allowed := range []bool{true, false} {
		t.Run(strconv.FormatBool(allowed), func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v10/channels/thread", writeJSON(map[string]any{"id": "thread", "guild_id": "g1", "parent_id": "parent", "type": 11}))
			mux.HandleFunc("/api/v10/users/@me", writeJSON(map[string]any{"id": "bot"}))
			required := int64(discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory)
			mux.HandleFunc("/api/v10/guilds/g1", writeJSON(map[string]any{"id": "g1", "owner_id": "owner", "roles": []map[string]any{{"id": "g1", "permissions": strconv.FormatInt(required, 10)}}}))
			mux.HandleFunc("/api/v10/guilds/g1/members/bot", writeJSON(map[string]any{"user": map[string]any{"id": "bot"}, "roles": []string{}}))
			deny := int64(0)
			if !allowed {
				deny = discordgo.PermissionReadMessageHistory
			}
			mux.HandleFunc("/api/v10/channels/parent", writeJSON(map[string]any{"id": "parent", "guild_id": "g1", "type": 0, "permission_overwrites": []map[string]any{{"id": "g1", "type": 0, "allow": "0", "deny": strconv.FormatInt(deny, 10)}}}))
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

func TestCanReadVoiceAndStageHistoryRequiresConnect(t *testing.T) {
	for _, kind := range []discordgo.ChannelType{discordgo.ChannelTypeGuildText, discordgo.ChannelTypeGuildVoice, discordgo.ChannelTypeGuildStageVoice} {
		for _, connect := range []bool{false, true} {
			t.Run(strconv.Itoa(int(kind))+"/connect="+strconv.FormatBool(connect), func(t *testing.T) {
				permissions := int64(discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory)
				if connect {
					permissions |= discordgo.PermissionVoiceConnect
				}
				mux := http.NewServeMux()
				mux.HandleFunc("/api/v10/users/@me", writeJSON(map[string]any{"id": "bot"}))
				mux.HandleFunc("/api/v10/guilds/g1", writeJSON(map[string]any{"id": "g1", "owner_id": "owner", "roles": []map[string]any{{"id": "g1", "permissions": strconv.FormatInt(permissions, 10)}}}))
				mux.HandleFunc("/api/v10/guilds/g1/members/bot", writeJSON(map[string]any{"user": map[string]any{"id": "bot"}, "roles": []string{}}))
				mux.HandleFunc("/api/v10/channels/c1", writeJSON(map[string]any{"id": "c1", "guild_id": "g1", "type": kind}))
				server := httptest.NewServer(mux)
				defer server.Close()
				restore := patchDiscordEndpoints(server.URL + "/api/v10/")
				defer restore()
				client, err := New("fixture-token")
				require.NoError(t, err)
				// A gateway cache must not override a current REST permission denial.
				require.NoError(t, client.session.State.GuildAdd(&discordgo.Guild{ID: "g1", OwnerID: "bot"}))
				require.NoError(t, client.session.State.ChannelAdd(&discordgo.Channel{ID: "c1", GuildID: "g1", Type: kind}))
				got, err := client.CanReadHistory(context.Background(), &discordgo.Channel{ID: "c1", GuildID: "g1", Type: kind})
				require.NoError(t, err)
				require.Equal(t, connect || kind == discordgo.ChannelTypeGuildText, got)
				_, err = client.CanReadHistory(context.Background(), &discordgo.Channel{ID: "c1", GuildID: "different", Type: kind})
				require.Error(t, err, "a cached channel must not certify a different live guild")
				_, err = client.CanReadHistory(context.Background(), &discordgo.Channel{ID: "c1", GuildID: "g1", Type: discordgo.ChannelType(99)})
				require.Error(t, err, "a cached channel must not certify a different live type")
			})
		}
	}
}
