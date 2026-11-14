package discorddesktop

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

// Desktop cache messages carry role mentions in mention_roles, like the bot
// API. Imports must record them, as bot sync does, and must not drop role
// mentions already stored for the same message.
func TestImportRecordsRoleMentions(t *testing.T) {
	const (
		guildID   = "999999999999999981"
		channelID = "111111111111111181"
		messageID = "333333333333333381"
		roleID    = "777777777777777781"
		userID    = "555555555555555581"
	)
	cacheMessage := `{"id":"` + messageID + `","channel_id":"` + channelID + `","content":"<@&` + roleID + `> deploy is done <@` + userID + `>","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222281","username":"alice"},"mentions":[{"id":"` + userID + `","username":"bob"}],"mention_roles":["` + roleID + `"]}`

	for _, preStored := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh import", true: "role mention already stored"}[preStored], func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			cachePath := filepath.Join(dir, "Cache", "Cache_Data")
			require.NoError(t, os.MkdirAll(cachePath, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), []byte("https://discord.com/channels/"+guildID+"/"+channelID+"\n"+cacheMessage+"\n"), 0o600))

			st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
			require.NoError(t, err)
			defer func() { _ = st.Close() }()
			if preStored {
				// What bot sync or tail stores for the same message.
				eventAt := "2026-04-23T18:20:43Z"
				require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{ID: channelID, GuildID: guildID, Kind: "text", Name: "general", RawJSON: `{}`}))
				require.NoError(t, st.UpsertMessages(ctx, []store.MessageMutation{{
					Record: store.MessageRecord{
						ID: messageID, GuildID: guildID, ChannelID: channelID, AuthorID: "222222222222222281", AuthorName: "alice",
						CreatedAt: eventAt, Content: "<@&" + roleID + "> deploy is done <@" + userID + ">", NormalizedContent: "deploy is done",
						RawJSON: cacheMessage,
					},
					Mentions: []store.MentionEventRecord{
						{MessageID: messageID, GuildID: guildID, ChannelID: channelID, AuthorID: "222222222222222281", TargetType: "user", TargetID: userID, TargetName: "bob", EventAt: eventAt},
						{MessageID: messageID, GuildID: guildID, ChannelID: channelID, AuthorID: "222222222222222281", TargetType: "role", TargetID: roleID, EventAt: eventAt},
					},
				}}))
			}

			_, err = Import(ctx, st, Options{Path: dir})
			require.NoError(t, err)

			roles, err := st.ListMentions(ctx, store.MentionListOptions{TargetType: "role", Limit: 10})
			require.NoError(t, err)
			require.Len(t, roles, 1)
			require.Equal(t, roleID, roles[0].TargetID)
			users, err := st.ListMentions(ctx, store.MentionListOptions{TargetType: "user", Limit: 10})
			require.NoError(t, err)
			require.Len(t, users, 1)
		})
	}
}
