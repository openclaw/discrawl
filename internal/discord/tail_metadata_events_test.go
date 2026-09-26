package discord

import (
	"context"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

type metadataEventRecorder struct {
	recordingHandler
	messageIDs, channelIDs, roleIDs []string
	threadLists                     int
}

func (h *metadataEventRecorder) OnMessageDelete(_ context.Context, e *discordgo.MessageDelete) error {
	h.messageIDs = append(h.messageIDs, e.ID)
	return nil
}

func (h *metadataEventRecorder) OnChannelDelete(_ context.Context, c *discordgo.Channel) error {
	h.channelIDs = append(h.channelIDs, c.ID)
	return nil
}

func (h *metadataEventRecorder) OnGuildRoleUpsert(_ context.Context, r *discordgo.GuildRole) error {
	h.roleIDs = append(h.roleIDs, r.Role.ID)
	return nil
}

func (h *metadataEventRecorder) OnGuildRoleDelete(_ context.Context, _, id string) error {
	h.roleIDs = append(h.roleIDs, id)
	return nil
}

func (h *metadataEventRecorder) OnThreadList(_ context.Context, _ string, _ []*discordgo.Channel) error {
	h.threadLists++
	return nil
}

func TestSupportedMetadataEventsAndBulkDeleteDispatchRealIdentities(t *testing.T) {
	t.Parallel()
	c := &Client{}
	h := &metadataEventRecorder{}
	queue := make(chan tailTask, 16)
	var handlers []any
	c.addMetadataTailHandlers(t.Context(), h, queue, newTailFatalState(), func(v any) { handlers = append(handlers, v) })
	for _, fn := range handlers {
		switch f := fn.(type) {
		case func(*discordgo.Session, *discordgo.MessageDeleteBulk):
			f(nil, nil)
			f(nil, &discordgo.MessageDeleteBulk{GuildID: "g", ChannelID: "c", Messages: []string{"1", "2"}})
		case func(*discordgo.Session, *discordgo.ChannelDelete):
			f(nil, &discordgo.ChannelDelete{Channel: &discordgo.Channel{ID: "c", GuildID: "g"}})
		case func(*discordgo.Session, *discordgo.ThreadDelete):
			f(nil, &discordgo.ThreadDelete{Channel: &discordgo.Channel{ID: "thread", GuildID: "g"}})
		case func(*discordgo.Session, *discordgo.GuildRoleCreate):
			f(nil, &discordgo.GuildRoleCreate{GuildRole: &discordgo.GuildRole{GuildID: "g", Role: &discordgo.Role{ID: "role"}}})
		case func(*discordgo.Session, *discordgo.GuildRoleUpdate):
			f(nil, &discordgo.GuildRoleUpdate{GuildRole: &discordgo.GuildRole{GuildID: "g", Role: &discordgo.Role{ID: "role"}}})
		case func(*discordgo.Session, *discordgo.GuildRoleDelete):
			f(nil, &discordgo.GuildRoleDelete{GuildID: "g", RoleID: "role"})
		case func(*discordgo.Session, *discordgo.ThreadListSync):
			f(nil, &discordgo.ThreadListSync{GuildID: "g"})
		default:
			t.Fatalf("unexpected handler %T", fn)
		}
	}
	for len(queue) > 0 {
		task := <-queue
		require.NoError(t, task.run(t.Context()))
	}
	require.Equal(t, []string{"1", "2"}, h.messageIDs)
	require.Equal(t, []string{"c", "thread"}, h.channelIDs)
	require.Equal(t, []string{"role", "role", "role"}, h.roleIDs)
	require.Equal(t, 1, h.threadLists)
}
