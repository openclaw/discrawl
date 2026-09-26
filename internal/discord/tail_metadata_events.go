package discord

import (
	"context"

	"github.com/bwmarrin/discordgo"
)

type channelDeleteHandler interface {
	OnChannelDelete(context.Context, *discordgo.Channel) error
}
type guildRoleHandler interface {
	OnGuildRoleUpsert(context.Context, *discordgo.GuildRole) error
	OnGuildRoleDelete(context.Context, string, string) error
}
type threadListHandler interface {
	OnThreadList(context.Context, string, []*discordgo.Channel) error
}

func (c *Client) addMetadataTailHandlers(ctx context.Context, h EventHandler, queue chan<- tailTask, fatal *tailFatalState, add func(any)) {
	add(func(_ *discordgo.Session, e *discordgo.MessageDeleteBulk) {
		if e == nil {
			return
		}
		for _, id := range e.Messages {
			if id == "" {
				continue
			}
			msg := &discordgo.Message{ID: id, ChannelID: e.ChannelID, GuildID: e.GuildID}
			event := &discordgo.MessageDelete{Message: msg}
			c.enqueueTailTask(ctx, queue, fatal, newMessageTailTask("MESSAGE_DELETE", func(call context.Context) error { return h.OnMessageDelete(call, event) }, msg))
		}
	})
	if handler, ok := h.(channelDeleteHandler); ok {
		apply := func(channel *discordgo.Channel, kind string) {
			c.enqueueTailTask(ctx, queue, fatal, newChannelTailTask(kind, func(call context.Context) error { return handler.OnChannelDelete(call, channel) }, channel))
		}
		add(func(_ *discordgo.Session, e *discordgo.ChannelDelete) {
			if e != nil {
				apply(e.Channel, "CHANNEL_DELETE")
			}
		})
		add(func(_ *discordgo.Session, e *discordgo.ThreadDelete) {
			if e != nil {
				apply(e.Channel, "THREAD_DELETE")
			}
		})
	}
	if handler, ok := h.(guildRoleHandler); ok {
		apply := func(role *discordgo.GuildRole, kind string) {
			if role != nil {
				c.enqueueTailTask(ctx, queue, fatal, newGuildTailTask(kind, func(call context.Context) error { return handler.OnGuildRoleUpsert(call, role) }, &discordgo.Guild{ID: role.GuildID}))
			}
		}
		add(func(_ *discordgo.Session, e *discordgo.GuildRoleCreate) {
			if e != nil {
				apply(e.GuildRole, "GUILD_ROLE_CREATE")
			}
		})
		add(func(_ *discordgo.Session, e *discordgo.GuildRoleUpdate) {
			if e != nil {
				apply(e.GuildRole, "GUILD_ROLE_UPDATE")
			}
		})
		add(func(_ *discordgo.Session, e *discordgo.GuildRoleDelete) {
			if e != nil {
				c.enqueueTailTask(ctx, queue, fatal, newGuildTailTask("GUILD_ROLE_DELETE", func(call context.Context) error { return handler.OnGuildRoleDelete(call, e.GuildID, e.RoleID) }, &discordgo.Guild{ID: e.GuildID}))
			}
		})
	}
	if handler, ok := h.(threadListHandler); ok {
		add(func(_ *discordgo.Session, e *discordgo.ThreadListSync) {
			if e != nil {
				c.enqueueTailTask(ctx, queue, fatal, newGuildTailTask("THREAD_LIST_SYNC", func(call context.Context) error { return handler.OnThreadList(call, e.GuildID, e.Threads) }, &discordgo.Guild{ID: e.GuildID}))
			}
		})
	}
}
