package syncer

import (
	"context"
	"errors"

	"github.com/bwmarrin/discordgo"
)

// The syncer owns exact refetch after checking collection scope. The Gateway
// adapter must not fetch excluded message content before this check.
func (t *tailHandler) TailHandlesUpdateRefetch() bool { return true }

func (t *tailHandler) OnChannelDelete(ctx context.Context, c *discordgo.Channel) error {
	if c == nil || c.ID == "" {
		return nil
	}
	if !t.allowGuild(c.GuildID) {
		return nil
	}
	if err := t.store.MarkChannelDeleted(ctx, c.GuildID, c.ID, "discord-gateway"); err != nil {
		return err
	}
	return t.refreshScope(ctx)
}

func (t *tailHandler) OnGuildRoleUpsert(ctx context.Context, r *discordgo.GuildRole) error {
	if r == nil || r.Role == nil || !t.allowGuild(r.GuildID) {
		return nil
	}
	return t.store.ApplyGuildRole(ctx, r.GuildID, r.Role.ID, r.Role)
}

func (t *tailHandler) OnGuildRoleDelete(ctx context.Context, guildID, roleID string) error {
	if !t.allowGuild(guildID) {
		return nil
	}
	return t.store.ApplyGuildRole(ctx, guildID, roleID, nil)
}

func (t *tailHandler) OnThreadList(ctx context.Context, guildID string, channels []*discordgo.Channel) error {
	if !t.allowGuild(guildID) {
		return nil
	}
	for _, c := range channels {
		if c == nil || c.ID == "" {
			continue
		}
		observed := *c
		if observed.GuildID != "" && observed.GuildID != guildID {
			return errors.New("thread list guild mismatch")
		}
		observed.GuildID = guildID
		if err := t.store.UpsertChannel(ctx, toChannelRecord(&observed, marshalJSONString(&observed, "{}"))); err != nil {
			return err
		}
	}
	return t.refreshScope(ctx)
}
