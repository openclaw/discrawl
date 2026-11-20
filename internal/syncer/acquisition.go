package syncer

import (
	"context"
	"errors"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
)

// NormalizeAcquiredMessage uses the native message/text owners without opening
// an archive, extracting attachment bodies or scheduling enrichment work.
func NormalizeAcquiredMessage(ctx context.Context, message *discordgo.Message, guild string) (store.MessageRecord, error) {
	if message == nil || message.ID == "" || message.ChannelID == "" || guild == "" || message.GuildID != "" && message.GuildID != guild {
		return store.MessageRecord{}, errors.New("invalid acquired message scope")
	}
	mutation, err := buildMessageMutation(ctx, message, "", guild, false, false)
	return mutation.Record, err
}
