package discord

import (
	"context"
	"fmt"
	"time"

	"github.com/bwmarrin/discordgo"
)

// ValidateMessageUpdateIdentity rejects conflicting known IDs before a fetched update is accepted.
// description identifies the fetch path in diagnostic messages.
func ValidateMessageUpdateIdentity(partial, full *discordgo.Message, description string) error {
	switch {
	case partial == nil || full == nil:
		return nil
	case full.ID != "" && partial.ID != "" && full.ID != partial.ID:
		return fmt.Errorf(
			"%s returned different message id: event=%s fetched=%s",
			description,
			partial.ID,
			full.ID,
		)
	case full.ChannelID != "" && partial.ChannelID != "" && full.ChannelID != partial.ChannelID:
		return fmt.Errorf(
			"%s returned different channel id: event=%s fetched=%s",
			description,
			partial.ChannelID,
			full.ChannelID,
		)
	case full.GuildID != "" && partial.GuildID != "" && full.GuildID != partial.GuildID:
		return fmt.Errorf(
			"%s returned different guild id: event=%s fetched=%s",
			description,
			partial.GuildID,
			full.GuildID,
		)
	default:
		return nil
	}
}

func newMessageTailTask(
	eventType string,
	run func(context.Context) error,
	messages ...*discordgo.Message,
) tailTask {
	task := tailTask{
		eventType:    eventType,
		failureClass: tailFailureClassOrdered,
		run:          run,
	}
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		setTailTaskID(&task.guildID, msg.GuildID)
		setTailTaskID(&task.channelID, msg.ChannelID)
		setTailTaskID(&task.messageID, msg.ID)
		if msg.Author != nil {
			setTailTaskID(&task.userID, msg.Author.ID)
		}
	}
	task.failureMetadata = newTailFailureMetadata(task)
	return task
}

func newChannelTailTask(
	eventType string,
	run func(context.Context) error,
	channels ...*discordgo.Channel,
) tailTask {
	task := tailTask{
		eventType:    eventType,
		failureClass: tailFailureClassOrdered,
		run:          run,
	}
	for _, channel := range channels {
		if channel == nil {
			continue
		}
		setTailTaskID(&task.guildID, channel.GuildID)
		setTailTaskID(&task.channelID, channel.ID)
	}
	return task
}

func newGuildTailTask(
	eventType string,
	run func(context.Context) error,
	guilds ...*discordgo.Guild,
) tailTask {
	task := tailTask{
		eventType:    eventType,
		failureClass: tailFailureClassOrdered,
		run:          run,
	}
	for _, guild := range guilds {
		if guild != nil {
			setTailTaskID(&task.guildID, guild.ID)
		}
	}
	return task
}

func newMemberTailTask(
	eventType string,
	run func(context.Context) error,
	members ...*discordgo.Member,
) tailTask {
	task := tailTask{
		eventType:    eventType,
		failureClass: tailFailureClassMember,
		receivedAt:   time.Now().UTC(),
		run:          run,
	}
	for _, member := range members {
		if member == nil {
			continue
		}
		setTailTaskID(&task.guildID, member.GuildID)
		if member.User != nil {
			setTailTaskID(&task.userID, member.User.ID)
		}
	}
	return task
}

func setTailTaskID(dst *string, value string) {
	if *dst == "" && value != "" {
		*dst = value
	}
}
