package syncer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
)

type ReconcileMember struct {
	GuildID string `json:"guild_id"`
	UserID  string `json:"user_id"`
}
type ReconcileMessage struct {
	GuildID   string `json:"guild_id"`
	ChannelID string `json:"channel_id"`
	MessageID string `json:"message_id"`
	CreatedAt string `json:"created_at"`
}
type ReconcilePlan struct {
	Archive  string             `json:"archive"`
	Members  []ReconcileMember  `json:"members"`
	Messages []ReconcileMessage `json:"messages"`
}
type ReconcileStats struct {
	Restored         int `json:"restored"`
	ConfirmedDeleted int `json:"confirmed_deleted"`
	AlreadyPresent   int `json:"already_present"`
}
type ReconcileClient interface {
	CanReadHistory(context.Context, *discordgo.Channel) (bool, error)
	Guild(context.Context, string) (*discordgo.Guild, error)
	Channel(context.Context, string) (*discordgo.Channel, error)
	GuildMember(context.Context, string, string) (*discordgo.Member, error)
	ChannelMessage(context.Context, string, string) (*discordgo.Message, error)
}

// ReconcileMissing uses exact Discord lookups to restore missing source records.
// Callers must hold the publisher's exclusive runtime. All lookups finish before
// writes start, so a late permission failure cannot publish partial observations.
func ReconcileMissing(ctx context.Context, s *store.Store, client ReconcileClient, plan ReconcilePlan) (ReconcileStats, error) {
	stats := ReconcileStats{}
	if plan.Archive == "" || len(plan.Members)+len(plan.Messages) == 0 || len(plan.Members)+len(plan.Messages) > 10000 {
		return stats, errors.New("invalid reconciliation plan size or archive")
	}
	validID := func(id string) bool {
		n, err := strconv.ParseUint(id, 10, 64)
		return err == nil && n > 0 && strconv.FormatUint(n, 10) == id
	}
	seen := map[string]bool{}
	for _, row := range plan.Members {
		key := "member:" + row.GuildID + ":" + row.UserID
		if !validID(row.GuildID) || !validID(row.UserID) || seen[key] {
			return stats, errors.New("invalid or duplicate member identity")
		}
		seen[key] = true
		var found int
		if err := s.DB().QueryRowContext(ctx, "select 1 from guilds where id=? and deleted_at is null", row.GuildID).Scan(&found); err != nil {
			return stats, errors.New("member guild is outside the live source scope")
		}
	}
	for _, row := range plan.Messages {
		key := "message:" + row.MessageID
		if !validID(row.GuildID) || !validID(row.ChannelID) || !validID(row.MessageID) || seen[key] {
			return stats, errors.New("invalid or duplicate message identity")
		}
		seen[key] = true
		if _, err := time.Parse(time.RFC3339Nano, row.CreatedAt); err != nil {
			return stats, errors.New("message creation time is required")
		}
		var found int
		if err := s.DB().QueryRowContext(ctx, "select 1 from channels c join guilds g on g.id=c.guild_id where c.id=? and c.guild_id=? and g.deleted_at is null", row.ChannelID, row.GuildID).Scan(&found); err != nil {
			return stats, errors.New("message channel is outside the live source scope")
		}
	}
	// Establish current parent access before interpreting a missing child response.
	guilds := map[string]struct{}{}
	channels := map[string]string{}
	for _, row := range plan.Members {
		guilds[row.GuildID] = struct{}{}
	}
	for _, row := range plan.Messages {
		guilds[row.GuildID] = struct{}{}
		channels[row.ChannelID] = row.GuildID
	}
	for id := range guilds {
		guild, err := client.Guild(ctx, id)
		if err != nil {
			return stats, reconcileLookupError("guild", err)
		}
		if guild == nil || guild.ID != id || guild.Unavailable {
			return stats, errors.New("guild access could not be verified")
		}
	}
	for id, guildID := range channels {
		channel, err := client.Channel(ctx, id)
		if err != nil {
			return stats, reconcileLookupError("channel", err)
		}
		if channel == nil || channel.ID != id || channel.GuildID != guildID {
			return stats, errors.New("channel access could not be verified")
		}
		allowed, err := client.CanReadHistory(ctx, channel)
		if err != nil {
			return stats, reconcileLookupError("message-history permissions", err)
		}
		if !allowed {
			return stats, errors.New("message history is not readable; no observations applied")
		}
	}
	type memberObservation struct {
		key  ReconcileMember
		live *discordgo.Member
	}
	type messageObservation struct {
		key         ReconcileMessage
		live        *discordgo.Message
		channelName string
	}
	members := []memberObservation{}
	messages := []messageObservation{}
	for _, row := range plan.Members {
		var found int
		err := s.DB().QueryRowContext(ctx, "select 1 from members where guild_id=? and user_id=?", row.GuildID, row.UserID).Scan(&found)
		if err == nil {
			stats.AlreadyPresent++
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return stats, errors.New("member lookup in source failed")
		}
		member, err := client.GuildMember(ctx, row.GuildID, row.UserID)
		if err != nil {
			if !confirmedDiscordMissing(err, 10007) {
				return stats, reconcileLookupError("member", err)
			}
			member = nil
		} else if member == nil || member.User == nil || member.User.ID != row.UserID || (member.GuildID != "" && member.GuildID != row.GuildID) {
			return stats, errors.New("member response identity mismatch")
		}
		members = append(members, memberObservation{row, member})
	}
	for _, row := range plan.Messages {
		var found int
		err := s.DB().QueryRowContext(ctx, "select 1 from messages where id=?", row.MessageID).Scan(&found)
		if err == nil {
			stats.AlreadyPresent++
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return stats, errors.New("message lookup in source failed")
		}
		var name string
		if err := s.DB().QueryRowContext(ctx, "select name from channels where id=? and guild_id=?", row.ChannelID, row.GuildID).Scan(&name); err != nil {
			return stats, errors.New("message channel lookup failed")
		}
		message, err := client.ChannelMessage(ctx, row.ChannelID, row.MessageID)
		if err != nil {
			if !confirmedDiscordMissing(err, 10008) {
				return stats, reconcileLookupError("message", err)
			}
			message = nil
		} else if message == nil || message.ID != row.MessageID || message.ChannelID != row.ChannelID || (message.GuildID != "" && message.GuildID != row.GuildID) {
			return stats, errors.New("message response identity mismatch")
		}
		messages = append(messages, messageObservation{row, message, name})
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	for _, observation := range members {
		if observation.live == nil {
			if err := s.MarkMemberDeleted(ctx, observation.key.GuildID, observation.key.UserID, "discord-rest", "confirmed-unknown-member"); err != nil {
				return stats, errors.New("member deletion observation could not be stored")
			}
			stats.ConfirmedDeleted++
		} else {
			if err := s.UpsertMember(ctx, toMemberRecord(observation.key.GuildID, observation.live)); err != nil {
				return stats, errors.New("recovered member could not be stored")
			}
			stats.Restored++
		}
	}
	for _, observation := range messages {
		if observation.live == nil {
			// A missing row needs an identity-bearing tombstone, not an invented live message.
			row := observation.key
			if err := s.UpsertMessageWithOptions(ctx, store.MessageRecord{ID: row.MessageID, GuildID: row.GuildID, ChannelID: row.ChannelID, CreatedAt: row.CreatedAt, DeletedAt: time.Now().UTC().Format(time.RFC3339Nano), RawJSON: `{"source":"discord-rest","code":10008}`}, store.WriteOptions{}); err != nil {
				return stats, errors.New("message deletion observation could not be stored")
			}
			stats.ConfirmedDeleted++
		} else {
			mutation, err := buildMessageMutation(ctx, observation.live, observation.channelName, observation.key.GuildID, false, false)
			if err != nil {
				return stats, errors.New("recovered message could not be normalized")
			}
			if err := s.UpsertMessages(ctx, []store.MessageMutation{mutation}); err != nil {
				return stats, errors.New("recovered message could not be stored")
			}
			stats.Restored++
		}
	}
	return stats, nil
}

func confirmedDiscordMissing(err error, code int) bool {
	var rest *discordgo.RESTError
	return errors.As(err, &rest) && rest.Response != nil && rest.Response.StatusCode == http.StatusNotFound && rest.Message != nil && rest.Message.Code == code
}

func reconcileLookupError(kind string, err error) error {
	var rest *discordgo.RESTError
	if errors.As(err, &rest) && rest.Response != nil && rest.Message != nil {
		return fmt.Errorf("%s reconciliation failed (HTTP %d, Discord code %d); no observations applied", kind, rest.Response.StatusCode, rest.Message.Code)
	}
	return fmt.Errorf("%s reconciliation request failed; no observations applied", kind)
}
