package syncer

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/store"
)

func (s *Syncer) retryAttachmentText(ctx context.Context, guildIDs []string) error {
	if !s.attachmentTextEnabled {
		return nil
	}
	candidates, err := s.store.AttachmentTextRetryCandidates(ctx, guildIDs, 5)
	if err != nil {
		return err
	}
	resolver := &tailHandler{store: s.store, client: s.client, guilds: makeGuildSet(guildIDs), exclusions: s.channelExclusions}
	for _, m := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		allowed, err := resolver.allowMessageChannel(ctx, m.GuildID, m.ChannelID)
		if err != nil {
			return err
		}
		if !allowed {
			continue
		}
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		msg, fetchErr := s.client.ChannelMessage(call, m.ChannelID, m.ID)
		if fetchErr == nil {
			fetchErr = validateTailMessageReplay(store.Failure{GuildID: m.GuildID, ChannelID: m.ChannelID, MessageID: m.ID}, msg)
		}
		if fetchErr == nil {
			if msg.GuildID == "" {
				msg.GuildID = m.GuildID
			}
			mutation, buildErr := buildMessageMutation(call, msg, "", m.GuildID, s.tailEmbeddings, true)
			if buildErr == nil {
				mutation.Options.ScopePolicy = resolver.scopePolicy()
				buildErr = s.store.UpsertMessages(call, []store.MessageMutation{mutation})
			}
			fetchErr = buildErr
		}
		cancel()
		if fetchErr != nil {
			code := discordclient.SafeFailureCode(fetchErr)
			if code == "" {
				code = "message_refetch_failed"
			}
			if err := s.store.RecordAttachmentRefetchFailure(ctx, m, code); err != nil {
				return err
			}
		}
	}
	return nil
}

type singleMemberClient interface {
	GuildMember(context.Context, string, string) (*discordgo.Member, error)
}

func (s *Syncer) replayMetadataFailures(ctx context.Context, guildIDs []string) error {
	rows, err := s.store.ListFailureReplayCandidates(ctx, store.FailureRef{Operation: "tail_metadata", Source: "discord"}, guildIDs, 5)
	if err != nil {
		return err
	}
	h := &tailHandler{store: s.store, client: s.client, guilds: makeGuildSet(guildIDs), exclusions: s.channelExclusions}
	for _, f := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		kind, id, ok := strings.Cut(f.RelatedID, ":")
		if !ok {
			continue
		}
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		switch {
		case kind == "GUILD_MEMBER_REMOVE":
			err = s.store.MarkMemberDeleted(call, f.GuildID, id, "discord-gateway", "member-remove-event-replay")
		case strings.HasPrefix(kind, "GUILD_MEMBER_"):
			getter, supported := s.client.(singleMemberClient)
			if !supported {
				err = errors.New("member exact fetch unavailable")
				break
			}
			var m *discordgo.Member
			m, err = getter.GuildMember(call, f.GuildID, id)
			if err == nil {
				if m == nil || m.User == nil || m.User.ID != id || (m.GuildID != "" && m.GuildID != f.GuildID) {
					err = errors.New("member identity mismatch")
				} else {
					err = h.OnMemberUpsert(call, f.GuildID, m)
				}
			}
		case kind == "CHANNEL_DELETE" || kind == "THREAD_DELETE":
			err = h.OnChannelDelete(call, &discordgo.Channel{ID: f.ChannelID, GuildID: f.GuildID})
		case f.ChannelID != "":
			var c *discordgo.Channel
			c, err = s.client.Channel(call, f.ChannelID)
			if err == nil {
				if c == nil || c.ID != f.ChannelID || c.GuildID != f.GuildID {
					err = errors.New("channel identity mismatch")
				} else {
					err = h.OnChannelUpsert(call, c)
				}
			}
		default:
			var g *discordgo.Guild
			g, err = s.client.Guild(call, f.GuildID)
			if err == nil {
				if g == nil || g.ID != f.GuildID {
					err = errors.New("guild identity mismatch")
				} else {
					err = h.OnGuildUpsert(call, g)
				}
			}
		}
		cancel()
		ref := tailMessageFailureRef(f)
		if err != nil {
			code := discordclient.SafeFailureCode(err)
			if code == "" {
				code = "metadata_replay_failed"
			}
			if err = s.store.RecordFailure(ctx, ref, errors.New(code)); err != nil {
				return err
			}
		} else {
			if err = s.store.ResolveFailureWithReason(ctx, ref, "metadata_reconciled"); err != nil {
				return err
			}
		}
	}
	return nil
}
