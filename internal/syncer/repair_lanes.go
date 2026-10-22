package syncer

import (
	"context"
	"errors"
	"net/http"
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
	ctx = withAttachmentScopeGuard(ctx, s.attachmentScopeGuard())
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
		started := time.Now()
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
				mutation.Options.PreserveNewer = true
				mutation.Options.ObservationStartedAt = started
				mutation.Options.AppendEvent = true
				mutation.Options.DeduplicateEvent = true
				mutation.EventType = "snapshot"
				buildErr = s.store.UpsertMessages(call, []store.MessageMutation{mutation})
			}
			fetchErr = buildErr
		}
		cancel()
		if errors.Is(fetchErr, store.ErrCollectionScope) {
			// Scope was revoked while the refetch was in flight: a policy
			// skip, not a retryable attachment failure.
			continue
		}
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
		started := time.Now()
		// A terminal outcome that supersedes the failed observation resolves
		// this generation instead of recording it again on every repair.
		resolution := "metadata_reconciled"
		switch {
		case kind == "GUILD_MEMBER_REMOVE":
			err = s.replayMemberRemove(call, f, id, started)
		case strings.HasPrefix(kind, "GUILD_MEMBER_"):
			getter, supported := s.client.(singleMemberClient)
			if !supported {
				err = errors.New("member exact fetch unavailable")
				break
			}
			var m *discordgo.Member
			m, err = getter.GuildMember(call, f.GuildID, id)
			if confirmedDiscordMissing(err, 10007) {
				// The member has left; the failed update has nothing to apply.
				err, resolution = nil, "provider_unavailable_404"
				break
			}
			if err == nil {
				if m == nil || m.User == nil || m.User.ID != id || (m.GuildID != "" && m.GuildID != f.GuildID) {
					err = errors.New("member identity mismatch")
				} else {
					err = s.store.UpsertObservedMember(call, toMemberRecord(f.GuildID, m), started)
				}
			}
		case kind == "CHANNEL_DELETE" || kind == "THREAD_DELETE":
			err = h.OnChannelDelete(call, &discordgo.Channel{ID: f.ChannelID, GuildID: f.GuildID})
		case kind == "GUILD_DELETE":
			err = s.replayGuildDelete(call, f.GuildID, started)
		case kind == "THREAD_LIST_SYNC":
			var channels []*discordgo.Channel
			channels, err = s.client.GuildThreadsActive(call, f.GuildID)
			if err == nil {
				err = s.storeScopeMetadata(call, f.GuildID, channels, s.channelExclusions, started)
			}
		case f.ChannelID != "":
			var deleted bool
			if deleted, err = s.store.ChannelTombstoned(call, f.ChannelID); err != nil || deleted {
				// An authoritative deletion supersedes the failed update.
				resolution = "superseded_by_deletion"
				break
			}
			var c *discordgo.Channel
			c, err = s.client.Channel(call, f.ChannelID)
			if confirmedDiscordMissing(err, 10003) {
				err, resolution = nil, "provider_unavailable_404"
				break
			}
			if err == nil {
				if c == nil || c.ID != f.ChannelID || c.GuildID != f.GuildID {
					err = errors.New("channel identity mismatch")
				} else {
					err = s.store.UpsertObservedChannel(call, toChannelRecord(c, marshalJSONString(c, "{}")), started)
					if err == nil {
						err = h.refreshScope(call)
					}
				}
			}
		default:
			var g *discordgo.Guild
			g, err = s.client.Guild(call, f.GuildID)
			if err == nil {
				if g == nil || g.ID != f.GuildID {
					err = errors.New("guild identity mismatch")
				} else {
					err = s.store.UpsertObservedGuild(call, store.GuildRecord{ID: g.ID, Name: g.Name, Icon: g.Icon, RawJSON: marshalJSONString(g, "{}")}, started)
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
			// A newer failure for this identity may have been recorded while
			// the replay was in flight; leave that generation pending.
			if err = s.store.ResolveFailureGeneration(ctx, f, resolution); err != nil {
				return err
			}
		}
	}
	return nil
}

// replayMemberRemove reconciles a failed GUILD_MEMBER_REMOVE against Discord's
// current membership instead of local timestamps, which a later full member
// enumeration may have stamped with an older snapshot. Unknown Member proves
// the removal; a present member is a newer observation (for example a rejoin).
// Without an exact member fetch, the removal event time remains the fence.
func (s *Syncer) replayMemberRemove(ctx context.Context, f store.Failure, userID string, started time.Time) error {
	getter, supported := s.client.(singleMemberClient)
	if !supported {
		return s.store.MarkObservedMemberDeleted(ctx, f.GuildID, userID, "discord-gateway", "member-remove-event-replay", f.FirstSeenAt)
	}
	m, err := getter.GuildMember(ctx, f.GuildID, userID)
	if memberGone(err) {
		return s.store.MarkObservedMemberDeleted(ctx, f.GuildID, userID, "discord-gateway", "member-remove-event-replay", started)
	}
	if err != nil {
		return err
	}
	if m == nil || m.User == nil || m.User.ID != userID || (m.GuildID != "" && m.GuildID != f.GuildID) {
		return errors.New("member identity mismatch")
	}
	return s.store.UpsertObservedMember(ctx, toMemberRecord(f.GuildID, m), started)
}

func memberGone(err error) bool {
	var rest *discordgo.RESTError
	return errors.As(err, &rest) && rest.Response != nil && rest.Message != nil &&
		rest.Response.StatusCode == http.StatusNotFound && rest.Message.Code == discordgo.ErrCodeUnknownMember
}

// replayGuildDelete reconciles a failed GUILD_DELETE. Lost access proves the
// removal, so the retained guild is tombstoned; a successful fetch means the
// bot has access again, and that newer observation is stored instead.
func (s *Syncer) replayGuildDelete(ctx context.Context, guildID string, started time.Time) error {
	g, err := s.client.Guild(ctx, guildID)
	if guildAccessGone(err) {
		// A Gateway rejoin stored while this request was in flight is newer.
		return s.store.MarkObservedGuildDeleted(ctx, guildID, "discord-gateway", "guild-delete-event-replay", started)
	}
	if err != nil {
		return err
	}
	if g == nil || g.ID != guildID {
		return errors.New("guild identity mismatch")
	}
	return s.store.UpsertObservedGuild(ctx, store.GuildRecord{ID: g.ID, Name: g.Name, Icon: g.Icon, RawJSON: marshalJSONString(g, "{}")}, started)
}

func guildAccessGone(err error) bool {
	var rest *discordgo.RESTError
	if !errors.As(err, &rest) || rest.Response == nil || rest.Message == nil {
		return false
	}
	switch {
	case rest.Response.StatusCode == http.StatusForbidden && rest.Message.Code == discordgo.ErrCodeMissingAccess:
		return true
	case rest.Response.StatusCode == http.StatusNotFound && rest.Message.Code == discordgo.ErrCodeUnknownGuild:
		return true
	}
	return false
}
