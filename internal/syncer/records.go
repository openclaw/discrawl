package syncer

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"golang.org/x/text/unicode/norm"
)

func toMemberRecord(guildID string, member *discordgo.Member) store.MemberRecord {
	if member == nil || member.User == nil {
		return store.MemberRecord{}
	}
	raw := marshalJSONString(member, "{}")
	roles := marshalJSONString(member.Roles, "[]")
	return store.MemberRecord{
		GuildID:       guildID,
		UserID:        member.User.ID,
		Username:      member.User.Username,
		GlobalName:    member.User.GlobalName,
		DisplayName:   displayName(member),
		Nick:          member.Nick,
		Discriminator: member.User.Discriminator,
		Avatar:        member.Avatar,
		Bot:           member.User.Bot,
		JoinedAt:      member.JoinedAt.Format(time.RFC3339Nano),
		RoleIDsJSON:   roles,
		RawJSON:       raw,
	}
}

func effectiveMessageGuildID(message *discordgo.Message, fallbackGuildID string) string {
	if message != nil && strings.TrimSpace(message.GuildID) != "" {
		return message.GuildID
	}
	return strings.TrimSpace(fallbackGuildID)
}

func toMessageRecord(message *discordgo.Message, channelName, guildID, normalizedContent string) store.MessageRecord {
	raw := marshalJSONString(message, "{}")
	authorID := ""
	authorName := ""
	if message.Author != nil {
		authorID = message.Author.ID
		authorName = strings.TrimSpace(message.Author.GlobalName)
		if authorName == "" {
			authorName = message.Author.Username
		}
	}
	replyTo := ""
	if message.MessageReference != nil {
		replyTo = message.MessageReference.MessageID
	}
	editedAt := ""
	if message.EditedTimestamp != nil {
		editedAt = message.EditedTimestamp.UTC().Format(time.RFC3339Nano)
	}
	return store.MessageRecord{
		ID:                message.ID,
		GuildID:           guildID,
		ChannelID:         message.ChannelID,
		ChannelName:       channelName,
		AuthorID:          authorID,
		AuthorName:        authorName,
		MessageType:       int(message.Type),
		CreatedAt:         message.Timestamp.UTC().Format(time.RFC3339Nano),
		EditedAt:          editedAt,
		Content:           message.Content,
		NormalizedContent: normalizedContent,
		ReplyToMessageID:  replyTo,
		Pinned:            message.Pinned,
		HasAttachments:    len(message.Attachments) > 0,
		RawJSON:           raw,
	}
}

func marshalJSONString(value any, fallback string) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fallback
	}
	return string(raw)
}

func normalizeMessage(message *discordgo.Message) string {
	return normalizeMessageParts(message, nil)
}

func normalizeMessageParts(message *discordgo.Message, attachmentParts []string) string {
	parts := []string{message.Content}
	if len(attachmentParts) != 0 {
		parts = append(parts, attachmentParts...)
	} else {
		for _, attachment := range message.Attachments {
			if attachment != nil && attachment.Filename != "" {
				parts = append(parts, attachment.Filename)
			}
		}
	}
	for _, embed := range message.Embeds {
		if embed == nil {
			continue
		}
		if embed.Title != "" {
			parts = append(parts, embed.Title)
		}
		if embed.Description != "" {
			parts = append(parts, embed.Description)
		}
	}
	if message.ReferencedMessage != nil && message.ReferencedMessage.Content != "" {
		parts = append(parts, "reply:"+message.ReferencedMessage.Content)
	}
	if message.Poll != nil {
		parts = append(parts, message.Poll.Question.Text)
		for _, answer := range message.Poll.Answers {
			if answer.Media != nil {
				parts = append(parts, answer.Media.Text)
			}
		}
	}
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = sanitizeNormalizedPart(part)
		if part != "" {
			filtered = append(filtered, part)
		}
	}
	return strings.Join(filtered, "\n")
}

func sanitizeNormalizedPart(raw string) string {
	raw = strings.ToValidUTF8(raw, "")
	raw = norm.NFKC.String(raw)

	var b strings.Builder
	b.Grow(len(raw))
	spacePending := false
	for _, r := range raw {
		switch {
		case isDroppedNormalizedRune(r):
			continue
		case unicode.IsSpace(r):
			spacePending = b.Len() > 0
		default:
			if spacePending {
				b.WriteByte(' ')
				spacePending = false
			}
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

func isDroppedNormalizedRune(r rune) bool {
	switch r {
	case '\u200b', '\u200c', '\u200d', '\ufeff':
		return true
	}
	return unicode.IsControl(r)
}

func displayName(member *discordgo.Member) string {
	if member == nil || member.User == nil {
		return ""
	}
	if member.Nick != "" {
		return member.Nick
	}
	if member.User.GlobalName != "" {
		return member.User.GlobalName
	}
	return member.User.Username
}

func maxSnowflake(current, candidate string) string {
	if current == "" {
		return candidate
	}
	if candidate == "" {
		return current
	}
	a, errA := strconv.ParseUint(current, 10, 64)
	b, errB := strconv.ParseUint(candidate, 10, 64)
	if errA != nil || errB != nil {
		if candidate > current {
			return candidate
		}
		return current
	}
	if b > a {
		return candidate
	}
	return current
}

func channelLatestScope(channelID string) string {
	return "channel:" + channelID + ":latest_message_id"
}

func channelBackfillScope(channelID string) string {
	return "channel:" + channelID + ":backfill_before_id"
}

func channelHistoryCompleteScope(channelID string) string {
	return "channel:" + channelID + ":history_complete"
}

func channelMessageUnavailableScope(channelID string) string {
	return "channel:" + channelID + ":unavailable"
}

func channelThreadCatalogUnavailableScope(channelID string) string {
	return "channel:" + channelID + ":thread_catalog_unavailable"
}

func channelArchivedThreadCursorScope(channelID string, private bool) string {
	kind := "public"
	if private {
		kind = "private"
	}
	return "channel:" + channelID + ":archived_" + kind + "_threads_after"
}

func makeGuildSet(ids []string) map[string]struct{} {
	if len(ids) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id != "" {
			set[id] = struct{}{}
		}
	}
	return set
}

func selectGuilds(all []*discordgo.UserGuild, requested []string) []*discordgo.UserGuild {
	if len(requested) == 0 {
		return all
	}
	set := makeGuildSet(requested)
	var out []*discordgo.UserGuild
	for _, guild := range all {
		if _, ok := set[guild.ID]; ok {
			out = append(out, guild)
		}
	}
	return out
}

func missingGuildIDs(all []*discordgo.UserGuild, requested []string) []string {
	if len(requested) == 0 {
		return nil
	}
	available := make(map[string]struct{}, len(all))
	for _, guild := range all {
		if guild != nil && guild.ID != "" {
			available[guild.ID] = struct{}{}
		}
	}
	seen := map[string]struct{}{}
	var missing []string
	for _, id := range requested {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if _, ok := available[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}
