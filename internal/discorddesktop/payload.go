package discorddesktop

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/openclaw/discrawl/internal/store"
)

func collectValue(snap snapshot, channelLookup map[string]store.ChannelRecord, value any, fallbackTime time.Time) {
	switch typed := value.(type) {
	case map[string]any:
		collectUserLabel(snap, typed)
		collectSelectedDirectMessageRoutes(snap, typed)
		if channel, ok := parseChannel(typed); ok {
			snap.channels[channel.ID] = channel
			channelLookup[channel.ID] = channel
			if channel.GuildID == DirectMessageGuildID {
				if _, ok := snap.guilds[channel.GuildID]; !ok {
					snap.guilds[channel.GuildID] = syntheticGuild(channel.GuildID, guildName(channel.GuildID))
				}
			}
		}
		if message, ok := parseMessage(typed, fallbackTime, channelLookup); ok {
			snap.messages[message.Record.ID] = message
		}
		for _, child := range typed {
			collectValue(snap, channelLookup, child, fallbackTime)
		}
	case []any:
		for _, child := range typed {
			collectValue(snap, channelLookup, child, fallbackTime)
		}
	}
}

func collectChannelRoutes(snap snapshot, data []byte) {
	for _, match := range channelRouteRE.FindAllSubmatch(data, -1) {
		if len(match) != 3 {
			continue
		}
		guildID := string(match[1])
		channelID := string(match[2])
		if !looksSnowflake(channelID) {
			continue
		}
		collectChannelRoute(snap, channelID, guildID)
	}
}

func collectSelectedDirectMessageRoutes(snap snapshot, raw map[string]any) {
	for _, candidate := range selectedChannelRouteCandidates(raw) {
		if selected, _ := candidate["selectedChannelIds"].(map[string]any); selected != nil {
			if channelID := stringField(selected, "null"); looksSnowflake(channelID) {
				collectChannelRoute(snap, channelID, DirectMessageGuildID)
			}
		}
		if guildValue, hasGuild := candidate["selectedGuildId"]; hasGuild && guildValue == nil {
			if channelID := stringField(candidate, "selectedChannelId"); looksSnowflake(channelID) {
				collectChannelRoute(snap, channelID, DirectMessageGuildID)
			}
		}
	}
}

func selectedChannelRouteCandidates(raw map[string]any) []map[string]any {
	candidates := []map[string]any{raw}
	for _, key := range []string{"_state", "state"} {
		if child, _ := raw[key].(map[string]any); child != nil {
			candidates = append(candidates, child)
		}
	}
	return candidates
}

func collectChannelRoute(snap snapshot, channelID, guildID string) {
	if !looksSnowflake(channelID) || guildID == "" {
		return
	}
	if existing, ok := snap.routes[channelID]; ok && existing != guildID {
		snap.routes[channelID] = ""
		return
	}
	snap.routes[channelID] = guildID
}

func parseChannel(raw map[string]any) (store.ChannelRecord, bool) {
	id := stringField(raw, "id")
	if !looksSnowflake(id) {
		return store.ChannelRecord{}, false
	}
	if _, hasChannelID := raw["channel_id"]; hasChannelID {
		return store.ChannelRecord{}, false
	}
	typeValue, hasType := intField(raw, "type")
	name := strings.TrimSpace(stringField(raw, "name"))
	recipients, hasRecipients := raw["recipients"].([]any)
	guildID := stringField(raw, "guild_id")
	isDM := guildID == "" && (typeValue == 1 || typeValue == 3 || hasRecipients)
	if !hasType && !hasRecipients && name == "" {
		return store.ChannelRecord{}, false
	}
	if isDM {
		guildID = DirectMessageGuildID
	}
	if guildID == "" {
		return store.ChannelRecord{}, false
	}
	if name == "" {
		name = recipientLabel(recipients)
	}
	if name == "" {
		if isDM {
			name = "dm-" + shortID(id)
		} else {
			name = "channel-" + shortID(id)
		}
	}
	rawJSON := channelRawJSON(raw, id, guildID, name, kindForChannelType(typeValue, isDM))
	return store.ChannelRecord{
		ID:      id,
		GuildID: guildID,
		Kind:    kindForChannelType(typeValue, isDM),
		Name:    name,
		RawJSON: rawJSON,
	}, true
}

func parseMessage(raw map[string]any, fallbackTime time.Time, channels map[string]store.ChannelRecord) (store.MessageMutation, bool) {
	id := stringField(raw, "id")
	channelID := stringField(raw, "channel_id")
	if !looksSnowflake(id) || !looksSnowflake(channelID) {
		return store.MessageMutation{}, false
	}
	author, _ := raw["author"].(map[string]any)
	content, hasContent := raw["content"].(string)
	if !hasContent && len(author) == 0 {
		return store.MessageMutation{}, false
	}
	createdAt := parseDiscordTime(stringField(raw, "timestamp"))
	if createdAt.IsZero() {
		createdAt = snowflakeTime(id)
	}
	if createdAt.IsZero() {
		createdAt = fallbackTime
	}
	if createdAt.IsZero() {
		return store.MessageMutation{}, false
	}
	guildID := stringField(raw, "guild_id")
	if guildID == "" {
		if channel, ok := channels[channelID]; ok && channel.GuildID != "" {
			guildID = channel.GuildID
		}
	}
	channelName := "channel-" + shortID(channelID)
	if channel, ok := channels[channelID]; ok && channel.Name != "" {
		channelName = channel.Name
	}
	authorID := stringField(author, "id")
	authorName := firstNonEmpty(
		stringField(author, "global_name"),
		stringField(author, "display_name"),
		stringField(author, "username"),
	)
	msgType, _ := intField(raw, "type")
	editedAt := parseDiscordTime(stringField(raw, "edited_timestamp"))
	attachments := parseAttachments(raw, id, guildID, channelID, authorID)
	mentions := parseMentions(raw, id, guildID, channelID, authorID, createdAt)
	normalized := normalizeText(content, attachmentText(attachments), embedText(raw))
	return store.MessageMutation{
		Record: store.MessageRecord{
			ID:                id,
			GuildID:           guildID,
			ChannelID:         channelID,
			ChannelName:       channelName,
			AuthorID:          authorID,
			AuthorName:        authorName,
			MessageType:       msgType,
			CreatedAt:         createdAt.UTC().Format(time.RFC3339Nano),
			EditedAt:          formatOptionalTime(editedAt),
			Content:           content,
			NormalizedContent: normalized,
			ReplyToMessageID:  messageReferenceID(raw),
			Pinned:            boolField(raw, "pinned"),
			HasAttachments:    len(attachments) > 0,
			RawJSON:           messageRawJSON(raw, id, guildID, channelID, authorID),
		},
		EventType:   "wiretap",
		PayloadJSON: messageRawJSON(raw, id, guildID, channelID, authorID),
		Options: store.WriteOptions{
			AppendEvent:      true,
			EnqueueEmbedding: false,
			PreserveNewer:    true,
			DeduplicateEvent: true,
		},
		Attachments: attachments,
		Mentions:    mentions,
	}, true
}

func parseAttachments(raw map[string]any, messageID, guildID, channelID, authorID string) []store.AttachmentRecord {
	items, _ := raw["attachments"].([]any)
	out := make([]store.AttachmentRecord, 0, len(items))
	for i, item := range items {
		attachment, _ := item.(map[string]any)
		if len(attachment) == 0 {
			continue
		}
		id := stringField(attachment, "id")
		if id == "" {
			id = fmt.Sprintf("%s:%d", messageID, i)
		}
		out = append(out, store.AttachmentRecord{
			AttachmentID: id,
			MessageID:    messageID,
			GuildID:      guildID,
			ChannelID:    channelID,
			AuthorID:     authorID,
			Filename:     firstNonEmpty(stringField(attachment, "filename"), id),
			ContentType:  stringField(attachment, "content_type"),
			Size:         int64Field(attachment, "size"),
			URL:          stringField(attachment, "url"),
			ProxyURL:     stringField(attachment, "proxy_url"),
		})
	}
	return out
}

func parseMentions(raw map[string]any, messageID, guildID, channelID, authorID string, eventAt time.Time) []store.MentionEventRecord {
	items, _ := raw["mentions"].([]any)
	out := make([]store.MentionEventRecord, 0, len(items))
	for _, item := range items {
		mention, _ := item.(map[string]any)
		id := stringField(mention, "id")
		if id == "" {
			continue
		}
		out = append(out, store.MentionEventRecord{
			MessageID:  messageID,
			GuildID:    guildID,
			ChannelID:  channelID,
			AuthorID:   authorID,
			TargetType: "user",
			TargetID:   id,
			TargetName: firstNonEmpty(stringField(mention, "global_name"), stringField(mention, "username")),
			EventAt:    eventAt.UTC().Format(time.RFC3339Nano),
		})
	}
	return out
}

func attachmentText(attachments []store.AttachmentRecord) []string {
	out := make([]string, 0, len(attachments))
	for _, attachment := range attachments {
		out = append(out, attachment.Filename)
	}
	return out
}

func embedText(raw map[string]any) []string {
	items, _ := raw["embeds"].([]any)
	out := []string{}
	for _, item := range items {
		embed, _ := item.(map[string]any)
		for _, key := range []string{"title", "description"} {
			if value := strings.TrimSpace(stringField(embed, key)); value != "" {
				out = append(out, value)
			}
		}
	}
	return out
}

func normalizeText(parts ...any) string {
	flat := []string{}
	for _, part := range parts {
		switch typed := part.(type) {
		case string:
			if text := cleanText(typed); text != "" {
				flat = append(flat, text)
			}
		case []string:
			for _, item := range typed {
				if text := cleanText(item); text != "" {
					flat = append(flat, text)
				}
			}
		}
	}
	return strings.Join(flat, "\n")
}

func cleanText(raw string) string {
	raw = strings.ToValidUTF8(raw, "")
	var b strings.Builder
	spacePending := false
	for _, r := range raw {
		switch {
		case r == '\u200b' || r == '\u200c' || r == '\u200d' || r == '\ufeff':
			continue
		case unicode.IsControl(r):
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

func messageReferenceID(raw map[string]any) string {
	ref, _ := raw["message_reference"].(map[string]any)
	return stringField(ref, "message_id")
}

func syntheticGuild(id, name string) store.GuildRecord {
	raw := marshalJSONString(map[string]any{
		"id":     id,
		"name":   name,
		"source": "discord_desktop",
	}, "{}")
	return store.GuildRecord{ID: id, Name: name, RawJSON: raw}
}

func syntheticChannel(id, guildID, name string) store.ChannelRecord {
	if name == "" {
		name = "channel-" + shortID(id)
	}
	raw := marshalJSONString(map[string]any{
		"id":       id,
		"guild_id": guildID,
		"name":     name,
		"source":   "discord_desktop",
	}, "{}")
	kind := "text"
	if guildID == DirectMessageGuildID {
		kind = "dm"
		if strings.Contains(name, ", ") {
			kind = "group_dm"
		}
	}
	return store.ChannelRecord{ID: id, GuildID: guildID, Kind: kind, Name: name, RawJSON: raw}
}

func guildName(id string) string {
	switch id {
	case DirectMessageGuildID:
		return DirectMessageGuildName
	default:
		return "Discord Desktop Guild " + id
	}
}

func kindForChannelType(typeValue int, dm bool) string {
	if dm {
		if typeValue == 3 {
			return "group_dm"
		}
		return "dm"
	}
	switch typeValue {
	case 0:
		return "text"
	case 5:
		return "announcement"
	case 10:
		return "thread_announcement"
	case 11:
		return "thread_public"
	case 12:
		return "thread_private"
	case 15:
		return "forum"
	default:
		return "desktop"
	}
}

func channelRawJSON(raw map[string]any, id, guildID, name, kind string) string {
	return marshalJSONString(map[string]any{
		"id":       id,
		"guild_id": guildID,
		"name":     name,
		"kind":     kind,
		"source":   "discord_desktop",
		"type":     raw["type"],
	}, "{}")
}

func messageRawJSON(raw map[string]any, id, guildID, channelID, authorID string) string {
	payload := map[string]any{
		"id":                 id,
		"guild_id":           guildID,
		"channel_id":         channelID,
		"author_id":          authorID,
		"source":             "discord_desktop",
		"type":               raw["type"],
		"timestamp":          raw["timestamp"],
		"edited_timestamp":   raw["edited_timestamp"],
		"message_reference":  raw["message_reference"],
		"attachment_count":   lenArray(raw["attachments"]),
		"mention_count":      lenArray(raw["mentions"]),
		"desktop_cache_note": "raw desktop cache payload intentionally not stored",
	}
	if author := sanitizedRawAuthor(raw, authorID); len(author) > 0 {
		payload["author"] = author
	}
	return marshalJSONString(payload, "{}")
}

func recipientLabel(items []any) string {
	names := []string{}
	for _, item := range items {
		recipient, _ := item.(map[string]any)
		name := firstNonEmpty(
			stringField(recipient, "global_name"),
			stringField(recipient, "display_name"),
			stringField(recipient, "username"),
		)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func parseDiscordTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC()
	}
	return time.Time{}
}

func snowflakeTime(id string) time.Time {
	value, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return time.Time{}
	}
	ms := int64((value >> 22) + 1420070400000)
	return time.UnixMilli(ms).UTC()
}

func formatOptionalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func looksSnowflake(value string) bool {
	if len(value) < 12 || len(value) > 24 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func shortID(id string) string {
	if len(id) <= 6 {
		return id
	}
	return id[len(id)-6:]
}

func stringField(raw map[string]any, key string) string {
	value, ok := raw[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func intField(raw map[string]any, key string) (int, bool) {
	value, ok := raw[key]
	if !ok || value == nil {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return int(typed), true
	case int:
		return typed, true
	case json.Number:
		i, err := typed.Int64()
		return int(i), err == nil
	default:
		return 0, false
	}
}

func int64Field(raw map[string]any, key string) int64 {
	value, ok := raw[key]
	if !ok || value == nil {
		return 0
	}
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case int64:
		return typed
	case int:
		return int64(typed)
	case json.Number:
		i, _ := typed.Int64()
		return i
	default:
		return 0
	}
}

func boolField(raw map[string]any, key string) bool {
	value, _ := raw[key].(bool)
	return value
}

func lenArray(value any) int {
	items, _ := value.([]any)
	return len(items)
}

func firstNonEmpty(items ...string) string {
	for _, item := range items {
		if strings.TrimSpace(item) != "" {
			return strings.TrimSpace(item)
		}
	}
	return ""
}

func mapValues[M ~map[string]T, T any](m M) []T {
	out := make([]T, 0, len(m))
	for _, value := range m {
		out = append(out, value)
	}
	return out
}

func marshalJSONString(value any, fallback string) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fallback
	}
	return string(raw)
}
