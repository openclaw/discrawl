package discorddesktop

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestPrimitiveValueHelpers(t *testing.T) {
	raw := map[string]any{
		"string":      "value",
		"blank":       "  ",
		"int":         3,
		"int64":       int64(4),
		"float":       float64(5),
		"json_number": json.Number("6"),
		"numeric":     "7",
		"bad_numeric": "nope",
		"truthy":      true,
		"array":       []any{"one", "two"},
	}

	require.Equal(t, "value", stringField(raw, "string"))
	require.Empty(t, stringField(raw, "blank"))
	require.Equal(t, "6", stringField(raw, "json_number"))
	require.Empty(t, stringField(raw, "int"))
	require.Empty(t, stringField(raw, "missing"))

	for key, want := range map[string]int{
		"int":         3,
		"float":       5,
		"json_number": 6,
	} {
		got, ok := intField(raw, key)
		require.True(t, ok, key)
		require.Equal(t, want, got, key)
	}
	_, ok := intField(raw, "bad_numeric")
	require.False(t, ok)
	_, ok = intField(raw, "int64")
	require.False(t, ok)
	_, ok = intField(raw, "numeric")
	require.False(t, ok)
	_, ok = intField(raw, "missing")
	require.False(t, ok)

	require.Equal(t, int64(3), int64Field(raw, "int"))
	require.Equal(t, int64(4), int64Field(raw, "int64"))
	require.Equal(t, int64(5), int64Field(raw, "float"))
	require.Equal(t, int64(6), int64Field(raw, "json_number"))
	require.Zero(t, int64Field(raw, "numeric"))
	require.Zero(t, int64Field(raw, "bad_numeric"))

	require.True(t, boolField(raw, "truthy"))
	require.False(t, boolField(raw, "missing"))
	require.Equal(t, 2, lenArray(raw["array"]))
	require.Zero(t, lenArray(raw["string"]))
	require.Equal(t, "fallback", firstNonEmpty("", "  ", "fallback", "later"))
	require.Empty(t, firstNonEmpty("", " "))
}

func TestDiscordValueFormatHelpers(t *testing.T) {
	require.Equal(t, "456789", shortID("123456789"))
	require.Equal(t, "short", shortID("short"))
	require.Equal(t, "Discord Direct Messages", guildName(DirectMessageGuildID))
	require.Equal(t, "Discord Desktop Guild 123456", guildName("123456"))

	require.Equal(t, "dm", kindForChannelType(1, true))
	require.Equal(t, "group_dm", kindForChannelType(3, true))
	require.Equal(t, "thread_public", kindForChannelType(11, false))
	require.Equal(t, "thread_private", kindForChannelType(12, false))
	require.Equal(t, "thread_announcement", kindForChannelType(10, false))
	require.Equal(t, "desktop", kindForChannelType(2, false))
	require.Equal(t, "desktop", kindForChannelType(4, false))
	require.Equal(t, "announcement", kindForChannelType(5, false))
	require.Equal(t, "forum", kindForChannelType(15, false))
	require.Equal(t, "desktop", kindForChannelType(16, false))
	require.Equal(t, "text", kindForChannelType(0, false))
}

func TestDiscordMessagePayloadHelpers(t *testing.T) {
	raw := map[string]any{
		"id":                "333333333333333333",
		"channel_id":        "111111111111111111",
		"guild_id":          "999999999999999999",
		"type":              float64(0),
		"timestamp":         "2026-05-08T12:00:00Z",
		"edited_timestamp":  "2026-05-08T12:05:00Z",
		"content":           "hello\u200b\nworld",
		"message_reference": map[string]any{"message_id": "222222222222222222"},
		"author": map[string]any{
			"id":            "444444444444444444",
			"username":      "peter",
			"global_name":   "Peter",
			"display_name":  "Peter S",
			"discriminator": "0",
			"bot":           true,
		},
		"attachments": []any{
			map[string]any{"filename": "trace.txt", "content_type": "text/plain", "size": float64(12), "url": "https://cdn.example/trace.txt"},
			map[string]any{"id": "att2"},
			"ignored",
		},
		"mentions": []any{
			map[string]any{"id": "555555555555555555", "username": "alice", "global_name": "Alice"},
			map[string]any{"username": "missing"},
		},
		"embeds": []any{
			map[string]any{"title": "Deploy", "description": "Ready"},
			map[string]any{"title": " "},
		},
	}
	at := parseDiscordTime("2026-05-08T12:00:00Z")
	attachments := parseAttachments(raw, "333333333333333333", "999999999999999999", "111111111111111111", "444444444444444444")
	require.Len(t, attachments, 2)
	require.Equal(t, "333333333333333333:0", attachments[0].AttachmentID)
	require.Equal(t, "trace.txt", attachments[0].Filename)
	require.Equal(t, "att2", attachments[1].Filename)
	require.Equal(t, []string{"trace.txt", "att2"}, attachmentText(attachments))

	mentions := parseMentions(raw, "333333333333333333", "999999999999999999", "111111111111111111", "444444444444444444", at)
	require.Equal(t, []store.MentionEventRecord{{
		MessageID:  "333333333333333333",
		GuildID:    "999999999999999999",
		ChannelID:  "111111111111111111",
		AuthorID:   "444444444444444444",
		TargetType: "user",
		TargetID:   "555555555555555555",
		TargetName: "Alice",
		EventAt:    at.Format(time.RFC3339Nano),
	}}, mentions)

	require.Equal(t, []string{"Deploy", "Ready"}, embedText(raw))
	require.Equal(t, "helloworld\ntrace.txt\natt2\nDeploy\nReady", normalizeText(raw["content"], attachmentText(attachments), embedText(raw)))
	require.Equal(t, "hidden text", cleanText("\u200bhidden\x00 text\n"))
	require.Equal(t, "222222222222222222", messageReferenceID(raw))
	require.Empty(t, messageReferenceID(map[string]any{}))

	require.Contains(t, syntheticGuild("g1", "Guild").RawJSON, "discord_desktop")
	require.Equal(t, "dm", syntheticChannel("c1", DirectMessageGuildID, "Alice").Kind)
	require.Equal(t, "group_dm", syntheticChannel("c2", DirectMessageGuildID, "Alice, Bob").Kind)
	require.Equal(t, "channel-123456", syntheticChannel("123456123456", "g1", "").Name)
	require.Contains(t, channelRawJSON(raw, "c1", "g1", "general", "text"), `"kind":"text"`)
	require.Contains(t, messageRawJSON(raw, "333333333333333333", "999999999999999999", "111111111111111111", "444444444444444444"), "desktop_cache_note")
	require.Equal(t, "Alice, Bob", recipientLabel([]any{
		map[string]any{"username": "Bob"},
		map[string]any{"global_name": "Alice"},
		map[string]any{},
	}))

	require.True(t, parseDiscordTime("2026-05-08T12:00:00.123Z").Equal(time.Date(2026, 5, 8, 12, 0, 0, 123000000, time.UTC)))
	require.True(t, parseDiscordTime("bad").IsZero())
	require.True(t, parseDiscordTime("").IsZero())
	require.False(t, snowflakeTime("175928847299117063").IsZero())
	require.True(t, snowflakeTime("bad").IsZero())
	require.Empty(t, formatOptionalTime(time.Time{}))
	require.Equal(t, "2026-05-08T12:00:00Z", formatOptionalTime(at))
	require.True(t, looksSnowflake("123456789012"))
	require.False(t, looksSnowflake("123"))
	require.False(t, looksSnowflake("12345678901x"))
}

func TestParseMessageIndexesComponentsV2Text(t *testing.T) {
	var raw map[string]any
	require.NoError(t, json.Unmarshal([]byte(`{
		"id": "333333333333333333",
		"channel_id": "111111111111111111",
		"guild_id": "999999999999999999",
		"flags": 32768,
		"content": "",
		"timestamp": "2026-09-28T10:15:00Z",
		"author": {"id": "444444444444444444", "username": "Hermit", "bot": true},
		"components": [{"type": 17, "components": [
			{"type": 10, "content": "### [OPEN] PR #160926 fix(secrets): redact gateway tokens"},
			{"type": 14, "divider": true},
			{"type": 9, "components": [{"type": 10, "content": "size: XL • P0"}],
				"accessory": {"type": 11, "media": {"url": "https://avatars.example/1.png"}, "description": "avatar"}},
			{"type": 12, "items": [{"media": {"url": "https://cdn.example/chart.png"}, "description": "chart"}]},
			{"type": 1, "components": [{"type": 2, "style": 5, "label": "View on GitHub", "url": "https://github.com/openclaw/openclaw/pull/160926"}]}
		]}]
	}`), &raw))

	message, ok := parseMessage(raw, time.Time{}, nil)
	require.True(t, ok)
	require.Empty(t, message.Record.Content)
	require.Equal(t, "### [OPEN] PR #160926 fix(secrets): redact gateway tokens\nsize: XL • P0\navatar\nchart\nView on GitHub\nhttps://github.com/openclaw/openclaw/pull/160926", message.Record.NormalizedContent)
	require.NotContains(t, message.Record.RawJSON, "components", "desktop raw payloads stay sanitized")

	delete(raw, "flags")
	message, ok = parseMessage(raw, time.Time{}, nil)
	require.True(t, ok)
	require.Empty(t, message.Record.NormalizedContent, "classic message components are not message bodies")
}
