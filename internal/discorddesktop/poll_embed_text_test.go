package discorddesktop

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

// Bot sync indexes poll questions and answers and embed author, field and
// footer text (messagetext.Parts). Desktop cache imports of the same message
// must make the same text searchable.
func TestParseMessageIndexesPollAndEmbedFieldText(t *testing.T) {
	t.Parallel()
	raw := map[string]any{
		"id": "333333333333333391", "channel_id": "111111111111111191", "guild_id": "999999999999999991",
		"timestamp": "2026-05-08T12:00:00Z", "content": "",
		"author": map[string]any{"id": "222222222222222291", "username": "ada"},
		"poll": map[string]any{
			"question": map[string]any{"text": "Ship the indexer?"},
			"answers":  []any{map[string]any{"answer_id": 1, "poll_media": map[string]any{"text": "yes with polls"}}},
		},
		"embeds": []any{map[string]any{
			"author": map[string]any{"name": "Deploy bot"},
			"title":  "Release 2", "description": "notes",
			"fields": []any{map[string]any{"name": "Risk", "value": "purple herring"}},
			"footer": map[string]any{"text": "built from main"},
		}},
	}
	mutation, ok := parseMessage(raw, time.Now(), map[string]store.ChannelRecord{})
	require.True(t, ok)
	for _, want := range []string{"Ship the indexer?", "yes with polls", "Deploy bot", "Release 2", "notes", "Risk", "purple herring", "built from main"} {
		require.Contains(t, mutation.Record.NormalizedContent, want)
	}
}
