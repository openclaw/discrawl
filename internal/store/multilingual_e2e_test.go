package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMultilingualLexicalSearchE2E(t *testing.T) {
	if os.Getenv("DISCRAWL_TOKENIZER_E2E") != "1" {
		t.Skip("set DISCRAWL_TOKENIZER_E2E=1 with optional tokenizer packages installed")
	}
	ctx := context.Background()
	s, err := OpenWithOptions(ctx, filepath.Join(t.TempDir(), "discrawl.db"), OpenOptions{
		LexicalLanguages:   []string{"ko", "ja", "zh", "ar"},
		LexicalKiwiCommand: os.Getenv("DISCRAWL_KIWI_HELPER"),
		LexicalKiwiModel:   os.Getenv("DISCRAWL_KIWI_MODEL"),
		LexicalJaCommand:   os.Getenv("DISCRAWL_JA_HELPER"),
		LexicalZhCommand:   os.Getenv("DISCRAWL_ZH_HELPER"),
	})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	base := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	fixtures := []struct {
		id      string
		content string
		query   string
	}{
		{id: "ko", content: "오늘 저녁먹음 기록", query: "저녁"},
		{id: "ja", content: "東京都庁に行きます", query: "東京"},
		{id: "zh", content: "自然语言处理很有趣", query: "语言"},
		{id: "ar", content: "والكتاب مفيد للطلاب", query: "كتاب"},
	}
	for i, fixture := range fixtures {
		require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
			ID:                fixture.id,
			GuildID:           "g1",
			ChannelID:         "c1",
			CreatedAt:         base.Add(time.Duration(i) * time.Minute).Format(time.RFC3339Nano),
			Content:           fixture.content,
			NormalizedContent: fixture.content,
			RawJSON:           `{}`,
		}))
	}
	for _, fixture := range fixtures {
		results, err := s.SearchMessages(ctx, SearchOptions{Query: fixture.query, Limit: 10})
		require.NoError(t, err, fixture.id)
		require.Contains(t, searchResultIDs(results), fixture.id, fixture.query)
	}
}
