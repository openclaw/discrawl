package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type multilingualBenchmarkCase struct {
	language string
	id       string
	content  string
	query    string
}

func TestMultilingualLexicalQualityBenchmark(t *testing.T) {
	if os.Getenv("DISCRAWL_TOKENIZER_E2E") != "1" {
		t.Skip("set DISCRAWL_TOKENIZER_E2E=1 with optional tokenizer packages installed")
	}
	ctx := context.Background()
	root := t.TempDir()
	baseline, err := Open(ctx, filepath.Join(root, "baseline.db"))
	require.NoError(t, err)
	defer func() { _ = baseline.Close() }()
	multilingual, err := OpenWithOptions(ctx, filepath.Join(root, "multilingual.db"), OpenOptions{
		LexicalLanguages:   []string{"ko", "ja", "zh", "ar"},
		LexicalPython:      os.Getenv("DISCRAWL_TOKENIZER_PYTHON"),
		LexicalKiwiCommand: os.Getenv("DISCRAWL_KIWI_HELPER"),
		LexicalKiwiModel:   os.Getenv("DISCRAWL_KIWI_MODEL"),
	})
	require.NoError(t, err)
	defer func() { _ = multilingual.Close() }()

	cases := multilingualBenchmarkCases()
	base := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	for i, item := range cases {
		message := MessageRecord{
			ID:                item.id,
			GuildID:           "g1",
			ChannelID:         "c1",
			CreatedAt:         base.Add(time.Duration(i) * time.Minute).Format(time.RFC3339Nano),
			Content:           item.content,
			NormalizedContent: item.content,
			RawJSON:           `{}`,
		}
		require.NoError(t, baseline.UpsertMessage(ctx, message))
		require.NoError(t, multilingual.UpsertMessage(ctx, message))
	}
	baselineBytes := sqliteDatabaseBytes(t, baseline)
	multilingualBytes := sqliteDatabaseBytes(t, multilingual)
	t.Logf(
		"database pages: unicode61=%d bytes multilingual=%d bytes (%.2fx)",
		baselineBytes,
		multilingualBytes,
		float64(multilingualBytes)/float64(baselineBytes),
	)

	baselineHits := make(map[string]int)
	multilingualHits := make(map[string]int)
	totals := make(map[string]int)
	for _, item := range cases {
		totals[item.language]++
		baselineResults, err := baseline.SearchMessages(ctx, SearchOptions{Query: item.query, Limit: 5})
		require.NoError(t, err)
		if containsSearchResult(baselineResults, item.id) {
			baselineHits[item.language]++
		}
		multilingualResults, err := multilingual.SearchMessages(ctx, SearchOptions{Query: item.query, Limit: 5})
		require.NoError(t, err)
		if containsSearchResult(multilingualResults, item.id) {
			multilingualHits[item.language]++
		}
	}
	for _, language := range []string{"ko", "ja", "zh", "ar"} {
		t.Logf(
			"%s recall@5: unicode61=%d/%d multilingual=%d/%d",
			language,
			baselineHits[language],
			totals[language],
			multilingualHits[language],
			totals[language],
		)
		require.Greater(t, multilingualHits[language], baselineHits[language], language)
		require.Equal(t, totals[language], multilingualHits[language], language)
	}
}

func sqliteDatabaseBytes(t *testing.T, s *Store) int64 {
	t.Helper()
	var pageCount int64
	var pageSize int64
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `pragma page_count`).Scan(&pageCount))
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `pragma page_size`).Scan(&pageSize))
	return pageCount * pageSize
}

func containsSearchResult(results []SearchResult, messageID string) bool {
	for _, result := range results {
		if result.MessageID == messageID {
			return true
		}
	}
	return false
}

func multilingualBenchmarkCases() []multilingualBenchmarkCase {
	return []multilingualBenchmarkCase{
		{language: "ko", id: "ko-1", content: "오늘저녁먹음", query: "저녁"},
		{language: "ko", id: "ko-2", content: "서울맛집추천", query: "맛집"},
		{language: "ko", id: "ko-3", content: "프로젝트검색기능", query: "검색"},
		{language: "ko", id: "ko-4", content: "회의기록정리", query: "기록"},
		{language: "ko", id: "ko-5", content: "운동계획세움", query: "계획"},
		{language: "ja", id: "ja-1", content: "東京都庁に行きます", query: "東京"},
		{language: "ja", id: "ja-2", content: "自然言語処理を学ぶ", query: "言語"},
		{language: "ja", id: "ja-3", content: "検索機能を改善する", query: "検索"},
		{language: "ja", id: "ja-4", content: "会議記録を整理する", query: "記録"},
		{language: "ja", id: "ja-5", content: "機械学習モデル", query: "学習"},
		{language: "zh", id: "zh-1", content: "自然语言处理很有趣", query: "语言"},
		{language: "zh", id: "zh-2", content: "北京大学校园很美", query: "大学"},
		{language: "zh", id: "zh-3", content: "搜索功能需要改进", query: "搜索"},
		{language: "zh", id: "zh-4", content: "会议记录已经完成", query: "记录"},
		{language: "zh", id: "zh-5", content: "机器学习模型上线", query: "学习"},
		{language: "ar", id: "ar-1", content: "والكتاب مفيد للطلاب", query: "كتاب"},
		{language: "ar", id: "ar-2", content: "والمدرسة تفتح صباحا", query: "مدرسة"},
		{language: "ar", id: "ar-3", content: "فالاجتماع مهم اليوم", query: "اجتماع"},
		{language: "ar", id: "ar-4", content: "بالسجل تفاصيل كاملة", query: "سجل"},
		{language: "ar", id: "ar-5", content: "كالبرنامج سريع جدا", query: "برنامج"},
	}
}
