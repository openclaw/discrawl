package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type stubLexicalTokenizer struct {
	tokenize func(string) string
	identity string
	query    func(string) [][]string
}

func openWithLexicalTokenizers(ctx context.Context, path string, tokenizers map[string]LexicalTokenizer) (*Store, error) {
	return openLexicalStore(ctx, path, tokenizers, false)
}

func (s stubLexicalTokenizer) Tokenize(_ context.Context, text string) (string, error) {
	return s.tokenize(text), nil
}

func (s stubLexicalTokenizer) QueryGroups(_ context.Context, text string) ([][]string, error) {
	if s.query != nil {
		return s.query(text), nil
	}
	var groups [][]string
	for word := range strings.FieldsSeq(text) {
		tokens := strings.Fields(s.tokenize(word))
		if len(tokens) == 0 {
			return nil, nil
		}
		for _, token := range tokens {
			groups = append(groups, []string{token})
		}
	}
	return groups, nil
}

func (s stubLexicalTokenizer) Identity() string { return lexicalFingerprint("stub", s.identity) }

func (stubLexicalTokenizer) Close() error {
	return nil
}

func TestSearchMessagesMultilingualIndexesEachConfiguredAnalyzer(t *testing.T) {
	ctx := context.Background()
	tokenizers := map[string]LexicalTokenizer{
		"ko": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
			"저녁먹음": "저녁 먹 음",
		})},
		"ja": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
			"東京都庁": "東京 都庁",
		})},
		"zh": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
			"自然语言处理": "自然 语言 处理",
		})},
		"ar": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
			"والكتاب": "و ال كتاب",
		})},
	}
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), tokenizers)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	base := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	messages := []MessageRecord{
		{ID: "ko", GuildID: "g1", ChannelID: "c1", CreatedAt: base.Format(time.RFC3339Nano), Content: "저녁먹음", NormalizedContent: "저녁먹음", RawJSON: `{}`},
		{ID: "ja", GuildID: "g1", ChannelID: "c1", CreatedAt: base.Add(time.Minute).Format(time.RFC3339Nano), Content: "東京都庁", NormalizedContent: "東京都庁", RawJSON: `{}`},
		{ID: "zh", GuildID: "g1", ChannelID: "c1", CreatedAt: base.Add(2 * time.Minute).Format(time.RFC3339Nano), Content: "自然语言处理", NormalizedContent: "自然语言处理", RawJSON: `{}`},
		{ID: "ar", GuildID: "g1", ChannelID: "c1", CreatedAt: base.Add(3 * time.Minute).Format(time.RFC3339Nano), Content: "والكتاب", NormalizedContent: "والكتاب", RawJSON: `{}`},
	}
	for _, message := range messages {
		require.NoError(t, s.UpsertMessage(ctx, message))
	}

	for query, wantID := range map[string]string{
		"저녁":   "ko",
		"東京":   "ja",
		"语言":   "zh",
		"كتاب": "ar",
	} {
		results, err := s.SearchMessages(ctx, SearchOptions{Query: query, Limit: 10})
		require.NoError(t, err, query)
		require.Equal(t, []string{wantID}, searchResultIDs(results), query)
	}
}

func TestSearchMessagesMultilingualRRFCombinesAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	tokenizers := map[string]LexicalTokenizer{
		"ko": stubLexicalTokenizer{tokenize: func(text string) string {
			text = strings.ReplaceAll(text, "검색", "shared")
			return strings.ReplaceAll(text, "検索", "")
		}},
		"ja": stubLexicalTokenizer{tokenize: func(text string) string {
			text = strings.ReplaceAll(text, "検索", "shared")
			return strings.ReplaceAll(text, "검색", "")
		}},
	}
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), tokenizers)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	base := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID: "mixed", GuildID: "g1", ChannelID: "c1", CreatedAt: base.Format(time.RFC3339Nano),
		Content: "검색 検索", NormalizedContent: "검색 検索", RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID: "single", GuildID: "g1", ChannelID: "c1", CreatedAt: base.Add(time.Minute).Format(time.RFC3339Nano),
		Content: "검색 only", NormalizedContent: "검색 only", RawJSON: `{}`,
	}))

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "검색 検索", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"mixed"}, searchResultIDs(results))
}

func TestRebuildSearchIndexesRebuildsMultilingualTables(t *testing.T) {
	ctx := context.Background()
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), map[string]LexicalTokenizer{
		"ko": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{"저녁먹음": "저녁 먹 음"})},
	})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID: "ko", GuildID: "g1", ChannelID: "c1", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content: "저녁먹음", NormalizedContent: "저녁먹음", RawJSON: `{}`,
	}))
	_, err = s.DB().ExecContext(ctx, `delete from message_fts_ko`)
	require.NoError(t, err)
	require.NoError(t, s.RebuildSearchIndexes(ctx))

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "저녁", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"ko"}, searchResultIDs(results))
}

func TestSearchMessagesMultilingualHonorsIncludeEmpty(t *testing.T) {
	ctx := context.Background()
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), map[string]LexicalTokenizer{
		"ko": stubLexicalTokenizer{tokenize: func(text string) string { return text }},
	})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID: "empty", GuildID: "g1", ChannelID: "c1", AuthorName: "needle",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), RawJSON: `{}`,
	}))
	results, err := s.SearchMessages(ctx, SearchOptions{Query: "needle", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "needle", Limit: 10, IncludeEmpty: true})
	require.NoError(t, err)
	require.Equal(t, []string{"empty"}, searchResultIDs(results))
}

func replaceLexicalTerms(replacements map[string]string) func(string) string {
	return func(text string) string {
		for from, to := range replacements {
			text = strings.ReplaceAll(text, from, to)
		}
		return text
	}
}

func TestArabicSearchMatchesVariantsButRequiresSeparateWords(t *testing.T) {
	ctx := context.Background()
	s, err := OpenWithOptions(ctx, filepath.Join(t.TempDir(), "archive.db"), OpenOptions{LexicalLanguages: []string{"ar"}})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{ID: "1", GuildID: "g", ChannelID: "c", Content: "والكتاب", NormalizedContent: "والكتاب", RawJSON: `{}`, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}))
	results, err := s.SearchMessages(ctx, SearchOptions{Query: "الكتاب"})
	require.NoError(t, err)
	require.Equal(t, []string{"1"}, searchResultIDs(results))
	results, err = s.SearchMessages(ctx, SearchOptions{Query: "الكتاب 7"})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestLexicalQueryKeepsDistinctWordGroups(t *testing.T) {
	tokenizer := stubLexicalTokenizer{query: func(string) [][]string {
		return [][]string{{"東京"}, {"行き", "行く"}}
	}}
	query, err := lexicalQuery(context.Background(), tokenizer, "東京 行きました")
	require.NoError(t, err)
	require.Equal(t, `("東京") AND ("行き" OR "行く")`, query)
}

func TestLexicalQueryIgnoresUnindexedPunctuationGroups(t *testing.T) {
	tokenizer := stubLexicalTokenizer{query: func(string) [][]string { return [][]string{{"搜索"}, {"，"}, {"功能"}, {"😀"}, {"7"}} }}
	query, err := lexicalQuery(context.Background(), tokenizer, "搜索，功能😀 7")
	require.NoError(t, err)
	require.Equal(t, `("搜索") AND ("功能") AND ("7")`, query)
}
