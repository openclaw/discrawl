package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMultilingualSearchPreservesMetadataFilters(t *testing.T) {
	ctx := context.Background()
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), map[string]LexicalTokenizer{
		"ko": stubLexicalTokenizer{tokenize: func(text string) string { return text }},
	})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC()
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID: "wanted", GuildID: "g1", ChannelID: "c1", ChannelName: "alpha",
		AuthorID: "u1", AuthorName: "alice", CreatedAt: now.Format(time.RFC3339Nano),
		Content: "needle", NormalizedContent: "needle", RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID: "other", GuildID: "g2", ChannelID: "c2", ChannelName: "beta",
		AuthorID: "u2", AuthorName: "bob", CreatedAt: now.Add(time.Minute).Format(time.RFC3339Nano),
		Content: "needle", NormalizedContent: "needle", RawJSON: `{}`,
	}))

	results, err := s.SearchMessages(ctx, SearchOptions{
		Query: "needle", GuildIDs: []string{"g1"}, Channel: "alpha", Author: "alice",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"wanted"}, searchResultIDs(results))

	results, err = s.SearchMessages(ctx, SearchOptions{
		Query: "needle", GuildIDs: []string{"g1"}, Channel: "missing",
	})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestMultilingualSearchReportsQueryTokenizerFailure(t *testing.T) {
	ctx := context.Background()
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), map[string]LexicalTokenizer{
		"ko": stubLexicalTokenizer{tokenize: func(text string) string { return text }},
	})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	s.lexicalTokenizers["ko"] = failingLexicalTokenizer{err: errors.New("query tokenizer failed")}

	_, err = s.SearchMessages(ctx, SearchOptions{Query: "needle"})
	require.ErrorContains(t, err, "tokenize ko query")
}

func TestMultilingualSearchFallsBackWhenDefaultFTSIsMissing(t *testing.T) {
	ctx := context.Background()
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), map[string]LexicalTokenizer{
		"ko": stubLexicalTokenizer{tokenize: func(text string) string { return text }},
	})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID: "fallback", GuildID: "g1", ChannelID: "c1",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content:   "needle", NormalizedContent: "needle", RawJSON: `{}`,
	}))
	_, err = s.DB().ExecContext(ctx, `drop table message_fts`)
	require.NoError(t, err)

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "needle", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"fallback"}, searchResultIDs(results))
}

func TestMultilingualDeleteRejectsUnsafeColumn(t *testing.T) {
	ctx := context.Background()
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), nil)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	defer rollback(tx)

	err = s.deleteLexicalMessagesTx(ctx, tx, "message_id", "unsafe")
	require.ErrorContains(t, err, "unsupported lexical delete column")
}

func TestFuseLexicalSearchResultsUsesDefaultLimit(t *testing.T) {
	ranking := make([]SearchResult, 25)
	for i := range ranking {
		ranking[i].MessageID = fmt.Sprintf("message-%02d", i)
	}
	results := fuseLexicalSearchResults([][]SearchResult{ranking}, 0)
	require.Len(t, results, 20)
	require.Equal(t, "message-00", results[0].MessageID)
}
