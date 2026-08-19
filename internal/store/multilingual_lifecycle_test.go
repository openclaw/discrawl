package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type failingLexicalTokenizer struct {
	err error
}

func (f failingLexicalTokenizer) Tokenize(context.Context, string) (string, error) {
	return "", f.err
}

func (failingLexicalTokenizer) Close() error {
	return nil
}

func TestMultilingualIndexesTrackBatchDeletesAndGuildPurge(t *testing.T) {
	ctx := context.Background()
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), map[string]LexicalTokenizer{
		"ko": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
			"저녁먹음": "저녁 먹 음",
			"회의기록": "회의 기록",
		})},
	})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC()
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{
		{Record: MessageRecord{
			ID: "first", GuildID: "g1", ChannelID: "c1",
			CreatedAt: now.Format(time.RFC3339Nano),
			Content:   "저녁먹음", NormalizedContent: "저녁먹음", RawJSON: `{}`,
		}},
		{Record: MessageRecord{
			ID: "second", GuildID: "g2", ChannelID: "c2",
			CreatedAt: now.Add(time.Minute).Format(time.RFC3339Nano),
			Content:   "회의기록", NormalizedContent: "회의기록", RawJSON: `{}`,
		}},
	}))

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "저녁", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"first"}, searchResultIDs(results))
	require.NoError(t, s.MarkMessageDeleted(
		ctx,
		"g1",
		"c1",
		"first",
		map[string]string{"deleted_at": now.Add(time.Hour).Format(time.RFC3339Nano)},
	))
	results, err = s.SearchMessages(ctx, SearchOptions{Query: "저녁", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "기록", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"second"}, searchResultIDs(results))
	require.NoError(t, s.DeleteGuildData(ctx, "g2"))
	results, err = s.SearchMessages(ctx, SearchOptions{Query: "기록", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestMultilingualIndexVersionSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "discrawl.db")
	tokenizers := func() map[string]LexicalTokenizer {
		return map[string]LexicalTokenizer{
			"ko": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
				"저녁먹음": "저녁 먹 음",
			})},
		}
	}
	s, err := openWithLexicalTokenizers(ctx, path, tokenizers())
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID: "ko", GuildID: "g1", ChannelID: "c1",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content:   "저녁먹음", NormalizedContent: "저녁먹음", RawJSON: `{}`,
	}))
	require.NoError(t, s.Close())

	s, err = openWithLexicalTokenizers(ctx, path, tokenizers())
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	results, err := s.SearchMessages(ctx, SearchOptions{Query: "저녁", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"ko"}, searchResultIDs(results))
}

func TestMultilingualIndexRebuildsAfterDisabledWrites(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "discrawl.db")
	tokenizers := func() map[string]LexicalTokenizer {
		return map[string]LexicalTokenizer{
			"ko": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
				"저녁먹음": "저녁 먹 음",
				"회의기록": "회의 기록",
			})},
		}
	}

	enabled, err := openWithLexicalTokenizers(ctx, path, tokenizers())
	require.NoError(t, err)
	require.NoError(t, enabled.UpsertMessage(ctx, MessageRecord{
		ID: "before", GuildID: "g1", ChannelID: "c1",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content:   "저녁먹음", NormalizedContent: "저녁먹음", RawJSON: `{}`,
	}))
	require.NoError(t, enabled.Close())

	disabled, err := openWithLexicalTokenizers(ctx, path, nil)
	require.NoError(t, err)
	require.NoError(t, disabled.UpsertMessage(ctx, MessageRecord{
		ID: "during", GuildID: "g1", ChannelID: "c1",
		CreatedAt: time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		Content:   "회의기록", NormalizedContent: "회의기록", RawJSON: `{}`,
	}))
	require.NoError(t, disabled.Close())

	reenabled, err := openWithLexicalTokenizers(ctx, path, tokenizers())
	require.NoError(t, err)
	defer func() { _ = reenabled.Close() }()
	results, err := reenabled.SearchMessages(ctx, SearchOptions{Query: "기록", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"during"}, searchResultIDs(results))
}

func TestMultilingualIndexesSearchThroughReadOnlyStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "discrawl.db")
	tokenizers := func() map[string]LexicalTokenizer {
		return map[string]LexicalTokenizer{
			"ko": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
				"저녁먹음": "저녁 먹 음",
			})},
		}
	}
	writer, err := openWithLexicalTokenizers(ctx, path, tokenizers())
	require.NoError(t, err)
	require.NoError(t, writer.UpsertMessage(ctx, MessageRecord{
		ID: "ko", GuildID: "g1", ChannelID: "c1",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content:   "저녁먹음", NormalizedContent: "저녁먹음", RawJSON: `{}`,
	}))
	require.NoError(t, writer.Close())

	reader, err := openReadOnlyWithLexicalTokenizers(ctx, path, tokenizers())
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	results, err := reader.SearchMessages(ctx, SearchOptions{Query: "저녁", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"ko"}, searchResultIDs(results))
}

func TestOpenReadOnlyWithOptionsWithoutLexicalLanguages(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "discrawl.db")
	writer, err := Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader, err := OpenReadOnlyWithOptions(ctx, path, OpenOptions{})
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}

func TestOpenReadOnlyWithOptionsKeepsMissingTokenizerLazy(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "discrawl.db")
	writer, err := Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader, err := OpenReadOnlyWithOptions(ctx, path, OpenOptions{
		LexicalLanguages:   []string{"ko"},
		LexicalKiwiCommand: "/definitely/missing/discrawl-kiwi",
		LexicalKiwiModel:   "/definitely/missing/kiwi-model",
	})
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}

func TestMultilingualTokenizerFailureAbortsWrite(t *testing.T) {
	ctx := context.Background()
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), nil)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	s.lexicalTokenizers = map[string]LexicalTokenizer{
		"ko": failingLexicalTokenizer{err: errors.New("tokenizer unavailable")},
	}

	err = s.UpsertMessage(ctx, MessageRecord{
		ID: "ko", GuildID: "g1", ChannelID: "c1",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content:   "저녁먹음", NormalizedContent: "저녁먹음", RawJSON: `{}`,
	})
	require.ErrorContains(t, err, "tokenize ko text")
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages`).Scan(&count))
	require.Zero(t, count)
}
