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

func (f failingLexicalTokenizer) Identity() string { return lexicalFingerprint("stub", "") }

func (f failingLexicalTokenizer) QueryGroups(context.Context, string) ([][]string, error) {
	return nil, f.err
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

func TestDisabledLexicalIndexDoesNotRetainPurgedGuildData(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "discrawl.db")
	enabled, err := openWithLexicalTokenizers(ctx, path, map[string]LexicalTokenizer{
		"ko": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
			"회의기록": "회의 기록",
		})},
	})
	require.NoError(t, err)
	require.NoError(t, enabled.UpsertMessage(ctx, MessageRecord{
		ID: "message", GuildID: "guild", ChannelID: "channel",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content:   "회의기록", NormalizedContent: "회의기록", RawJSON: `{}`,
	}))
	require.NoError(t, enabled.Close())

	disabled, err := openWithLexicalTokenizers(ctx, path, nil)
	require.NoError(t, err)
	defer func() { _ = disabled.Close() }()
	require.NoError(t, disabled.DeleteGuildData(ctx, "guild"))

	var tables int
	require.NoError(t, disabled.DB().QueryRowContext(
		ctx,
		`select count(*) from sqlite_schema where type = 'table' and name = 'message_fts_ko'`,
	).Scan(&tables))
	require.Zero(t, tables)
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

func TestMultilingualDeleteUpsertDoesNotRequireTokenizer(t *testing.T) {
	ctx := context.Background()
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "discrawl.db"), map[string]LexicalTokenizer{
		"ko": stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
			"저녁먹음": "저녁 먹 음",
		})},
	})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	message := MessageRecord{
		ID: "ko", GuildID: "g1", ChannelID: "c1",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content:   "저녁먹음", NormalizedContent: "저녁먹음", RawJSON: `{}`,
	}
	require.NoError(t, s.UpsertMessage(ctx, message))
	s.lexicalTokenizers["ko"] = failingLexicalTokenizer{err: errors.New("tokenizer unavailable")}

	message.DeletedAt = time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessage(ctx, message))
	var lexicalRows int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_fts_ko`).Scan(&lexicalRows))
	require.Zero(t, lexicalRows)
	s.lexicalTokenizers["ko"] = stubLexicalTokenizer{tokenize: replaceLexicalTerms(map[string]string{
		"저녁먹음": "저녁 먹 음",
	})}

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "저녁", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestLexicalAnalyzerChangeRebuildsOnlyAffectedIndex(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "discrawl.db")
	tokenizers := func(identity, term string) map[string]LexicalTokenizer {
		return map[string]LexicalTokenizer{
			"ko": stubLexicalTokenizer{identity: identity, tokenize: func(string) string { return term }},
			"ja": stubLexicalTokenizer{tokenize: func(text string) string { return text }},
		}
	}
	writer, err := openWithLexicalTokenizers(ctx, path, tokenizers("old", "before"))
	require.NoError(t, err)
	require.NoError(t, writer.UpsertMessage(ctx, MessageRecord{
		ID: "message", GuildID: "guild", ChannelID: "channel", RawJSON: `{}`,
		Content: "surface", NormalizedContent: "surface", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}))
	var unchanged string
	require.NoError(t, writer.DB().QueryRowContext(ctx, `select updated_at from sync_state where scope = ?`, lexicalFTSScope("ja")).Scan(&unchanged))
	require.NoError(t, writer.Close())

	reader, err := openReadOnlyWithLexicalTokenizers(ctx, path, tokenizers("new", "after"))
	require.NoError(t, err)
	_, err = reader.SearchMessages(ctx, SearchOptions{Query: "needle"})
	require.ErrorContains(t, err, "lexical rebuild")
	require.NoError(t, reader.Close())

	writer, err = openWithLexicalTokenizers(ctx, path, tokenizers("new", "after"))
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()
	results, err := writer.SearchMessages(ctx, SearchOptions{Query: "needle"})
	require.NoError(t, err)
	require.Equal(t, []string{"message"}, searchResultIDs(results))
	var content, unchangedAfter string
	require.NoError(t, writer.DB().QueryRowContext(ctx, `select content from message_fts_ko`).Scan(&content))
	require.Equal(t, "after", content)
	require.NoError(t, writer.DB().QueryRowContext(ctx, `select updated_at from sync_state where scope = ?`, lexicalFTSScope("ja")).Scan(&unchangedAfter))
	require.Equal(t, unchanged, unchangedAfter)

	writer.lexicalTokenizers["ko"] = failingLexicalTokenizer{err: errors.New("analyzer failed")}
	require.ErrorContains(t, writer.RebuildLexicalIndexes(ctx), "analyzer failed")
	require.NoError(t, writer.DB().QueryRowContext(ctx, `select content from message_fts_ko`).Scan(&content))
	require.Equal(t, "after", content, "failed rebuild rolls back the old index")
}

func TestLexicalIdentityTracksEffectiveConfiguration(t *testing.T) {
	opts := OpenOptions{LexicalKiwiCommand: "/opt/helper-one", LexicalKiwiModel: "/opt/model-one", LexicalJaCommand: "/opt/ja-one", LexicalZhCommand: "/opt/zh-one"}
	for _, language := range []string{"ko", "ja", "zh", "ar"} {
		original, err := lexicalAnalyzerIdentity(opts, language)
		require.NoError(t, err)
		changed := opts
		changed.LexicalKiwiCommand = "/opt/helper-two"
		changed.LexicalJaCommand = "/opt/ja-two"
		changed.LexicalZhCommand = "/opt/zh-two"
		identity, err := lexicalAnalyzerIdentity(changed, language)
		require.NoError(t, err)
		if language == "ar" {
			require.Equal(t, original, identity)
		} else {
			require.NotEqual(t, original, identity)
		}
	}
	original, err := lexicalAnalyzerIdentity(opts, "ko")
	require.NoError(t, err)
	opts.LexicalKiwiModel = "/opt/model-two"
	changed, err := lexicalAnalyzerIdentity(opts, "ko")
	require.NoError(t, err)
	require.NotEqual(t, original, changed)
}

func TestForcedLexicalOpenRebuildsEachMessageOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{ID: "1", GuildID: "g", ChannelID: "c", Content: "والكتاب", NormalizedContent: "والكتاب", RawJSON: `{}`, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}))
	require.NoError(t, s.Close())
	for range 2 {
		calls := 0
		s, err = openLexicalStore(ctx, path, map[string]LexicalTokenizer{"ar": stubLexicalTokenizer{tokenize: func(text string) string { calls++; return text }}}, true)
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		require.NoError(t, s.Close())
	}
}
