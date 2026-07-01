package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/embed"
	"github.com/stretchr/testify/require"
)

func TestUpsertMessagesBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{
		{
			Record: MessageRecord{
				ID:                "m1",
				GuildID:           "g1",
				ChannelID:         "c1",
				MessageType:       0,
				CreatedAt:         now,
				Content:           "one",
				NormalizedContent: "one",
				RawJSON:           `{"id":"m1"}`,
			},
			EventType:   "upsert",
			PayloadJSON: `{"id":"m1"}`,
			Options: WriteOptions{
				AppendEvent: true,
			},
		},
		{
			Record: MessageRecord{
				ID:                "m2",
				GuildID:           "g1",
				ChannelID:         "c1",
				MessageType:       0,
				CreatedAt:         now,
				Content:           "two",
				NormalizedContent: "two",
				RawJSON:           `{"id":"m2"}`,
			},
			EventType:   "upsert",
			PayloadJSON: `{"id":"m2"}`,
			Options: WriteOptions{
				AppendEvent: true,
			},
		},
	}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from messages")
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])

	_, rows, err = s.ReadOnlyQuery(ctx, "select count(*) from message_events")
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])
}

func TestUpsertMessagesRefreshesDuplicateAttachmentID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	first := MessageMutation{
		Record: MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "first",
			NormalizedContent: "first",
			HasAttachments:    true,
			RawJSON:           `{"id":"m1"}`,
		},
		Attachments: []AttachmentRecord{{
			AttachmentID: "a1",
			MessageID:    "m1",
			GuildID:      "g1",
			ChannelID:    "c1",
			Filename:     "first.txt",
			TextContent:  "cached text",
			MediaPath:    "attachments/a1.txt",
			FetchStatus:  "done",
			FetchError:   "stale failure",
		}},
	}
	second := MessageMutation{
		Record: MessageRecord{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c2",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "second",
			NormalizedContent: "second",
			HasAttachments:    true,
			RawJSON:           `{"id":"m2"}`,
		},
		Attachments: []AttachmentRecord{{
			AttachmentID: "a1",
			MessageID:    "m2",
			GuildID:      "g1",
			ChannelID:    "c2",
			Filename:     "second.txt",
			TextContent:  "fresh text",
			FetchStatus:  "done",
		}},
	}

	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{first}))
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{second}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select attachment_id, message_id, channel_id, filename, text_content, media_path, fetch_status, fetch_error from message_attachments")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"a1", "m2", "c2", "second.txt", "fresh text", "attachments/a1.txt", "done", ""}}, rows)
}

func TestUpsertMessagesNormalizesTimestampStrings(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{
		{
			Record: MessageRecord{
				ID:                "m1",
				GuildID:           "g1",
				ChannelID:         "c1",
				MessageType:       0,
				CreatedAt:         "2026-04-25T12:00:00Z",
				Content:           "exact",
				NormalizedContent: "exact",
				RawJSON:           `{"id":"m1"}`,
			},
		},
		{
			Record: MessageRecord{
				ID:                "m2",
				GuildID:           "g1",
				ChannelID:         "c1",
				MessageType:       0,
				CreatedAt:         "2026-04-25T08:00:00.5-04:00",
				EditedAt:          "2026-04-25T08:00:01.25-04:00",
				Content:           "half",
				NormalizedContent: "half",
				RawJSON:           `{"id":"m2"}`,
			},
			Mentions: []MentionEventRecord{{
				MessageID:  "m2",
				GuildID:    "g1",
				ChannelID:  "c1",
				TargetType: "user",
				TargetID:   "u1",
				EventAt:    "2026-04-25T08:00:00.5-04:00",
			}},
		},
	}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select id, created_at from messages order by created_at asc")
	require.NoError(t, err)
	require.Equal(t, [][]string{
		{"m1", "2026-04-25T12:00:00.000000000Z"},
		{"m2", "2026-04-25T12:00:00.500000000Z"},
	}, rows)

	_, rows, err = s.ReadOnlyQuery(ctx, "select edited_at from messages where id = 'm2'")
	require.NoError(t, err)
	require.Equal(t, "2026-04-25T12:00:01.250000000Z", rows[0][0])

	_, rows, err = s.ReadOnlyQuery(ctx, "select event_at from mention_events where message_id = 'm2'")
	require.NoError(t, err)
	require.Equal(t, "2026-04-25T12:00:00.500000000Z", rows[0][0])
}

func TestUpsertMessagesHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	err = s.UpsertMessages(canceled, []MessageMutation{{
		Record: MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			MessageType:       0,
			CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
			Content:           "one",
			NormalizedContent: "one",
			RawJSON:           `{"id":"m1"}`,
		},
	}})
	require.ErrorIs(t, err, context.Canceled)

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from messages")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestUpsertMessagesSkipsEventsAndEmbeddingsByDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{{
		Record: MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "one",
			NormalizedContent: "one",
			RawJSON:           `{"id":"m1"}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"m1"}`,
	}}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from message_events")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])

	_, rows, err = s.ReadOnlyQuery(ctx, "select count(*) from embedding_jobs")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestUpsertMessageWithEmbeddingsQueuesJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}, WriteOptions{EnqueueEmbedding: true}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from embedding_jobs")
	require.NoError(t, err)
	require.Equal(t, "1", rows[0][0])
}

func TestUpsertMessageWithEmbeddingsQueuesExistingMessageWithoutJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	record := MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}
	require.NoError(t, s.UpsertMessage(ctx, record))
	require.NoError(t, s.UpsertMessageWithOptions(ctx, record, WriteOptions{EnqueueEmbedding: true}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select state, attempts from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"pending", "0"}}, rows)
}

func TestMarkMessageDeletedClearsSearchAndEmbeddingState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, record := range []MessageRecord{
		{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "vanishing needle",
			NormalizedContent: "vanishing needle",
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
		{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u2",
			AuthorName:        "Alice",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "active reference",
			NormalizedContent: "active reference",
			RawJSON:           `{"author":{"username":"Alice"}}`,
		},
	} {
		require.NoError(t, s.UpsertMessageWithOptions(ctx, record, WriteOptions{EnqueueEmbedding: true}))
	}

	stats, err := s.DrainEmbeddingJobs(ctx, &fakeEmbeddingProvider{
		batches: []embed.EmbeddingBatch{{Vectors: [][]float32{{1, 0}, {0, 1}}}},
	}, EmbeddingDrainOptions{Provider: "ollama", Model: "nomic-embed-text", Limit: 10, BatchSize: 2})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Succeeded)

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "needle", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"m1"}, searchResultIDs(results))

	semanticResults, err := s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector: []float32{1, 0},
		Provider:    "ollama",
		Model:       "nomic-embed-text",
		Dimensions:  2,
		Limit:       10,
	})
	require.NoError(t, err)
	require.Contains(t, searchResultIDs(semanticResults), "m1")

	require.NoError(t, s.MarkMessageDeleted(ctx, "g1", "c1", "m1", map[string]string{"deleted": "1"}))

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "needle", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)

	semanticResults, err = s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector: []float32{1, 0},
		Provider:    "ollama",
		Model:       "nomic-embed-text",
		Dimensions:  2,
		Limit:       10,
	})
	require.NoError(t, err)
	require.NotContains(t, searchResultIDs(semanticResults), "m1")

	for _, query := range []string{
		"select count(*) from message_fts where message_id = 'm1'",
		"select count(*) from message_embeddings where message_id = 'm1'",
		"select count(*) from embedding_jobs where message_id = 'm1'",
	} {
		_, rows, err := s.ReadOnlyQuery(ctx, query)
		require.NoError(t, err, query)
		require.Equal(t, "0", rows[0][0], query)
	}

	requeued, err := s.RequeueAllEmbeddingJobs(ctx, EmbeddingDrainOptions{Provider: "ollama", Model: "nomic-embed-text"})
	require.NoError(t, err)
	require.Equal(t, 1, requeued)
	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestDrainEmbeddingJobsStoresVectorsAndSkipsEmptyInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         now,
		Content:           "abcdef世界",
		NormalizedContent: "abcdef世界",
		RawJSON:           `{}`,
	}, WriteOptions{EnqueueEmbedding: true}))
	require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         now,
		Content:           "",
		NormalizedContent: "   ",
		RawJSON:           `{}`,
	}, WriteOptions{EnqueueEmbedding: true}))

	provider := &fakeEmbeddingProvider{
		batches: []embed.EmbeddingBatch{{
			Vectors: [][]float32{{1.25, 2.5}},
		}},
	}
	stats, err := s.DrainEmbeddingJobs(ctx, provider, EmbeddingDrainOptions{
		Provider:      "ollama",
		Model:         "nomic-embed-text",
		Limit:         10,
		BatchSize:     2,
		MaxInputChars: 7,
	})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Processed)
	require.Equal(t, 1, stats.Succeeded)
	require.Equal(t, 1, stats.Skipped)
	require.Equal(t, 0, stats.RemainingBacklog)
	require.Equal(t, [][]string{{"abcdef世"}}, provider.inputs)

	_, rows, err := s.ReadOnlyQuery(ctx, "select message_id, provider, model, input_version, dimensions from message_embeddings")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"m1", "ollama", "nomic-embed-text", EmbeddingInputVersion, "2"}}, rows)

	var blob []byte
	require.NoError(t, s.DB().QueryRowContext(ctx, `select embedding_blob from message_embeddings where message_id = 'm1'`).Scan(&blob))
	vector, err := DecodeEmbeddingVector(blob)
	require.NoError(t, err)
	require.Equal(t, []float32{1.25, 2.5}, vector)

	_, rows, err = s.ReadOnlyQuery(ctx, "select message_id, state, provider, model, input_version from embedding_jobs order by message_id")
	require.NoError(t, err)
	require.Equal(t, [][]string{
		{"m1", "done", "ollama", "nomic-embed-text", EmbeddingInputVersion},
		{"m2", "done", "ollama", "nomic-embed-text", EmbeddingInputVersion},
	}, rows)
}

func TestUpsertMessageWithEmbeddingsDoesNotRequeueUnchangedDoneJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	record := MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}
	require.NoError(t, s.UpsertMessageWithOptions(ctx, record, WriteOptions{EnqueueEmbedding: true}))

	stats, err := s.DrainEmbeddingJobs(ctx, &fakeEmbeddingProvider{
		batches: []embed.EmbeddingBatch{{Vectors: [][]float32{{1, 2}}}},
	}, EmbeddingDrainOptions{Provider: "ollama", Model: "nomic-embed-text", Limit: 10, BatchSize: 1})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Succeeded)

	require.NoError(t, s.UpsertMessageWithOptions(ctx, record, WriteOptions{EnqueueEmbedding: true}))
	_, rows, err := s.ReadOnlyQuery(ctx, "select state, attempts, last_error from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"done", "0", ""}}, rows)

	backlog, err := s.EmbeddingBacklog(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, backlog)

	record.NormalizedContent = "hello updated"
	record.Content = "hello updated"
	require.NoError(t, s.UpsertMessageWithOptions(ctx, record, WriteOptions{EnqueueEmbedding: true}))
	_, rows, err = s.ReadOnlyQuery(ctx, "select state, attempts, last_error from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"pending", "0", ""}}, rows)
}

func TestDrainEmbeddingJobsFailsWholeBatchOnDimensionMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}, WriteOptions{EnqueueEmbedding: true}))

	stats, err := s.DrainEmbeddingJobs(ctx, &fakeEmbeddingProvider{
		batches: []embed.EmbeddingBatch{{
			Dimensions: 3,
			Vectors:    [][]float32{{1, 2}},
		}},
	}, EmbeddingDrainOptions{Provider: "ollama", Model: "nomic-embed-text", Limit: 10, BatchSize: 1})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Failed)

	_, rows, err := s.ReadOnlyQuery(ctx, "select state, attempts, last_error from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, "pending", rows[0][0])
	require.Equal(t, "1", rows[0][1])
	require.Contains(t, rows[0][2], "dimensions mismatch")

	_, rows, err = s.ReadOnlyQuery(ctx, "select count(*) from message_embeddings")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestDrainEmbeddingJobsMarksFailedAfterMaxAttempts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}, WriteOptions{EnqueueEmbedding: true}))
	_, err = s.DB().ExecContext(ctx, `update embedding_jobs set attempts = 2 where message_id = 'm1'`)
	require.NoError(t, err)

	stats, err := s.DrainEmbeddingJobs(ctx, &fakeEmbeddingProvider{err: errors.New("provider down")}, EmbeddingDrainOptions{
		Provider: "ollama",
		Model:    "nomic-embed-text",
		Limit:    10,
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Failed)

	_, rows, err := s.ReadOnlyQuery(ctx, "select state, attempts, last_error from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"failed", "3", "provider down"}}, rows)
}

func TestDrainEmbeddingJobsStopsOnRateLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, id := range []string{"m1", "m2"} {
		require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
			ID:                id,
			GuildID:           "g1",
			ChannelID:         "c1",
			MessageType:       0,
			CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
			Content:           "hello",
			NormalizedContent: "hello",
			RawJSON:           `{}`,
		}, WriteOptions{EnqueueEmbedding: true}))
	}

	provider := &fakeEmbeddingProvider{err: &embed.HTTPError{StatusCode: 429, Body: "slow down"}}
	stats, err := s.DrainEmbeddingJobs(ctx, provider, EmbeddingDrainOptions{
		Provider:  "ollama",
		Model:     "nomic-embed-text",
		Limit:     10,
		BatchSize: 1,
	})
	require.NoError(t, err)
	require.True(t, stats.RateLimited)
	require.Equal(t, 0, stats.Processed)
	require.Equal(t, 0, stats.Failed)
	require.Equal(t, 1, stats.Requeued)
	require.Equal(t, 2, stats.RemainingBacklog)
	require.Len(t, provider.inputs, 1)

	_, rows, err := s.ReadOnlyQuery(ctx, "select message_id, state, attempts, last_error, coalesce(locked_at, '') from embedding_jobs order by message_id")
	require.NoError(t, err)
	require.Equal(t, [][]string{
		{"m1", "pending", "0", "embedding request failed with HTTP 429: slow down", ""},
		{"m2", "pending", "0", "", ""},
	}, rows)
}

func TestDrainEmbeddingJobsDeletesStaleVectorsForEmptyContent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	record := MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}
	require.NoError(t, s.UpsertMessageWithOptions(ctx, record, WriteOptions{EnqueueEmbedding: true}))

	_, err = s.DrainEmbeddingJobs(ctx, &fakeEmbeddingProvider{
		batches: []embed.EmbeddingBatch{{Vectors: [][]float32{{1, 2}}}},
	}, EmbeddingDrainOptions{Provider: "ollama", Model: "nomic-embed-text", Limit: 10, BatchSize: 1})
	require.NoError(t, err)

	record.Content = ""
	record.NormalizedContent = "   "
	require.NoError(t, s.UpsertMessageWithOptions(ctx, record, WriteOptions{EnqueueEmbedding: true}))

	stats, err := s.DrainEmbeddingJobs(ctx, &fakeEmbeddingProvider{}, EmbeddingDrainOptions{
		Provider:  "ollama",
		Model:     "nomic-embed-text",
		Limit:     10,
		BatchSize: 1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Processed)
	require.Equal(t, 1, stats.Skipped)

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from message_embeddings")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])

	_, rows, err = s.ReadOnlyQuery(ctx, "select state, provider, model from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"done", "ollama", "nomic-embed-text"}}, rows)
}

func TestPendingEmbeddingJobsSkipsFreshLocks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}, WriteOptions{EnqueueEmbedding: true}))

	now := time.Now().UTC()
	staleBefore := now.Add(-embeddingLockTimeout).Format(timeLayout)
	jobs, err := s.pendingEmbeddingJobs(ctx, 10, staleBefore)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	claimed, err := s.lockEmbeddingJobs(ctx, jobs, now.Format(timeLayout), staleBefore)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	jobs, err = s.pendingEmbeddingJobs(ctx, 10, staleBefore)
	require.NoError(t, err)
	require.Empty(t, jobs)

	claimed, err = s.lockEmbeddingJobs(ctx, claimed, now.Add(time.Minute).Format(timeLayout), staleBefore)
	require.NoError(t, err)
	require.Empty(t, claimed)
}

func TestRequeueAllEmbeddingJobsUsesCurrentIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, id := range []string{"m1", "m2"} {
		require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
			ID:                id,
			GuildID:           "g1",
			ChannelID:         "c1",
			MessageType:       0,
			CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
			Content:           "hello",
			NormalizedContent: "hello",
			RawJSON:           `{}`,
		}))
	}
	_, err = s.DB().ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, provider, model, input_version, last_error, updated_at)
		values('m1', 'failed', 3, 'old', 'old-model', 'old-input', 'old error', ?)
	`, time.Now().UTC().Format(timeLayout))
	require.NoError(t, err)

	requeued, err := s.RequeueAllEmbeddingJobs(ctx, EmbeddingDrainOptions{
		Provider:     "ollama",
		Model:        "nomic-embed-text",
		InputVersion: EmbeddingInputVersion,
	})
	require.NoError(t, err)
	require.Equal(t, 2, requeued)

	_, rows, err := s.ReadOnlyQuery(ctx, "select message_id, state, attempts, provider, model, input_version, last_error from embedding_jobs order by message_id")
	require.NoError(t, err)
	require.Equal(t, [][]string{
		{"m1", "pending", "0", "ollama", "nomic-embed-text", EmbeddingInputVersion, ""},
		{"m2", "pending", "0", "ollama", "nomic-embed-text", EmbeddingInputVersion, ""},
	}, rows)
}

func TestEmbeddingHelpersAndIdentityResetBranches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	opts := normalizeEmbeddingDrainOptions(EmbeddingDrainOptions{
		Provider:      " OLLAMA ",
		Model:         " model ",
		Limit:         1,
		BatchSize:     5,
		MaxInputChars: 3,
		Now:           func() time.Time { return now },
	})
	require.Equal(t, "ollama", opts.Provider)
	require.Equal(t, "model", opts.Model)
	require.Equal(t, 1, opts.BatchSize)
	require.Equal(t, EmbeddingInputVersion, opts.InputVersion)

	stats, err := s.DrainEmbeddingJobs(ctx, nil, opts)
	require.ErrorContains(t, err, "embedding provider is nil")
	require.Equal(t, "ollama", stats.Provider)

	require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         now.Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}, WriteOptions{EnqueueEmbedding: true}))
	_, err = s.DB().ExecContext(ctx, `
		update embedding_jobs
		set provider = 'old', model = 'old-model', input_version = 'old-version', attempts = 2, last_error = 'bad', locked_at = 'locked'
		where message_id = 'm1'
	`)
	require.NoError(t, err)
	require.NoError(t, s.resetEmbeddingJobIdentity(ctx, "m1", opts, true))
	_, rows, err := s.ReadOnlyQuery(ctx, "select provider, model, input_version, attempts, last_error, coalesce(locked_at, '') from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"ollama", "model", EmbeddingInputVersion, "0", "", ""}}, rows)

	_, err = s.DB().ExecContext(ctx, `update embedding_jobs set attempts = 2, locked_at = 'locked' where message_id = 'm1'`)
	require.NoError(t, err)
	require.NoError(t, s.resetEmbeddingJobIdentity(ctx, "m1", opts, false))
	_, rows, err = s.ReadOnlyQuery(ctx, "select attempts, coalesce(locked_at, '') from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"2", ""}}, rows)

	require.True(t, sameEmbeddingIdentity(embeddingJob{Provider: "ollama", Model: "model", InputVersion: EmbeddingInputVersion}, opts))
	require.True(t, emptyEmbeddingIdentity(embeddingJob{}))
	_, err = validateEmbeddingBatch(embed.EmbeddingBatch{Vectors: [][]float32{{1}, {2}}}, 1)
	require.ErrorContains(t, err, "returned 2 vectors")
	_, err = validateEmbeddingBatch(embed.EmbeddingBatch{Vectors: [][]float32{{}}}, 1)
	require.ErrorContains(t, err, "empty vector")
	require.Empty(t, trimStoredError(nil))
	require.Len(t, []rune(trimStoredError(errors.New(strings.Repeat("x", maxStoredErrorChars+10)))), maxStoredErrorChars)
	require.Equal(t, "abcdef", capRunes("abcdef", 0))
	require.Equal(t, "abc", capRunes("abcdef", 3))
	_, err = DecodeEmbeddingVector([]byte{1, 2, 3})
	require.ErrorContains(t, err, "not a float32 multiple")
}

func TestConcurrentMessageUpsertsShareSingleWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errCh <- s.UpsertMessage(ctx, MessageRecord{
				ID:                stringify(i),
				GuildID:           "g1",
				ChannelID:         "c1",
				MessageType:       0,
				CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
				Content:           "hello",
				NormalizedContent: "hello",
				RawJSON:           `{}`,
			})
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from messages")
	require.NoError(t, err)
	require.Equal(t, "8", rows[0][0])
}

type fakeEmbeddingProvider struct {
	batches []embed.EmbeddingBatch
	err     error
	inputs  [][]string
}

func (f *fakeEmbeddingProvider) Embed(_ context.Context, inputs []string) (embed.EmbeddingBatch, error) {
	copied := append([]string(nil), inputs...)
	f.inputs = append(f.inputs, copied)
	if f.err != nil {
		return embed.EmbeddingBatch{}, f.err
	}
	if len(f.batches) == 0 {
		return embed.EmbeddingBatch{}, nil
	}
	batch := f.batches[0]
	f.batches = f.batches[1:]
	return batch, nil
}

func TestMessageFTSUsesSnowflakeRowID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	record := MessageRecord{
		ID:                "1469950701764350208",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "first body",
		NormalizedContent: "first body",
		RawJSON:           `{"author":{"username":"Peter"}}`,
	}
	require.NoError(t, s.UpsertMessage(ctx, record))

	record.Content = "second body"
	record.NormalizedContent = "second body"
	require.NoError(t, s.UpsertMessage(ctx, record))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*), min(rowid), max(rowid), min(content) from message_fts")
	require.NoError(t, err)
	require.Equal(t, []string{"1", "1469950701764350208", "1469950701764350208", "second body"}, rows[0])
}

func TestMemberFTSUpdatesOnUpsert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	record := MemberRecord{
		GuildID:       "g1",
		UserID:        "u1",
		Username:      "peter",
		DisplayName:   "Peter",
		RoleIDsJSON:   `[]`,
		RawJSON:       `{"bio":"Maintainer","github":"steipete"}`,
		Discriminator: "0",
	}
	require.NoError(t, s.UpsertMember(ctx, record))

	record.RawJSON = `{"bio":"Updated bio","github":"steipete","website":"https://steipete.me"}`
	require.NoError(t, s.UpsertMember(ctx, record))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*), min(profile_text) from member_fts")
	require.NoError(t, err)
	require.Equal(t, []string{"1", "Updated bio steipete https://steipete.me"}, rows[0])
}

func TestOpenRebuildsLegacyFTSRowIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)

	messageID := "1469950701764350208"
	channelID := "c1"
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: channelID, GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                messageID,
		GuildID:           "g1",
		ChannelID:         channelID,
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "panic database is locked",
		NormalizedContent: "panic database is locked",
		RawJSON:           `{"author":{"username":"Peter"}}`,
	}))
	require.NoError(t, s.Close())

	sqlDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `delete from message_fts`)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `
		insert into message_fts(message_id, guild_id, channel_id, author_id, author_name, channel_name, content)
		values(?, ?, ?, ?, ?, ?, ?)
	`, messageID, "g1", channelID, "u1", "Peter", "general", "panic database is locked")
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `delete from sync_state where scope = 'schema:message_fts_rowid_version'`)
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	s, err = Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	_, rows, err := s.ReadOnlyQuery(ctx, "select rowid, message_id from message_fts")
	require.NoError(t, err)
	require.Equal(t, []string{messageID, messageID}, rows[0])

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "panic", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, messageID, results[0].MessageID)
}
