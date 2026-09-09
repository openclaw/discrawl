package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/embed"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingAdmissionKeepsDirectMessagesLocal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, row := range []struct{ id, guild, content string }{
		{"1", "@me", "local only"},
		{"2", "@me", "never queued"},
		{"3", "guild", "eligible guild text"},
	} {
		require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
			ID: row.id, GuildID: row.guild, ChannelID: row.guild,
			Content: row.content, NormalizedContent: row.content,
			CreatedAt: "2026-01-01T00:00:00Z", RawJSON: `{}`,
		}, WriteOptions{EnqueueEmbedding: true}))
	}
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from embedding_jobs where message_id in ('1', '2')`).Scan(&count))
	require.Zero(t, count)

	// Preserve an older job/vector from a prior version, without transmitting it.
	_, err = s.DB().ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, provider, model, input_version, updated_at)
		values('1', 'pending', 'old', 'old', 'old', '2000-01-01T00:00:00Z');
		insert into message_embeddings(message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at)
		values('1', 'old', 'old', 'old', 1, x'0000803f', '2000-01-01T00:00:00Z');
	`)
	require.NoError(t, err)
	opts := EmbeddingDrainOptions{
		Provider: "remote", Model: "synthetic", Limit: 1, BatchSize: 1,
		Now: func() time.Time { return time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC) },
	}
	requeued, err := s.RequeueAllEmbeddingJobs(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, 1, requeued)
	provider := &fakeEmbeddingProvider{batches: []embed.EmbeddingBatch{{Vectors: [][]float32{{1}}}}}
	stats, err := s.DrainEmbeddingJobs(ctx, provider, opts)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"eligible guild text"}}, provider.inputs)
	require.Equal(t, 1, stats.Succeeded)
	require.Zero(t, stats.RemainingBacklog)

	var state, identity, content string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select state, provider from embedding_jobs where message_id = '1'`).Scan(&state, &identity))
	require.Equal(t, "pending", state)
	require.Equal(t, "old", identity)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content from messages where id = '1'`).Scan(&content))
	require.Equal(t, "local only", content)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_embeddings where message_id = '1'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from embedding_jobs where message_id = '2'`).Scan(&count))
	require.Zero(t, count)
}
