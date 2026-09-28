package store

import (
	"context"
	"math"
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BenchmarkSearchMessagesSemanticExact includes SQLite scanning, vector decoding,
// scoring, ranking and result hydration; fixture creation is outside the timer.
func BenchmarkSearchMessagesSemanticExact(b *testing.B) {
	const (
		candidates = 10_000
		dimensions = 1536
		limit      = 20
	)
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(b.TempDir(), "discrawl.db"))
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, s.Close()) })

	require.NoError(b, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(b, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	base := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	messages := make([]MessageMutation, candidates)
	for i := range messages {
		content := "Archived Discord message " + strconv.Itoa(i)
		messages[i].Record = MessageRecord{
			ID:                "m" + strconv.Itoa(i),
			GuildID:           "g1",
			ChannelID:         "c1",
			AuthorID:          "u1",
			CreatedAt:         base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			Content:           content,
			NormalizedContent: content,
			RawJSON:           `{"author":{"username":"benchmark"}}`,
		}
	}
	require.NoError(b, s.UpsertMessages(ctx, messages))

	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(b, err)
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values(?, ?, ?, ?, ?, ?, ?)
	`)
	require.NoError(b, err)
	defer func() { _ = stmt.Close() }()

	rng := rand.New(rand.NewPCG(1, 2))
	var query []float32
	for _, message := range messages {
		values := make([]float32, dimensions)
		var squaredNorm float64
		for j := range values {
			values[j] = float32(rng.NormFloat64())
			squaredNorm += float64(values[j]) * float64(values[j])
		}
		norm := math.Sqrt(squaredNorm)
		for j := range values {
			values[j] = float32(float64(values[j]) / norm)
		}
		if query == nil {
			query = values
		}
		blob, err := EncodeEmbeddingVector(values)
		require.NoError(b, err)
		_, err = stmt.ExecContext(ctx, message.Record.ID, "benchmark", "dense-1536", EmbeddingInputVersion, dimensions, blob, base.Format(timeLayout))
		require.NoError(b, err)
	}
	require.NoError(b, tx.Commit())

	opts := SemanticSearchOptions{
		QueryVector:   query,
		Provider:      "benchmark",
		Model:         "dense-1536",
		InputVersion:  EmbeddingInputVersion,
		Dimensions:    dimensions,
		VectorBackend: "exact",
		GuildIDs:      []string{"g1"},
		Limit:         limit,
	}
	results, err := s.SearchMessagesSemantic(ctx, opts)
	require.NoError(b, err)
	require.Len(b, results, limit)
	require.Equal(b, "m0", results[0].MessageID)

	b.ReportAllocs()
	for b.Loop() {
		results, err = s.SearchMessagesSemantic(ctx, opts)
		if err != nil {
			b.Fatal(err)
		}
		if len(results) != limit || results[0].MessageID != "m0" {
			b.Fatal("unexpected semantic search results")
		}
	}
}
