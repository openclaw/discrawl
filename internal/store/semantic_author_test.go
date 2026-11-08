package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Discord payloads carry "global_name": "" for users without a display name.
// Semantic --author must still match their username.
func TestSemanticSearchAuthorMatchesUsernameWithoutDisplayName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{{Record: MessageRecord{
		ID: "m-alice", GuildID: "g1", ChannelID: "c1", AuthorID: "u-alice", AuthorName: "alice", CreatedAt: createdAt,
		Content: "release checklist", NormalizedContent: "release checklist",
		RawJSON: `{"author":{"id":"u-alice","username":"alice","global_name":""}}`,
	}}}))
	blob, err := EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `insert into message_embeddings(message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at) values('m-alice', 'test', 'test', ?, 2, ?, ?)`, EmbeddingInputVersion, blob, createdAt)
	require.NoError(t, err)

	rows, err := s.SearchMessagesSemantic(ctx, SemanticSearchOptions{QueryVector: []float32{1, 0}, Provider: "test", Model: "test", InputVersion: EmbeddingInputVersion, Dimensions: 2, VectorBackend: "exact", Author: "alice", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"m-alice"}, searchResultIDs(rows))
	require.Equal(t, "alice", rows[0].AuthorName)
}
