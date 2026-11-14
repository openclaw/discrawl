package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The LIKE fallback (used when FTS is unavailable) must restrict --author to
// the message author, not to any message whose payload mentions that name.
func TestSearchFallbackAuthorMatchesOnlyTheAuthor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{
		{Record: MessageRecord{
			ID: "m-alice", GuildID: "g1", ChannelID: "c1", AuthorID: "u-alice", AuthorName: "alice", CreatedAt: createdAt,
			Content: "release checklist", NormalizedContent: "release checklist",
			RawJSON: `{"author":{"id":"u-alice","username":"alice","global_name":""}}`,
		}},
		{Record: MessageRecord{
			ID: "m-bob", GuildID: "g1", ChannelID: "c1", AuthorID: "u-bob", AuthorName: "bob", CreatedAt: createdAt,
			Content: "release checklist for alice", NormalizedContent: "release checklist for alice",
			RawJSON: `{"author":{"id":"u-bob","username":"bob","global_name":""},"mentions":[{"id":"u-alice","username":"alice","global_name":""}]}`,
		}},
	}))

	for _, author := range []string{"alice", "u-alice"} {
		rows, err := s.searchFallback(ctx, SearchOptions{Query: "release", Author: author, Limit: 10})
		require.NoError(t, err)
		require.Equal(t, []string{"m-alice"}, searchResultIDs(rows), author)
	}
	rows, err := s.SearchMessages(ctx, SearchOptions{Query: "release", Author: "alice", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"m-alice"}, searchResultIDs(rows), "FTS control")
}

func TestSearchFallbackMatchesEachAuthorName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()
	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{{Record: MessageRecord{
		ID: "m-alice", GuildID: "g1", ChannelID: "c1", AuthorID: "u-alice", AuthorName: "Release Captain", CreatedAt: createdAt,
		Content: "release checklist", NormalizedContent: "release checklist",
		RawJSON: `{"author":{"id":"u-alice","username":"alice","global_name":"Global Alice"},"member":{"nick":"Release Captain"}}`,
	}}}))
	for _, author := range []string{"u-alice", "alice", "Global Alice", "Release Captain"} {
		rows, err := s.searchFallback(ctx, SearchOptions{Query: "release", Author: author, Limit: 10})
		require.NoError(t, err)
		require.Equal(t, []string{"m-alice"}, searchResultIDs(rows), author)
	}
}
