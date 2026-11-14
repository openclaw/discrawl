package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A channel ID selects exactly that channel, even when another channel's name
// contains the ID's digits. Name selectors keep exact and partial matching.
func TestChannelIDFilterSelectsOnlyThatChannel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()
	multilingual, err := OpenWithOptions(ctx, filepath.Join(t.TempDir(), "discrawl.db"), OpenOptions{LexicalLanguages: []string{"ar"}})
	require.NoError(t, err)
	defer func() { _ = multilingual.Close() }()
	seedChannelIDFilterFixture(t, ctx, s)
	seedChannelIDFilterFixture(t, ctx, multilingual)

	paths := map[string]func(channel string) []string{
		"fts": func(channel string) []string {
			rows, err := s.SearchMessages(ctx, SearchOptions{Query: "standup", Channel: channel, Limit: 10})
			require.NoError(t, err)
			return searchResultIDs(rows)
		},
		"multilingual": func(channel string) []string {
			rows, err := multilingual.SearchMessages(ctx, SearchOptions{Query: "standup", Channel: channel, Limit: 10})
			require.NoError(t, err)
			return searchResultIDs(rows)
		},
		"fallback": func(channel string) []string {
			rows, err := s.searchFallback(ctx, SearchOptions{Query: "standup", Channel: channel, Limit: 10})
			require.NoError(t, err)
			return searchResultIDs(rows)
		},
		"semantic": func(channel string) []string {
			rows, err := s.SearchMessagesSemantic(ctx, SemanticSearchOptions{QueryVector: []float32{1, 0}, Provider: "test", Model: "test", InputVersion: EmbeddingInputVersion, Dimensions: 2, VectorBackend: "exact", Channel: channel, Limit: 10})
			require.NoError(t, err)
			return searchResultIDs(rows)
		},
		"messages": func(channel string) []string {
			rows, err := s.ListMessages(ctx, MessageListOptions{Channel: channel, Limit: 10})
			require.NoError(t, err)
			return messageRowIDs(rows)
		},
		"attachments": func(channel string) []string {
			rows, err := s.ListAttachments(ctx, AttachmentListOptions{Channel: channel, Limit: 10})
			require.NoError(t, err)
			ids := []string{}
			for _, row := range rows {
				ids = append(ids, row.MessageID)
			}
			return ids
		},
		"mentions": func(channel string) []string {
			rows, err := s.ListMentions(ctx, MentionListOptions{Channel: channel, Limit: 10})
			require.NoError(t, err)
			ids := []string{}
			for _, row := range rows {
				ids = append(ids, row.MessageID)
			}
			return ids
		},
	}
	for name, query := range paths {
		t.Run(name, func(t *testing.T) {
			require.ElementsMatch(t, []string{"m-general"}, query(channelIDFilterGeneral), "channel ID selector")
			require.ElementsMatch(t, []string{"m-notes"}, query(channelIDFilterNotes), "other channel ID selector")
			require.ElementsMatch(t, []string{"m-general"}, query("general"), "exact name selector")
			require.ElementsMatch(t, []string{"m-notes"}, query("notes"), "partial name selector")
		})
	}
}

const channelIDFilterGeneral, channelIDFilterNotes = "700000000000000042", "700000000000000043"

func seedChannelIDFilterFixture(t *testing.T, ctx context.Context, s *Store) {
	t.Helper()
	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	var mutations []MessageMutation
	for _, c := range []struct{ id, name, message string }{
		{channelIDFilterGeneral, "general", "m-general"},
		{channelIDFilterNotes, "notes-" + channelIDFilterGeneral, "m-notes"},
	} {
		require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: c.id, GuildID: "g1", Kind: "text", Name: c.name, RawJSON: `{}`}))
		mutations = append(mutations, MessageMutation{
			Record: MessageRecord{
				ID: c.message, GuildID: "g1", ChannelID: c.id, ChannelName: c.name, AuthorID: "u1", AuthorName: "Peter",
				CreatedAt: createdAt, Content: "standup summary", NormalizedContent: "standup summary", HasAttachments: true,
				RawJSON: `{"author":{"username":"peter"}}`,
			},
			Attachments: []AttachmentRecord{{AttachmentID: "a-" + c.message, MessageID: c.message, GuildID: "g1", ChannelID: c.id, AuthorID: "u1", Filename: "notes.txt", URL: "https://example.test/notes.txt"}},
			Mentions:    []MentionEventRecord{{MessageID: c.message, GuildID: "g1", ChannelID: c.id, AuthorID: "u1", TargetType: "user", TargetID: "u2", TargetName: "Shadow", EventAt: createdAt}},
		})
	}
	require.NoError(t, s.UpsertMessages(ctx, mutations))
	blob, err := EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	for _, id := range []string{"m-general", "m-notes"} {
		_, err := s.DB().ExecContext(ctx, `insert into message_embeddings(message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at) values(?, 'test', 'test', ?, 2, ?, ?)`, id, EmbeddingInputVersion, blob, createdAt)
		require.NoError(t, err)
	}
}
