package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDirectMessageConversations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	base := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "dm1", GuildID: DirectMessageGuildID, Kind: "dm", Name: "Vincent K", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "dm2", GuildID: DirectMessageGuildID, Kind: "dm", Name: "Alice", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "dm-empty", GuildID: DirectMessageGuildID, Kind: "dm", Name: "Empty", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	for _, message := range []MessageRecord{
		{
			ID:                "m1",
			GuildID:           DirectMessageGuildID,
			ChannelID:         "dm1",
			ChannelName:       "Vincent K",
			AuthorID:          "u1",
			AuthorName:        "Vincent K",
			CreatedAt:         base.Format(time.RFC3339Nano),
			Content:           "hello",
			NormalizedContent: "hello",
			RawJSON:           `{"author":{"username":"vincentkoc","global_name":"Vincent K"}}`,
		},
		{
			ID:                "m2",
			GuildID:           DirectMessageGuildID,
			ChannelID:         "dm1",
			ChannelName:       "Vincent K",
			AuthorID:          "self",
			AuthorName:        "Peter",
			CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
			Content:           "reply",
			NormalizedContent: "reply",
			RawJSON:           `{"author":{"username":"steipete","global_name":"Peter"}}`,
		},
		{
			ID:                "m3",
			GuildID:           DirectMessageGuildID,
			ChannelID:         "dm2",
			ChannelName:       "Alice",
			AuthorID:          "u2",
			AuthorName:        "Alice",
			CreatedAt:         base.Add(2 * time.Minute).Format(time.RFC3339Nano),
			Content:           "newer",
			NormalizedContent: "newer",
			RawJSON:           `{"author":{"username":"alice","global_name":"Alice Example"}}`,
		},
		{
			ID:                "m4",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u2",
			AuthorName:        "Alice",
			CreatedAt:         base.Add(3 * time.Minute).Format(time.RFC3339Nano),
			Content:           "guild message",
			NormalizedContent: "guild message",
			RawJSON:           `{"author":{"username":"alice"}}`,
		},
	} {
		require.NoError(t, s.UpsertMessage(ctx, message))
	}

	rows, err := s.DirectMessageConversations(ctx, DirectMessageConversationOptions{})
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Equal(t, "dm2", rows[0].ChannelID)
	require.Equal(t, "Alice", rows[0].Name)
	require.Equal(t, 1, rows[0].MessageCount)
	require.Equal(t, 1, rows[0].AuthorCount)
	require.Equal(t, base.Add(2*time.Minute), rows[0].FirstMessageAt)
	require.Equal(t, base.Add(2*time.Minute), rows[0].LastMessageAt)
	require.Equal(t, "dm1", rows[1].ChannelID)
	require.Equal(t, 2, rows[1].MessageCount)
	require.Equal(t, 2, rows[1].AuthorCount)
	require.Equal(t, "dm-empty", rows[2].ChannelID)
	require.Zero(t, rows[2].MessageCount)
	require.True(t, rows[2].FirstMessageAt.IsZero())
	require.True(t, rows[2].LastMessageAt.IsZero())

	rows, err = s.DirectMessageConversations(ctx, DirectMessageConversationOptions{With: "vincent"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "dm1", rows[0].ChannelID)

	rows, err = s.DirectMessageConversations(ctx, DirectMessageConversationOptions{With: "Alice Example"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "dm2", rows[0].ChannelID)

	rows, err = s.DirectMessageConversations(ctx, DirectMessageConversationOptions{With: "u1"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "dm1", rows[0].ChannelID)

	rows, err = s.DirectMessageConversations(ctx, DirectMessageConversationOptions{Limit: 1})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "dm2", rows[0].ChannelID)
}

func TestDeleteGuildDataAndOrphanChannels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	now := time.Date(2026, 4, 24, 13, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Delete Me", RawJSON: `{}`}))
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g2", Name: "Keep Me", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g2", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{GuildID: "g1", UserID: "u1", Username: "gone", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{GuildID: "g2", UserID: "u2", Username: "kept", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{
		{
			Record: MessageRecord{
				ID:                "m1",
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "general",
				AuthorID:          "u1",
				AuthorName:        "Gone",
				CreatedAt:         now,
				Content:           "remove me",
				NormalizedContent: "remove me",
				HasAttachments:    true,
				RawJSON:           `{"author":{"username":"gone"}}`,
			},
			EventType:   "wiretap",
			PayloadJSON: `{}`,
			Options:     WriteOptions{AppendEvent: true, EnqueueEmbedding: true},
			Attachments: []AttachmentRecord{{
				AttachmentID: "a1",
				MessageID:    "m1",
				GuildID:      "g1",
				ChannelID:    "c1",
				AuthorID:     "u1",
				Filename:     "gone.txt",
			}},
			Mentions: []MentionEventRecord{{
				MessageID:  "m1",
				GuildID:    "g1",
				ChannelID:  "c1",
				AuthorID:   "u1",
				TargetType: "user",
				TargetID:   "u2",
				TargetName: "Kept",
				EventAt:    now,
			}},
		},
		{
			Record: MessageRecord{
				ID:                "m2",
				GuildID:           "g2",
				ChannelID:         "c2",
				ChannelName:       "general",
				AuthorID:          "u2",
				AuthorName:        "Kept",
				CreatedAt:         now,
				Content:           "keep me",
				NormalizedContent: "keep me",
				RawJSON:           `{"author":{"username":"kept"}}`,
			},
		},
	}))

	require.NoError(t, s.DeleteGuildData(ctx, "g1"))
	for _, query := range []string{
		"select count(*) from guilds where id = 'g1'",
		"select count(*) from channels where guild_id = 'g1'",
		"select count(*) from members where guild_id = 'g1'",
		"select count(*) from messages where guild_id = 'g1'",
		"select count(*) from message_fts where guild_id = 'g1'",
		"select count(*) from message_events where guild_id = 'g1'",
		"select count(*) from message_attachments where guild_id = 'g1'",
		"select count(*) from mention_events where guild_id = 'g1'",
		"select count(*) from embedding_jobs where message_id = 'm1'",
	} {
		_, rows, err := s.ReadOnlyQuery(ctx, query)
		require.NoError(t, err, query)
		require.Equal(t, "0", rows[0][0], query)
	}
	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from messages where guild_id = 'g2'")
	require.NoError(t, err)
	require.Equal(t, "1", rows[0][0])

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "dm-keep", GuildID: DirectMessageGuildID, Kind: "dm", Name: "Keep", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "dm-orphan", GuildID: DirectMessageGuildID, Kind: "dm", Name: "Remove", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "dm-m1",
		GuildID:           DirectMessageGuildID,
		ChannelID:         "dm-keep",
		ChannelName:       "Keep",
		AuthorID:          "u3",
		AuthorName:        "Keep",
		CreatedAt:         now,
		Content:           "keep dm",
		NormalizedContent: "keep dm",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.DeleteOrphanChannels(ctx, DirectMessageGuildID))
	_, rows, err = s.ReadOnlyQuery(ctx, "select id from channels where guild_id = '@me' order by id")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"dm-keep"}}, rows)
}
