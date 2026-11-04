package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAttachmentTextAndMentionsAreQueryable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "maintainers", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}))

	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{{
		Record: MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "maintainers",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         createdAt,
			Content:           "",
			NormalizedContent: "trace.txt stack trace line one stack trace line two",
			HasAttachments:    true,
			RawJSON:           `{"author":{"username":"peter"}}`,
		},
		Attachments: []AttachmentRecord{{
			AttachmentID: "a1",
			MessageID:    "m1",
			GuildID:      "g1",
			ChannelID:    "c1",
			AuthorID:     "u1",
			Filename:     "trace.txt",
			ContentType:  "text/plain",
			URL:          "https://example.test/trace.txt",
			TextContent:  "stack trace line one stack trace line two",
		}},
		Mentions: []MentionEventRecord{
			{
				MessageID:  "m1",
				GuildID:    "g1",
				ChannelID:  "c1",
				AuthorID:   "u1",
				TargetType: "user",
				TargetID:   "u2",
				TargetName: "Shadow",
				EventAt:    createdAt,
			},
			{
				MessageID:  "m1",
				GuildID:    "g1",
				ChannelID:  "c1",
				AuthorID:   "u1",
				TargetType: "role",
				TargetID:   "r1",
				TargetName: "Maintainer",
				EventAt:    createdAt,
			},
		},
	}}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select filename, text_content from message_attachments")
	require.NoError(t, err)
	require.Equal(t, []string{"trace.txt", "stack trace line one stack trace line two"}, rows[0])

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "stack", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m1", results[0].MessageID)
	require.Contains(t, results[0].Content, "stack trace")

	messages, err := s.ListMessages(ctx, MessageListOptions{Channel: "maintainers", Limit: 10})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Contains(t, messages[0].Content, "stack trace")
	require.Equal(t, "trace.txt", messages[0].AttachmentNames)
	require.Contains(t, messages[0].AttachmentText, "stack trace line one")

	mentions, err := s.ListMentions(ctx, MentionListOptions{Target: "Shadow", Limit: 10})
	require.NoError(t, err)
	require.Len(t, mentions, 1)
	require.Equal(t, "u2", mentions[0].TargetID)
	require.Equal(t, "Shadow", mentions[0].TargetName)
	require.Contains(t, mentions[0].Content, "stack trace")

	roleMentions, err := s.ListMentions(ctx, MentionListOptions{TargetType: "role", Limit: 10})
	require.NoError(t, err)
	require.Len(t, roleMentions, 1)
	require.Equal(t, "r1", roleMentions[0].TargetID)

	filtered, err := s.ListMentions(ctx, MentionListOptions{
		Channel:    "#maint",
		Author:     "pet",
		TargetType: "user",
		Since:      parseTime(createdAt).Add(-time.Minute),
		Before:     parseTime(createdAt).Add(time.Minute),
		Limit:      10,
	})
	require.NoError(t, err)
	require.Len(t, filtered, 1)
}

func TestListMessagesResolvesMentionNamesForDisplay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "maintainers", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g1",
		UserID:      "u4",
		Username:    "fallback",
		DisplayName: "Fallback User",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}))

	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	rawContent := "ping <@u2> <@!u3> <@&r1> in <#c1>"
	fallbackContent := "ask <@u4> in <#c1>"
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{
		{
			Record: MessageRecord{
				ID:                "m1",
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "maintainers",
				AuthorID:          "u1",
				AuthorName:        "Peter",
				MessageType:       0,
				CreatedAt:         createdAt,
				Content:           rawContent,
				NormalizedContent: rawContent,
				RawJSON:           `{}`,
			},
			Mentions: []MentionEventRecord{
				{MessageID: "m1", GuildID: "g1", ChannelID: "c1", AuthorID: "u1", TargetType: "user", TargetID: "u2", TargetName: "Shadow", EventAt: createdAt},
				{MessageID: "m1", GuildID: "g1", ChannelID: "c1", AuthorID: "u1", TargetType: "user", TargetID: "u3", TargetName: "Vincent", EventAt: createdAt},
				{MessageID: "m1", GuildID: "g1", ChannelID: "c1", AuthorID: "u1", TargetType: "role", TargetID: "r1", TargetName: "Maintainers", EventAt: createdAt},
				{MessageID: "m1", GuildID: "g1", ChannelID: "c1", AuthorID: "u1", TargetType: "channel", TargetID: "c1", TargetName: "maintainers", EventAt: createdAt},
			},
		},
		{
			Record: MessageRecord{
				ID:                "m2",
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "maintainers",
				AuthorID:          "u1",
				AuthorName:        "Peter",
				MessageType:       0,
				CreatedAt:         createdAt,
				Content:           fallbackContent,
				NormalizedContent: fallbackContent,
				RawJSON:           `{}`,
			},
		},
	}))

	messages, err := s.ListMessages(ctx, MessageListOptions{Channel: "maintainers", Limit: 10})
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, rawContent, messages[0].Content)
	require.Equal(t, "ping @Shadow @Vincent @Maintainers in #maintainers", messages[0].DisplayContent)
	require.Equal(t, fallbackContent, messages[1].Content)
	require.Equal(t, "ask @Fallback User in #maintainers", messages[1].DisplayContent)
}
