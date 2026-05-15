package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpandAttachmentChannelIDsIncludesForumThreads(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "forum", GuildID: "g1", Kind: "forum", Name: "ideas", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "thread", GuildID: "g1", Kind: "thread_public", Name: "launch", ThreadParentID: "forum", RawJSON: `{}`}))

	ids, err := s.ExpandAttachmentChannelIDs(ctx, []string{"forum"})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"forum", "thread"}, ids)
}

func TestListAttachmentsCanExcludeGuilds(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, seedAttachmentForGuild(ctx, s, "g1", "c1", "m1", "a1"))
	require.NoError(t, seedAttachmentForGuild(ctx, s, DirectMessageGuildID, "dm1", "m2", "a2"))

	rows, err := s.ListAttachments(ctx, AttachmentListOptions{ExcludeGuildIDs: []string{DirectMessageGuildID}})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "a1", rows[0].AttachmentID)
}

func seedAttachmentForGuild(ctx context.Context, s *Store, guildID, channelID, messageID, attachmentID string) error {
	if err := s.UpsertGuild(ctx, GuildRecord{ID: guildID, Name: guildID, RawJSON: `{}`}); err != nil {
		return err
	}
	if err := s.UpsertChannel(ctx, ChannelRecord{ID: channelID, GuildID: guildID, Kind: "text", Name: channelID, RawJSON: `{}`}); err != nil {
		return err
	}
	return s.UpsertMessages(ctx, []MessageMutation{{
		Record: MessageRecord{
			ID:                messageID,
			GuildID:           guildID,
			ChannelID:         channelID,
			ChannelName:       channelID,
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         "2026-05-15T12:00:00Z",
			Content:           "attached",
			NormalizedContent: "attached file.png",
			HasAttachments:    true,
			RawJSON:           `{}`,
		},
		Attachments: []AttachmentRecord{{
			AttachmentID: attachmentID,
			MessageID:    messageID,
			GuildID:      guildID,
			ChannelID:    channelID,
			AuthorID:     "u1",
			Filename:     "file.png",
			ContentType:  "image/png",
			Size:         7,
			URL:          "https://example.test/file.png",
		}},
	}})
}
