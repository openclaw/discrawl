package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

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

func TestAttachmentMediaUpdatesAndFilters(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, seedAttachmentForGuild(ctx, s, "g1", "c1", "m1", "a1"))
	require.NoError(t, seedAttachmentForGuild(ctx, s, "g1", "c1", "m2", "a2"))
	require.NoError(t, s.UpdateAttachmentMedia(ctx, AttachmentMediaUpdate{
		AttachmentID:  "a1",
		MediaPath:     "attachments/aa/hash-file.png",
		ContentSHA256: "hash",
		ContentSize:   4,
		FetchedAt:     "2026-05-15T12:05:00Z",
		FetchStatus:   "fetched",
	}))
	require.NoError(t, s.UpdateAttachmentFetchStatus(ctx, "a2", "2026-05-15T12:06:00Z", "failed", "boom"))
	require.NoError(t, seedAttachmentForGuild(ctx, s, "g1", "c1", "m1", "a1"))

	rows, err := s.ListAttachments(ctx, AttachmentListOptions{MissingOnly: true})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "a2", rows[0].AttachmentID)
	require.Equal(t, "failed", rows[0].FetchStatus)
	require.Equal(t, "boom", rows[0].FetchError)

	rows, err = s.ListAttachments(ctx, AttachmentListOptions{
		GuildIDs:    []string{"g1"},
		ChannelIDs:  []string{"c1"},
		Channel:     "#c1",
		Author:      "Peter",
		Filename:    "file",
		ContentType: "image/",
		Since:       time.Date(2026, 5, 15, 11, 0, 0, 0, time.UTC),
		Before:      time.Date(2026, 5, 15, 13, 0, 0, 0, time.UTC),
		Limit:       1,
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "a1", rows[0].AttachmentID)
	require.Equal(t, "attachments/aa/hash-file.png", rows[0].MediaPath)
	require.Equal(t, "hash", rows[0].ContentSHA256)
	require.Equal(t, int64(4), rows[0].ContentSize)
	require.Equal(t, "fetched", rows[0].FetchStatus)
	require.False(t, rows[0].FetchedAt.IsZero())

	rows, err = s.ListAttachments(ctx, AttachmentListOptions{MessageID: "missing"})
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestAttachmentUpsertMovesDuplicateIDAndKeepsMedia(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, seedAttachmentForGuild(ctx, s, "g1", "c1", "m1", "a1"))
	require.NoError(t, s.UpdateAttachmentMedia(ctx, AttachmentMediaUpdate{
		AttachmentID:  "a1",
		MediaPath:     "attachments/aa/hash-file.png",
		ContentSHA256: "hash",
		ContentSize:   4,
		FetchedAt:     "2026-05-15T12:05:00Z",
		FetchStatus:   "fetched",
	}))
	require.NoError(t, seedAttachmentForGuild(ctx, s, "g1", "c2", "m2", "a1"))

	rows, err := s.ListAttachments(ctx, AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Empty(t, rows)

	rows, err = s.ListAttachments(ctx, AttachmentListOptions{MessageID: "m2"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "a1", rows[0].AttachmentID)
	require.Equal(t, "m2", rows[0].MessageID)
	require.Equal(t, "c2", rows[0].ChannelID)
	require.Equal(t, "attachments/aa/hash-file.png", rows[0].MediaPath)
	require.Equal(t, "hash", rows[0].ContentSHA256)
	require.Equal(t, int64(4), rows[0].ContentSize)
	require.Equal(t, "fetched", rows[0].FetchStatus)
}

func seedAttachmentForGuild(ctx context.Context, s *Store, guildID, channelID, messageID, attachmentID string) error {
	if err := s.UpsertGuild(ctx, GuildRecord{ID: guildID, Name: guildID, RawJSON: `{}`}); err != nil {
		return err
	}
	if err := s.UpsertChannel(ctx, ChannelRecord{ID: channelID, GuildID: guildID, Kind: "text", Name: channelID, RawJSON: `{}`}); err != nil {
		return err
	}
	if err := s.UpsertMember(ctx, MemberRecord{GuildID: guildID, UserID: "u1", Username: "peter", DisplayName: "Peter", RoleIDsJSON: `[]`, RawJSON: `{}`}); err != nil {
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
