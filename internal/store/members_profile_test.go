package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMembersSearchesArchivedProfileText(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.MergeMembers(ctx, "g1", []MemberRecord{
		{
			GuildID:     "g1",
			UserID:      "u1",
			Username:    "peter",
			DisplayName: "Peter",
			RoleIDsJSON: `[]`,
			RawJSON:     `{"bio":"Platform engineer","links":["https://github.com/steipete","https://x.com/steipete","https://steipete.me"]}`,
		},
		{
			GuildID:     "g1",
			UserID:      "u2",
			Username:    "other",
			DisplayName: "Other",
			RoleIDsJSON: `[]`,
			RawJSON:     `{"bio":"Designer"}`,
		},
	}))

	rows, err := s.Members(ctx, "g1", "Platform", 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "u1", rows[0].UserID)

	rows, err = s.Members(ctx, "g1", "steipete.me", 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "steipete", rows[0].GitHubLogin)
	require.Equal(t, "steipete", rows[0].XHandle)
}

func TestMemberProfileOrdersRecentMessagesNewestFirst(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"bio":"Maintainer"}`,
	}))

	for i, id := range []string{"m1", "m2", "m3"} {
		require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
			ID:                id,
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         time.Date(2026, 3, 8, 12, 0, i, 0, time.UTC).Format(time.RFC3339Nano),
			Content:           id,
			NormalizedContent: id,
			RawJSON:           `{}`,
		}))
	}

	profile, err := s.MemberProfile(ctx, "g1", "u1", 2)
	require.NoError(t, err)
	require.Len(t, profile.RecentMessages, 2)
	require.Equal(t, "m3", profile.RecentMessages[0].MessageID)
	require.Equal(t, "m2", profile.RecentMessages[1].MessageID)
	require.Equal(t, "Maintainer", profile.Member.Bio)
}
