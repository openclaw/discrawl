package report

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPublishedReportExcludesDirectMessagesFromEveryStatistic(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	seedReportActivity(t, s, "guild", "private-thread", "guild-author", now.Add(-60*24*time.Hour), 2)
	opts := Options{Now: now, Published: true}
	want, err := Build(ctx, s, opts)
	require.NoError(t, err)
	require.Equal(t, 2, want.TotalMessages)
	require.Equal(t, "private-thread", want.TopChannels[0].Name)

	seedReportActivity(t, s, store.DirectMessageGuildID, "dm-channel", "dm-author", now, 12)
	got, err := Build(ctx, s, opts)
	require.NoError(t, err)
	require.Equal(t, want, got, "DM activity must not affect even the window anchor or rankings")

	local, err := Build(ctx, s, Options{Now: now})
	require.NoError(t, err)
	explicitFull, err := Build(ctx, s, Options{Now: now, Published: false})
	require.NoError(t, err)
	require.Equal(t, local, explicitFull)
	require.Equal(t, 14, local.TotalMessages)
	require.Equal(t, 2, local.TotalChannels)
	require.Equal(t, 2, local.TotalMembers)
	require.Equal(t, now, local.LatestMessageAt)
	require.Equal(t, "dm-channel", local.TopChannels[0].Name)
	require.Equal(t, "dm-author", local.TopAuthors[0].Name)
	for _, window := range local.Windows {
		require.Equal(t, 12, window.Messages)
		require.Equal(t, 12, window.Attachments)
	}
	require.Equal(t, RankedCount{Name: "2026-09-01", Count: 12}, local.BusiestDays[0])
}

func TestPublishedReportWithOnlyDirectMessagesIsEmpty(t *testing.T) {
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	opts := Options{Now: now, Published: true}
	want, err := Build(t.Context(), s, opts)
	require.NoError(t, err)
	seedReportActivity(t, s, store.DirectMessageGuildID, "dm-channel", "dm-author", now.Add(time.Hour), 5)
	got, err := Build(t.Context(), s, opts)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Zero(t, got.TotalMessages)
	require.True(t, got.LatestMessageAt.IsZero())
	local, err := Build(t.Context(), s, Options{Now: now})
	require.NoError(t, err)
	require.Equal(t, 5, local.TotalMessages)
}

func seedReportActivity(t *testing.T, s *store.Store, guild, channel, author string, at time.Time, count int) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: guild, Name: guild, RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID: channel, GuildID: guild, Name: channel, Kind: "private_thread", RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID: guild, UserID: author, DisplayName: author, RoleIDsJSON: `[]`, RawJSON: `{}`,
	}))
	for i := range count {
		require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
			ID: fmt.Sprintf("%s-%d", channel, i), GuildID: guild, ChannelID: channel,
			AuthorID: author, CreatedAt: at.Format(time.RFC3339Nano),
			Content: "synthetic activity", NormalizedContent: "synthetic activity",
			HasAttachments: true, RawJSON: `{}`,
		}))
	}
}
