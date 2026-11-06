package report

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestBuildQuiet(t *testing.T) {
	ctx := context.Background()
	s, now := seedQuietStore(t, ctx)
	defer func() { _ = s.Close() }()

	t.Run("happy path mixed active and silent channels", func(t *testing.T) {
		quiet, err := BuildQuiet(ctx, s, QuietOptions{Now: now, Since: 30 * 24 * time.Hour})
		require.NoError(t, err)

		require.Equal(t, now, quiet.GeneratedAt)
		require.Equal(t, now.Add(-30*24*time.Hour), quiet.Since)
		require.Equal(t, now, quiet.Until)
		require.Equal(t, 3, quiet.Totals.Channels)
		require.Len(t, quiet.Channels, 3)

		ids := []string{quiet.Channels[0].ChannelID, quiet.Channels[1].ChannelID, quiet.Channels[2].ChannelID}
		require.Equal(t, []string{"c0", "c2", "c9"}, ids)
		require.Empty(t, quiet.Channels[0].LastMessage)
		require.Equal(t, -1, quiet.Channels[0].DaysSilent)
		require.Equal(t, 45, quiet.Channels[1].DaysSilent)
		require.Equal(t, 40, quiet.Channels[2].DaysSilent)
	})

	t.Run("guild filter", func(t *testing.T) {
		quiet, err := BuildQuiet(ctx, s, QuietOptions{Now: now, Since: 30 * 24 * time.Hour, GuildID: "g1"})
		require.NoError(t, err)
		require.Len(t, quiet.Channels, 2)
		for _, row := range quiet.Channels {
			require.Equal(t, "g1", row.GuildID)
		}
	})

	t.Run("negative since is normalized", func(t *testing.T) {
		quiet, err := BuildQuiet(ctx, s, QuietOptions{Now: now, Since: -30 * 24 * time.Hour})
		require.NoError(t, err)
		require.Equal(t, now.Add(-30*24*time.Hour), quiet.Since)
	})

	t.Run("default now and since defaults", func(t *testing.T) {
		quiet, err := BuildQuiet(ctx, s, QuietOptions{})
		require.NoError(t, err)
		require.WithinDuration(t, quiet.GeneratedAt.Add(-30*24*time.Hour), quiet.Since, 2*time.Second)
		require.WithinDuration(t, quiet.GeneratedAt, quiet.Until, 2*time.Second)
	})

	t.Run("future messages do not hide quiet channels", func(t *testing.T) {
		oldMessageAt := now.Add(-45 * 24 * time.Hour)
		require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c8", GuildID: "g1", Kind: "text", Name: "future-skew", RawJSON: `{}`}))
		require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{
			{Record: store.MessageRecord{ID: "m8-old", GuildID: "g1", ChannelID: "c8", ChannelName: "future-skew", AuthorID: "u1", AuthorName: "u1", CreatedAt: oldMessageAt.Format(time.RFC3339Nano), Content: "stale", NormalizedContent: "stale", RawJSON: `{}`}},
			{Record: store.MessageRecord{ID: "m8-future", GuildID: "g1", ChannelID: "c8", ChannelName: "future-skew", AuthorID: "u1", AuthorName: "u1", CreatedAt: now.Add(24 * time.Hour).Format(time.RFC3339Nano), Content: "future", NormalizedContent: "future", RawJSON: `{}`}},
		}))

		quiet, err := BuildQuiet(ctx, s, QuietOptions{Now: now, Since: 30 * 24 * time.Hour, GuildID: "g1"})
		require.NoError(t, err)

		var skewed *QuietChannel
		for i := range quiet.Channels {
			if quiet.Channels[i].ChannelID == "c8" {
				skewed = &quiet.Channels[i]
				break
			}
		}
		require.NotNil(t, skewed)
		require.Equal(t, oldMessageAt.Format(time.RFC3339), skewed.LastMessage)
		require.Equal(t, 45, skewed.DaysSilent)
	})
}

func seedQuietStore(t *testing.T, ctx context.Context) (*store.Store, time.Time) {
	t.Helper()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)

	now := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild 1", RawJSON: `{}`}))
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g2", Name: "Guild 2", RawJSON: `{}`}))

	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c0", GuildID: "g1", Kind: "text", Name: "never", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "recent", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "stale-45", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c3", GuildID: "g1", Kind: "thread_public", Name: "stale-35", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c5", GuildID: "g1", Kind: "category", Name: "structural", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c6", GuildID: "g1", Kind: "voice", Name: "lobby", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c9", GuildID: "g2", Kind: "text", Name: "other-guild-stale", RawJSON: `{}`}))

	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{
		{Record: store.MessageRecord{ID: "m1", GuildID: "g1", ChannelID: "c1", ChannelName: "recent", AuthorID: "u1", AuthorName: "u1", CreatedAt: now.Add(-2 * 24 * time.Hour).Format(time.RFC3339Nano), Content: "recent", NormalizedContent: "recent", RawJSON: `{}`}},
		{Record: store.MessageRecord{ID: "m2", GuildID: "g1", ChannelID: "c2", ChannelName: "stale-45", AuthorID: "u1", AuthorName: "u1", CreatedAt: now.Add(-45 * 24 * time.Hour).Format(time.RFC3339Nano), Content: "stale", NormalizedContent: "stale", RawJSON: `{}`}},
		{Record: store.MessageRecord{ID: "m3", GuildID: "g1", ChannelID: "c3", ChannelName: "stale-35", AuthorID: "u1", AuthorName: "u1", CreatedAt: now.Add(-35 * 24 * time.Hour).Format(time.RFC3339Nano), Content: "stale", NormalizedContent: "stale", RawJSON: `{}`}},
		{Record: store.MessageRecord{ID: "m4", GuildID: "g2", ChannelID: "c9", ChannelName: "other-guild-stale", AuthorID: "u1", AuthorName: "u1", CreatedAt: now.Add(-40 * 24 * time.Hour).Format(time.RFC3339Nano), Content: "stale", NormalizedContent: "stale", RawJSON: `{}`}},
	}))

	return s, now
}
