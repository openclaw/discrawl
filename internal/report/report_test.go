package report

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestBuildRenderAndUpdateReadme(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Date(2026, 4, 21, 5, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u1", Username: "peter", DisplayName: "Peter", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now.Add(-time.Hour).Format(time.RFC3339Nano),
			Content:           "shipping the snapshot report",
			NormalizedContent: "shipping the snapshot report",
			RawJSON:           `{}`,
		},
	}}))

	activity, err := Build(ctx, s, Options{Now: now})
	require.NoError(t, err)
	require.Equal(t, 1, activity.TotalMessages)
	require.Equal(t, "general", activity.TopChannels[0].Name)
	require.Equal(t, "Peter", activity.TopAuthors[0].Name)

	section, err := RenderMarkdown(activity)
	require.NoError(t, err)
	require.Contains(t, section, "Discord Activity Report")
	require.Contains(t, section, "Latest archived message: 2026-04-21 04:00 UTC")
	require.NotContains(t, section, "AI Field Notes")

	readme := UpdateReadme([]byte("# Backup\n"), section)
	require.Contains(t, string(readme), StartMarker)
	require.Contains(t, string(readme), EndMarker)
	require.Contains(t, string(UpdateReadme(nil, "## Empty\n")), "## Empty")
	updated := UpdateReadme(readme, "## New\n")
	require.Contains(t, string(updated), "## New")
	require.NotContains(t, string(updated), "Latest archived message")
}

func TestWriteReadmeCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "README.md")
	require.NoError(t, WriteReadme(path, "## Report\n"))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "## Report")
}

func TestHelpersHandleEmptyAndLongContent(t *testing.T) {
	require.Equal(t, "unknown", escapeMD(""))
	require.Equal(t, "a\\|b", escapeMD("a|b"))
	require.Equal(t, "_No activity._", MarkdownTable(nil, "Name"))
	require.Contains(t, MarkdownTable([]RankedCount{{Name: "a|b", Count: 1000}}, "Name"), "a\\|b")
	require.Equal(t, "n/a", formatTime(time.Time{}))
	require.False(t, parseTime("2026-04-22 12:00:00").IsZero())
	require.True(t, parseTime("nope").IsZero())
	require.Error(t, WriteReadme(filepath.Join(t.TempDir(), "nested", "README.md"), ""))
}

func TestBuildReportsClosedStoreError(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	require.NoError(t, s.Close())

	_, err = Build(ctx, s, Options{})
	require.ErrorContains(t, err, "scan report totals")
}
