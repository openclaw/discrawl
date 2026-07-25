package syncer

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestCategoryScopeFiltersChannelsAndNestedThreads(t *testing.T) {
	t.Parallel()

	allowedCategory := &discordgo.Channel{ID: "allowed-category", Type: discordgo.ChannelTypeGuildCategory}
	rejectedCategory := &discordgo.Channel{ID: "rejected-category", Type: discordgo.ChannelTypeGuildCategory}
	allowedForum := &discordgo.Channel{ID: "allowed-forum", ParentID: allowedCategory.ID, Type: discordgo.ChannelTypeGuildForum}
	allowedThread := &discordgo.Channel{ID: "allowed-thread", ParentID: allowedForum.ID, Type: discordgo.ChannelTypeGuildPublicThread}
	allowedText := &discordgo.Channel{ID: "allowed-text", ParentID: allowedCategory.ID, Type: discordgo.ChannelTypeGuildText}
	rejectedText := &discordgo.Channel{ID: "rejected-text", ParentID: rejectedCategory.ID, Type: discordgo.ChannelTypeGuildText}
	rootText := &discordgo.Channel{ID: "root-text", Type: discordgo.ChannelTypeGuildText}
	feed := &discordgo.Channel{ID: "feed", ParentID: allowedCategory.ID, Type: discordgo.ChannelTypeGuildNews}

	scope := newChannelScope(nil, []string{"announcement"}, []string{allowedCategory.ID})
	filtered := filterExcludedDiscordChannels(
		[]*discordgo.Channel{
			allowedCategory,
			rejectedCategory,
			allowedForum,
			allowedThread,
			allowedText,
			rejectedText,
			rootText,
			feed,
		},
		scope,
	)
	require.Equal(t, []string{"allowed-category", "allowed-forum", "allowed-thread", "allowed-text"}, channelIDs(filtered))
	require.Equal(t, []string{"allowed-forum", "allowed-text"}, scopedThreadParentIDs(filtered, scope))
}

func TestCategoryScopeFiltersIncompleteStoredChannels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, channel := range []store.ChannelRecord{
		{ID: "allowed-category", GuildID: "g1", Kind: "category", RawJSON: `{}`},
		{ID: "rejected-category", GuildID: "g1", Kind: "category", RawJSON: `{}`},
		{ID: "allowed-text", GuildID: "g1", ParentID: "allowed-category", Kind: "text", RawJSON: `{}`},
		{ID: "rejected-text", GuildID: "g1", ParentID: "rejected-category", Kind: "text", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertChannel(ctx, channel))
	}

	svc := New(&fakeClient{}, s, nil)
	svc.SetIncludedCategories([]string{"allowed-category"})
	filtered, err := svc.filterExcludedStoredChannelIDs(ctx, "g1", []string{"allowed-text", "rejected-text"}, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"allowed-text"}, filtered)
}

func TestTailRepairUsesIncrementalScope(t *testing.T) {
	t.Parallel()

	opts := tailRepairSyncOptions([]string{"g1"})
	require.Equal(t, []string{"g1"}, opts.GuildIDs)
	require.False(t, opts.Full)
	require.True(t, opts.SkipMembers)
	require.True(t, opts.LatestOnly)
	require.Equal(t, "tail_repair", opts.RepairReason)
}
