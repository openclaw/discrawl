package syncer

import (
	"context"
	"path/filepath"
	"testing"
	"time"

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

func TestCategoryScopeIntersectionDeniesDisjointScopes(t *testing.T) {
	t.Parallel()

	configured := newChannelScope(nil, nil, []string{"category-a"})
	requested := newChannelScope(nil, nil, []string{"category-b"})
	merged := configured.merged(requested)

	require.True(t, merged.categoryScopeSet)
	require.Empty(t, merged.allowedCategoryIDs)
	channel := &discordgo.Channel{ID: "text-a", ParentID: "category-a", Type: discordgo.ChannelTypeGuildText}
	require.True(t, merged.excludesDiscordChannel(channel, map[string]*discordgo.Channel{channel.ID: channel}))
}

func TestCategoryScopeLeavesEmptyConfigurationUnrestricted(t *testing.T) {
	t.Parallel()

	scope := newChannelScope(nil, nil, nil)
	require.False(t, scope.categoryScopeSet)
	channel := &discordgo.Channel{ID: "root-text", Type: discordgo.ChannelTypeGuildText}
	require.False(t, scope.excludesDiscordChannel(channel, map[string]*discordgo.Channel{channel.ID: channel}))
}

func TestCategoryScopeMergePreservesUnsetAndIntersectionStates(t *testing.T) {
	t.Parallel()

	unset := newChannelScope(nil, nil, nil)
	categoryA := newChannelScope(nil, nil, []string{"category-a"})
	categoryAB := newChannelScope(nil, nil, []string{"category-a", "category-b"})

	merged := unset.merged(unset)
	require.False(t, merged.categoryScopeSet)
	require.Nil(t, merged.allowedCategoryIDs)

	merged = unset.merged(categoryA)
	require.True(t, merged.categoryScopeSet)
	require.Equal(t, map[string]struct{}{"category-a": {}}, merged.allowedCategoryIDs)

	merged = categoryA.merged(unset)
	require.True(t, merged.categoryScopeSet)
	require.Equal(t, map[string]struct{}{"category-a": {}}, merged.allowedCategoryIDs)

	merged = categoryA.merged(categoryAB)
	require.True(t, merged.categoryScopeSet)
	require.Equal(t, map[string]struct{}{"category-a": {}}, merged.allowedCategoryIDs)
}

func TestTargetedStoredThreadUsesFullCategoryAncestry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, channel := range []store.ChannelRecord{
		{ID: "category-a", GuildID: "g1", Kind: "category", RawJSON: `{}`},
		{ID: "forum-a", GuildID: "g1", ParentID: "category-a", Kind: "forum", RawJSON: `{}`},
		{ID: "thread-a", GuildID: "g1", ParentID: "forum-a", Kind: "thread_public", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertChannel(ctx, channel))
	}

	svc := New(&fakeClient{}, s, nil)
	svc.SetIncludedCategories([]string{"category-a"})
	channels, targeted, err := svc.channelList(
		ctx,
		"g1",
		[]string{"thread-a"},
		channelCatalogFull,
		svc.effectiveChannelExclusions(SyncOptions{}),
		map[string]struct{}{"g1": {}},
		nil,
	)
	require.NoError(t, err)
	require.True(t, targeted)
	require.Equal(t, []string{"thread-a"}, channelIDs(channels))
}

func TestSyncAppliesCategoryAndChannelExclusions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC()
	client := &fakeClient{
		guilds:    []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Guild"}},
		channels: map[string][]*discordgo.Channel{"g1": {
			{ID: "category-a", GuildID: "g1", Type: discordgo.ChannelTypeGuildCategory},
			{ID: "category-b", GuildID: "g1", Type: discordgo.ChannelTypeGuildCategory},
			{ID: "allowed", GuildID: "g1", ParentID: "category-a", Type: discordgo.ChannelTypeGuildText},
			{ID: "blocked-id", GuildID: "g1", ParentID: "category-a", Type: discordgo.ChannelTypeGuildText},
			{ID: "blocked-kind", GuildID: "g1", ParentID: "category-a", Type: discordgo.ChannelTypeGuildNews},
			{ID: "outside", GuildID: "g1", ParentID: "category-b", Type: discordgo.ChannelTypeGuildText},
		}},
		messages: map[string][]*discordgo.Message{
			"allowed":      {{ID: "1", GuildID: "g1", ChannelID: "allowed", Content: "keep", Timestamp: now, Author: &discordgo.User{ID: "u1"}}},
			"blocked-id":   {{ID: "2", GuildID: "g1", ChannelID: "blocked-id", Content: "drop", Timestamp: now, Author: &discordgo.User{ID: "u1"}}},
			"blocked-kind": {{ID: "3", GuildID: "g1", ChannelID: "blocked-kind", Content: "drop", Timestamp: now, Author: &discordgo.User{ID: "u1"}}},
			"outside":      {{ID: "4", GuildID: "g1", ChannelID: "outside", Content: "drop", Timestamp: now, Author: &discordgo.User{ID: "u1"}}},
		},
	}

	svc := New(client, s, nil)
	svc.SetIncludedCategories([]string{"category-a"})
	svc.SetChannelExclusions([]string{"blocked-id"}, []string{"announcement"})
	stats, err := svc.Sync(ctx, SyncOptions{GuildIDs: []string{"g1"}, SkipMembers: true})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, client.messageCalls["allowed"])
	require.Zero(t, client.messageCalls["blocked-id"])
	require.Zero(t, client.messageCalls["blocked-kind"])
	require.Zero(t, client.messageCalls["outside"])
}

func TestTailAppliesCategoryAndChannelExclusions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, channel := range []store.ChannelRecord{
		{ID: "category-a", GuildID: "g1", Kind: "category", RawJSON: `{}`},
		{ID: "category-b", GuildID: "g1", Kind: "category", RawJSON: `{}`},
		{ID: "allowed", GuildID: "g1", ParentID: "category-a", Kind: "text", RawJSON: `{}`},
		{ID: "blocked-id", GuildID: "g1", ParentID: "category-a", Kind: "text", RawJSON: `{}`},
		{ID: "blocked-kind", GuildID: "g1", ParentID: "category-a", Kind: "announcement", RawJSON: `{}`},
		{ID: "outside", GuildID: "g1", ParentID: "category-b", Kind: "text", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertChannel(ctx, channel))
	}

	handler := &tailHandler{
		guilds:                 makeGuildSet([]string{"g1"}),
		store:                  s,
		exclusions:             newChannelScope([]string{"blocked-id"}, []string{"announcement"}, []string{"category-a"}),
		kindExcludedChannelIDs: map[string]struct{}{},
		knownChannelIDs:        map[string]struct{}{},
	}
	require.NoError(t, handler.seedChannelExclusions(ctx))

	for index, channelID := range []string{"allowed", "blocked-id", "blocked-kind", "outside"} {
		require.NoError(t, handler.OnMessageCreate(ctx, &discordgo.Message{
			ID:        string(rune('1' + index)),
			GuildID:   "g1",
			ChannelID: channelID,
			Content:   channelID,
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "u1"},
		}))
	}

	require.NoError(t, handler.OnChannelUpsert(ctx, &discordgo.Channel{
		ID:       "new-thread",
		GuildID:  "g1",
		ParentID: "allowed",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	}))
	require.NoError(t, handler.OnMessageCreate(ctx, &discordgo.Message{
		ID:        "5",
		GuildID:   "g1",
		ChannelID: "new-thread",
		Content:   "new thread",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1"},
	}))

	messages, err := s.ListMessages(ctx, store.MessageListOptions{GuildIDs: []string{"g1"}, IncludeEmpty: true})
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, []string{"allowed", "new-thread"}, []string{messages[0].ChannelID, messages[1].ChannelID})
}

func TestTailRepairOffsetScheduling(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 25, 3, 0, 0, 0, time.UTC)
	require.Equal(t, 6*time.Hour, nextTailRepairDelay(now, 6*time.Hour, 0))
	require.Equal(t, 5*time.Hour, nextTailRepairDelay(now, 6*time.Hour, 2*time.Hour))
	require.Equal(t, 5*time.Hour, nextTailRepairDelay(now, 6*time.Hour, 8*time.Hour))

	svc := &Syncer{}
	svc.SetRepairOffset(-time.Hour)
	require.Zero(t, svc.repairOffset())
	svc.SetRepairOffset(90 * time.Minute)
	require.Equal(t, 90*time.Minute, svc.repairOffset())
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
