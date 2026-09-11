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

func categoryExclusionFixture() []*discordgo.Channel {
	return []*discordgo.Channel{
		{ID: "blocked-category", GuildID: "g1", Type: discordgo.ChannelTypeGuildCategory},
		{ID: "blocked-forum", GuildID: "g1", ParentID: "blocked-category", Type: discordgo.ChannelTypeGuildForum},
		{ID: "blocked-thread", GuildID: "g1", ParentID: "blocked-forum", Type: discordgo.ChannelTypeGuildPublicThread},
		{ID: "blocked-text", GuildID: "g1", ParentID: "blocked-category", Type: discordgo.ChannelTypeGuildText},
		{ID: "blocked-private-thread", GuildID: "g1", ParentID: "blocked-text", Type: discordgo.ChannelTypeGuildPrivateThread},
		{ID: "new-category", GuildID: "g1", Type: discordgo.ChannelTypeGuildCategory},
		{ID: "allowed", GuildID: "g1", ParentID: "new-category", Type: discordgo.ChannelTypeGuildText},
		{ID: "root", GuildID: "g1", Type: discordgo.ChannelTypeGuildText},
	}
}

func TestCategoryExclusionFiltersDescendantsWithoutAllowlist(t *testing.T) {
	t.Parallel()
	channels := categoryExclusionFixture()
	scope := newChannelScope([]string{"blocked-category"}, nil, nil)
	require.Equal(t, []string{"new-category", "allowed", "root"}, channelIDs(filterExcludedDiscordChannels(channels, scope)))
	// Exclusions still win when the same category is explicitly included.
	scope = newChannelScope([]string{"blocked-category"}, nil, []string{"blocked-category"})
	require.Empty(t, filterExcludedDiscordChannels(channels, scope))
}

func TestCategoryExclusionAppliesToStoredRepairAndTail(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	for _, channel := range categoryExclusionFixture() {
		require.NoError(t, s.UpsertChannel(ctx, toChannelRecord(channel, "{}")))
	}
	svc := New(&fakeClient{}, s, nil)
	svc.SetChannelExclusions([]string{"blocked-category"}, nil)
	ids := []string{"blocked-thread", "blocked-private-thread", "allowed", "root"}
	filtered, err := svc.filterExcludedStoredChannelIDs(ctx, "g1", ids, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"allowed", "root"}, filtered)

	channels, targeted, err := svc.channelList(ctx, "g1", []string{"blocked-thread"}, channelCatalogFull,
		svc.effectiveChannelExclusions(SyncOptions{}), makeGuildSet([]string{"g1"}), nil, nil)
	require.NoError(t, err)
	require.True(t, targeted)
	require.Empty(t, channels)

	handler := &tailHandler{guilds: makeGuildSet([]string{"g1"}), store: s, exclusions: svc.channelExclusions}
	require.NoError(t, handler.seedChannelExclusions(ctx))
	for i, id := range ids {
		require.NoError(t, handler.OnMessageCreate(ctx, &discordgo.Message{
			ID: string(rune('1' + i)), GuildID: "g1", ChannelID: id, Content: id,
			Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1"},
		}))
	}
	messages, err := s.ListMessages(ctx, store.MessageListOptions{GuildIDs: []string{"g1"}, IncludeEmpty: true})
	require.NoError(t, err)
	require.Len(t, messages, 2)
	for _, message := range messages {
		require.Contains(t, []string{"allowed", "root"}, message.ChannelID)
	}
}

func TestTailCategoryExclusionTracksAncestorUpdates(t *testing.T) {
	t.Parallel()
	handler := &tailHandler{exclusions: newChannelScope([]string{"blocked-category"}, nil, nil)}
	thread := &discordgo.Channel{ID: "thread", ParentID: "forum", Type: discordgo.ChannelTypeGuildPublicThread}
	forum := &discordgo.Channel{ID: "forum", ParentID: "new-category", Type: discordgo.ChannelTypeGuildForum}
	// Child metadata can arrive before its parent.
	handler.trackChannelExclusion(thread)
	handler.trackChannelExclusion(forum)
	require.False(t, handler.excludeChannel(thread.ID))
	forum.ParentID = "blocked-category"
	handler.trackChannelExclusion(forum)
	require.True(t, handler.excludeChannel(thread.ID))
	forum.ParentID = "new-category"
	handler.trackChannelExclusion(forum)
	require.False(t, handler.excludeChannel(thread.ID))
}

func TestCategoryExclusionHandlesIncompleteAndCyclicAncestry(t *testing.T) {
	t.Parallel()
	scope := newChannelScope([]string{"blocked-category"}, nil, nil)
	thread := &discordgo.Channel{ID: "thread", ParentID: "forum", Type: discordgo.ChannelTypeGuildPublicThread}
	forum := &discordgo.Channel{ID: "forum", ParentID: "blocked-category", Type: discordgo.ChannelTypeGuildForum}
	catalog := map[string]*discordgo.Channel{thread.ID: thread, forum.ID: forum}
	// The excluded category need not itself be present in the catalog.
	require.True(t, scope.excludesDiscordChannel(thread, catalog))
	forum.ParentID = thread.ID
	require.False(t, scope.excludesDiscordChannel(thread, catalog))
}
