package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestIncludeStoredThreadTargetsFreezesDirectNativeRoots(t *testing.T) {
	rows := []store.ChannelRow{
		{ID: "root", GuildID: "g1", Kind: "text"},
		{ID: "news", GuildID: "g1", Kind: "announcement"},
		{ID: "forum", GuildID: "g1", Kind: "forum"},
		{ID: "category", GuildID: "g1", Kind: "category"},
		{ID: "voice", GuildID: "g1", Kind: "voice"},
		{ID: "public", GuildID: "g1", ParentID: "root", Kind: "thread_public"},
		{ID: "private", GuildID: "g1", ParentID: "root", Kind: "thread_private", IsArchived: true},
		{ID: "announcement", GuildID: "g1", ParentID: "news", Kind: "thread_announcement"},
		{ID: "numeric", GuildID: "g1", ParentID: "root", Kind: "type_11"},
		{ID: "other-guild", GuildID: "g2", ParentID: "root", Kind: "thread_public"},
		{ID: "leaf", GuildID: "g1", ParentID: "other", Kind: "thread_public"},
		{ID: "leaf-child", GuildID: "g1", ParentID: "leaf", Kind: "thread_public"},
		{ID: "nested", GuildID: "g1", ParentID: "public", Kind: "thread_public"},
		{ID: "sibling", GuildID: "g1", ParentID: "other", Kind: "thread_public"},
		{ID: "forum-child", GuildID: "g1", ParentID: "forum", Kind: "thread_public"},
		{ID: "category-child", GuildID: "g1", ParentID: "category", Kind: "thread_public"},
		{ID: "voice-child", GuildID: "g1", ParentID: "voice", Kind: "thread_public"},
		{ID: "unknown", GuildID: "g1", ParentID: "root", Kind: "thread_unknown"},
		{ID: "not-thread", GuildID: "g1", ParentID: "root", Kind: "text"},
	}
	requested := []string{"root", "news", "leaf", "forum", "category", "voice", "root", "missing"}
	want := []string{"announcement", "category", "forum", "leaf", "missing", "news", "numeric", "private", "public", "root", "voice"}
	require.Equal(t, want, includeStoredThreadTargets(rows, "g1", requested))
	require.Equal(t, []string{"root", "news", "leaf", "forum", "category", "voice", "root", "missing"}, requested)
	require.Equal(t, []string{"category", "forum", "leaf", "missing", "news", "root", "voice"}, includeStoredThreadTargets(rows, "g2", requested))
}

func TestLatestDiscoveryThenTargetedWindowIncludesStoredThreadsOnlyWhenRequested(t *testing.T) {
	ctx := t.Context()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	root := &discordgo.Channel{ID: "root", GuildID: "g1", Type: discordgo.ChannelTypeGuildText}
	thread := &discordgo.Channel{ID: "new-thread", GuildID: "g1", ParentID: root.ID, Type: discordgo.ChannelTypeGuildPublicThread}
	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1"}}, guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1"}},
		channels: map[string][]*discordgo.Channel{"g1": {root}}, activeThreads: map[string][]*discordgo.Channel{root.ID: {thread}},
	}
	svc := New(client, db, nil)
	_, err = svc.Sync(ctx, SyncOptions{LatestOnly: true, SkipMembers: true})
	require.NoError(t, err)
	rows, err := db.Channels(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, rows, 2, "the ordinary first phase discovers the new text thread")
	since := time.Now().UTC().Add(-time.Hour)
	opts := SyncOptions{Full: true, Since: since, ChannelIDs: []string{root.ID}, SkipMembers: true}
	_, err = svc.Sync(ctx, opts)
	require.NoError(t, err)
	proof, err := db.GetSyncState(ctx, channelHistoryWindowScope(thread.ID, since))
	require.NoError(t, err)
	require.Empty(t, proof, "default explicit text selection does not include children")
	opts.IncludeStoredThreads = true
	_, err = svc.Sync(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, []string{root.ID}, opts.ChannelIDs, "expanded targets do not mutate caller state")
	proof, err = db.GetSyncState(ctx, channelHistoryWindowScope(thread.ID, since))
	require.NoError(t, err)
	var window HistoryWindow
	require.NoError(t, json.Unmarshal([]byte(proof), &window))
	require.Equal(t, thread.ID, window.ChannelID)
	require.Equal(t, int(thread.Type), window.ChannelType)
	require.Equal(t, since, window.Since)
	require.Equal(t, "end", window.Boundary, "empty discovered threads still need a verified window")
}

type storedThreadPermissionClient struct {
	*fakeClient
	denied string
}

func (c *storedThreadPermissionClient) CanReadHistory(_ context.Context, channel *discordgo.Channel) (bool, error) {
	return channel.ID != c.denied, nil
}

func TestIncludedStoredThreadsKeepExclusionsAndHistoryFailureGuards(t *testing.T) {
	for _, mode := range []string{"parent", "child", "kind", "category", "denied", "api"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			for _, row := range []store.ChannelRecord{
				{ID: "category", GuildID: "g1", Kind: "category", RawJSON: `{}`},
				{ID: "root", GuildID: "g1", ParentID: "category", Kind: "text", RawJSON: `{}`},
				{ID: "thread", GuildID: "g1", ParentID: "root", Kind: "thread_private", IsArchived: true, RawJSON: `{}`},
			} {
				require.NoError(t, db.UpsertChannel(ctx, row))
			}
			client := &storedThreadPermissionClient{fakeClient: &fakeClient{
				guilds: []*discordgo.UserGuild{{ID: "g1"}}, guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1"}},
			}}
			opts := SyncOptions{Full: true, Since: time.Now().UTC().Add(-time.Hour), ChannelIDs: []string{"root"}, IncludeStoredThreads: true, SkipMembers: true}
			switch mode {
			case "parent":
				opts.ExcludeChannelIDs = []string{"root"}
			case "child":
				opts.ExcludeChannelIDs = []string{"thread"}
			case "kind":
				opts.ExcludeChannelKinds = []string{"thread_private"}
			case "category":
				opts.IncludeCategoryIDs = []string{"other-category"}
			case "denied":
				client.denied = "thread"
			case "api":
				client.messageErrors = map[string]error{"thread": errors.New("fixture failure")}
			}
			_, err = New(client, db, nil).Sync(ctx, opts)
			if mode == "api" {
				require.ErrorContains(t, err, "fixture failure")
			} else {
				require.NoError(t, err)
			}
			proof, err := db.GetSyncState(ctx, channelHistoryWindowScope("thread", opts.Since))
			require.NoError(t, err)
			require.Empty(t, proof)
			if mode == "denied" {
				marker, err := db.GetSyncState(ctx, channelMessageUnavailableScope("thread"))
				require.NoError(t, err)
				require.Contains(t, marker, "missing_history_permissions")
			}
		})
	}
}

func TestIncludeStoredThreadsRejectsMissingTargets(t *testing.T) {
	_, err := New(nil, nil, nil).Sync(t.Context(), SyncOptions{IncludeStoredThreads: true})
	require.ErrorContains(t, err, "requires explicit channel targets")
}
