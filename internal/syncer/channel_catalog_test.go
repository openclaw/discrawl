package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func errMissingAccess() error {
	return errors.New("HTTP 403 Forbidden, {\"message\": \"Missing Access\", \"code\": 50001}")
}

func TestActiveThreadCatalogEdges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	allChannels := map[string]*discordgo.Channel{
		"voice": {ID: "voice", GuildID: "g1", Type: discordgo.ChannelTypeGuildVoice},
	}
	client := &fakeClient{}
	svc := New(client, nil, nil)
	require.NoError(t, svc.appendActiveThreadCatalog(ctx, allChannels, "g1", []string{"voice"}))
	require.Zero(t, client.guildThreadCalls)

	allChannels["forum"] = &discordgo.Channel{ID: "forum", GuildID: "g1", Type: discordgo.ChannelTypeGuildForum}
	client.guildThreadErrs = map[string]error{"g1": errMissingAccess()}
	require.NoError(t, svc.appendActiveThreadCatalog(ctx, allChannels, "g1", []string{"forum"}))
	require.Equal(t, 1, client.guildThreadCalls)

	client.guildThreadErrs = map[string]error{"g1": errors.New("boom")}
	require.ErrorContains(t, svc.appendActiveThreadCatalog(ctx, allChannels, "g1", []string{"forum"}), "boom")

	client.guildThreadErrs = nil
	client.guildThreads = map[string][]*discordgo.Channel{
		"g1": {
			nil,
			{ID: "other-thread", GuildID: "g1", ParentID: "other", Type: discordgo.ChannelTypeGuildPublicThread},
			{ID: "forum-thread", GuildID: "g1", ParentID: "forum", Type: discordgo.ChannelTypeGuildPublicThread},
		},
	}
	require.NoError(t, svc.appendActiveThreadCatalog(ctx, allChannels, "g1", []string{"forum"}))
	require.Contains(t, allChannels, "forum-thread")
	require.NotContains(t, allChannels, "other-thread")

	require.Nil(t, selectStoredChannels(nil, makeGuildSet([]string{"c1"})))
	require.Nil(t, selectStoredChannels([]store.ChannelRow{{ID: "c1"}}, nil))
	require.Equal(t, []string{"b", "a"}, uniqueIDs([]string{"", "b", "a", "b"}))
	require.False(t, isThreadChannel(nil))
	require.False(t, isThreadParent(nil))
	require.False(t, isMessageChannel(nil))
	require.False(t, isMessageChannel(&discordgo.Channel{Type: discordgo.ChannelTypeGuildCategory}))
}

func TestSyncChannelSubsetExpandsRequestedForumThreads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "f1",
		GuildID: "g1",
		Kind:    "forum",
		Name:    "support",
		RawJSON: `{"id":"f1"}`,
	}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "f1", GuildID: "g1", Name: "support", Type: discordgo.ChannelTypeGuildForum},
			},
		},
		activeThreads: map[string][]*discordgo.Channel{
			"f1": {{
				ID:            "t1",
				GuildID:       "g1",
				ParentID:      "f1",
				Name:          "bug-report",
				Type:          discordgo.ChannelTypeGuildPublicThread,
				LastMessageID: "10",
			}},
		},
		messages: map[string][]*discordgo.Message{
			"t1": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "t1",
				Content:   "forum thread body",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}, ChannelIDs: []string{"f1"}})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Channels)
	require.Equal(t, 1, stats.Threads)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, client.guildChanCalls)
	require.NotZero(t, client.threadCalls)
	require.Equal(t, 1, client.messageCalls["t1"])

	results, err := s.SearchMessages(ctx, store.SearchOptions{Query: "forum thread"})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "t1", results[0].ChannelID)
}

func TestSyncChannelSubsetFetchesRequestedArchivedThreadDirectly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))

	archiveAt := time.Now().UTC().Add(-time.Hour)
	client := &fakeClient{
		guilds: []*discordgo.UserGuild{
			{ID: "g-other", Name: "Other Guild"},
			{ID: "g1", Name: "Guild"},
		},
		guildByID: map[string]*discordgo.Guild{
			"g-other": {ID: "g-other", Name: "Other Guild"},
			"g1":      {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g-other": {
				{ID: "f-other", GuildID: "g-other", Name: "other forum", Type: discordgo.ChannelTypeGuildForum},
			},
			"g1": {
				{ID: "f1", GuildID: "g1", Name: "support", Type: discordgo.ChannelTypeGuildForum},
				{ID: "c2", GuildID: "g1", Name: "random", Type: discordgo.ChannelTypeGuildText},
			},
		},
		channelByID: map[string]*discordgo.Channel{
			"t-archived": {
				ID:       "t-archived",
				GuildID:  "g1",
				ParentID: "f1",
				Name:     "archived support thread",
				Type:     discordgo.ChannelTypeGuildPublicThread,
				ThreadMetadata: &discordgo.ThreadMetadata{
					Archived:         true,
					ArchiveTimestamp: archiveAt,
				},
				LastMessageID: "10",
			},
		},
		messages: map[string][]*discordgo.Message{
			"t-archived": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "t-archived",
				Content:   "archived support body",
				Timestamp: archiveAt,
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{
		Full:       true,
		GuildIDs:   []string{"g-other", "g1"},
		ChannelIDs: []string{"t-archived"},
	})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Guilds)
	require.Equal(t, 1, stats.Channels)
	require.Equal(t, 1, stats.Threads)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 2, client.guildChanCalls)
	require.Equal(t, 2, client.channelCalls["t-archived"])
	require.Zero(t, client.threadCalls)
	require.Empty(t, client.archivedCalls)
	require.Equal(t, 1, client.messageCalls["t-archived"])

	results, err := s.SearchMessages(ctx, store.SearchOptions{Query: "archived support"})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "t-archived", results[0].ChannelID)

	_, err = svc.Sync(ctx, SyncOptions{
		Full:       true,
		GuildIDs:   []string{"g-other"},
		ChannelIDs: []string{"t-archived"},
	})
	require.ErrorContains(t, err, "requested channel t-archived belongs to guild g1, not g-other")
}

func TestSyncToleratesArchivedThread403(t *testing.T) {
	t.Parallel()

	// Discord blocks archived thread listing on community Rules Screening channels
	// even for bots with Administrator permission. A 403 from ThreadsArchived
	// (for either public or private) should be skipped, not abort the entire sync,
	// and repeat unavailable warnings should be suppressed until the catalog
	// succeeds again.
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "rules", GuildID: "g1", Name: "rules-and-info", Type: discordgo.ChannelTypeGuildText},
			},
		},
		archivedErrors: map[string]error{
			"rules": errMissingAccess(),
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "hello",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	out := &lockedBuffer{}
	svc := New(client, s, newTestLogger(out))
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	// ThreadsArchived was invoked for "rules" (public + private),
	// confirming the error path was reached and tolerated, not bypassed.
	require.Equal(t, 2, client.archivedCalls["rules"])
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, client.messageCalls["c1"])
	require.Contains(t, out.String(), `level=WARN msg="thread archive crawl failed"`)

	cursor, err := s.GetSyncState(ctx, channelMessageUnavailableScope("rules"))
	require.NoError(t, err)
	require.Empty(t, cursor)

	cursor, err = s.GetSyncState(ctx, channelThreadCatalogUnavailableScope("rules"))
	require.NoError(t, err)
	require.Equal(t, "missing_access", cursor)

	out = &lockedBuffer{}
	svc = New(client, s, newTestLogger(out))
	allChannels := map[string]*discordgo.Channel{"rules": client.channels["g1"][1]}
	require.NoError(t, svc.appendThreadCatalog(ctx, allChannels, []string{"rules"}))
	require.NotContains(t, out.String(), `level=WARN msg="thread archive crawl failed"`)

	client.archivedErrors = nil
	out = &lockedBuffer{}
	svc = New(client, s, newTestLogger(out))
	require.NoError(t, svc.appendThreadCatalog(ctx, allChannels, []string{"rules"}))

	cursor, err = s.GetSyncState(ctx, channelThreadCatalogUnavailableScope("rules"))
	require.NoError(t, err)
	require.Empty(t, cursor)

	client.archivedErrors = map[string]error{
		"rules": errMissingAccess(),
	}
	out = &lockedBuffer{}
	svc = New(client, s, newTestLogger(out))
	require.NoError(t, svc.appendThreadCatalog(ctx, allChannels, []string{"rules"}))
	require.Contains(t, out.String(), `level=WARN msg="thread archive crawl failed"`)
}

func TestSyncSkipsInaccessibleForumThreadCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "f1", GuildID: "g1", Name: "private-forum", Type: discordgo.ChannelTypeGuildForum},
			},
		},
		threadErrors: map[string]error{
			"f1": errMissingAccess(),
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "still syncs accessible channels",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, client.messageCalls["c1"])
	require.GreaterOrEqual(t, client.threadCalls, 1)

	cursor, err := s.GetSyncState(ctx, channelThreadCatalogUnavailableScope("f1"))
	require.NoError(t, err)
	require.Equal(t, "missing_access", cursor)
}

func TestSyncKeepsThreadCatalogUnavailableMarkerAfterMessageRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
			},
		},
		threadErrors: map[string]error{
			"c1": errMissingAccess(),
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "still readable",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	out := &lockedBuffer{}
	svc := New(client, s, newTestLogger(out))
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, client.messageCalls["c1"])

	unavailable, err := s.GetSyncState(ctx, channelMessageUnavailableScope("c1"))
	require.NoError(t, err)
	require.Empty(t, unavailable)

	threadUnavailable, err := s.GetSyncState(ctx, channelThreadCatalogUnavailableScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "missing_access", threadUnavailable)

	out = &lockedBuffer{}
	svc = New(client, s, newTestLogger(out))
	stats, err = svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Zero(t, stats.Messages)
	require.NotContains(t, out.String(), `level=WARN msg="channel thread crawl skipped"`)

	client.threadErrors = nil
	out = &lockedBuffer{}
	svc = New(client, s, newTestLogger(out))
	allChannels := map[string]*discordgo.Channel{"c1": client.channels["g1"][0]}
	activeUnavailable, err := svc.appendActiveThreads(ctx, allChannels, "c1", true)
	require.NoError(t, err)
	require.False(t, activeUnavailable)

	threadUnavailable, err = s.GetSyncState(ctx, channelThreadCatalogUnavailableScope("c1"))
	require.NoError(t, err)
	require.Empty(t, threadUnavailable)

	client.threadErrors = map[string]error{
		"c1": errMissingAccess(),
	}
	out = &lockedBuffer{}
	svc = New(client, s, newTestLogger(out))
	activeUnavailable, err = svc.appendActiveThreads(ctx, allChannels, "c1", true)
	require.NoError(t, err)
	require.True(t, activeUnavailable)
	require.Contains(t, out.String(), `level=WARN msg="channel thread crawl skipped"`)
}

func TestSyncChannelSubsetMergesStoredAndLiveTargets(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "c1",
		GuildID: "g1",
		Kind:    "text",
		Name:    "general",
		RawJSON: `{"id":"c1"}`,
	}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "random", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "stored hit",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
			"c2": {{
				ID:        "20",
				GuildID:   "g1",
				ChannelID: "c2",
				Content:   "live miss",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}, ChannelIDs: []string{"c1", "c2"}})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Messages)
	require.Equal(t, 1, client.guildChanCalls)
	require.Equal(t, 1, client.messageCalls["c1"])
	require.Equal(t, 1, client.messageCalls["c2"])

	latest, err := s.GetSyncState(ctx, channelLatestScope("c2"))
	require.NoError(t, err)
	require.Equal(t, "20", latest)
}

func TestSyncSkipsUnchangedThreadsWhenHistoryComplete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:       "t1",
		GuildID:  "g1",
		ParentID: "f1",
		Kind:     "thread_public",
		Name:     "bug-report",
		RawJSON:  `{"id":"t1"}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("t1"), "200"))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("t1"), "1"))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "f1", GuildID: "g1", Name: "support", Type: discordgo.ChannelTypeGuildForum},
			},
		},
		activeThreads: map[string][]*discordgo.Channel{
			"f1": {{
				ID:            "t1",
				GuildID:       "g1",
				ParentID:      "f1",
				Name:          "bug-report",
				Type:          discordgo.ChannelTypeGuildPublicThread,
				LastMessageID: "200",
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Zero(t, stats.Messages)
	require.Zero(t, client.messageCalls["t1"])
}

func TestSyncSkipsUnchangedTextChannelsWhenHistoryComplete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "c1",
		GuildID: "g1",
		Kind:    "text",
		Name:    "general",
		RawJSON: `{"id":"c1"}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "200"))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{
				ID:            "c1",
				GuildID:       "g1",
				Name:          "general",
				Type:          discordgo.ChannelTypeGuildText,
				LastMessageID: "200",
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Zero(t, stats.Messages)
	require.Zero(t, client.messageCalls["c1"])
}

func TestFullSyncReusesStoredThreadParents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "c1",
		GuildID: "g1",
		Kind:    "text",
		Name:    "general",
		RawJSON: `{"id":"c1"}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:       "t1",
		GuildID:  "g1",
		ParentID: "c1",
		Kind:     "thread_public",
		Name:     "bug-report",
		RawJSON:  `{"id":"t1"}`,
	}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "random", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"t1": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "t1",
				Content:   "stored thread body",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Threads)
	require.Equal(t, 1, stats.Messages)
	require.Zero(t, client.threadCalls)
	require.Equal(t, 1, client.messageCalls["t1"])
}

func TestFullSyncUsesGuildActiveThreadsForStoredParents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "c1",
		GuildID: "g1",
		Kind:    "text",
		Name:    "general",
		RawJSON: `{"id":"c1"}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:       "t1",
		GuildID:  "g1",
		ParentID: "c1",
		Kind:     "thread_public",
		Name:     "bug-report",
		RawJSON:  `{"id":"t1"}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("t1"), "1"))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("t1"), "10"))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
			},
		},
		guildThreads: map[string][]*discordgo.Channel{
			"g1": {{
				ID:            "t1",
				GuildID:       "g1",
				ParentID:      "c1",
				Name:          "bug-report",
				Type:          discordgo.ChannelTypeGuildPublicThread,
				LastMessageID: "10",
			}},
		},
	}

	svc := New(client, s, nil)
	_, err = svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 1, client.guildThreadCalls)
	require.Zero(t, client.threadCalls)
}

func TestFullSyncDiscoversActiveThreadUnderNewParent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "c1",
		GuildID: "g1",
		Kind:    "text",
		Name:    "general",
		RawJSON: `{"id":"c1"}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:       "t1",
		GuildID:  "g1",
		ParentID: "c1",
		Kind:     "thread_public",
		Name:     "old-thread",
		RawJSON:  `{"id":"t1"}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("t1"), "1"))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("t1"), "10"))

	now := time.Now().UTC()
	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "random", Type: discordgo.ChannelTypeGuildText},
			},
		},
		guildThreads: map[string][]*discordgo.Channel{
			"g1": {
				{
					ID:            "t1",
					GuildID:       "g1",
					ParentID:      "c1",
					Name:          "old-thread",
					Type:          discordgo.ChannelTypeGuildPublicThread,
					LastMessageID: "10",
				},
				{
					ID:            "t2",
					GuildID:       "g1",
					ParentID:      "c2",
					Name:          "new-thread",
					Type:          discordgo.ChannelTypeGuildPublicThread,
					LastMessageID: "20",
				},
			},
		},
		messages: map[string][]*discordgo.Message{
			"t2": {{
				ID:        "20",
				GuildID:   "g1",
				ChannelID: "t2",
				Content:   "new parent thread body",
				Timestamp: now,
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Threads)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, client.guildThreadCalls)
	require.Zero(t, client.threadCalls)
	require.Equal(t, 1, client.messageCalls["t2"])

	results, err := s.SearchMessages(ctx, store.SearchOptions{Query: "new parent thread"})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "t2", results[0].ChannelID)
}

func TestFullSyncFallsBackToBroadThreadDiscoveryWithoutStoredThreads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "random", Type: discordgo.ChannelTypeGuildText},
			},
		},
	}

	svc := New(client, s, nil)
	_, err = svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 6, client.threadCalls)
}
