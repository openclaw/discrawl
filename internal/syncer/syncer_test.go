package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/store"
)

type fakeClient struct {
	guilds           []*discordgo.UserGuild
	guildByID        map[string]*discordgo.Guild
	channels         map[string][]*discordgo.Channel
	activeThreads    map[string][]*discordgo.Channel
	guildThreads     map[string][]*discordgo.Channel
	threadErrors     map[string]error
	guildThreadErrs  map[string]error
	publicArchived   map[string][]*discordgo.Channel
	privateArchive   map[string][]*discordgo.Channel
	archivedErrors   map[string]error
	archivedCalls    map[string]int
	members          map[string][]*discordgo.Member
	messages         map[string][]*discordgo.Message
	messageErrors    map[string]error
	messageCalls     map[string]int
	messageRequests  []messageRequest
	messageBlocks    map[string]chan struct{}
	messageStarted   chan string
	beforeErrors     map[string]map[string]error
	memberDelay      time.Duration
	memberErr        error
	tailCalls        int
	tailHandled      chan struct{}
	messageDelay     time.Duration
	guildChanCalls   int
	threadCalls      int
	guildThreadCalls int
	memberCalls      int
	mu               sync.Mutex
	inFlight         int
	maxInFlight      int
}

type messageRequest struct {
	channelID string
	beforeID  string
	afterID   string
}

func (f *fakeClient) Self(context.Context) (*discordgo.User, error) {
	return &discordgo.User{ID: "bot"}, nil
}

func (f *fakeClient) Guilds(context.Context) ([]*discordgo.UserGuild, error) {
	return f.guilds, nil
}

func (f *fakeClient) Guild(_ context.Context, guildID string) (*discordgo.Guild, error) {
	return f.guildByID[guildID], nil
}

func (f *fakeClient) GuildChannels(_ context.Context, guildID string) ([]*discordgo.Channel, error) {
	f.guildChanCalls++
	return f.channels[guildID], nil
}

func (f *fakeClient) ThreadsActive(_ context.Context, channelID string) ([]*discordgo.Channel, error) {
	f.threadCalls++
	if err := f.threadErrors[channelID]; err != nil {
		return nil, err
	}
	return f.activeThreads[channelID], nil
}

func (f *fakeClient) GuildThreadsActive(_ context.Context, guildID string) ([]*discordgo.Channel, error) {
	f.guildThreadCalls++
	if err := f.guildThreadErrs[guildID]; err != nil {
		return nil, err
	}
	if f.guildThreads != nil {
		return f.guildThreads[guildID], nil
	}
	var out []*discordgo.Channel
	for _, threads := range f.activeThreads {
		out = append(out, threads...)
	}
	return out, nil
}

func (f *fakeClient) ThreadsArchived(_ context.Context, channelID string, private bool) ([]*discordgo.Channel, error) {
	f.threadCalls++
	if f.archivedCalls == nil {
		f.archivedCalls = make(map[string]int)
	}
	f.archivedCalls[channelID]++
	if err := f.archivedErrors[channelID]; err != nil {
		return nil, err
	}
	if private {
		return f.privateArchive[channelID], nil
	}
	return f.publicArchived[channelID], nil
}

func (f *fakeClient) GuildMembers(ctx context.Context, guildID string) ([]*discordgo.Member, error) {
	f.memberCalls++
	if f.memberDelay > 0 {
		timer := time.NewTimer(f.memberDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if f.memberErr != nil {
		return nil, f.memberErr
	}
	return f.members[guildID], nil
}

func (f *fakeClient) ChannelMessages(ctx context.Context, channelID string, limit int, beforeID, afterID string) ([]*discordgo.Message, error) {
	f.mu.Lock()
	if f.messageCalls == nil {
		f.messageCalls = make(map[string]int)
	}
	f.messageCalls[channelID]++
	f.messageRequests = append(f.messageRequests, messageRequest{
		channelID: channelID,
		beforeID:  beforeID,
		afterID:   afterID,
	})
	f.mu.Unlock()
	if f.messageStarted != nil {
		select {
		case f.messageStarted <- channelID:
		default:
		}
	}
	if block := f.messageBlocks[channelID]; block != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-block:
		}
	}
	if err := f.messageErrors[channelID]; err != nil {
		return nil, err
	}
	if err := f.beforeErrors[channelID][beforeID]; err != nil {
		return nil, err
	}
	if f.messageDelay > 0 {
		f.mu.Lock()
		f.inFlight++
		if f.inFlight > f.maxInFlight {
			f.maxInFlight = f.inFlight
		}
		f.mu.Unlock()
		timer := time.NewTimer(f.messageDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			f.mu.Lock()
			f.inFlight--
			f.mu.Unlock()
			return nil, ctx.Err()
		case <-timer.C:
		}
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}
	all := f.messages[channelID]
	if afterID != "" {
		var filtered []*discordgo.Message
		for _, msg := range all {
			if msg.ID > afterID {
				filtered = append(filtered, msg)
			}
		}
		return filtered, nil
	}
	if beforeID == "" {
		if len(all) <= limit {
			return all, nil
		}
		return all[:limit], nil
	}
	var filtered []*discordgo.Message
	for _, msg := range all {
		if msg.ID < beforeID {
			filtered = append(filtered, msg)
		}
	}
	if len(filtered) <= limit {
		return filtered, nil
	}
	return filtered[:limit], nil
}

func (f *fakeClient) ChannelMessage(_ context.Context, channelID, messageID string) (*discordgo.Message, error) {
	for _, msg := range f.messages[channelID] {
		if msg.ID == messageID {
			return msg, nil
		}
	}
	return nil, nil
}

func (f *fakeClient) Tail(ctx context.Context, handler discordclient.EventHandler) error {
	f.tailCalls++
	msg := &discordgo.Message{
		ID:        "3",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "tail event",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "peter"},
	}
	if err := handler.OnMessageCreate(ctx, msg); err != nil {
		return err
	}
	if f.tailHandled != nil {
		select {
		case f.tailHandled <- struct{}{}:
		default:
		}
	}
	<-ctx.Done()
	return nil
}

func TestSyncFullAndIncremental(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild One"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild One"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
			},
		},
		activeThreads: map[string][]*discordgo.Channel{
			"c1": {{ID: "t1", GuildID: "g1", ParentID: "c1", Name: "thread", Type: discordgo.ChannelTypeGuildPublicThread}},
		},
		members: map[string][]*discordgo.Member{
			"g1": {{
				GuildID: "g1",
				Nick:    "Peter",
				User:    &discordgo.User{ID: "u1", Username: "peter"},
			}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "100",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "panic locked database",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "peter"},
			}},
			"t1": {{
				ID:        "200",
				GuildID:   "g1",
				ChannelID: "t1",
				Content:   "thread post",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "peter"},
			}},
		},
	}

	svc := New(client, s, nil)
	discovered, err := svc.DiscoverGuilds(ctx)
	require.NoError(t, err)
	require.Len(t, discovered, 1)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Guilds)
	require.Equal(t, 2, stats.Channels)
	require.Equal(t, 1, stats.Threads)
	require.Equal(t, 1, stats.Members)
	require.Equal(t, 2, stats.Messages)

	results, err := s.SearchMessages(ctx, store.SearchOptions{Query: "panic"})
	require.NoError(t, err)
	require.Len(t, results, 1)

	client.messages["c1"] = append(client.messages["c1"], &discordgo.Message{
		ID:        "101",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "new message",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "peter"},
	})
	stats, err = svc.Sync(ctx, SyncOptions{Full: false})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)
}

func TestSyncUsesConfiguredConcurrency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "one", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "two", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "one", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}}},
			"c2": {{ID: "20", GuildID: "g1", ChannelID: "c2", Content: "two", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}}},
		},
		messageDelay: 40 * time.Millisecond,
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 2})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Messages)

	client.mu.Lock()
	maxInFlight := client.maxInFlight
	client.mu.Unlock()
	require.GreaterOrEqual(t, maxInFlight, 2)
}

func TestSyncMemberRefreshTimeoutStillMarksSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

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
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "100",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "hello",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "peter"},
			}},
		},
		memberDelay: 100 * time.Millisecond,
	}

	svc := New(client, s, nil)
	svc.memberRefreshTimeout = 10 * time.Millisecond

	stats, err := svc.Sync(ctx, SyncOptions{Full: true})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Members)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, client.memberCalls)

	lastSync, err := s.GetSyncState(ctx, "sync:last_success")
	require.NoError(t, err)
	require.NotEmpty(t, lastSync)
}

func TestSyncRequiredMemberRefreshFailsOnCrawlError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}},
		},
		memberErr: errors.New("rate limited"),
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{LatestOnly: true, RequireMembers: true})
	require.ErrorContains(t, err, "crawl guild members: rate limited")
	require.Zero(t, stats.Members)
	require.Equal(t, 1, client.memberCalls)

	lastSync, err := s.GetSyncState(ctx, "sync:last_success")
	require.NoError(t, err)
	require.Empty(t, lastSync)
}

func TestSyncRequiredMemberRefreshBypassesFreshSnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.SetSyncState(
		ctx,
		guildMemberSyncSuccessScope("g1"),
		time.Now().UTC().Format(time.RFC3339Nano),
	))
	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}},
		},
		members: map[string][]*discordgo.Member{
			"g1": {{User: &discordgo.User{ID: "u1", Username: "user"}}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{LatestOnly: true, RequireMembers: true})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Members)
	require.Equal(t, 1, client.memberCalls)
}

func TestSyncRejectsUnknownRequestedGuild(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
	}
	svc := New(client, s, nil)

	stats, err := svc.Sync(ctx, SyncOptions{GuildIDs: []string{"missing"}})
	require.ErrorContains(t, err, "requested guilds not accessible: missing")
	require.Zero(t, stats)
	lastSync, err := s.GetSyncState(ctx, "sync:last_success")
	require.NoError(t, err)
	require.Empty(t, lastSync)
}

func TestSyncSkipsMemberRefreshWhenExistingSnapshotPresent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "user",
		DisplayName: "User",
		RoleIDsJSON: "[]",
		RawJSON:     `{"user":{"id":"u1"}}`,
	}))

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
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "100",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "hello",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Members)
	require.Zero(t, client.memberCalls)

	lastSuccess, err := s.GetSyncState(ctx, guildMemberSyncSuccessScope("g1"))
	require.NoError(t, err)
	require.NotEmpty(t, lastSuccess)
}

func TestSyncSkipMembersFlagSkipsMemberRefresh(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

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
		members: map[string][]*discordgo.Member{
			"g1": {{
				User: &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "100",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "hello",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{SkipMembers: true})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Members)
	require.Zero(t, client.memberCalls)
}

func TestSyncLatestOnlySkipsChannelsWithoutLatestCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "empty", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "100",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "would bootstrap without latest-only",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{LatestOnly: true})
	require.NoError(t, err)
	require.Zero(t, stats.Messages)
	require.Zero(t, client.messageCalls["c1"])
}

func TestSyncLatestOnlySkipsUnchangedIncompleteChannel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "200"))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText, LastMessageID: "200"},
			},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{LatestOnly: true})
	require.NoError(t, err)
	require.Zero(t, stats.Messages)
	require.Zero(t, client.messageCalls["c1"])
}

func TestSyncLatestOnlyUsesIncrementalCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:       "archived",
		GuildID:  "g1",
		ParentID: "c1",
		Kind:     "thread_public",
		Name:     "archived-thread",
		RawJSON:  `{"id":"archived"}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "100"))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("active"), "200"))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("archived"), "300"))

	now := time.Now().UTC()
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
				LastMessageID: "100",
			}},
		},
		guildThreads: map[string][]*discordgo.Channel{
			"g1": {{
				ID:            "active",
				GuildID:       "g1",
				ParentID:      "c1",
				Name:          "active-thread",
				Type:          discordgo.ChannelTypeGuildPublicThread,
				LastMessageID: "201",
			}},
		},
		messages: map[string][]*discordgo.Message{
			"active": {{
				ID:        "201",
				GuildID:   "g1",
				ChannelID: "active",
				Content:   "new active thread message",
				Timestamp: now,
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{LatestOnly: true, GuildIDs: []string{"g1"}, SkipMembers: true})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, client.guildThreadCalls)
	require.Zero(t, client.threadCalls)
	require.Zero(t, client.messageCalls["archived"])
	require.Equal(t, 1, client.messageCalls["active"])
}

func TestSyncFullAutoBatchesIncompleteStoredChannels(t *testing.T) {
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
		Name:     "thread-1",
		RawJSON:  `{"id":"t1"}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:       "t2",
		GuildID:  "g1",
		ParentID: "f1",
		Kind:     "thread_public",
		Name:     "thread-2",
		RawJSON:  `{"id":"t2"}`,
	}))

	now := time.Now().UTC()
	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		members: map[string][]*discordgo.Member{
			"g1": {{
				User: &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
		messages: map[string][]*discordgo.Message{
			"t1": {{ID: "10", GuildID: "g1", ChannelID: "t1", Content: "first", Timestamp: now, Author: &discordgo.User{ID: "u1", Username: "user"}}},
			"t2": {{ID: "20", GuildID: "g1", ChannelID: "t2", Content: "second", Timestamp: now, Author: &discordgo.User{ID: "u1", Username: "user"}}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Messages)
	require.Equal(t, 1, stats.Members)
	require.Zero(t, client.guildChanCalls)
	require.Zero(t, client.threadCalls)
	require.Equal(t, 1, client.memberCalls)
	require.Equal(t, 1, client.messageCalls["t1"])
	require.Equal(t, 1, client.messageCalls["t2"])
}

func TestSyncFullAutoBatchHonorsSkipMembers(t *testing.T) {
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

	now := time.Now().UTC()
	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		members: map[string][]*discordgo.Member{
			"g1": {{
				User: &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "first", Timestamp: now, Author: &discordgo.User{ID: "u1", Username: "user"}}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}, SkipMembers: true})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)
	require.Zero(t, stats.Members)
	require.Zero(t, client.memberCalls)
}

func TestSyncFullUsesIncrementalCatalogWhenArchiveAlreadyComplete(t *testing.T) {
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
		Name:     "archived-thread",
		RawJSON:  `{"id":"t1"}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "200"))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("t1"), "300"))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("t1"), "1"))

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
	require.Equal(t, 1, stats.Channels)
	require.Zero(t, client.messageCalls["t1"])
	require.Zero(t, client.threadCalls)
	require.Equal(t, 1, client.guildThreadCalls)
}

func TestSetAttachmentTextEnabled(t *testing.T) {
	t.Parallel()

	svc := New(&fakeClient{}, nil, nil)
	require.True(t, svc.attachmentTextEnabled)

	svc.SetAttachmentTextEnabled(false)
	require.False(t, svc.attachmentTextEnabled)
}

func TestSyncChannelSubsetUsesStoredMetadata(t *testing.T) {
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

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}, ChannelIDs: []string{"c1"}})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)
	require.Zero(t, client.guildChanCalls)
	require.Zero(t, client.threadCalls)
	require.Zero(t, client.memberCalls)

	cursor, err := s.GetSyncState(ctx, "channel:c1:latest_message_id")
	require.NoError(t, err)
	require.Equal(t, "10", cursor)
}

func TestSyncSkipsMissingAccessChannels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "private", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "ok", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}}},
		},
		messageErrors: map[string]error{
			"c2": errors.New("HTTP 403 Forbidden, {\"message\": \"Missing Access\", \"code\": 50001}"),
		},
	}

	out := &lockedBuffer{}
	svc := New(client, s, newTestLogger(out))
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 2})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)

	logs := out.String()
	require.Contains(t, logs, `level=WARN msg="channel message crawl skipped"`)
	require.Contains(t, logs, `channel_id=c2`)

	cursor, err := s.GetSyncState(ctx, channelMessageUnavailableScope("c2"))
	require.NoError(t, err)
	require.Equal(t, "missing_access", cursor)
}

func TestSyncSuppressesRepeatedMissingAccessWarnings(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.SetSyncState(ctx, channelMessageUnavailableScope("c2"), "missing_access"))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "private", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "ok", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}}},
		},
		messageErrors: map[string]error{
			"c2": errors.New("HTTP 403 Forbidden, {\"message\": \"Missing Access\", \"code\": 50001}"),
		},
	}

	out := &lockedBuffer{}
	svc := New(client, s, newTestLogger(out))
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 2})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)

	logs := out.String()
	require.NotContains(t, logs, `msg="channel message crawl skipped"`)
	require.Contains(t, logs, `skipped_missing_access_channels=1`)

	cursor, err := s.GetSyncState(ctx, channelMessageUnavailableScope("c2"))
	require.NoError(t, err)
	require.Equal(t, "missing_access", cursor)
}

func TestSyncSkipsUnknownChannels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "gone", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "ok", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}}},
		},
		messageErrors: map[string]error{
			"c2": errors.New("HTTP 404 Not Found, {\"message\": \"Unknown Channel\", \"code\": 10003}"),
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 2})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)

	cursor, err := s.GetSyncState(ctx, "channel:c2:unavailable")
	require.NoError(t, err)
	require.Equal(t, "unknown_channel", cursor)
}

func TestSyncSkipsRetryableChannelErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "flaky", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "ok", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}}},
		},
		messageErrors: map[string]error{
			"c2": context.DeadlineExceeded,
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 2})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)

	cursor, err := s.GetSyncState(ctx, "channel:c2:latest_message_id")
	require.NoError(t, err)
	require.Empty(t, cursor)

	unavailable, err := s.GetSyncState(ctx, "channel:c2:unavailable")
	require.NoError(t, err)
	require.Empty(t, unavailable)
}

func TestSyncClearsUnavailableMarkerAfterSuccessfulRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.SetSyncState(ctx, "channel:c1:unavailable", "missing_access"))

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
		messages: map[string][]*discordgo.Message{
			"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "ok", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)

	unavailable, err := s.GetSyncState(ctx, "channel:c1:unavailable")
	require.NoError(t, err)
	require.Empty(t, unavailable)
}
