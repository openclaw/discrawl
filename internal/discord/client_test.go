package discord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestClientRESTWrappers(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/users/@me", writeJSON(map[string]any{"id": "bot"}))
	mux.HandleFunc("/api/v10/users/@me/guilds", writeJSON([]map[string]any{
		{"id": "g1", "name": "Guild One"},
	}))
	mux.HandleFunc("/api/v10/guilds/g1", writeJSON(map[string]any{"id": "g1", "name": "Guild One"}))
	mux.HandleFunc("/api/v10/guilds/g1/channels", writeJSON([]map[string]any{
		{"id": "c1", "guild_id": "g1", "name": "general", "type": 0},
	}))
	mux.HandleFunc("/api/v10/guilds/g1/threads/active", writeJSON(map[string]any{
		"threads": []map[string]any{
			{"id": "tg1", "guild_id": "g1", "parent_id": "c1", "name": "guild-thread", "type": 11},
		},
		"members":  []any{},
		"has_more": false,
	}))
	mux.HandleFunc("/api/v10/guilds/g1/members", writeJSON([]map[string]any{
		{
			"guild_id": "g1",
			"user":     map[string]any{"id": "u1", "username": "peter"},
			"roles":    []string{},
		},
	}))
	mux.HandleFunc("/api/v10/channels/c1/threads/active", writeJSON(map[string]any{
		"threads": []map[string]any{
			{"id": "t1", "guild_id": "g1", "parent_id": "c1", "name": "thread", "type": 11},
		},
		"members":  []any{},
		"has_more": false,
	}))
	mux.HandleFunc("/api/v10/channels/c1/threads/archived/public", writeJSON(map[string]any{
		"threads": []map[string]any{
			{
				"id":        "t2",
				"guild_id":  "g1",
				"parent_id": "c1",
				"name":      "archived-public",
				"type":      11,
				"thread_metadata": map[string]any{
					"archived":              true,
					"auto_archive_duration": 60,
					"archive_timestamp":     time.Now().UTC().Format(time.RFC3339),
					"locked":                false,
					"invitable":             true,
				},
			},
		},
		"members":  []any{},
		"has_more": false,
	}))
	mux.HandleFunc("/api/v10/channels/c1/threads/archived/private", writeJSON(map[string]any{
		"threads": []map[string]any{
			{
				"id":        "t3",
				"guild_id":  "g1",
				"parent_id": "c1",
				"name":      "archived-private",
				"type":      12,
				"thread_metadata": map[string]any{
					"archived":              true,
					"auto_archive_duration": 60,
					"archive_timestamp":     time.Now().UTC().Format(time.RFC3339),
					"locked":                true,
					"invitable":             false,
				},
			},
		},
		"members":  []any{},
		"has_more": false,
	}))
	mux.HandleFunc("/api/v10/channels/c1/messages", writeJSON([]map[string]any{
		{
			"id":         "m1",
			"guild_id":   "g1",
			"channel_id": "c1",
			"content":    "hello",
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
			"author":     map[string]any{"id": "u1", "username": "peter"},
		},
	}))
	mux.HandleFunc("/api/v10/channels/c1/messages/m1", writeJSON(map[string]any{
		"id":         "m1",
		"guild_id":   "g1",
		"channel_id": "c1",
		"content":    "hello",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"author":     map[string]any{"id": "u1", "username": "peter"},
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	self, err := client.Self(ctx)
	require.NoError(t, err)
	require.Equal(t, "bot", self.ID)

	guilds, err := client.Guilds(ctx)
	require.NoError(t, err)
	require.Len(t, guilds, 1)

	guild, err := client.Guild(ctx, "g1")
	require.NoError(t, err)
	require.Equal(t, "Guild One", guild.Name)

	channels, err := client.GuildChannels(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, channels, 1)

	members, err := client.GuildMembers(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, members, 1)

	active, err := client.ThreadsActive(ctx, "c1")
	require.NoError(t, err)
	require.Len(t, active, 1)

	guildActive, err := client.GuildThreadsActive(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, guildActive, 1)

	publicArchived, err := client.ThreadsArchived(ctx, "c1", false)
	require.NoError(t, err)
	require.Len(t, publicArchived, 1)

	privateArchived, err := client.ThreadsArchived(ctx, "c1", true)
	require.NoError(t, err)
	require.Len(t, privateArchived, 1)

	messages, err := client.ChannelMessages(ctx, "c1", 100, "", "")
	require.NoError(t, err)
	require.Len(t, messages, 1)

	message, err := client.ChannelMessage(ctx, "c1", "m1")
	require.NoError(t, err)
	require.Equal(t, "m1", message.ID)
}

func TestTailRequiresHandler(t *testing.T) {
	client, err := New("token")
	require.NoError(t, err)
	require.Error(t, client.Tail(context.Background(), nil))
	require.NoError(t, (&Client{}).Close())
}

func TestRunTailTaskRecoversPanics(t *testing.T) {
	t.Parallel()

	client := &Client{tailHandlerTimeout: 10 * time.Millisecond}
	err := client.runTailTask(context.Background(), func(context.Context) error {
		panic("boom")
	})
	require.ErrorContains(t, err, "tail handler panic: boom")

	client.tailHandlerTimeout = 0
	err = client.runTailTask(context.Background(), func(context.Context) error {
		panic("again")
	})
	require.ErrorContains(t, err, "tail handler panic: again")
}

func TestRequestContextHonorsExistingDeadlineAndDisabledTimeout(t *testing.T) {
	t.Parallel()

	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := &Client{requestTimeout: 20 * time.Millisecond}
	reqCtx, reqCancel := client.requestContext(parent)
	reqCancel()
	parentDeadline, ok := parent.Deadline()
	require.True(t, ok)
	reqDeadline, ok := reqCtx.Deadline()
	require.True(t, ok)
	require.Equal(t, parentDeadline, reqDeadline)

	reqCtx, reqCancel = (&Client{}).requestContext(context.Background())
	defer reqCancel()
	_, ok = reqCtx.Deadline()
	require.False(t, ok)
}

func TestTailQueueAndWorkerSizing(t *testing.T) {
	client := &Client{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	workCh := make(chan func(context.Context) error)
	errCh := make(chan error, 1)
	client.enqueueTailTask(ctx, workCh, errCh, func(context.Context) error { return nil })
	require.Empty(t, errCh)

	ctx = context.Background()
	fullWorkCh := make(chan func(context.Context) error)
	client.enqueueTailTask(ctx, fullWorkCh, errCh, func(context.Context) error { return nil })
	require.ErrorContains(t, <-errCh, "tail worker queue full")
	errCh <- errors.New("existing")
	client.enqueueTailTask(ctx, fullWorkCh, errCh, func(context.Context) error { return nil })
	require.ErrorContains(t, <-errCh, "existing")

	prev := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prev)
	require.Equal(t, 4, defaultTailWorkerCount())
	runtime.GOMAXPROCS(8)
	require.Equal(t, 8, defaultTailWorkerCount())
	runtime.GOMAXPROCS(32)
	require.Equal(t, 16, defaultTailWorkerCount())
	require.Equal(t, defaultTailWorkerCount()*32, defaultTailQueueSize())
}

func TestClientChannelMessagesTimesOut(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/channels/c1/messages", func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.requestTimeout = 20 * time.Millisecond

	start := time.Now()
	_, err = client.ChannelMessages(context.Background(), "c1", 100, "", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context deadline exceeded"))
	require.Less(t, time.Since(start), time.Second)
}

func TestUniqueChannels(t *testing.T) {
	channels := uniqueChannels([]*discordgo.Channel{
		{ID: "2"},
		{ID: "1"},
		{ID: "2"},
		nil,
	})
	require.Len(t, channels, 2)
	require.Equal(t, "1", channels[0].ID)
	require.Equal(t, "2", channels[1].ID)
}

func TestTailReceivesGatewayEvents(t *testing.T) {
	mux := http.NewServeMux()
	upgrader := websocket.Upgrader{}
	gatewayURL := ""
	mux.HandleFunc("/api/v10/gateway", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"url": gatewayURL})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	gatewayURL = "ws" + server.URL[len("http"):] + "/gateway"
	gatewayHandler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade gateway: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if err := conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		}); err != nil {
			t.Errorf("write hello: %v", err)
			return
		}
		_, _, err = conn.ReadMessage()
		if err != nil {
			t.Errorf("read identify: %v", err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "READY",
			"s":  1,
			"d": map[string]any{
				"session_id": "session",
				"user":       map[string]any{"id": "bot", "username": "bot"},
			},
		}); err != nil {
			t.Errorf("write ready: %v", err)
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		events := []map[string]any{
			{"op": 0, "t": "MESSAGE_CREATE", "s": 2, "d": map[string]any{"id": "m1", "guild_id": "g1", "channel_id": "c1", "content": "hello", "timestamp": now, "author": map[string]any{"id": "u1", "username": "user"}}},
			{"op": 0, "t": "MESSAGE_UPDATE", "s": 3, "d": map[string]any{"id": "m1", "guild_id": "g1", "channel_id": "c1", "content": "hello 2", "timestamp": now, "author": map[string]any{"id": "u1", "username": "user"}}},
			{"op": 0, "t": "MESSAGE_DELETE", "s": 4, "d": map[string]any{"id": "m1", "guild_id": "g1", "channel_id": "c1"}},
			{"op": 0, "t": "CHANNEL_CREATE", "s": 5, "d": map[string]any{"id": "c1", "guild_id": "g1", "name": "general", "type": 0}},
			{"op": 0, "t": "GUILD_MEMBER_ADD", "s": 6, "d": map[string]any{"guild_id": "g1", "user": map[string]any{"id": "u1", "username": "user"}, "roles": []string{}}},
			{"op": 0, "t": "GUILD_MEMBER_REMOVE", "s": 7, "d": map[string]any{"guild_id": "g1", "user": map[string]any{"id": "u1", "username": "user"}}},
		}
		for _, event := range events {
			if err := conn.WriteJSON(event); err != nil {
				t.Errorf("write event %v: %v", event["t"], err)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	mux.HandleFunc("/gateway", gatewayHandler)
	mux.HandleFunc("/gateway/", gatewayHandler)

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := &recordingHandler{}
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	require.NoError(t, client.Tail(ctx, handler))
	require.Equal(t, 1, handler.creates)
	require.Equal(t, 1, handler.updates)
	require.Equal(t, 1, handler.deletes)
	require.Equal(t, 1, handler.channels)
	require.Equal(t, 1, handler.memberUpserts)
	require.Equal(t, 1, handler.memberDeletes)
	require.Equal(t, 1, handler.ready)
}

func TestTailFailsFastWhenWorkerQueueFills(t *testing.T) {
	mux := http.NewServeMux()
	upgrader := websocket.Upgrader{}
	gatewayURL := ""
	mux.HandleFunc("/api/v10/gateway", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"url": gatewayURL})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	gatewayURL = "ws" + server.URL[len("http"):] + "/gateway"
	gatewayHandler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade gateway: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if err := conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		}); err != nil {
			t.Errorf("write hello: %v", err)
			return
		}
		_, _, err = conn.ReadMessage()
		if err != nil {
			t.Errorf("read identify: %v", err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "READY",
			"s":  1,
			"d": map[string]any{
				"session_id": "session",
				"user":       map[string]any{"id": "bot", "username": "bot"},
			},
		}); err != nil {
			t.Errorf("write ready: %v", err)
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		for i := range 4 {
			if err := conn.WriteJSON(map[string]any{
				"op": 0,
				"t":  "MESSAGE_CREATE",
				"s":  i + 2,
				"d": map[string]any{
					"id":         fmt.Sprintf("m%d", i),
					"guild_id":   "g1",
					"channel_id": "c1",
					"content":    "hello",
					"timestamp":  now,
					"author":     map[string]any{"id": "u1", "username": "user"},
				},
			}); err != nil {
				t.Errorf("write create %d: %v", i, err)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	mux.HandleFunc("/gateway", gatewayHandler)
	mux.HandleFunc("/gateway/", gatewayHandler)

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1
	client.tailHandlerTimeout = time.Second

	err = client.Tail(context.Background(), &slowHandler{sleep: 200 * time.Millisecond})
	require.ErrorContains(t, err, "tail worker queue full")
}

func writeJSON(v any) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
}

func patchDiscordEndpoints(apiBase string) func() {
	oldDiscord := discordgo.EndpointDiscord
	oldAPI := discordgo.EndpointAPI
	oldGuilds := discordgo.EndpointGuilds
	oldChannels := discordgo.EndpointChannels
	oldUsers := discordgo.EndpointUsers
	oldGateway := discordgo.EndpointGateway
	oldUser := discordgo.EndpointUser
	oldUserGuilds := discordgo.EndpointUserGuilds
	oldGuild := discordgo.EndpointGuild
	oldGuildChannels := discordgo.EndpointGuildChannels
	oldGuildMembers := discordgo.EndpointGuildMembers
	oldChannelThreads := discordgo.EndpointChannelThreads
	oldChannelActiveThreads := discordgo.EndpointChannelActiveThreads
	oldChannelPublicArchivedThreads := discordgo.EndpointChannelPublicArchivedThreads
	oldChannelPrivateArchivedThreads := discordgo.EndpointChannelPrivateArchivedThreads
	oldChannelMessages := discordgo.EndpointChannelMessages
	oldChannelMessage := discordgo.EndpointChannelMessage

	discordgo.EndpointDiscord = apiBase[:len(apiBase)-len("api/v10/")]
	discordgo.EndpointAPI = apiBase
	discordgo.EndpointGuilds = apiBase + "guilds/"
	discordgo.EndpointChannels = apiBase + "channels/"
	discordgo.EndpointUsers = apiBase + "users/"
	discordgo.EndpointGateway = apiBase + "gateway"
	discordgo.EndpointUser = func(uID string) string { return discordgo.EndpointUsers + uID }
	discordgo.EndpointUserGuilds = func(uID string) string { return discordgo.EndpointUsers + uID + "/guilds" }
	discordgo.EndpointGuild = func(gID string) string { return discordgo.EndpointGuilds + gID }
	discordgo.EndpointGuildChannels = func(gID string) string { return discordgo.EndpointGuilds + gID + "/channels" }
	discordgo.EndpointGuildMembers = func(gID string) string { return discordgo.EndpointGuilds + gID + "/members" }
	discordgo.EndpointChannelThreads = func(cID string) string { return discordgo.EndpointChannels + cID + "/threads" }
	discordgo.EndpointChannelActiveThreads = func(cID string) string { return discordgo.EndpointChannelThreads(cID) + "/active" }
	discordgo.EndpointChannelPublicArchivedThreads = func(cID string) string { return discordgo.EndpointChannelThreads(cID) + "/archived/public" }
	discordgo.EndpointChannelPrivateArchivedThreads = func(cID string) string { return discordgo.EndpointChannelThreads(cID) + "/archived/private" }
	discordgo.EndpointChannelMessages = func(cID string) string { return discordgo.EndpointChannels + cID + "/messages" }
	discordgo.EndpointChannelMessage = func(cID, mID string) string { return discordgo.EndpointChannelMessages(cID) + "/" + mID }

	return func() {
		discordgo.EndpointDiscord = oldDiscord
		discordgo.EndpointAPI = oldAPI
		discordgo.EndpointGuilds = oldGuilds
		discordgo.EndpointChannels = oldChannels
		discordgo.EndpointUsers = oldUsers
		discordgo.EndpointGateway = oldGateway
		discordgo.EndpointUser = oldUser
		discordgo.EndpointUserGuilds = oldUserGuilds
		discordgo.EndpointGuild = oldGuild
		discordgo.EndpointGuildChannels = oldGuildChannels
		discordgo.EndpointGuildMembers = oldGuildMembers
		discordgo.EndpointChannelThreads = oldChannelThreads
		discordgo.EndpointChannelActiveThreads = oldChannelActiveThreads
		discordgo.EndpointChannelPublicArchivedThreads = oldChannelPublicArchivedThreads
		discordgo.EndpointChannelPrivateArchivedThreads = oldChannelPrivateArchivedThreads
		discordgo.EndpointChannelMessages = oldChannelMessages
		discordgo.EndpointChannelMessage = oldChannelMessage
	}
}

type recordingHandler struct {
	creates       int
	updates       int
	deletes       int
	channels      int
	memberUpserts int
	memberDeletes int
	ready         int
}

func (r *recordingHandler) OnTailReady(context.Context) error {
	r.ready++
	return nil
}

func (r *recordingHandler) OnMessageCreate(context.Context, *discordgo.Message) error {
	r.creates++
	return nil
}

func (r *recordingHandler) OnMessageUpdate(context.Context, *discordgo.Message) error {
	r.updates++
	return nil
}

func (r *recordingHandler) OnMessageDelete(context.Context, *discordgo.MessageDelete) error {
	r.deletes++
	return nil
}

func (r *recordingHandler) OnChannelUpsert(context.Context, *discordgo.Channel) error {
	r.channels++
	return nil
}

func (r *recordingHandler) OnMemberUpsert(context.Context, string, *discordgo.Member) error {
	r.memberUpserts++
	return nil
}

func (r *recordingHandler) OnMemberDelete(context.Context, string, string) error {
	r.memberDeletes++
	return nil
}

type slowHandler struct {
	sleep time.Duration
}

func (s *slowHandler) OnMessageCreate(context.Context, *discordgo.Message) error {
	time.Sleep(s.sleep)
	return nil
}

func (s *slowHandler) OnMessageUpdate(context.Context, *discordgo.Message) error {
	return nil
}

func (s *slowHandler) OnMessageDelete(context.Context, *discordgo.MessageDelete) error {
	return nil
}

func (s *slowHandler) OnChannelUpsert(context.Context, *discordgo.Channel) error {
	return nil
}

func (s *slowHandler) OnMemberUpsert(context.Context, string, *discordgo.Member) error {
	return nil
}

func (s *slowHandler) OnMemberDelete(context.Context, string, string) error {
	return nil
}
