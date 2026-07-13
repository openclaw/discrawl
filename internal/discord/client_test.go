package discord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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

func TestTailRemovesGatewayHandlersWhenOpenFails(t *testing.T) {
	oldGateway := discordgo.EndpointGateway
	discordgo.EndpointGateway = "://invalid-gateway"
	defer func() {
		discordgo.EndpointGateway = oldGateway
	}()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	require.Zero(t, discordSessionHandlerCount(client.session))

	require.Error(t, client.Tail(context.Background(), &recordingHandler{}))
	require.Zero(t, discordSessionHandlerCount(client.session))
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

func TestTailTaskMetadataIsNilSafe(t *testing.T) {
	t.Parallel()

	task := newMessageTailTask(
		"MESSAGE_UPDATE",
		nil,
		nil,
		&discordgo.Message{
			ID:        "m1",
			GuildID:   "g1",
			ChannelID: "c1",
			Author:    &discordgo.User{ID: "u1"},
		},
	)
	require.Equal(t, tailTask{
		eventType: "MESSAGE_UPDATE",
		guildID:   "g1",
		channelID: "c1",
		messageID: "m1",
		userID:    "u1",
	}, task)

	metadata := newTailFailureMetadata(tailTask{
		channelID: "c1",
		messageID: "m1",
	})
	metadataCtx := withTailFailureMetadata(context.Background(), metadata)
	EnrichTailFailureMetadata(metadataCtx, &discordgo.Message{
		ID:        "m1",
		GuildID:   "g-refetched",
		ChannelID: "c1",
		Author:    &discordgo.User{ID: "u-refetched"},
	})
	guildID, channelID, messageID, userID := metadata.snapshot()
	require.Equal(t, "g-refetched", guildID)
	require.Equal(t, "c1", channelID)
	require.Equal(t, "m1", messageID)
	require.Equal(t, "u-refetched", userID)
	emptyCtx := context.Background()
	EnrichTailFailureMetadata(emptyCtx, &discordgo.Message{GuildID: "ignored"})
	EnrichTailFailureMetadata(emptyCtx, nil)
	require.Equal(t, emptyCtx, withTailFailureMetadata(emptyCtx, nil))

	require.Equal(t, "c2", newChannelTailTask(
		"CHANNEL_UPDATE",
		nil,
		nil,
		&discordgo.Channel{ID: "c2"},
	).channelID)
	require.Equal(t, "u2", newMemberTailTask(
		"GUILD_MEMBER_UPDATE",
		nil,
		nil,
		&discordgo.Member{User: &discordgo.User{ID: "u2"}},
	).userID)
}

func TestTailFailureReportingSuppressesShutdownCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	handler := &failureContinuationHandler{
		failureReported: make(chan struct{}),
		failures:        make(chan TailFailure, 1),
	}
	reportTailFailure(ctx, handler, tailTask{eventType: "MESSAGE_CREATE"}, context.Canceled)
	require.Empty(t, handler.failures)
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
	workCh := make(chan tailTask)
	errCh := make(chan error, 1)
	task := tailTask{run: func(context.Context) error { return nil }}
	client.enqueueTailTask(ctx, workCh, errCh, task)
	require.Empty(t, errCh)

	ctx = context.Background()
	fullWorkCh := make(chan tailTask)
	client.enqueueTailTask(ctx, fullWorkCh, errCh, task)
	require.ErrorContains(t, <-errCh, "tail worker queue full")
	errCh <- errors.New("existing")
	client.enqueueTailTask(ctx, fullWorkCh, errCh, task)
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
	require.Zero(t, discordSessionHandlerCount(client.session))

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
	require.Zero(t, discordSessionHandlerCount(client.session))
}

func TestTailContinuesAfterHandlerFailure(t *testing.T) {
	tests := []struct {
		name     string
		fail     func(context.Context) error
		wantKind string
	}{
		{
			name: "returned error",
			fail: func(context.Context) error {
				return errors.New("sensitive returned error")
			},
			wantKind: "returned_error",
		},
		{
			name: "panic",
			fail: func(context.Context) error {
				panic("sensitive panic value")
			},
			wantKind: "panic",
		},
		{
			name: "timeout",
			fail: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantKind: "timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			handler := &failureContinuationHandler{
				fail:            tt.fail,
				cancel:          cancel,
				failureReported: make(chan struct{}),
				failures:        make(chan TailFailure, 1),
				laterHandled:    make(chan struct{}),
			}
			server := newTailTestGateway(t, func(conn *websocket.Conn) {
				now := time.Now().UTC().Format(time.RFC3339)
				if err := conn.WriteJSON(messageCreateEvent(2, "failed", now)); err != nil {
					t.Errorf("write failed event: %v", err)
					return
				}
				select {
				case <-handler.failureReported:
				case <-ctx.Done():
					t.Error("tail failure was not reported")
					return
				}
				if err := conn.WriteJSON(messageCreateEvent(3, "later", now)); err != nil {
					t.Errorf("write later event: %v", err)
					return
				}
				select {
				case <-handler.laterHandled:
				case <-ctx.Done():
					t.Error("later tail event was not handled")
				}
			})
			defer server.Close()

			restore := patchDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()

			client, err := New("token")
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			client.session.ShouldReconnectOnError = false
			client.tailWorkerCount = 1
			client.tailQueueSize = 1
			client.tailHandlerTimeout = 25 * time.Millisecond

			require.NoError(t, client.Tail(ctx, handler))
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			failure := <-handler.failures
			require.Equal(t, TailFailure{
				EventType: "MESSAGE_CREATE",
				Kind:      tt.wantKind,
				GuildID:   "g1",
				ChannelID: "c1",
				MessageID: "failed",
				UserID:    "u1",
			}, failure)
			select {
			case <-handler.laterHandled:
			default:
				t.Fatal("Tail returned before the later event was handled")
			}
		})
	}
}

func TestTailEscalatesAfterConsecutiveHandlerFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handler := &persistentFailureHandler{
		failures: make(chan TailFailure, defaultTailHandlerFailureLimit),
	}
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		for sequence := 0; sequence < defaultTailHandlerFailureLimit; sequence++ {
			messageID := fmt.Sprintf("failed-%d", sequence+1)
			if err := conn.WriteJSON(messageCreateEvent(sequence+2, messageID, now)); err != nil {
				t.Errorf("write failed event: %v", err)
				return
			}
			select {
			case <-handler.failures:
			case <-ctx.Done():
				t.Error("tail failure was not reported")
				return
			}
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = defaultTailHandlerFailureLimit

	err = client.Tail(ctx, handler)
	require.ErrorContains(
		t,
		err,
		"tail handler circuit breaker opened after 3 consecutive failures",
	)
	require.NoError(t, ctx.Err())
	require.EqualValues(t, defaultTailHandlerFailureLimit, handler.calls.Load())
}

func TestTailFailureCircuitResetsAfterSuccess(t *testing.T) {
	t.Parallel()

	circuit := &tailFailureCircuit{limit: 2}
	require.False(t, circuit.recordFailure())
	circuit.recordSuccess()
	require.False(t, circuit.recordFailure())
	require.True(t, circuit.recordFailure())
	require.False(t, circuit.recordFailure())
}

func TestTailMessageUpdateFailureUsesRefetchedMetadata(t *testing.T) {
	tests := []struct {
		name     string
		fail     func(context.Context) error
		wantKind string
	}{
		{
			name: "returned error",
			fail: func(context.Context) error {
				return errors.New("sensitive returned error")
			},
			wantKind: "returned_error",
		},
		{
			name: "panic",
			fail: func(context.Context) error {
				panic("sensitive panic value")
			},
			wantKind: "panic",
		},
		{
			name: "timeout",
			fail: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantKind: "timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			handler := &messageUpdateFailureHandler{
				fail:            tt.fail,
				cancel:          cancel,
				failureReported: make(chan struct{}),
				failures:        make(chan TailFailure, 1),
				updates:         make(chan *discordgo.Message, 1),
			}
			now := time.Now().UTC().Format(time.RFC3339)
			server := newTailTestGatewayWithRoutes(
				t,
				func(mux *http.ServeMux) {
					mux.HandleFunc("/api/v10/channels/c1/messages/m1", writeJSON(map[string]any{
						"id":         "m1",
						"guild_id":   "g-refetched",
						"channel_id": "c1",
						"content":    "full message",
						"timestamp":  now,
						"author":     map[string]any{"id": "u-refetched", "username": "test-user"},
					}))
				},
				func(conn *websocket.Conn) {
					if err := conn.WriteJSON(messageUpdateEvent(2, "m1", now)); err != nil {
						t.Errorf("write update event: %v", err)
						return
					}
					select {
					case <-handler.failureReported:
					case <-ctx.Done():
						t.Error("tail failure was not reported")
					}
				},
			)
			defer server.Close()

			restore := patchDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()

			client, err := New("token")
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			client.session.ShouldReconnectOnError = false
			client.tailWorkerCount = 1
			client.tailQueueSize = 1
			client.tailHandlerTimeout = 25 * time.Millisecond

			require.NoError(t, client.Tail(ctx, handler))
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			update := <-handler.updates
			require.Equal(t, "g-refetched", update.GuildID)
			require.Equal(t, "u-refetched", update.Author.ID)
			require.Equal(t, TailFailure{
				EventType: "MESSAGE_UPDATE",
				Kind:      tt.wantKind,
				GuildID:   "g-refetched",
				ChannelID: "c1",
				MessageID: "m1",
				UserID:    "u-refetched",
			}, <-handler.failures)
		})
	}
}

func TestTailReadyHandlerFailureRemainsTerminal(t *testing.T) {
	gatewayDone := make(chan struct{})
	server := newTailTestGateway(t, func(*websocket.Conn) {
		<-gatewayDone
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false

	readyErr := errors.New("ready failed")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = client.Tail(ctx, &readyFailureHandler{err: readyErr})
	close(gatewayDone)
	require.ErrorIs(t, err, readyErr)
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

func newTailTestGateway(t *testing.T, afterReady func(*websocket.Conn)) *httptest.Server {
	return newTailTestGatewayWithRoutes(t, nil, afterReady)
}

func newTailTestGatewayWithRoutes(
	t *testing.T,
	addRoutes func(*http.ServeMux),
	afterReady func(*websocket.Conn),
) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	if addRoutes != nil {
		addRoutes(mux)
	}
	upgrader := websocket.Upgrader{}
	mux.HandleFunc("/api/v10/gateway", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"url": "ws://" + r.Host + "/gateway"})
	})
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
		if _, _, err := conn.ReadMessage(); err != nil {
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
		afterReady(conn)
	}
	mux.HandleFunc("/gateway", gatewayHandler)
	mux.HandleFunc("/gateway/", gatewayHandler)
	return httptest.NewServer(mux)
}

func messageCreateEvent(sequence int, messageID, timestamp string) map[string]any {
	return map[string]any{
		"op": 0,
		"t":  "MESSAGE_CREATE",
		"s":  sequence,
		"d": map[string]any{
			"id":         messageID,
			"guild_id":   "g1",
			"channel_id": "c1",
			"content":    "safe test content",
			"timestamp":  timestamp,
			"author":     map[string]any{"id": "u1", "username": "test-user"},
		},
	}
}

func messageUpdateEvent(sequence int, messageID, timestamp string) map[string]any {
	return map[string]any{
		"op": 0,
		"t":  "MESSAGE_UPDATE",
		"s":  sequence,
		"d": map[string]any{
			"id":         messageID,
			"channel_id": "c1",
			"content":    "",
			"timestamp":  timestamp,
		},
	}
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

func discordSessionHandlerCount(session *discordgo.Session) int {
	handlers := reflect.ValueOf(session).Elem().FieldByName("handlers")
	total := 0
	for _, key := range handlers.MapKeys() {
		total += handlers.MapIndex(key).Len()
	}
	return total
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

type failureContinuationHandler struct {
	fail            func(context.Context) error
	cancel          context.CancelFunc
	failureReported chan struct{}
	failures        chan TailFailure
	laterHandled    chan struct{}
	failureOnce     sync.Once
	laterOnce       sync.Once
}

func (h *failureContinuationHandler) OnTailFailure(failure TailFailure) {
	h.failureOnce.Do(func() {
		h.failures <- failure
		close(h.failureReported)
	})
}

func (h *failureContinuationHandler) OnMessageCreate(ctx context.Context, msg *discordgo.Message) error {
	if msg == nil {
		return nil
	}
	switch msg.ID {
	case "failed":
		return h.fail(ctx)
	case "later":
		h.laterOnce.Do(func() {
			close(h.laterHandled)
			h.cancel()
		})
	}
	return nil
}

func (h *failureContinuationHandler) OnMessageUpdate(context.Context, *discordgo.Message) error {
	return nil
}

func (h *failureContinuationHandler) OnMessageDelete(context.Context, *discordgo.MessageDelete) error {
	return nil
}

func (h *failureContinuationHandler) OnChannelUpsert(context.Context, *discordgo.Channel) error {
	return nil
}

func (h *failureContinuationHandler) OnMemberUpsert(context.Context, string, *discordgo.Member) error {
	return nil
}

func (h *failureContinuationHandler) OnMemberDelete(context.Context, string, string) error {
	return nil
}

type persistentFailureHandler struct {
	recordingHandler
	failures chan TailFailure
	calls    atomic.Int32
}

func (h *persistentFailureHandler) OnTailFailure(failure TailFailure) {
	h.failures <- failure
}

func (h *persistentFailureHandler) OnMessageCreate(context.Context, *discordgo.Message) error {
	h.calls.Add(1)
	return errors.New("persistent handler failure")
}

type readyFailureHandler struct {
	recordingHandler
	err error
}

func (h *readyFailureHandler) OnTailReady(context.Context) error {
	return h.err
}

type messageUpdateFailureHandler struct {
	recordingHandler
	fail            func(context.Context) error
	cancel          context.CancelFunc
	failureReported chan struct{}
	failures        chan TailFailure
	updates         chan *discordgo.Message
	failureOnce     sync.Once
}

func (h *messageUpdateFailureHandler) OnTailFailure(failure TailFailure) {
	h.failureOnce.Do(func() {
		h.failures <- failure
		close(h.failureReported)
		h.cancel()
	})
}

func (h *messageUpdateFailureHandler) OnMessageUpdate(ctx context.Context, msg *discordgo.Message) error {
	h.updates <- msg
	return h.fail(ctx)
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
