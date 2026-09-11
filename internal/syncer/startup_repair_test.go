package syncer

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

type startupTailClient struct {
	*fakeClient
	serve func(context.Context, discordclient.EventHandler) error
}

func (c *startupTailClient) Tail(ctx context.Context, handler discordclient.EventHandler) error {
	return c.serve(ctx, handler)
}

func TestStartupRepairRecoversGapWhileCaptureContinues(t *testing.T) {
	for _, interval := range []time.Duration{0, 6 * time.Hour} {
		for _, liveFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/live-first=%t", interval, liveFirst), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
				require.NoError(t, err)
				defer func() { _ = s.Close() }()
				require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{ID: "1", GuildID: "g1", ChannelID: "c1", Content: "before restart", NormalizedContent: "before restart"}))
				require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "1"))
				blocked := make(chan struct{})
				client := &startupTailClient{fakeClient: &fakeClient{
					guilds:         []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
					guildByID:      map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Guild"}},
					channels:       map[string][]*discordgo.Channel{"g1": {{ID: "c1", GuildID: "g1", Type: discordgo.ChannelTypeGuildText, LastMessageID: "11"}}},
					messages:       map[string][]*discordgo.Message{"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "offline message", Timestamp: time.Now(), Author: &discordgo.User{ID: "u1"}}}},
					messageBlocks:  map[string]chan struct{}{"c1": blocked},
					messageStarted: make(chan string, 1),
				}}
				captured := make(chan struct{})
				client.serve = func(ctx context.Context, handler discordclient.EventHandler) error {
					capture := func() error {
						if err := handler.OnMessageCreate(ctx, &discordgo.Message{ID: "11", GuildID: "g1", ChannelID: "c1", Content: "live message", Timestamp: time.Now(), Author: &discordgo.User{ID: "u1"}}); err != nil {
							return err
						}
						close(captured)
						return nil
					}
					if liveFirst {
						if err := capture(); err != nil {
							return err
						}
					}
					if err := handler.(discordclient.TailReadyHandler).OnTailReady(ctx); err != nil {
						return err
					}
					select {
					case <-client.messageStarted:
					case <-ctx.Done():
						return ctx.Err()
					}
					// REST is blocked, but the connected Gateway still writes events.
					if !liveFirst {
						if err := capture(); err != nil {
							return err
						}
					}
					close(blocked)
					<-ctx.Done()
					return nil
				}
				svc := New(client, s, nil)
				svc.SetTailRepairOnStart(true)
				svc.SetTailEmbeddings(true)
				done := make(chan error, 1)
				go func() { defer close(done); done <- svc.RunTail(ctx, []string{"g1"}, interval) }()
				defer func() { cancel(); <-done }()
				select {
				case <-captured:
				case <-ctx.Done():
					t.Fatal("capture or immediate startup repair did not progress")
				}
				require.Eventually(t, func() bool {
					var count int
					_ = s.DB().QueryRowContext(ctx, `select count(*) from messages where id in ('10','11')`).Scan(&count)
					return count == 2
				}, 2*time.Second, 10*time.Millisecond)
				cancel()
				require.NoError(t, <-done)
				var queued int
				require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from embedding_jobs where message_id in ('10','11')`).Scan(&queued))
				require.Equal(t, 2, queued)
			})
		}
	}
}

func TestStartupRepairWaitsForReadyAndJoinsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	connected, allowReady, started, joined := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	client := &startupTailClient{fakeClient: &fakeClient{}, serve: func(ctx context.Context, h discordclient.EventHandler) error {
		close(connected)
		select {
		case <-allowReady:
		case <-ctx.Done():
			return nil
		}
		if err := h.(discordclient.TailReadyHandler).OnTailReady(ctx); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}}
	svc := New(client, s, nil)
	svc.SetTailRepairOnStart(true)
	var ready atomic.Bool
	svc.SetTailReadyCallback(func(context.Context) error { ready.Store(true); return nil })
	svc.tailRepair = func(ctx context.Context, opts SyncOptions) (SyncStats, error) {
		if !ready.Load() {
			t.Error("repair started before ownership-ready callback")
		}
		close(started)
		<-ctx.Done()
		close(joined)
		return SyncStats{}, ctx.Err()
	}
	done := make(chan error, 1)
	go func() { done <- svc.RunTail(ctx, nil, time.Millisecond) }()
	<-connected
	select {
	case <-started:
		t.Fatal("repair ran before Gateway ready")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowReady)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("startup repair did not start")
	}
	cancel()
	require.NoError(t, <-done)
	select {
	case <-joined:
	default:
		t.Fatal("repair was not joined before return")
	}
}

func TestStartupRepairCursorSurvivesInterruptedOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := store.Open(t.Context(), path)
	require.NoError(t, err)
	require.NoError(t, s.SetSyncState(t.Context(), channelLatestScope("c1"), "1"))
	handler := &tailHandler{store: s, preserveHistoryCursor: true}
	message := &discordgo.Message{ID: "11", GuildID: "g1", ChannelID: "c1", Content: "live before repair", Timestamp: time.Now(), Author: &discordgo.User{ID: "u1"}}
	require.NoError(t, handler.OnMessageCreate(t.Context(), message))
	cursor, err := s.GetSyncState(t.Context(), channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", cursor)
	freshness, err := s.GetSyncState(t.Context(), "tail:last_event")
	require.NoError(t, err)
	require.Equal(t, "11", freshness)
	// The first owner exits before REST repair can persist anything.
	require.NoError(t, s.Close())
	s, err = store.Open(t.Context(), path)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client := &startupTailClient{fakeClient: &fakeClient{
		guilds:    []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Guild"}},
		channels:  map[string][]*discordgo.Channel{"g1": {{ID: "c1", GuildID: "g1", Type: discordgo.ChannelTypeGuildText, LastMessageID: "11"}}},
		messages: map[string][]*discordgo.Message{"c1": {
			message,
			{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "missed offline", Timestamp: time.Now(), Author: &discordgo.User{ID: "u1"}},
		}},
	}, serve: func(ctx context.Context, h discordclient.EventHandler) error {
		if err := h.(discordclient.TailReadyHandler).OnTailReady(ctx); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}}
	svc := New(client, s, nil)
	svc.SetTailRepairOnStart(true)
	done := make(chan error, 1)
	go func() { defer close(done); done <- svc.RunTail(ctx, []string{"g1"}, 6*time.Hour) }()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool {
		var count int
		_ = s.DB().QueryRowContext(ctx, `select count(*) from messages where id in ('10','11')`).Scan(&count)
		cursor, _ := s.GetSyncState(ctx, channelLatestScope("c1"))
		return count == 2 && cursor == "11"
	}, 2*time.Second, 10*time.Millisecond)
	// Later live events still cannot claim unverified history coverage.
	handler.store = s
	later := *message
	later.ID = "12"
	require.NoError(t, handler.OnMessageCreate(ctx, &later))
	cursor, err = s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "11", cursor)
	cancel()
	require.NoError(t, <-done)
}
