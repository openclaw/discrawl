package syncer

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

const productionTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// backdateMarker rewrites a marker's updated_at in the on-disk format the store
// produces, so the retry window can be exercised without waiting a week.
func backdateMarker(ctx context.Context, t *testing.T, dbPath, scope string, age time.Duration) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	res, err := db.ExecContext(ctx,
		`update sync_state set updated_at = ? where scope = ?`,
		time.Now().UTC().Add(-age).Format(productionTimeLayout), scope)
	require.NoError(t, err)
	rows, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows, "marker %s must exist before backdating", scope)
}

func TestRoutineSyncSkipsFreshUnavailableMarkerAndRetriesAfterWindow(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "blocked", GuildID: "g1", Kind: "text", Name: "mods", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "open", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))

	blocked := &discordgo.Channel{ID: "blocked", GuildID: "g1", Name: "mods", Type: discordgo.ChannelTypeGuildText, LastMessageID: "10"}
	open := &discordgo.Channel{ID: "open", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText, LastMessageID: "10"}
	channels := []*discordgo.Channel{blocked, open}

	client := &fakeClient{
		messageErrors: map[string]error{"blocked": errors.New(`HTTP 403 Forbidden, {"message": "Missing Access", "code": 50001}`)},
		messages:      map[string][]*discordgo.Message{},
	}
	svc := New(client, s, nil)

	// Run 1: no marker yet, so the channel is attempted and the 403 marks it.
	_, err = svc.syncMessageChannels(ctx, "g1", channels, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, client.messageCalls["blocked"])
	reason, err := s.GetSyncState(ctx, channelMessageUnavailableScope("blocked"))
	require.NoError(t, err)
	require.Equal(t, "missing_access", reason)

	// Run 2: the marker is fresh, so the channel is not attempted at all.
	_, err = svc.syncMessageChannels(ctx, "g1", channels, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, client.messageCalls["blocked"], "a fresh marker must skip the channel entirely")
	require.Equal(t, 2, client.messageCalls["open"], "unmarked channels must still sync")

	// Run 3: the marker has aged past the window, so the channel is retried and
	// the still-failing request refreshes the marker for another window.
	backdateMarker(ctx, t, dbPath, channelMessageUnavailableScope("blocked"), 8*24*time.Hour)
	_, err = svc.syncMessageChannels(ctx, "g1", channels, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, 2, client.messageCalls["blocked"], "an expired marker must be retried once")

	fresh, err := s.FreshUnavailableChannelIDs(ctx)
	require.NoError(t, err)
	require.Contains(t, fresh, "blocked", "a repeated failure must refresh the window")

	// Run 4: refreshed marker skips again.
	_, err = svc.syncMessageChannels(ctx, "g1", channels, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, 2, client.messageCalls["blocked"])

	// Run 5: access is restored. The expired marker lets the retry through, the
	// sync succeeds, and clearUnavailableChannel deletes the marker.
	backdateMarker(ctx, t, dbPath, channelMessageUnavailableScope("blocked"), 8*24*time.Hour)
	delete(client.messageErrors, "blocked")
	_, err = svc.syncMessageChannels(ctx, "g1", channels, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, 3, client.messageCalls["blocked"])
	reason, err = s.GetSyncState(ctx, channelMessageUnavailableScope("blocked"))
	require.NoError(t, err)
	require.Empty(t, reason, "a successful sync must clear the marker")

	// Run 6: with the marker gone the channel syncs normally every run.
	_, err = svc.syncMessageChannels(ctx, "g1", channels, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, 4, client.messageCalls["blocked"])
}

func TestExplicitChannelRequestIgnoresUnavailableMarker(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "blocked", GuildID: "g1", Kind: "text", Name: "mods", RawJSON: `{}`}))
	require.NoError(t, s.SetSyncState(ctx, channelMessageUnavailableScope("blocked"), "missing_access"))

	blocked := &discordgo.Channel{ID: "blocked", GuildID: "g1", Name: "mods", Type: discordgo.ChannelTypeGuildText, LastMessageID: "10"}
	client := &fakeClient{messages: map[string][]*discordgo.Message{}}
	svc := New(client, s, nil)

	// Naming the channel explicitly must attempt it despite the fresh marker.
	_, err = svc.syncMessageChannels(ctx, "g1", []*discordgo.Channel{blocked}, SyncOptions{ChannelIDs: []string{"blocked"}})
	require.NoError(t, err)
	require.Equal(t, 1, client.messageCalls["blocked"])
}

func TestFilterFreshUnavailableChannelsDegradesSafely(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	channels := []*discordgo.Channel{{ID: "c1"}}

	// No syncer, no store, and no channels are all pass-through.
	out, err := (*Syncer)(nil).filterFreshUnavailableChannels(ctx, "g1", channels, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, channels, out)

	svc := New(&fakeClient{}, nil, nil)
	out, err = svc.filterFreshUnavailableChannels(ctx, "g1", channels, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, channels, out)

	out, err = svc.filterFreshUnavailableChannels(ctx, "g1", nil, SyncOptions{})
	require.NoError(t, err)
	require.Empty(t, out)
}
