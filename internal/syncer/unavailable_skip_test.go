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

func TestFullSyncAttemptsChannelWithFreshUnavailableMarker(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "blocked", GuildID: "g1", Kind: "text", Name: "mods", RawJSON: `{}`}))
	require.NoError(t, s.SetSyncState(ctx, channelMessageUnavailableScope("blocked"), "missing_access"))

	blocked := &discordgo.Channel{ID: "blocked", GuildID: "g1", Name: "mods", Type: discordgo.ChannelTypeGuildText, LastMessageID: "10"}
	channels := []*discordgo.Channel{blocked}
	client := &fakeClient{messages: map[string][]*discordgo.Message{}}
	svc := New(client, s, nil)

	// A routine run passes over the channel while the marker is inside the window.
	_, err = svc.syncMessageChannels(ctx, "g1", channels, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, 0, client.messageCalls["blocked"])

	// A full run asks for the complete job, so the same marker does not hold it
	// back: this is the path that picks up a channel whose access was restored
	// without waiting out the window.
	_, err = svc.syncMessageChannels(ctx, "g1", channels, SyncOptions{Full: true})
	require.NoError(t, err)
	require.Equal(t, 1, client.messageCalls["blocked"], "a full sync must attempt a channel carrying a fresh marker")

	// The successful read clears the marker, so routine syncs resume too.
	reason, err := s.GetSyncState(ctx, channelMessageUnavailableScope("blocked"))
	require.NoError(t, err)
	require.Empty(t, reason)
	_, err = svc.syncMessageChannels(ctx, "g1", channels, SyncOptions{})
	require.NoError(t, err)
	require.Equal(t, 2, client.messageCalls["blocked"])
}

// Both cached channels must be retried even when absent from the live catalog.
func TestFullSyncEntrypointReachesChannelWithFreshUnavailableMarker(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	// 'order by c.id' lists the fresh-marked channel first, so a pass cannot be
	// an artifact of the expired one happening to sort ahead of it.
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "restored", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "still-blocked", RawJSON: `{}`}))
	require.NoError(t, s.SetSyncState(ctx, channelMessageUnavailableScope("c1"), "missing_access"))
	require.NoError(t, s.SetSyncState(ctx, channelMessageUnavailableScope("c2"), "missing_access"))
	backdateMarker(ctx, t, dbPath, channelMessageUnavailableScope("c2"), 8*24*time.Hour)

	client := &fakeClient{
		guilds:    []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Guild"}},
		channelByID: map[string]*discordgo.Channel{
			"c1": {ID: "c1", GuildID: "g1", Name: "restored", Type: discordgo.ChannelTypeGuildText, LastMessageID: "10"},
			"c2": {ID: "c2", GuildID: "g1", Name: "still-blocked", Type: discordgo.ChannelTypeGuildText, LastMessageID: "10"},
		},
		messages: map[string][]*discordgo.Message{},
	}
	svc := New(client, s, nil)

	_, err = svc.Sync(ctx, SyncOptions{Full: true})
	require.NoError(t, err)
	require.Equal(t, 1, client.messageCalls["c2"], "an expired marker must be planned into a full sync")
	require.Equal(t, 1, client.messageCalls["c1"], "a full sync must reach a channel carrying a fresh marker")

	// Both reads succeeded, so both markers are gone and routine syncs resume.
	for _, id := range []string{"c1", "c2"} {
		reason, err := s.GetSyncState(ctx, channelMessageUnavailableScope(id))
		require.NoError(t, err)
		require.Empty(t, reason, "a successful read must clear the marker on %s", id)
	}
}

func TestFullSyncUnavailableBacklogDoesNotStarveCatalog(t *testing.T) {
	for _, age := range []time.Duration{time.Hour, 8 * 24 * time.Hour} {
		t.Run(age.String(), func(t *testing.T) {
			ctx := t.Context()
			dbPath := filepath.Join(t.TempDir(), "archive.db")
			st, err := store.Open(ctx, dbPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{ID: "blocked", GuildID: "g1", Kind: "text", Name: "blocked", RawJSON: `{}`}))
			require.NoError(t, st.SetSyncState(ctx, channelMessageUnavailableScope("blocked"), "missing_access"))
			backdateMarker(ctx, t, dbPath, channelMessageUnavailableScope("blocked"), age)
			client := &fakeClient{
				guilds:    []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
				guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Guild"}},
				channels: map[string][]*discordgo.Channel{"g1": {
					{ID: "blocked", GuildID: "g1", Name: "blocked", Type: discordgo.ChannelTypeGuildText},
					{ID: "new", GuildID: "g1", Name: "new", Type: discordgo.ChannelTypeGuildText, LastMessageID: "20"},
				}},
				messageErrors: map[string]error{"blocked": errors.New("HTTP 403 Forbidden: Missing Access")},
				messages:      map[string][]*discordgo.Message{"new": {{ID: "20", GuildID: "g1", ChannelID: "new", Content: "discovered checklist", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "fixture"}}}},
			}
			svc := New(client, st, nil)
			for run := 1; run <= 2; run++ {
				stats, err := svc.Sync(ctx, SyncOptions{Full: true, SkipMembers: true, Concurrency: 1})
				require.NoError(t, err)
				require.Equal(t, run, client.guildChanCalls, "unavailable-only backlog must not suppress discovery")
				require.Equal(t, run, client.messageCalls["blocked"], "explicit full sync retries each marked channel once")
				require.Equal(t, 2, stats.Channels, "a channel visited during resume and discovery is counted once")
				results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "discovered", Limit: 10})
				require.NoError(t, err)
				require.Len(t, results, 1)
			}
		})
	}
}

func TestFullSyncRetriesUnavailableCompletedChannelAlongsideBackfill(t *testing.T) {
	ctx := t.Context()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	for _, id := range []string{"complete", "resume"} {
		require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{ID: id, GuildID: "g1", Kind: "text", Name: id, RawJSON: `{}`}))
	}
	require.NoError(t, st.UpsertMessage(ctx, store.MessageRecord{ID: "10", GuildID: "g1", ChannelID: "complete", AuthorID: "u1", Content: "existing", NormalizedContent: "existing", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), RawJSON: `{}`}))
	require.NoError(t, st.SetSyncState(ctx, channelHistoryCompleteScope("complete"), "1"))
	require.NoError(t, st.SetSyncState(ctx, channelLatestScope("complete"), "10"))
	require.NoError(t, st.SetSyncState(ctx, channelMessageUnavailableScope("complete"), "missing_access"))
	client := &fakeClient{
		guilds:    []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Guild"}},
		messages:  map[string][]*discordgo.Message{},
	}
	_, err = New(client, st, nil).Sync(ctx, SyncOptions{Full: true, SkipMembers: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 1, client.messageCalls["complete"], "a completed history must not hide an explicit permission retry")
	require.Equal(t, 1, client.messageCalls["resume"])
	require.Zero(t, client.guildChanCalls, "ordinary cached backfill keeps its fast path")
	marker, err := st.GetSyncState(ctx, channelMessageUnavailableScope("complete"))
	require.NoError(t, err)
	require.Empty(t, marker)
}
