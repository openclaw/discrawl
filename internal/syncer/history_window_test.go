package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestFullSinceRecordsEmptyWindowWithoutCompletingOlderHistory(t *testing.T) {
	for _, olderMessage := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "older-only"}[olderMessage], func(t *testing.T) {
			ctx := context.Background()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			since := time.Now().UTC().Add(-24 * time.Hour)
			channel := &discordgo.Channel{ID: "c1", GuildID: "g1", Name: "fixture", Type: discordgo.ChannelTypeGuildText}
			client := &fakeClient{
				guilds:    []*discordgo.UserGuild{{ID: "g1", Name: "fixture"}},
				guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "fixture"}},
				channels:  map[string][]*discordgo.Channel{"g1": {channel}},
				messages:  map[string][]*discordgo.Message{},
			}
			if olderMessage {
				client.messages["c1"] = []*discordgo.Message{{ID: "101", GuildID: "g1", ChannelID: "c1", Timestamp: since.Add(-time.Hour), Author: &discordgo.User{ID: "u1"}}}
			}
			stats, err := New(client, db, nil).Sync(ctx, SyncOptions{Full: true, Since: since, SkipMembers: true, Concurrency: 1})
			require.NoError(t, err)
			require.Zero(t, stats.Messages)
			markers, err := db.SyncStateBySuffix(ctx, ":latest_message_id")
			require.NoError(t, err)
			require.Len(t, markers, 1, "the exact full route retains an empty-valued latest checkpoint")
			proof, err := db.GetSyncState(ctx, channelHistoryWindowScope("c1", since))
			require.NoError(t, err)
			require.NotEmpty(t, proof, "a successful bounded traversal must retain its actual window, including zero results")
			var window HistoryWindow
			require.NoError(t, json.Unmarshal([]byte(proof), &window))
			require.Equal(t, since, window.Since)
			require.Equal(t, "g1", window.GuildID)
			require.Equal(t, "c1", window.ChannelID)
			require.Equal(t, int(discordgo.ChannelTypeGuildText), window.ChannelType)
			complete, err := db.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
			require.NoError(t, err)
			require.Empty(t, complete, "a window proof does not change full-history completion")
			repo := filepath.Join(t.TempDir(), "snapshot")
			_, err = share.Export(ctx, db, share.Options{RepoPath: repo})
			require.NoError(t, err)
			imported, err := store.Open(ctx, filepath.Join(t.TempDir(), "imported.db"))
			require.NoError(t, err)
			defer func() { _ = imported.Close() }()
			_, err = share.Import(ctx, imported, share.Options{RepoPath: repo})
			require.NoError(t, err)
			roundTrip, err := imported.GetSyncState(ctx, channelHistoryWindowScope("c1", since))
			require.NoError(t, err)
			require.Equal(t, proof, roundTrip)
		})
	}
}

type windowPermissionClient struct {
	*fakeClient
	calls  int
	denyAt int
}

func (f *windowPermissionClient) CanReadHistory(context.Context, *discordgo.Channel) (bool, error) {
	f.calls++
	return f.calls != f.denyAt, nil
}

func TestHistoryWindowFailurePreservesPreviousProof(t *testing.T) {
	for _, mode := range []string{"permission-before", "permission-after", "api", "persistence", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			require.NoError(t, db.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "fixture", RawJSON: `{}`}))
			require.NoError(t, db.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "fixture", RawJSON: `{}`}))
			since := time.Now().UTC().Add(-time.Hour)
			previous := `{"retained":"prior immutable window"}`
			require.NoError(t, db.SetSyncState(ctx, channelHistoryWindowScope("c1", since), previous))
			client := &windowPermissionClient{fakeClient: &fakeClient{messages: map[string][]*discordgo.Message{}}}
			switch mode {
			case "permission-before":
				client.denyAt = 1
			case "permission-after":
				client.denyAt = 2
			case "api":
				client.messageErrors = map[string]error{"c1": errors.New("fixture API failure")}
			case "persistence":
				client.messages["c1"] = []*discordgo.Message{{ID: "101", GuildID: "g1", ChannelID: "c1", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1"}}}
				_, err = db.DB().ExecContext(ctx, `create trigger reject_fixture before insert on messages begin select raise(abort, 'fixture persistence failure'); end`)
				require.NoError(t, err)
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			_, err = New(client, db, nil).syncWindowChannelHistory(ctx, &discordgo.Channel{ID: "c1", GuildID: "g1", Type: discordgo.ChannelTypeGuildText}, channelSyncState{}, false, since, nil)
			require.Error(t, err)
			actual, err := db.GetSyncState(context.Background(), channelHistoryWindowScope("c1", since))
			require.NoError(t, err)
			require.Equal(t, previous, actual)
			complete, err := db.GetSyncState(context.Background(), channelHistoryCompleteScope("c1"))
			require.NoError(t, err)
			require.Empty(t, complete)
		})
	}
}

func TestHistoryWindowsRetainDistinctBoundsAndIgnoreLatestOnly(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	channel := &discordgo.Channel{ID: "c1", GuildID: "g1", Type: discordgo.ChannelTypeGuildText}
	client := &windowPermissionClient{fakeClient: &fakeClient{messages: map[string][]*discordgo.Message{}}}
	svc := New(client, db, nil)
	first := time.Now().UTC().Add(-48 * time.Hour)
	second := first.Add(24 * time.Hour)
	_, err = svc.syncWindowChannelHistory(ctx, channel, channelSyncState{}, false, first, nil)
	require.NoError(t, err)
	before, err := db.GetSyncState(ctx, channelHistoryWindowScope("c1", first))
	require.NoError(t, err)
	_, err = svc.syncWindowChannelHistory(ctx, channel, channelSyncState{}, false, second, nil)
	require.NoError(t, err)
	after, err := db.GetSyncState(ctx, channelHistoryWindowScope("c1", first))
	require.NoError(t, err)
	require.Equal(t, before, after, "a distinct lower bound cannot replace or union the prior interval")
	other, err := db.GetSyncState(ctx, channelHistoryWindowScope("c1", second))
	require.NoError(t, err)
	require.NotEmpty(t, other)
	_, err = svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	unchanged, err := db.GetSyncState(ctx, channelHistoryWindowScope("c1", second))
	require.NoError(t, err)
	require.Equal(t, other, unchanged, "latest-only cannot extend a bounded certificate")
	var previous HistoryWindow
	require.NoError(t, json.Unmarshal([]byte(other), &previous))
	_, err = svc.syncWindowChannelHistory(ctx, channel, channelSyncState{}, false, second, nil)
	require.NoError(t, err)
	extended, err := db.GetSyncState(ctx, channelHistoryWindowScope("c1", second))
	require.NoError(t, err)
	var current HistoryWindow
	require.NoError(t, json.Unmarshal([]byte(extended), &current))
	require.Equal(t, previous.Since, current.Since)
	require.False(t, current.Until.Before(previous.Until))
}

func TestHistoryWindowFillsSparseRowsWithoutResettingHistoricalCursor(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, db.UpsertGuild(ctx, store.GuildRecord{ID: "g1", RawJSON: `{}`}))
	require.NoError(t, db.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", RawJSON: `{}`}))
	require.NoError(t, db.UpsertMessage(ctx, store.MessageRecord{ID: "100", GuildID: "g1", ChannelID: "c1", CreatedAt: time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339Nano), RawJSON: `{}`}))
	require.NoError(t, db.SetSyncState(ctx, channelLatestScope("c1"), "100"))
	require.NoError(t, db.SetSyncState(ctx, channelBackfillScope("c1"), "50"))
	require.NoError(t, db.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))
	client := &windowPermissionClient{fakeClient: &fakeClient{messages: map[string][]*discordgo.Message{"c1": {
		{ID: "200", GuildID: "g1", ChannelID: "c1", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1"}},
		{ID: "150", GuildID: "g1", ChannelID: "c1", Timestamp: time.Now().UTC().Add(-time.Hour), Author: &discordgo.User{ID: "u1"}},
	}}}}
	_, err = New(client, db, nil).syncChannelMessages(ctx, "g1", &discordgo.Channel{ID: "c1", GuildID: "g1", Type: discordgo.ChannelTypeGuildText}, true, false, time.Now().UTC().Add(-24*time.Hour), false, nil)
	require.NoError(t, err)
	backfill, err := db.GetSyncState(ctx, channelBackfillScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "50", backfill)
	complete, err := db.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)
	var count int
	require.NoError(t, db.DB().QueryRowContext(ctx, "select count(*) from messages where channel_id='c1'").Scan(&count))
	require.Equal(t, 3, count)
	require.Empty(t, client.messageRequests[0].beforeID, "a new window starts at the live head, not the retained cursor")
}

func TestHistoryWindowSecondPageFailureKeepsPriorProofAndCursor(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, db.UpsertGuild(ctx, store.GuildRecord{ID: "g1", RawJSON: `{}`}))
	require.NoError(t, db.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", RawJSON: `{}`}))
	since := time.Now().UTC().Add(-time.Hour)
	require.NoError(t, db.SetSyncState(ctx, channelHistoryWindowScope("c1", since), "retained-proof"))
	require.NoError(t, db.SetSyncState(ctx, channelBackfillScope("c1"), "50"))
	var messages []*discordgo.Message
	for id := 200; id >= 101; id-- {
		messages = append(messages, &discordgo.Message{ID: strconv.Itoa(id), GuildID: "g1", ChannelID: "c1", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1"}})
	}
	client := &windowPermissionClient{fakeClient: &fakeClient{messages: map[string][]*discordgo.Message{"c1": messages}, beforeErrors: map[string]map[string]error{"c1": {"101": context.DeadlineExceeded}}}}
	_, err = New(client, db, nil).syncWindowChannelHistory(ctx, &discordgo.Channel{ID: "c1", GuildID: "g1", Type: discordgo.ChannelTypeGuildText}, channelSyncState{}, false, since, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	proof, err := db.GetSyncState(ctx, channelHistoryWindowScope("c1", since))
	require.NoError(t, err)
	require.Equal(t, "retained-proof", proof)
	backfill, err := db.GetSyncState(ctx, channelBackfillScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "50", backfill)
}
