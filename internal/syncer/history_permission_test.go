package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestHistoryPermissionDenialExportsUnavailableAndRecovers(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		for _, denyAt := range []int{1, 2} {
			t.Run(fmt.Sprintf("workers-%d/check-%d", concurrency, denyAt), func(t *testing.T) {
				ctx := t.Context()
				db, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				require.NoError(t, db.UpsertGuild(ctx, store.GuildRecord{ID: "g1", RawJSON: `{}`}))
				require.NoError(t, db.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", RawJSON: `{}`}))
				channel := &discordgo.Channel{ID: "c1", GuildID: "g1", Type: discordgo.ChannelTypeGuildText}
				client := &windowPermissionClient{fakeClient: &fakeClient{messages: map[string][]*discordgo.Message{}}}
				svc := New(client, db, nil)
				since := time.Now().UTC().Add(-time.Hour)
				opts := SyncOptions{Full: true, Since: since, Concurrency: concurrency}
				run := func() {
					t.Helper()
					_, err := svc.syncMessageChannels(ctx, "g1", []*discordgo.Channel{channel}, opts)
					require.NoError(t, err)
				}
				run()
				scope := channelHistoryWindowScope("c1", since)
				previous, err := db.GetSyncState(ctx, scope)
				require.NoError(t, err)
				var proof HistoryWindow
				require.NoError(t, json.Unmarshal([]byte(previous), &proof))
				require.Equal(t, since, proof.Since)

				client.denyAt = client.calls + denyAt
				run()
				unavailable, err := db.GetSyncState(ctx, channelMessageUnavailableScope("c1"))
				require.NoError(t, err)
				require.Equal(t, "missing_history_permissions", unavailable)
				retained, err := db.GetSyncState(ctx, scope)
				require.NoError(t, err)
				require.Equal(t, previous, retained)
				complete, err := db.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
				require.NoError(t, err)
				require.Empty(t, complete)

				repo := filepath.Join(t.TempDir(), "snapshot")
				_, err = share.Export(ctx, db, share.Options{RepoPath: repo})
				require.NoError(t, err)
				imported, err := store.Open(ctx, filepath.Join(t.TempDir(), "imported.db"))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, imported.Close()) })
				_, err = share.Import(ctx, imported, share.Options{RepoPath: repo})
				require.NoError(t, err)
				for key, want := range map[string]string{scope: previous, channelMessageUnavailableScope("c1"): unavailable} {
					got, err := imported.GetSyncState(ctx, key)
					require.NoError(t, err)
					require.Equal(t, want, got)
				}

				client.denyAt = 0
				calls := client.calls
				run()
				require.Equal(t, calls+2, client.calls, "full retry must bypass the fresh unavailable marker")
				unavailable, err = db.GetSyncState(ctx, channelMessageUnavailableScope("c1"))
				require.NoError(t, err)
				require.Empty(t, unavailable)
				recovered, err := db.GetSyncState(ctx, scope)
				require.NoError(t, err)
				require.NotEqual(t, previous, recovered, "successful recovery must refresh the same window")
			})
		}
	}
}

func TestHistoryPermissionCheckErrorIsNotDenial(t *testing.T) {
	for _, checkErr := range []error{errors.New("channel identity changed before history permission check"), errors.New("required history permissions are denied"), context.DeadlineExceeded} {
		t.Run(checkErr.Error(), func(t *testing.T) {
			ctx := t.Context()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			client := &windowPermissionClient{fakeClient: &fakeClient{}, permissionErr: checkErr}
			svc := New(client, db, nil)
			channel := &discordgo.Channel{ID: "c1", GuildID: "g1", Type: discordgo.ChannelTypeGuildText}
			_, err = svc.syncWindowChannelHistory(ctx, channel, channelSyncState{}, false, time.Now().Add(-time.Hour), nil)
			require.ErrorIs(t, err, checkErr)
			require.Empty(t, unavailableReason(err), "an unproven check is not a permission denial")
		})
	}
}

func TestHistoryPermissionMarkerWriteFailureIsFatal(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
			ctx := t.Context()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			channel := &discordgo.Channel{ID: "c1", GuildID: "g1", Type: discordgo.ChannelTypeGuildText}
			client := &windowPermissionClient{fakeClient: &fakeClient{messages: map[string][]*discordgo.Message{}}}
			var logs bytes.Buffer
			svc := New(client, db, slog.New(slog.NewTextHandler(&logs, nil)))
			since := time.Now().UTC().Add(-time.Hour)
			opts := SyncOptions{Full: true, Since: since, Concurrency: concurrency}
			_, err = svc.syncMessageChannels(ctx, "g1", []*discordgo.Channel{channel}, opts)
			require.NoError(t, err)
			scope := channelHistoryWindowScope("c1", since)
			previous, err := db.GetSyncState(ctx, scope)
			require.NoError(t, err)
			require.NotEmpty(t, previous)
			_, err = db.DB().ExecContext(ctx, `create trigger reject_unavailable before insert on sync_state
                when new.scope = 'channel:c1:unavailable'
                begin select raise(abort, 'fixture marker write failure'); end`)
			require.NoError(t, err)
			client.denyAt = client.calls + 1
			_, err = svc.syncMessageChannels(ctx, "g1", []*discordgo.Channel{channel}, opts)
			require.ErrorIs(t, err, errHistoryPermissionDenied)
			require.Contains(t, logs.String(), "fixture marker write failure")
			retained, err := db.GetSyncState(ctx, scope)
			require.NoError(t, err)
			require.Equal(t, previous, retained)
			marker, err := db.GetSyncState(ctx, channelMessageUnavailableScope("c1"))
			require.NoError(t, err)
			require.Empty(t, marker)
		})
	}
}
