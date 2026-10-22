package syncer

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func repairFrontierStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func TestAutomaticReplayKeepsHistoryFrontier(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "100"))
	ref := store.FailureRef{Operation: tailMessageFailureOperation, Source: "discord", GuildID: "g1", ChannelID: "c1", MessageID: "300", RelatedKind: tailMessageFailureRelatedKind, RelatedID: "create"}
	require.NoError(t, s.RecordFailure(ctx, ref, errors.New("handler failed")))
	client := &fakeClient{messages: map[string][]*discordgo.Message{"c1": {{
		ID: "300", ChannelID: "c1", GuildID: "g1", Content: "recovered", Timestamp: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		Author: &discordgo.User{ID: "u1", Username: "user"},
	}}}}

	// Catch-up may have failed before 101..299 were fetched; recovering 300
	// alone must leave the frontier for ordinary REST repair to continue.
	stats, err := New(client, s, nil).replayTailMessages(ctx, []string{"g1"}, TailMessageReplayLimit, false)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Recovered)
	cursor, err := s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "100", cursor)
	report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Zero(t, report.UnresolvedCount)
}

func TestFullCatalogLeavesStoredThreadRebuildsUntouched(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	parentRaw := `{"id":"parent","guild_id":"g1","type":0,"permission_overwrites":[]}`
	threadRaw := `{"id":"thread","guild_id":"g1","parent_id":"parent","type":11,"retained":"provider evidence"}`
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "parent", GuildID: "g1", Kind: "text", Name: "parent", RawJSON: parentRaw}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "thread", GuildID: "g1", ParentID: "parent", Kind: "thread_public", Name: "thread", RawJSON: threadRaw}))
	require.NoError(t, s.MarkChannelDeleted(ctx, "g1", "thread", "discord-gateway"))
	client := &fakeClient{channels: map[string][]*discordgo.Channel{"g1": {{
		ID: "parent", GuildID: "g1", Name: "parent", Type: discordgo.ChannelTypeGuildText, PermissionOverwrites: []*discordgo.PermissionOverwrite{},
	}}}}

	_, _, err := New(client, s, nil).channelList(ctx, "g1", nil, channelCatalogFull, channelExclusions{}, nil, nil, nil)
	require.NoError(t, err)
	var raw, deleted string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select raw_json,coalesce(deleted_at,'') from channels where id='thread'`).Scan(&raw, &deleted))
	require.Equal(t, threadRaw, raw, "a stored rebuild is not a provider observation")
	require.NotEmpty(t, deleted, "a cached rebuild must not resurrect a deleted thread")
}

type guildErrorClient struct {
	fakeClient
	err error
}

func (c *guildErrorClient) Guild(context.Context, string) (*discordgo.Guild, error) {
	return nil, c.err
}

func TestGuildDeleteReplayTombstonesWhenAccessIsGone(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	for _, tc := range []struct {
		name      string
		status    int
		code      int
		tombstone bool
	}{
		{name: "missing access", status: http.StatusForbidden, code: discordgo.ErrCodeMissingAccess, tombstone: true},
		{name: "unknown guild", status: http.StatusNotFound, code: discordgo.ErrCodeUnknownGuild, tombstone: true},
		{name: "transient", status: http.StatusInternalServerError, code: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := repairFrontierStore(t)
			require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
			client := &guildErrorClient{err: &discordgo.RESTError{
				Response: &http.Response{StatusCode: tc.status},
				Message:  &discordgo.APIErrorMessage{Code: tc.code},
			}}
			err := New(client, s, nil).replayGuildDelete(ctx, "g1", time.Now())
			var deleted string
			require.NoError(t, s.DB().QueryRowContext(ctx, `select coalesce(deleted_at,'') from guilds where id='g1'`).Scan(&deleted))
			if tc.tombstone {
				require.NoError(t, err)
				require.NotEmpty(t, deleted)
				return
			}
			require.Error(t, err, "transient failures stay pending")
			require.Empty(t, deleted)
		})
	}

	s := repairFrontierStore(t)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	rejoined := &fakeClient{guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Renamed"}}}
	require.NoError(t, New(rejoined, s, nil).replayGuildDelete(ctx, "g1", time.Now()))
	var name, deleted string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select name,coalesce(deleted_at,'') from guilds where id='g1'`).Scan(&name, &deleted))
	require.Equal(t, "Renamed", name, "current access is a newer observation than the failed deletion")
	require.Empty(t, deleted)
}
