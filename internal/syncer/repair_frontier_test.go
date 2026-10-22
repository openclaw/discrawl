package syncer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
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

	// A Gateway rejoin stored while the deletion request was in flight wins.
	s := repairFrontierStore(t)
	started := time.Now().UTC()
	time.Sleep(2 * time.Millisecond) // the rejoin is strictly after the request started
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Rejoined", RawJSON: `{}`}))
	gone := &guildErrorClient{err: &discordgo.RESTError{
		Response: &http.Response{StatusCode: http.StatusForbidden},
		Message:  &discordgo.APIErrorMessage{Code: discordgo.ErrCodeMissingAccess},
	}}
	require.NoError(t, New(gone, s, nil).replayGuildDelete(ctx, "g1", started))
	var rejoinedDeleted string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select coalesce(deleted_at,'') from guilds where id='g1'`).Scan(&rejoinedDeleted))
	require.Empty(t, rejoinedDeleted)

	s = repairFrontierStore(t)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	rejoined := &fakeClient{guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Renamed"}}}
	require.NoError(t, New(rejoined, s, nil).replayGuildDelete(ctx, "g1", time.Now()))
	var name, deleted string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select name,coalesce(deleted_at,'') from guilds where id='g1'`).Scan(&name, &deleted))
	require.Equal(t, "Renamed", name, "current access is a newer observation than the failed deletion")
	require.Empty(t, deleted)
}

func TestRunningHandlerDropsStaleScopeDenial(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	for _, c := range []store.ChannelRecord{
		{ID: "x", GuildID: "g1", Kind: "category", Name: "excluded", RawJSON: `{}`},
		{ID: "y", GuildID: "g1", Kind: "category", Name: "allowed", RawJSON: `{}`},
		{ID: "c", GuildID: "g1", ParentID: "x", Kind: "text", Name: "moved", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertChannel(ctx, c))
	}
	handler := &tailHandler{store: s, guilds: makeGuildSet([]string{"g1"}), exclusions: newChannelScope([]string{"x"}, nil, nil)}
	require.NoError(t, handler.refreshScope(ctx))
	allowed, err := handler.allowMessageChannel(ctx, "g1", "c")
	require.NoError(t, err)
	require.False(t, allowed)

	// Metadata replay (a different handler) repairs the move into "y".
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c", GuildID: "g1", ParentID: "y", Kind: "text", Name: "moved", RawJSON: `{"parent_id":"y"}`}))
	replay := &tailHandler{store: s, guilds: makeGuildSet([]string{"g1"}), exclusions: handler.exclusions}
	require.NoError(t, replay.refreshScope(ctx))

	allowed, err = handler.allowMessageChannel(ctx, "g1", "c")
	require.NoError(t, err)
	require.True(t, allowed, "the running handler must not keep a repaired denial")
}

func TestRunningHandlerRevalidatesCachedAllowance(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	for _, c := range []store.ChannelRecord{
		{ID: "x", GuildID: "g1", Kind: "category", Name: "excluded", RawJSON: `{}`},
		{ID: "y", GuildID: "g1", Kind: "category", Name: "allowed", RawJSON: `{}`},
		{ID: "c", GuildID: "g1", ParentID: "y", Kind: "text", Name: "moved", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertChannel(ctx, c))
	}
	handler := &tailHandler{store: s, guilds: makeGuildSet([]string{"g1"}), exclusions: newChannelScope([]string{"x"}, nil, nil)}
	require.NoError(t, handler.refreshScope(ctx))
	allowed, err := handler.allowMessageChannel(ctx, "g1", "c")
	require.NoError(t, err)
	require.True(t, allowed)

	time.Sleep(2 * time.Millisecond)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c", GuildID: "g1", ParentID: "x", Kind: "text", Name: "moved", RawJSON: `{"parent_id":"x"}`}))
	allowed, err = handler.allowMessageChannel(ctx, "g1", "c")
	require.NoError(t, err)
	require.False(t, allowed, "a stale allowance must not permit content fetches")
}

type memberLookupClient struct {
	fakeClient
	member *discordgo.Member
	err    error
}

func (c *memberLookupClient) GuildMember(context.Context, string, string) (*discordgo.Member, error) {
	return c.member, c.err
}

func TestMemberRemoveReplayUsesCurrentMembership(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	removedAt := time.Now().UTC().Add(-time.Minute)
	failure := store.Failure{GuildID: "g1", FirstSeenAt: removedAt}
	deleted := func(s *store.Store) string {
		var value string
		require.NoError(t, s.DB().QueryRowContext(ctx, `select coalesce(deleted_at,'') from members where guild_id='g1' and user_id='u1'`).Scan(&value))
		return value
	}

	// An older full enumeration stamped the member after the failed removal.
	s := repairFrontierStore(t)
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u1", Username: "stale", RawJSON: `{}`}))
	gone := &memberLookupClient{err: &discordgo.RESTError{
		Response: &http.Response{StatusCode: http.StatusNotFound},
		Message:  &discordgo.APIErrorMessage{Code: discordgo.ErrCodeUnknownMember},
	}}
	require.NoError(t, New(gone, s, nil).replayMemberRemove(ctx, failure, "u1", time.Now().UTC()))
	require.NotEmpty(t, deleted(s), "Unknown Member proves the removal despite the later merge timestamp")

	s = repairFrontierStore(t)
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u1", Username: "old", RawJSON: `{}`}))
	present := &memberLookupClient{member: &discordgo.Member{GuildID: "g1", User: &discordgo.User{ID: "u1", Username: "rejoined"}}}
	require.NoError(t, New(present, s, nil).replayMemberRemove(ctx, failure, "u1", time.Now().UTC().Add(time.Second)))
	require.Empty(t, deleted(s), "a current member is a newer observation than the failed removal")

	transient := &memberLookupClient{err: errors.New("provider unavailable")}
	require.Error(t, New(transient, s, nil).replayMemberRemove(ctx, failure, "u1", time.Now().UTC()))
}

type scopeRevokingPageClient struct {
	fakeClient
	revoke func()
	calls  int
}

func (c *scopeRevokingPageClient) ChannelMessages(context.Context, string, int, string, string) ([]*discordgo.Message, error) {
	c.calls++
	if c.calls == 1 {
		c.revoke()
	}
	return []*discordgo.Message{{
		ID: "500", ChannelID: "c", GuildID: "g1", Content: "excluded content", Timestamp: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		Author: &discordgo.User{ID: "u1", Username: "user"},
	}}, nil
}

func TestPaginationStopsWhenChannelLeavesScope(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	for _, c := range []store.ChannelRecord{
		{ID: "x", GuildID: "g1", Kind: "category", Name: "excluded", RawJSON: `{}`},
		{ID: "c", GuildID: "g1", Kind: "text", Name: "moving", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertChannel(ctx, c))
	}
	client := &scopeRevokingPageClient{}
	svc := New(client, s, nil)
	svc.SetChannelExclusions([]string{"x"}, nil)
	_, err := refreshCollectionScopes(ctx, s, svc.channelExclusions)
	require.NoError(t, err)
	client.revoke = func() {
		// A concurrent Gateway update moves the channel under the exclusion.
		require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c", GuildID: "g1", ParentID: "x", Kind: "text", Name: "moving", RawJSON: `{"parent_id":"x"}`}))
		_, err := refreshCollectionScopes(ctx, s, svc.channelExclusions)
		require.NoError(t, err)
	}
	channel := &discordgo.Channel{ID: "c", GuildID: "g1", Name: "moving", Type: discordgo.ChannelTypeGuildText, LastMessageID: "900"}
	_, err = svc.syncChannelMessages(ctx, "g1", channel, true, false, time.Time{}, false, nil)
	require.ErrorIs(t, err, store.ErrCollectionScope)
	require.True(t, svc.skipSyncError(ctx, channel, err), "scope loss skips this channel without failing the run")
	require.Equal(t, 1, client.calls, "no further pages are requested after scope loss")
	var stored int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages where channel_id='c'`).Scan(&stored))
	require.Zero(t, stored)
}

func TestAttachmentDownloadsRecheckCollectionScope(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("attachment body"))
	}))
	defer server.Close()
	message := &discordgo.Message{ID: "10", GuildID: "g", ChannelID: "c", Timestamp: time.Now(), Attachments: []*discordgo.MessageAttachment{{ID: "a", Filename: "a.txt", ContentType: "text/plain", URL: server.URL, Size: 15}}}

	revoked := withAttachmentScopeGuard(t.Context(), func(context.Context, string) error { return store.ErrCollectionScope })
	_, err := buildMessageMutation(revoked, message, "", "g", false, true)
	require.ErrorIs(t, err, store.ErrCollectionScope)
	require.Zero(t, requests.Load(), "a revoked channel's attachment is never requested")

	allowed := withAttachmentScopeGuard(t.Context(), func(context.Context, string) error { return nil })
	_, err = buildMessageMutation(allowed, message, "", "g", false, true)
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load())
}

func TestScopeLossResolvesChannelWorkAsPolicyOutcome(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	svc := New(&fakeClient{}, s, nil)
	require.NoError(t, svc.recordChannelFailure(ctx, "g1", "c", errors.New("provider timeout")))
	require.NoError(t, svc.recordChannelFailure(ctx, "g1", "c", fmt.Errorf("write page: %w", store.ErrCollectionScope)))
	report, err := s.ListFailures(ctx, store.FailureListOptions{IncludeResolved: true}, time.Now())
	require.NoError(t, err)
	require.Zero(t, report.UnresolvedCount, "an excluded channel must not leave status degraded")
	require.Len(t, report.Failures, 1)
	require.Equal(t, "excluded_by_collection_policy", report.Failures[0].ResolutionReason)
}
