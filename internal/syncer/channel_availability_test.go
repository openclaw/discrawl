package syncer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

const unknownChannelMessageErr = "HTTP 404 Not Found, {\"message\": \"Unknown Channel\", \"code\": 10003}"

func discordRESTErr(status, code int) error {
	return &discordgo.RESTError{Response: &http.Response{StatusCode: status}, Message: &discordgo.APIErrorMessage{Code: code, Message: "fixture"}}
}

func newUnknownChannelSyncFixture(t *testing.T) (*store.Store, *fakeClient) {
	t.Helper()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	gone := &discordgo.Channel{ID: "c2", GuildID: "g1", Name: "gone", Type: discordgo.ChannelTypeGuildText}
	client := &fakeClient{
		guilds:    []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Guild"}},
		channels: map[string][]*discordgo.Channel{"g1": {
			{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
			gone,
		}},
		channelByID: map[string]*discordgo.Channel{"c2": gone},
		messages: map[string][]*discordgo.Message{
			"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "ok", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}}},
		},
		messageErrors: map[string]error{"c2": errors.New(unknownChannelMessageErr)},
	}
	return s, client
}

func channelFailures(t *testing.T, s *store.Store, channelID string) []store.Failure {
	t.Helper()
	report, err := s.ListFailures(context.Background(), store.FailureListOptions{IncludeResolved: true, ChannelID: channelID}, time.Now())
	require.NoError(t, err)
	return report.Failures
}

func unresolvedCount(t *testing.T, s *store.Store) int {
	t.Helper()
	report, err := s.ListFailures(context.Background(), store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	return report.UnresolvedCount
}

// A message-request 404 alone is not proof: when the exact channel lookup still
// serves the channel, or cannot answer, the failure stays retryable.
func TestUnknownChannelAmbiguous404StaysRetryable(t *testing.T) {
	t.Parallel()
	for name, lookupErr := range map[string]error{
		"channel still served": nil,
		"missing access":       discordRESTErr(http.StatusForbidden, discordgo.ErrCodeMissingAccess),
		"server error":         discordRESTErr(http.StatusBadGateway, 0),
		"plain 404 text":       errors.New(unknownChannelMessageErr),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			s, client := newUnknownChannelSyncFixture(t)
			if lookupErr != nil {
				client.channelErrors = map[string]error{"c2": lookupErr}
			}
			_, err := New(client, s, nil).Sync(ctx, SyncOptions{Full: true, Concurrency: 2})
			require.NoError(t, err)
			require.Equal(t, 1, client.channelCalls["c2"], "one corroborating lookup")
			failures := channelFailures(t, s, "c2")
			require.Len(t, failures, 1)
			require.True(t, failures[0].ResolvedAt.IsZero(), "ambiguous 404 stays pending")
			marker, err := s.GetSyncState(ctx, "channel:c2:unavailable")
			require.NoError(t, err)
			if lookupErr == nil {
				require.Empty(t, marker, "a served channel is retried on the next routine sync")
			} else {
				require.Equal(t, "unknown_channel", marker)
			}
			deleted, err := s.ChannelTombstoned(ctx, "c2")
			require.NoError(t, err)
			require.False(t, deleted)
		})
	}
}

// Corroborated Unknown Channel resolves as a provider outcome without deleting
// anything, keeps failure history and the marker, and recovers when access
// returns; a later 404 is recorded again before it is settled.
func TestUnknownChannelConfirmedIsProviderUnavailableAndRecovers(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, client := newUnknownChannelSyncFixture(t)
	client.messageErrors = nil
	client.messages["c2"] = []*discordgo.Message{{ID: "20", GuildID: "g1", ChannelID: "c2", Content: "archived", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}}}
	svc := New(client, s, nil)
	_, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 2})
	require.NoError(t, err)

	client.messageErrors = map[string]error{"c2": errors.New(unknownChannelMessageErr)}
	client.channelErrors = map[string]error{"c2": discordRESTErr(http.StatusNotFound, discordgo.ErrCodeUnknownChannel)}
	_, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 2})
	require.NoError(t, err)

	failures := channelFailures(t, s, "c2")
	require.Len(t, failures, 1)
	require.False(t, failures[0].ResolvedAt.IsZero())
	require.Equal(t, providerUnavailableResolution, failures[0].ResolutionReason)
	require.Contains(t, failures[0].ErrorMessage, "Unknown Channel", "history keeps the observation")
	require.Zero(t, unresolvedCount(t, s))
	marker, err := s.GetSyncState(ctx, "channel:c2:unavailable")
	require.NoError(t, err)
	require.Equal(t, "unknown_channel", marker, "marker keeps routine syncs from re-requesting it")
	deleted, err := s.ChannelTombstoned(ctx, "c2")
	require.NoError(t, err)
	require.False(t, deleted, "unavailability is not a deletion")
	status, err := s.Status(ctx, "db", "")
	require.NoError(t, err)
	require.Equal(t, 2, status.MessageCount, "archived history is preserved")

	// Routine syncs respect the marker: no further message requests.
	calls := client.messageCalls["c2"]
	_, err = svc.Sync(ctx, SyncOptions{Concurrency: 2})
	require.NoError(t, err)
	require.Equal(t, calls, client.messageCalls["c2"])

	// Access returns: a full/expired-window crawl succeeds, clears the marker.
	client.messageErrors, client.channelErrors = nil, nil
	client.messages["c2"] = append(client.messages["c2"], &discordgo.Message{ID: "21", GuildID: "g1", ChannelID: "c2", Content: "back", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}})
	_, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 2})
	require.NoError(t, err)
	marker, err = s.GetSyncState(ctx, "channel:c2:unavailable")
	require.NoError(t, err)
	require.Empty(t, marker, "recovered channel is re-admitted")
	status, err = s.Status(ctx, "db", "")
	require.NoError(t, err)
	require.Equal(t, 3, status.MessageCount)

	// A later 404 is recorded as a new observation, then settled again.
	client.messageErrors = map[string]error{"c2": errors.New(unknownChannelMessageErr)}
	client.channelErrors = map[string]error{"c2": discordRESTErr(http.StatusNotFound, discordgo.ErrCodeUnknownChannel)}
	_, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	failures = channelFailures(t, s, "c2")
	require.Len(t, failures, 1)
	require.Equal(t, providerUnavailableResolution, failures[0].ResolutionReason)
	require.Positive(t, failures[0].RetryCount)
	require.Zero(t, unresolvedCount(t, s))
}

// The repair lane settles failures recorded before this behavior existed:
// recorded deletions (including an ancestor), explicit exclusions and
// corroborated Unknown Channel answers. Other failures stay pending.
func TestReconcileChannelMessageFailuresSettlesOnlySupersededOrConfirmed(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	for _, c := range []store.ChannelRecord{
		{ID: "cat-x", GuildID: "g1", Kind: "category", RawJSON: `{}`},
		{ID: "gone", GuildID: "g1", Kind: "text", RawJSON: `{}`},
		{ID: "served", GuildID: "g1", Kind: "text", RawJSON: `{}`},
		{ID: "flaky", GuildID: "g1", Kind: "text", RawJSON: `{}`},
		{ID: "slow", GuildID: "g1", Kind: "text", RawJSON: `{}`},
		{ID: "deleted-parent", GuildID: "g1", Kind: "text", RawJSON: `{}`},
		{ID: "orphan-thread", GuildID: "g1", ParentID: "deleted-parent", ThreadParentID: "deleted-parent", Kind: "thread_public", RawJSON: `{}`},
		{ID: "excluded", GuildID: "g1", ParentID: "cat-x", Kind: "text", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertChannel(ctx, c))
	}
	client := &fakeClient{
		channelByID: map[string]*discordgo.Channel{"served": {ID: "served", GuildID: "g1"}},
		channelErrors: map[string]error{
			"gone":  discordRESTErr(http.StatusNotFound, discordgo.ErrCodeUnknownChannel),
			"flaky": discordRESTErr(http.StatusTooManyRequests, 0),
		},
	}
	svc := New(client, s, nil)
	svc.SetChannelExclusions([]string{"cat-x"}, nil)
	_, err = refreshCollectionScopes(ctx, s, svc.channelExclusions)
	require.NoError(t, err)
	require.NoError(t, s.MarkChannelDeleted(ctx, "g1", "deleted-parent", "discord-gateway"))
	for id, cause := range map[string]string{
		"gone": unknownChannelMessageErr, "served": unknownChannelMessageErr, "flaky": unknownChannelMessageErr,
		"slow": "context deadline exceeded", "orphan-thread": unknownChannelMessageErr, "excluded": "context deadline exceeded",
	} {
		require.NoError(t, s.RecordFailure(ctx, store.FailureRef{Operation: syncMessagesFailureOperation, Source: "discord", GuildID: "g1", ChannelID: id}, errors.New(cause)))
		require.NoError(t, s.SetSyncState(ctx, "channel:"+id+":unavailable", "unknown_channel"))
	}

	require.NoError(t, svc.reconcileChannelMessageFailures(ctx, []string{"g1"}))

	want := map[string]string{
		"gone": providerUnavailableResolution, "orphan-thread": "superseded_by_deletion", "excluded": "excluded_by_collection_policy",
		"served": "", "flaky": "", "slow": "",
	}
	for id, reason := range want {
		failures := channelFailures(t, s, id)
		require.Len(t, failures, 1, id)
		require.Equal(t, reason, failures[0].ResolutionReason, id)
		require.Equal(t, reason == "", failures[0].ResolvedAt.IsZero(), id)
	}
	require.Zero(t, client.channelCalls["orphan-thread"], "a recorded deletion needs no provider lookup")
	require.Zero(t, client.channelCalls["slow"], "non-404 failures are not looked up")
	marker, err := s.GetSyncState(ctx, "channel:served:unavailable")
	require.NoError(t, err)
	require.Empty(t, marker, "a served channel is retried by the next routine sync")
	marker, err = s.GetSyncState(ctx, "channel:gone:unavailable")
	require.NoError(t, err)
	require.Equal(t, "unknown_channel", marker)
	deleted, err := s.ChannelTombstoned(ctx, "gone")
	require.NoError(t, err)
	require.False(t, deleted)
	require.Equal(t, 3, unresolvedCount(t, s))
}

// The bounded lane rotates through every pending row: rows that stay pending
// (non-404 or inconclusive) cannot starve a later settleable failure.
func TestReconcileChannelMessageFailuresRotatesPastPendingRows(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	for i := range unknownChannelReplayLimit + 50 {
		require.NoError(t, s.RecordFailure(ctx, store.FailureRef{Operation: syncMessagesFailureOperation, Source: "discord", GuildID: "g1", ChannelID: fmt.Sprintf("slow-%03d", i)}, errors.New("context deadline exceeded")))
	}
	require.NoError(t, s.RecordFailure(ctx, store.FailureRef{Operation: syncMessagesFailureOperation, Source: "discord", GuildID: "g1", ChannelID: "gone"}, errors.New(unknownChannelMessageErr)))
	client := &fakeClient{channelErrors: map[string]error{"gone": discordRESTErr(http.StatusNotFound, discordgo.ErrCodeUnknownChannel)}}
	svc := New(client, s, nil)

	require.NoError(t, svc.reconcileChannelMessageFailures(ctx, []string{"g1"}))
	require.True(t, channelFailures(t, s, "gone")[0].ResolvedAt.IsZero(), "first page holds only older pending rows")
	require.NoError(t, svc.reconcileChannelMessageFailures(ctx, []string{"g1"}))
	gone := channelFailures(t, s, "gone")[0]
	require.Equal(t, providerUnavailableResolution, gone.ResolutionReason, "second run continues after the cursor")
	require.Equal(t, unknownChannelReplayLimit+50, unresolvedCount(t, s))

	// The cursor wraps once the end is reached.
	require.NoError(t, svc.reconcileChannelMessageFailures(ctx, []string{"g1"}))
	cursor, err := s.GetSyncState(ctx, channelFailureReplayCursorScope)
	require.NoError(t, err)
	require.NotEmpty(t, cursor)
	require.Equal(t, 1, client.channelCalls["gone"])
}
