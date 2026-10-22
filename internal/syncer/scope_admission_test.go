package syncer

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestAttachmentGuardHonorsTombstonesWithoutExclusions(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "cat", GuildID: "g", Kind: "category", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c", GuildID: "g", ParentID: "cat", Kind: "text", RawJSON: `{}`}))
	svc := New(&fakeClient{}, s, nil)
	guard := svc.attachmentScopeGuard()
	require.NotNil(t, guard, "unconfigured collection still guards attachment I/O")
	require.NoError(t, guard(ctx, "c"))
	require.NoError(t, s.MarkChannelDeleted(ctx, "g", "cat", "gateway"))
	require.ErrorIs(t, guard(ctx, "c"), store.ErrCollectionScope)
	require.ErrorIs(t, svc.requirePersistedScope(ctx, "c"), store.ErrCollectionScope)
}

func TestReplayResolvesTransactionalScopeRejectionAsExclusion(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", RawJSON: `{}`}))
	require.NoError(t, s.RecordFailure(ctx, store.FailureRef{
		Operation: tailMessageFailureOperation, Source: "discord", GuildID: "g1", ChannelID: "c1",
		MessageID: "70", RelatedKind: tailMessageFailureRelatedKind, RelatedID: "create",
	}, context.DeadlineExceeded))
	client := &exactReplayClient{messages: map[string]*discordgo.Message{"c1/70": {
		ID: "70", GuildID: "g1", ChannelID: "c1", Content: "late", Timestamp: time.Now().UTC(),
		Author: &discordgo.User{ID: "u1", Username: "user"},
	}}}
	// The channel is tombstoned after the fetch and builder succeed, so only
	// the transactional write guard observes the scope change.
	client.onFetch = func(context.Context) {
		require.NoError(t, s.MarkChannelDeleted(ctx, "g1", "c1", "gateway"))
	}
	stats, err := New(client, s, nil).replayTailMessageFailures(ctx, []string{"g1"}, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.OutOfScope)
	require.Zero(t, stats.Deferred)
	report, err := s.ListFailures(ctx, store.FailureListOptions{IncludeResolved: true}, time.Now())
	require.NoError(t, err)
	require.Zero(t, report.UnresolvedCount)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "excluded_by_collection_policy", report.Failures[0].ResolutionReason)
}

func TestTailSkipsTombstonedChannelBeforeContentIO(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte("body"))
	}))
	defer server.Close()
	s := repairFrontierStore(t)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "cat", GuildID: "g", Kind: "category", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c", GuildID: "g", ParentID: "cat", Kind: "text", RawJSON: `{}`}))
	require.NoError(t, s.MarkChannelDeleted(ctx, "g", "cat", "gateway"))
	h := &tailHandler{store: s, attachmentTextEnabled: true}
	m := &discordgo.Message{ID: "1", GuildID: "g", ChannelID: "c", Content: "x", Timestamp: time.Now(), Author: &discordgo.User{ID: "u"}, Attachments: []*discordgo.MessageAttachment{{ID: "a", Filename: "a.txt", ContentType: "text/plain", URL: server.URL, Size: 4}}}
	require.NoError(t, h.OnMessageCreate(ctx, m))
	require.Zero(t, requests.Load(), "a tombstoned descendant is never downloaded")
	var stored int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages where id='1'`).Scan(&stored))
	require.Zero(t, stored)
}

func TestTailResolvesTransactionalScopeRejectionAsPolicyOutcome(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c", GuildID: "g", Kind: "text", RawJSON: `{}`}))
	ref := tailMessageFailureIdentity("g", "c", "2", "create")
	require.NoError(t, s.RecordFailure(ctx, ref, context.DeadlineExceeded))
	// The channel is tombstoned while its attachment is downloading, after
	// admission, so only the transactional write guard observes it.
	tombstoned := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		tombstoned <- s.MarkChannelDeleted(context.Background(), "g", "c", "gateway")
		_, _ = w.Write([]byte("body"))
	}))
	defer server.Close()
	h := &tailHandler{store: s, attachmentTextEnabled: true}
	m := &discordgo.Message{ID: "2", GuildID: "g", ChannelID: "c", Content: "x", Timestamp: time.Now(), Author: &discordgo.User{ID: "u"}, Attachments: []*discordgo.MessageAttachment{{ID: "a", Filename: "a.txt", ContentType: "text/plain", URL: server.URL, Size: 4}}}
	require.NoError(t, h.OnMessageCreate(ctx, m), "scope revocation is a policy outcome, not a handler failure")
	require.NoError(t, <-tombstoned)
	report, err := s.ListFailures(ctx, store.FailureListOptions{IncludeResolved: true}, time.Now())
	require.NoError(t, err)
	require.Zero(t, report.UnresolvedCount)
	require.Equal(t, "excluded_by_collection_policy", report.Failures[0].ResolutionReason)

	other := errors.New("disk full")
	require.ErrorIs(t, h.scopeRejection(ctx, "MESSAGE_CREATE", "create", m, other), other)
}

func TestReplaySkipsTombstonedChannelBeforeFetch(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", RawJSON: `{}`}))
	require.NoError(t, s.MarkChannelDeleted(ctx, "g1", "c1", "gateway"))
	require.NoError(t, s.RecordFailure(ctx, tailMessageFailureIdentity("g1", "c1", "80", "create"), context.DeadlineExceeded))
	client := &exactReplayClient{}
	stats, err := New(client, s, nil).replayTailMessageFailures(ctx, []string{"g1"}, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.OutOfScope)
	require.Empty(t, client.calls, "a tombstoned channel is not fetched")
}

func TestMessageUpdateRefetchCannotReplaceNewerRecoverySnapshot(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c", GuildID: "g", Kind: "text", RawJSON: `{}`}))
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	stale := &discordgo.Message{ID: "m1", GuildID: "g", ChannelID: "c", Content: "stale", Timestamp: created, Author: &discordgo.User{ID: "u", Username: "user"}}
	client := &tailSnapshotClient{message: stale}
	// A recovery lane stores a newer snapshot while the Gateway refetch is in
	// flight; neither response carries a provider edit timestamp.
	client.onFetch = func(context.Context) {
		require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{ID: "m1", GuildID: "g", ChannelID: "c", AuthorID: "u", Content: "newer", NormalizedContent: "newer", RawJSON: `{"id":"m1","content":"newer"}`, CreatedAt: created.Format(time.RFC3339Nano)}))
	}
	h := &tailHandler{store: s, client: client}
	require.NoError(t, h.OnMessageUpdate(ctx, &discordgo.Message{ID: "m1", GuildID: "g", ChannelID: "c"}))
	require.EqualValues(t, 1, client.calls.Load())
	var content string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content from messages where id='m1'`).Scan(&content))
	require.Equal(t, "newer", content, "a delayed refetch cannot replace the newer recovery snapshot")
	var events int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events where message_id='m1' and event_type='update'`).Scan(&events))
	require.Equal(t, 1, events, "the genuine Gateway update is still recorded")
}
