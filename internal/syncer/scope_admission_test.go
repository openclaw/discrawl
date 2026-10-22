package syncer

import (
	"context"
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
