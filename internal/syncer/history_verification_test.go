package syncer

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestNeedsHistoryVerification(t *testing.T) {
	t.Parallel()

	channel := &discordgo.Channel{ID: "c1", LastMessageID: "200"}
	silent := &discordgo.Channel{ID: "c1"}

	require.False(t, needsHistoryVerification(nil, channelSyncState{BackfillComplete: true}))
	// Not marked complete: the normal backfill path already covers it.
	require.False(t, needsHistoryVerification(channel, channelSyncState{}))
	// Marked complete and messages are actually stored: trustworthy.
	require.False(t, needsHistoryVerification(channel, channelSyncState{BackfillComplete: true, HasMessages: true}))
	// Marked complete, nothing stored, Discord reports content: verify.
	require.True(t, needsHistoryVerification(channel, channelSyncState{BackfillComplete: true}))
	// Already verified empty once: do not crawl again.
	require.False(t, needsHistoryVerification(channel, channelSyncState{BackfillComplete: true, VerifiedEmpty: true}))
	// Discord reports no content either: nothing to recover.
	require.False(t, needsHistoryVerification(silent, channelSyncState{BackfillComplete: true}))

	// shouldSkipChannelSync must defer to it.
	require.False(t, shouldSkipChannelSync(channel, channelSyncState{BackfillComplete: true, Latest: "300"}))
	require.True(t, shouldSkipChannelSync(channel, channelSyncState{BackfillComplete: true, Latest: "300", HasMessages: true}))
	require.True(t, shouldSkipChannelSync(channel, channelSyncState{BackfillComplete: true, Latest: "300", VerifiedEmpty: true}))
	// A channel Discord reports as empty still skips on an empty cursor.
	require.True(t, shouldSkipChannelSync(silent, channelSyncState{BackfillComplete: true}))
}

func verificationFixture(t *testing.T, messages []*discordgo.Message) (context.Context, *store.Store, *fakeClient, *Syncer, *discordgo.Channel) {
	t.Helper()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))

	// The stranded shape seen in production: history_complete plus a latest
	// cursor at or ahead of the channel head, but no message rows at all.
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "300"))

	client := &fakeClient{messages: map[string][]*discordgo.Message{"c1": messages}}
	channel := &discordgo.Channel{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText, LastMessageID: "300"}
	return ctx, s, client, New(client, s, nil), channel
}

func storedMessage(id string) *discordgo.Message {
	return &discordgo.Message{
		ID:        id,
		ChannelID: "c1",
		GuildID:   "g1",
		Author:    &discordgo.User{ID: "u1", Username: "user"},
		Content:   "hello",
		Timestamp: time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
	}
}

func TestHistoryVerificationRecoversStrandedChannel(t *testing.T) {
	t.Parallel()

	ctx, s, client, svc, channel := verificationFixture(t, []*discordgo.Message{storedMessage("300"), storedMessage("100")})

	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, false, nil)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Positive(t, client.messageCalls["c1"])

	has, err := s.ChannelHasMessages(ctx, "c1")
	require.NoError(t, err)
	require.True(t, has)

	// A recovered channel is not marked verified empty.
	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Empty(t, marker)

	// Second run sees stored messages and skips again.
	before := client.messageCalls["c1"]
	count, err = svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, false, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Equal(t, before, client.messageCalls["c1"])
}

func TestHistoryVerificationStopsAfterGenuinelyEmptyChannel(t *testing.T) {
	t.Parallel()

	ctx, s, client, svc, channel := verificationFixture(t, nil)

	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, false, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	firstRun := client.messageCalls["c1"]
	require.Positive(t, firstRun)

	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", marker)

	// Every later run must be a no-op: no re-fetch loop.
	for range 3 {
		count, err = svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, false, nil)
		require.NoError(t, err)
		require.Zero(t, count)
	}
	require.Equal(t, firstRun, client.messageCalls["c1"])
}

func TestHistoryVerificationSkipsChannelWithMessages(t *testing.T) {
	t.Parallel()

	ctx, s, client, svc, channel := verificationFixture(t, []*discordgo.Message{storedMessage("300")})
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID: "300", GuildID: "g1", ChannelID: "c1", ChannelName: "general",
		AuthorID: "u1", AuthorName: "user", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content: "hello", NormalizedContent: "hello", RawJSON: `{}`,
	}))

	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, false, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, client.messageCalls["c1"])
}

func TestHistoryVerificationIgnoresChannelWithoutLastMessage(t *testing.T) {
	t.Parallel()

	ctx, s, client, svc, _ := verificationFixture(t, nil)
	require.NoError(t, s.DeleteSyncState(ctx, channelLatestScope("c1")))
	silent := &discordgo.Channel{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}

	count, err := svc.syncChannelMessages(ctx, "g1", silent, false, false, time.Time{}, false, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, client.messageCalls["c1"])

	// Nothing was crawled, so nothing may claim to have been verified.
	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Empty(t, marker)
}

func TestHistoryVerificationFullRunRechecksVerifiedEmpty(t *testing.T) {
	t.Parallel()

	ctx, _, client, svc, channel := verificationFixture(t, []*discordgo.Message{storedMessage("300")})
	require.NoError(t, svc.store.SetSyncState(ctx, channelVerifiedEmptyScope("c1"), "1"))

	// A routine run trusts the marker.
	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, false, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, client.messageCalls["c1"])

	// An explicit full run re-checks it and recovers the message.
	count, err = svc.syncChannelMessages(ctx, "g1", channel, true, false, time.Time{}, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Positive(t, client.messageCalls["c1"])
}

func TestVerifiedEmptyNotWrittenForWindowedSync(t *testing.T) {
	t.Parallel()

	// Every message predates the since window, so filterMessagesSince drops
	// them all before they are persisted. That is not evidence of emptiness.
	ctx, s, _, svc, channel := verificationFixture(t, []*discordgo.Message{storedMessage("300")})
	since := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	_, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, since, false, nil)
	require.NoError(t, err)

	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Empty(t, marker, "a since-windowed crawl must not mark a channel verified empty")

	require.NoError(t, (*Syncer)(nil).recordVerifiedEmptyChannel(ctx, "c1", time.Time{}))
	require.NoError(t, svc.recordVerifiedEmptyChannel(ctx, "", time.Time{}))
}

func TestChannelHasMessagesProbe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	has, err := s.ChannelHasMessages(ctx, "")
	require.NoError(t, err)
	require.False(t, has)

	has, err = s.ChannelHasMessages(ctx, "c1")
	require.NoError(t, err)
	require.False(t, has)

	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID: "100", GuildID: "g1", ChannelID: "c1", ChannelName: "general",
		AuthorID: "u1", AuthorName: "user", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content: "hello", NormalizedContent: "hello", RawJSON: `{}`,
	}))

	has, err = s.ChannelHasMessages(ctx, "c1")
	require.NoError(t, err)
	require.True(t, has)
}

// LatestOnly is the default for routine syncs, so stranded channels reach the
// verification path through it. Both of its branches issue the same first
// request as a full crawl (before=""), so a zero-result page is the same
// evidence of emptiness in either mode.
func TestHistoryVerificationUnderLatestOnly(t *testing.T) {
	t.Parallel()

	ctx, s, _, svc, channel := verificationFixture(t, []*discordgo.Message{storedMessage("300"), storedMessage("100")})

	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	has, err := s.ChannelHasMessages(ctx, "c1")
	require.NoError(t, err)
	require.True(t, has)

	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Empty(t, marker, "a channel that yielded messages must not be marked verified empty")
}

func TestHistoryVerificationUnderLatestOnlyMarksEmptyChannel(t *testing.T) {
	t.Parallel()

	ctx, s, client, svc, channel := verificationFixture(t, nil)

	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	firstRun := client.messageCalls["c1"]
	require.Positive(t, firstRun)

	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", marker)

	// And it does not loop on later runs.
	_, err = svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, firstRun, client.messageCalls["c1"])
}

func TestHistoryVerificationUnderLatestOnlyForThread(t *testing.T) {
	t.Parallel()

	ctx, s, _, svc, _ := verificationFixture(t, []*discordgo.Message{storedMessage("300")})
	thread := &discordgo.Channel{
		ID: "c1", GuildID: "g1", ParentID: "f1", Name: "thread",
		Type: discordgo.ChannelTypeGuildPublicThread, LastMessageID: "300",
	}

	count, err := svc.syncChannelMessages(ctx, "g1", thread, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	has, err := s.ChannelHasMessages(ctx, "c1")
	require.NoError(t, err)
	require.True(t, has)
}
