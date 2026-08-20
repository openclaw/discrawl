package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
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
	windowed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	require.False(t, needsHistoryVerification(nil, channelSyncState{BackfillComplete: true}, time.Time{}))
	// Not marked complete: the normal backfill path already covers it.
	require.False(t, needsHistoryVerification(channel, channelSyncState{}, time.Time{}))
	// Marked complete and messages are actually stored: trustworthy.
	require.False(t, needsHistoryVerification(channel, channelSyncState{BackfillComplete: true, HasMessages: true}, time.Time{}))
	// Marked complete, nothing stored, Discord reports content: verify.
	require.True(t, needsHistoryVerification(channel, channelSyncState{BackfillComplete: true}, time.Time{}))
	// Already verified empty once: do not crawl again.
	require.False(t, needsHistoryVerification(channel, channelSyncState{BackfillComplete: true, VerifiedEmpty: true}, time.Time{}))
	// Discord reports no content either: nothing to recover.
	require.False(t, needsHistoryVerification(silent, channelSyncState{BackfillComplete: true}, time.Time{}))
	// A windowed run cannot complete a recovery, so it does not start one.
	require.False(t, needsHistoryVerification(channel, channelSyncState{BackfillComplete: true}, windowed))

	// shouldSkipChannelSync must defer to it.
	require.False(t, shouldSkipChannelSync(channel, channelSyncState{BackfillComplete: true, Latest: "300"}, time.Time{}))
	require.True(t, shouldSkipChannelSync(channel, channelSyncState{BackfillComplete: true, Latest: "300", HasMessages: true}, time.Time{}))
	require.True(t, shouldSkipChannelSync(channel, channelSyncState{BackfillComplete: true, Latest: "300", VerifiedEmpty: true}, time.Time{}))
	// A channel Discord reports as empty still skips on an empty cursor.
	require.True(t, shouldSkipChannelSync(silent, channelSyncState{BackfillComplete: true}, time.Time{}))
	// Under a window the stranded channel takes its unchanged pre-verification
	// path, which for a cursor at the channel head is a skip.
	require.True(t, shouldSkipChannelSync(channel, channelSyncState{BackfillComplete: true, Latest: "300"}, windowed))
}

func verificationFixture(t *testing.T, messages []*discordgo.Message) (context.Context, *store.Store, *fakeClient, *Syncer, *discordgo.Channel) {
	t.Helper()
	return verificationFixtureAt(t, messages, "300")
}

// verificationFixtureAt builds the stranded shape around an explicit channel
// head so a fixture can carry more than one page of history.
func verificationFixtureAt(t *testing.T, messages []*discordgo.Message, head string) (context.Context, *store.Store, *fakeClient, *Syncer, *discordgo.Channel) {
	t.Helper()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))

	// The stranded shape seen in production: history_complete plus a latest
	// cursor at or ahead of the channel head, but no message rows at all.
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), head))

	client := &fakeClient{messages: map[string][]*discordgo.Message{"c1": messages}}
	channel := &discordgo.Channel{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText, LastMessageID: head}
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

// storedMessages returns n messages newest first. The ids are equal-width
// decimals so the fake client's string comparisons order them the way Discord
// orders snowflakes, and the store accepts them as canonical.
func storedMessages(n int) []*discordgo.Message {
	out := make([]*discordgo.Message, 0, n)
	for i := n - 1; i >= 0; i-- {
		out = append(out, storedMessage(strconv.Itoa(1000+i)))
	}
	return out
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

	// A windowed run must not verify at all: it can only reach back to the
	// window, and completing there would lock the channel into a fraction of
	// its history. It takes its unchanged pre-verification path instead.
	ctx, s, client, svc, channel := verificationFixture(t, []*discordgo.Message{storedMessage("300")})
	since := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, since, false, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, client.messageCalls["c1"], "a windowed run must not start a verification crawl")

	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Empty(t, marker, "a since-windowed crawl must not mark a channel verified empty")

	// The untouched history_complete marker is the direct evidence that no
	// verification began: verifyChannelHistory clears it before crawling.
	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)

	require.NoError(t, (*Syncer)(nil).recordVerifiedEmptyChannel(ctx, "c1"))
	require.NoError(t, svc.recordVerifiedEmptyChannel(ctx, ""))
	require.NoError(t, (*Syncer)(nil).restoreHistoryCompleteAfterFailedVerification(ctx, "c1"))
	require.NoError(t, svc.restoreHistoryCompleteAfterFailedVerification(ctx, ""))
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
// verification path through it. Verification ignores latest-only and crawls the
// whole channel; see TestHistoryVerificationLatestOnlyCrawlsWholeChannel for
// the multi-page case that separates the two.
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

	// Threads already took the full-crawl branch before this change, and they
	// still do: more than one page of history comes back whole.
	ctx, s, _, svc, _ := verificationFixtureAt(t, storedMessages(250), "1249")
	thread := &discordgo.Channel{
		ID: "c1", GuildID: "g1", ParentID: "f1", Name: "thread",
		Type: discordgo.ChannelTypeGuildPublicThread, LastMessageID: "1249",
	}

	count, err := svc.syncChannelMessages(ctx, "g1", thread, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 250, count)

	oldest, newest, err := s.ChannelMessageBounds(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, "1000", oldest)
	require.Equal(t, "1249", newest)
}

// A non-thread channel in latest-only mode is the shape the one-page bug hit:
// the zeroed cursor sent it down syncLatestChannelHistory, which stored the
// newest page and stopped, and history_complete then made every later run skip
// it. A verification must crawl the channel to its first message.
func TestHistoryVerificationLatestOnlyCrawlsWholeChannel(t *testing.T) {
	t.Parallel()

	ctx, s, client, svc, channel := verificationFixtureAt(t, storedMessages(250), "1249")

	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 250, count, "a verification must not stop after one page")

	// Reaching the oldest message is the proof the crawl ran to the start: the
	// one-page path leaves the oldest stored id at the 151st message.
	oldest, newest, err := s.ChannelMessageBounds(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, "1000", oldest)
	require.Equal(t, "1249", newest)

	// The crawl reached the start, so it restored history_complete itself.
	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)

	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Empty(t, marker)

	// The next run skips a channel that is now genuinely complete, and the
	// history it locks in is the whole history.
	calls := client.messageCalls["c1"]
	count, err = svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Equal(t, calls, client.messageCalls["c1"])

	oldest, _, err = s.ChannelMessageBounds(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, "1000", oldest)
}

// A windowed run leaves a stranded channel exactly as it found it, including
// under latest-only, so nothing about it changes until an unwindowed run.
func TestHistoryVerificationSkippedForWindowedLatestOnlySync(t *testing.T) {
	t.Parallel()

	ctx, s, client, svc, channel := verificationFixtureAt(t, storedMessages(250), "1249")
	since := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, since, true, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, client.messageCalls["c1"])

	has, err := s.ChannelHasMessages(ctx, "c1")
	require.NoError(t, err)
	require.False(t, has)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)

	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Empty(t, marker)

	// The unwindowed run that follows still repairs it.
	count, err = svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 250, count)
}

// A verification that fails before storing anything leaves the channel in the
// state that triggered it, so the next run tries again.
func TestHistoryVerificationRestoresMarkerWhenNothingWasStored(t *testing.T) {
	t.Parallel()

	ctx, s, client, svc, channel := verificationFixtureAt(t, storedMessages(250), "1249")
	client.messageErrors = map[string]error{"c1": errors.New("discord unavailable")}

	_, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.Error(t, err)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete, "a crawl that stored nothing must leave the channel verifiable")

	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Empty(t, marker, "a failed crawl is not evidence of emptiness")

	delete(client.messageErrors, "c1")
	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 250, count)
}

// A verification that fails after storing part of the history must not leave
// history_complete over those partial rows: the rows make HasMessages true, so
// the marker would make every later run skip the channel for good.
func TestHistoryVerificationLeavesMarkerClearedAfterPartialCrawl(t *testing.T) {
	t.Parallel()

	ctx, s, client, svc, channel := verificationFixtureAt(t, storedMessages(250), "1249")
	client.beforeErrors = map[string]map[string]error{"c1": {"1150": errors.New("discord unavailable")}}

	_, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.Error(t, err)

	oldest, _, err := s.ChannelMessageBounds(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, "1150", oldest, "the first page should have been stored")

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Empty(t, complete, "a partial history must not be marked complete")

	cursor, err := s.GetSyncState(ctx, channelBackfillScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1150", cursor)

	// The channel is a resumable backfill, so a full run finishes it.
	delete(client.beforeErrors, "c1")
	count, err := svc.syncChannelMessages(ctx, "g1", channel, true, false, time.Time{}, false, nil)
	require.NoError(t, err)
	require.Equal(t, 150, count)

	oldest, _, err = s.ChannelMessageBounds(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, "1000", oldest)

	complete, err = s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)
}
