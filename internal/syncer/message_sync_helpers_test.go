package syncer

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestMessageChannelSelectionAndTimeoutHelpers(t *testing.T) {
	t.Parallel()

	parent := &discordgo.Channel{ID: "forum", GuildID: "g1", Name: "forum", Type: discordgo.ChannelTypeGuildForum}
	thread := &discordgo.Channel{ID: "thread", GuildID: "g1", ParentID: "forum", Name: "thread", Type: discordgo.ChannelTypeGuildPublicThread}
	text := &discordgo.Channel{ID: "text", GuildID: "g1", Name: "text", Type: discordgo.ChannelTypeGuildText}
	voice := &discordgo.Channel{ID: "voice", GuildID: "g1", Name: "voice", Type: discordgo.ChannelTypeGuildVoice}

	rows := filterMessageChannels([]*discordgo.Channel{nil, parent, thread, text, voice}, []string{"forum"})
	require.Equal(t, []string{"thread"}, channelIDs(rows))
	require.False(t, requestedMessageTarget(nil, nil, map[string]struct{}{}))
	require.True(t, requestedMessageTarget(text, map[string]*discordgo.Channel{"text": text}, map[string]struct{}{"text": {}}))
	require.False(t, requestedMessageTarget(thread, map[string]*discordgo.Channel{}, map[string]struct{}{"forum": {}}))

	ctx, cancel := (*Syncer)(nil).messageChannelContext(context.Background())
	require.NoError(t, ctx.Err())
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	svc := New(&fakeClient{}, nil, nil)
	svc.messageChannelTimeout = time.Second
	ctx, cancel = svc.messageChannelContext(context.Background())
	defer cancel()
	_, ok := ctx.Deadline()
	require.True(t, ok)

	parentCtx, parentCancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	defer parentCancel()
	ctx, cancel = svc.messageChannelContext(parentCtx)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	parentDeadline, _ := parentCtx.Deadline()
	require.Equal(t, parentDeadline, deadline)
}

func TestChannelSyncStateHelpers(t *testing.T) {
	t.Parallel()

	channel := &discordgo.Channel{ID: "c1", LastMessageID: "200"}
	require.False(t, shouldSkipChannelSync(nil, channelSyncState{BackfillComplete: true}))
	require.True(t, shouldSkipChannelSync(&discordgo.Channel{ID: "c1"}, channelSyncState{BackfillComplete: true, Latest: ""}))
	require.False(t, shouldSkipChannelSync(channel, channelSyncState{BackfillComplete: true, Latest: ""}))
	require.True(t, shouldSkipChannelSync(channel, channelSyncState{BackfillComplete: true, Latest: "300", HasMessages: true}))
	require.False(t, shouldSkipLatestOnlyChannelSync(nil, channelSyncState{Latest: "300"}))
	require.False(t, shouldSkipLatestOnlyChannelSync(channel, channelSyncState{}))
	require.True(t, shouldSkipLatestOnlyChannelSync(channel, channelSyncState{Latest: "300"}))

	messages := []*discordgo.Message{
		{ID: "3", Timestamp: time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)},
		{ID: "2", Timestamp: time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC)},
		{ID: "1", Timestamp: time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)},
	}
	filtered, reached := filterMessagesSince(messages, time.Date(2026, 5, 8, 10, 30, 0, 0, time.UTC))
	require.True(t, reached)
	require.Equal(t, []string{"3", "2"}, messageIDs(filtered))
	filtered, reached = filterMessagesSince(messages, time.Time{})
	require.False(t, reached)
	require.Len(t, filtered, 3)
}

func TestChannelSyncStateStoreHelpers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "100",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "User",
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}))

	svc := New(&fakeClient{}, s, nil)
	state := channelSyncState{}
	require.NoError(t, svc.seedChannelSyncState(ctx, "c1", &state))
	require.Equal(t, "100", state.Latest)
	require.Equal(t, "100", state.BackfillCursor)

	state = channelSyncState{StoredLatest: "100"}
	require.NoError(t, svc.seedChannelSyncState(ctx, "missing-channel", &state))
	require.True(t, state.BackfillComplete)

	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "200"))
	require.NoError(t, s.SetSyncState(ctx, channelBackfillScope("c1"), "100"))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))
	loaded, err := svc.loadChannelSyncState(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, channelSyncState{Latest: "200", StoredLatest: "200", BackfillCursor: "100", BackfillComplete: true, HasMessages: true}, loaded)
}

func TestMessageChannelSyncBranches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	svc := New(&fakeClient{}, s, nil)
	count, err := svc.syncMessageChannels(ctx, "g1", nil, SyncOptions{})
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, svc.clearUnavailableChannel(ctx, ""))
	require.NoError(t, (*Syncer)(nil).clearUnavailableChannel(ctx, "c1"))

	channel := &discordgo.Channel{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}
	client := &fakeClient{
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "100",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "hello",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}
	svc = New(client, s, nil)
	count, err = svc.syncMessageChannelsSerial(ctx, "g1", []*discordgo.Channel{channel}, SyncOptions{Full: true}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	errChannel := &discordgo.Channel{ID: "c-err", GuildID: "g1", Name: "errors", Type: discordgo.ChannelTypeGuildText}
	client.messageErrors = map[string]error{"c-err": errors.New(`HTTP 500 Internal Server Error`)}
	count, err = svc.syncMessageChannelsSerial(ctx, "g1", []*discordgo.Channel{errChannel}, SyncOptions{Full: true}, nil)
	require.NoError(t, err)
	require.Zero(t, count)

	client.messageErrors = map[string]error{"c-err": errors.New("hard failure")}
	count, err = svc.syncMessageChannelsSerial(ctx, "g1", []*discordgo.Channel{errChannel}, SyncOptions{Full: true}, nil)
	require.ErrorContains(t, err, "sync channel c-err")
	require.Zero(t, count)
}

func TestMessageChannelConcurrentErrorAndProgressBranches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	channels := []*discordgo.Channel{
		{ID: "c1", GuildID: "g1", Name: "one", Type: discordgo.ChannelTypeGuildText},
		{ID: "c2", GuildID: "g1", Name: "two", Type: discordgo.ChannelTypeGuildText},
	}
	client := &fakeClient{
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "101",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "one",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
		messageErrors: map[string]error{"c2": errors.New("hard failure")},
	}
	svc := New(client, s, slog.New(slog.DiscardHandler))
	count, err := svc.syncMessageChannelsConcurrent(ctx, "g1", channels, SyncOptions{Full: true}, 2, newMessageSyncProgress(svc, "g1", len(channels), SyncOptions{Full: true, Concurrency: 2}))
	require.ErrorContains(t, err, "sync channel c2")
	require.LessOrEqual(t, count, 1)

	progress := &messageSyncProgress{}
	progress.start(nil)
	progress.touch(nil, 1)
	progress.finish(nil)
	progress.logWaitHeartbeat()
	require.Equal(t, "skipped", syncErrorOutcome(errors.New("plain")))
}

func TestMessageChannelConcurrentFatalErrorCancelsPeers(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	channels := []*discordgo.Channel{
		{ID: "slow", GuildID: "g1", Name: "slow", Type: discordgo.ChannelTypeGuildText},
		{ID: "err", GuildID: "g1", Name: "err", Type: discordgo.ChannelTypeGuildText},
	}
	slowBlock := make(chan struct{})
	errBlock := make(chan struct{})
	started := make(chan string, 2)
	client := &fakeClient{
		messageBlocks:  map[string]chan struct{}{"slow": slowBlock, "err": errBlock},
		messageStarted: started,
		messageErrors:  map[string]error{"err": errors.New("hard failure")},
	}
	svc := New(client, s, slog.New(slog.DiscardHandler))

	done := make(chan struct {
		count int
		err   error
	}, 1)
	go func() {
		count, err := svc.syncMessageChannelsConcurrent(ctx, "g1", channels, SyncOptions{Full: true}, 2, nil)
		done <- struct {
			count int
			err   error
		}{count: count, err: err}
	}()

	seen := map[string]struct{}{}
	deadline := time.After(time.Second)
	for len(seen) < 2 {
		select {
		case channelID := <-started:
			seen[channelID] = struct{}{}
		case <-deadline:
			t.Fatalf("timed out waiting for concurrent channel starts; saw %v", seen)
		}
	}
	require.Contains(t, seen, "slow")
	require.Contains(t, seen, "err")
	close(errBlock)

	result := <-done
	count, err := result.count, result.err
	require.ErrorContains(t, err, "sync channel err")
	require.Zero(t, count)
	require.NoError(t, ctx.Err())
	client.mu.Lock()
	slowCalls := client.messageCalls["slow"]
	client.mu.Unlock()
	require.Equal(t, 1, slowCalls)
}

func channelIDs(channels []*discordgo.Channel) []string {
	out := make([]string, 0, len(channels))
	for _, channel := range channels {
		out = append(out, channel.ID)
	}
	return out
}

func messageIDs(messages []*discordgo.Message) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		out = append(out, message.ID)
	}
	return out
}
