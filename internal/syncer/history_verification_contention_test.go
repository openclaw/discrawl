package syncer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestHistoryVerificationBoundsRestoreWhenConnectionPoolIsBusy(t *testing.T) {
	t.Parallel()

	ctx, s, client, svc, channel := verificationFixtureAt(t, storedMessages(250), "1249")
	s.DB().SetMaxOpenConns(1)
	client.messageStarted = make(chan string, 1)
	client.messageBlocks = map[string]chan struct{}{"c1": make(chan struct{})}
	crawlCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := svc.syncChannelMessages(crawlCtx, "g1", channel, false, false, time.Time{}, true, nil)
		result <- err
	}()
	select {
	case <-client.messageStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("verification did not reach the message request")
	}

	// Exhaust the pool only after verification cleared history_complete.
	conn, err := s.DB().Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("verification did not exit after releasing the connection")
		}
	})
	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(7 * time.Second):
		t.Fatal("marker restoration exceeded the existing five-second failure-cleanup budget")
	}
	require.NoError(t, conn.Close())
	marker, err := s.GetSyncState(ctx, channelVerifiedEmptyScope("c1"))
	require.NoError(t, err)
	require.Empty(t, marker, "failed cleanup must not claim the channel was verified empty")
	pending, err := s.GetSyncState(ctx, channelHistoryVerificationScope("c1"))
	require.NoError(t, err)
	require.JSONEq(t, `{}`, pending)

	close(client.messageBlocks["c1"])
	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 250, count, "the next default sync must retry recovery after the cleanup timeout")
	pending, err = s.GetSyncState(ctx, channelHistoryVerificationScope("c1"))
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestHistoryVerificationSurvivesWindowedIngestion(t *testing.T) {
	t.Parallel()
	ctx, s, client, svc, channel := verificationFixtureAt(t, storedMessages(250), "1249")
	client.messageErrors = map[string]error{"c1": errors.New("discord unavailable")}
	_, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.Error(t, err)
	delete(client.messageErrors, "c1")

	newMessage := storedMessage("1250")
	client.messages["c1"] = append([]*discordgo.Message{newMessage}, client.messages["c1"]...)
	channel.LastMessageID = "1250"
	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, newMessage.Timestamp, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count, "the windowed run only ingests the new message")

	count, err = svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 251, count, "ordinary ingestion must not discard pending older history")
	oldest, newest, err := s.ChannelMessageBounds(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, "1000", oldest)
	require.Equal(t, "1250", newest)
}

func TestHistoryVerificationRetainsCheckpointWhenOlderRowsArrive(t *testing.T) {
	t.Parallel()
	ctx, s, client, svc, channel := verificationFixtureAt(t, storedMessages(250), "1249")
	client.beforeErrors = map[string]map[string]error{"c1": {"1150": errors.New("discord unavailable")}}
	_, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.Error(t, err)
	delete(client.beforeErrors, "c1")

	// Ordinary ingestion can add an isolated old row without filling the gap.
	_, err = svc.persistMessagePage(ctx, []*discordgo.Message{storedMessage("1000")}, channel.Name, channel.GuildID, false)
	require.NoError(t, err)
	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 150, count, "recovery must resume at its own checkpoint, not the unrelated oldest row")
	var rows int
	require.NoError(t, s.DB().QueryRowContext(ctx, "SELECT count(*) FROM messages WHERE channel_id = 'c1'").Scan(&rows))
	require.Equal(t, 250, rows)
}

func TestHistoryVerificationCheckpointSurvivesWindowedFullSync(t *testing.T) {
	t.Parallel()
	ctx, s, client, svc, channel := verificationFixtureAt(t, storedMessages(250), "1249")
	client.beforeErrors = map[string]map[string]error{"c1": {"1150": errors.New("discord unavailable")}}
	_, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.Error(t, err)
	delete(client.beforeErrors, "c1")
	checkpoint, err := s.GetSyncState(ctx, channelHistoryVerificationScope("c1"))
	require.NoError(t, err)
	require.NoError(t, s.SetSyncState(ctx, channelBackfillScope("c1"), "1100"))

	_, err = svc.syncChannelMessages(ctx, "g1", channel, true, false, time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC), false, nil)
	require.NoError(t, err)
	afterWindow, err := s.GetSyncState(ctx, channelHistoryVerificationScope("c1"))
	require.NoError(t, err)
	require.Equal(t, checkpoint, afterWindow, "windowed full sync does not own recovery's checkpoint")
	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 150, count)
	var rows int
	require.NoError(t, s.DB().QueryRowContext(ctx, "SELECT count(*) FROM messages WHERE channel_id = 'c1'").Scan(&rows))
	require.Equal(t, 250, rows)
}

func TestHistoryVerificationResumesPendingWithoutCompletionOrCursor(t *testing.T) {
	t.Parallel()
	ctx, s, client, svc, channel := verificationFixtureAt(t, storedMessages(250), "1249")
	require.NoError(t, s.SetSyncState(ctx, channelHistoryVerificationScope("c1"), `{}`))
	require.NoError(t, s.DeleteSyncState(ctx, channelHistoryCompleteScope("c1")))
	require.NoError(t, s.DeleteSyncState(ctx, channelLatestScope("c1")))
	// An interrupted --full recheck may retain the previous empty verdict.
	require.NoError(t, s.SetSyncState(ctx, channelVerifiedEmptyScope("c1"), "1"))

	count, err := svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Now().UTC(), true, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	pending, err := s.GetSyncState(ctx, channelHistoryVerificationScope("c1"))
	require.NoError(t, err)
	require.JSONEq(t, `{}`, pending, "a windowed run must leave recovery intent intact")

	count, err = svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 250, count)
	pending, err = s.GetSyncState(ctx, channelHistoryVerificationScope("c1"))
	require.NoError(t, err)
	require.Empty(t, pending)
	calls := client.messageCalls["c1"]
	count, err = svc.syncChannelMessages(ctx, "g1", channel, false, false, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Equal(t, calls, client.messageCalls["c1"])
}
