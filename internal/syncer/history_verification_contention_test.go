package syncer

import (
	"context"
	"testing"
	"time"

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
}
