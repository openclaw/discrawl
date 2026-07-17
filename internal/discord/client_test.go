package discord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestClientRESTWrappers(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/users/@me", writeJSON(map[string]any{"id": "bot"}))
	mux.HandleFunc("/api/v10/users/@me/guilds", writeJSON([]map[string]any{
		{"id": "g1", "name": "Guild One"},
	}))
	mux.HandleFunc("/api/v10/guilds/g1", writeJSON(map[string]any{"id": "g1", "name": "Guild One"}))
	mux.HandleFunc("/api/v10/guilds/g1/channels", writeJSON([]map[string]any{
		{"id": "c1", "guild_id": "g1", "name": "general", "type": 0},
	}))
	mux.HandleFunc("/api/v10/guilds/g1/threads/active", writeJSON(map[string]any{
		"threads": []map[string]any{
			{"id": "tg1", "guild_id": "g1", "parent_id": "c1", "name": "guild-thread", "type": 11},
		},
		"members":  []any{},
		"has_more": false,
	}))
	mux.HandleFunc("/api/v10/guilds/g1/members", writeJSON([]map[string]any{
		{
			"guild_id": "g1",
			"user":     map[string]any{"id": "u1", "username": "peter"},
			"roles":    []string{},
		},
	}))
	mux.HandleFunc("/api/v10/channels/c1/threads/active", writeJSON(map[string]any{
		"threads": []map[string]any{
			{"id": "t1", "guild_id": "g1", "parent_id": "c1", "name": "thread", "type": 11},
		},
		"members":  []any{},
		"has_more": false,
	}))
	mux.HandleFunc("/api/v10/channels/c1/threads/archived/public", writeJSON(map[string]any{
		"threads": []map[string]any{
			{
				"id":        "t2",
				"guild_id":  "g1",
				"parent_id": "c1",
				"name":      "archived-public",
				"type":      11,
				"thread_metadata": map[string]any{
					"archived":              true,
					"auto_archive_duration": 60,
					"archive_timestamp":     time.Now().UTC().Format(time.RFC3339),
					"locked":                false,
					"invitable":             true,
				},
			},
		},
		"members":  []any{},
		"has_more": false,
	}))
	mux.HandleFunc("/api/v10/channels/c1/threads/archived/private", writeJSON(map[string]any{
		"threads": []map[string]any{
			{
				"id":        "t3",
				"guild_id":  "g1",
				"parent_id": "c1",
				"name":      "archived-private",
				"type":      12,
				"thread_metadata": map[string]any{
					"archived":              true,
					"auto_archive_duration": 60,
					"archive_timestamp":     time.Now().UTC().Format(time.RFC3339),
					"locked":                true,
					"invitable":             false,
				},
			},
		},
		"members":  []any{},
		"has_more": false,
	}))
	mux.HandleFunc("/api/v10/channels/c1/messages", writeJSON([]map[string]any{
		{
			"id":         "m1",
			"guild_id":   "g1",
			"channel_id": "c1",
			"content":    "hello",
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
			"author":     map[string]any{"id": "u1", "username": "peter"},
		},
	}))
	mux.HandleFunc("/api/v10/channels/c1/messages/m1", writeJSON(map[string]any{
		"id":         "m1",
		"guild_id":   "g1",
		"channel_id": "c1",
		"content":    "hello",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"author":     map[string]any{"id": "u1", "username": "peter"},
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	self, err := client.Self(ctx)
	require.NoError(t, err)
	require.Equal(t, "bot", self.ID)

	guilds, err := client.Guilds(ctx)
	require.NoError(t, err)
	require.Len(t, guilds, 1)

	guild, err := client.Guild(ctx, "g1")
	require.NoError(t, err)
	require.Equal(t, "Guild One", guild.Name)

	channels, err := client.GuildChannels(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, channels, 1)

	members, err := client.GuildMembers(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, members, 1)

	active, err := client.ThreadsActive(ctx, "c1")
	require.NoError(t, err)
	require.Len(t, active, 1)

	guildActive, err := client.GuildThreadsActive(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, guildActive, 1)

	publicArchived, err := client.ThreadsArchived(ctx, "c1", false)
	require.NoError(t, err)
	require.Len(t, publicArchived, 1)

	privateArchived, err := client.ThreadsArchived(ctx, "c1", true)
	require.NoError(t, err)
	require.Len(t, privateArchived, 1)

	messages, err := client.ChannelMessages(ctx, "c1", 100, "", "")
	require.NoError(t, err)
	require.Len(t, messages, 1)

	message, err := client.ChannelMessage(ctx, "c1", "m1")
	require.NoError(t, err)
	require.Equal(t, "m1", message.ID)
}

func TestTailRequiresHandler(t *testing.T) {
	client, err := New("token")
	require.NoError(t, err)
	require.Error(t, client.Tail(context.Background(), nil))
	require.NoError(t, (&Client{}).Close())
}

func TestTailClassifiesLocalTLSGatewayOpenFailureWithoutRecordingOrLoggingDetails(t *testing.T) {
	tlsServer := httptest.NewUnstartedServer(http.NotFoundHandler())
	tlsServer.Config.ErrorLog = log.New(io.Discard, "", 0)
	tlsServer.StartTLS()
	defer tlsServer.Close()

	gatewayURL := "wss" + tlsServer.URL[len("https"):] + "/gateway"
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v10/gateway" {
			t.Errorf("gateway path = %q, want %q", r.URL.Path, "/api/v10/gateway")
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"url": gatewayURL}); err != nil {
			t.Errorf("encode gateway response: %v", err)
		}
	}))
	defer apiServer.Close()

	restoreEndpoints := patchDiscordEndpoints(apiServer.URL + "/api/v10/")
	defer restoreEndpoints()

	var logged strings.Builder
	oldLogger := discordgo.Logger
	discordgo.Logger = func(_ int, _ int, format string, args ...any) {
		_, _ = fmt.Fprintf(&logged, format, args...)
	}
	defer func() {
		discordgo.Logger = oldLogger
	}()

	const sensitiveToken = "sensitive-tls-token"
	client, err := New(sensitiveToken)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.Dialer = &websocket.Dialer{HandshakeTimeout: time.Second}
	require.Zero(t, discordSessionHandlerCount(client.session))

	handler := &gatewayOpenFailureHandler{}
	err = client.Tail(context.Background(), handler)
	require.EqualError(t, err, "open discord gateway")
	require.True(t, IsGatewayOpenError(err))
	require.False(t, IsFatalTailError(err))
	require.Error(t, errors.Unwrap(err))
	require.NotContains(t, err.Error(), sensitiveToken)
	require.Empty(t, logged.String())
	require.Zero(t, handler.recordCalls.Load())
	require.Zero(t, discordSessionHandlerCount(client.session))
}

func TestRunTailTaskRecoversPanics(t *testing.T) {
	t.Parallel()

	client := &Client{tailHandlerTimeout: 10 * time.Millisecond}
	err := client.runTailTask(context.Background(), func(context.Context) error {
		panic("boom")
	})
	require.EqualError(t, err, "tail handler panic")
	require.NotContains(t, err.Error(), "boom")

	client.tailHandlerTimeout = 0
	err = client.runTailTask(context.Background(), func(context.Context) error {
		panic("again")
	})
	require.EqualError(t, err, "tail handler panic")
	require.NotContains(t, err.Error(), "again")
}

func TestTailFailureErrorContractsAndDeadlineHelpers(t *testing.T) {
	t.Parallel()

	cause := errors.New("safe cause")
	require.EqualError(
		t,
		&tailHandlerDeadlineError{timeout: time.Second, cause: cause},
		"tail handler returned after deadline 1s: safe cause",
	)
	require.EqualError(
		t,
		&tailHandlerDeadlineError{timeout: time.Second, returnedNil: true},
		"tail handler returned nil after deadline 1s",
	)
	require.EqualError(
		t,
		&tailHandlerDeadlineError{timeout: time.Second, detached: true},
		"tail handler timed out after 1s",
	)
	require.ErrorIs(t, (&tailHandlerDeadlineError{cause: cause}).Unwrap(), cause)
	require.ErrorIs(t, (&tailHandlerDeadlineError{}).Unwrap(), context.DeadlineExceeded)

	joinErr := &tailHandlerJoinError{cause: context.Canceled}
	require.EqualError(t, joinErr, "tail handler did not stop after cancellation")
	require.ErrorIs(t, joinErr.Unwrap(), context.Canceled)
	var nilJoinErr *tailHandlerJoinError
	require.NoError(t, nilJoinErr.Unwrap())

	var nilGatewayErr *GatewayOpenError
	require.NoError(t, nilGatewayErr.Unwrap())

	startedAt := time.Unix(100, 0)
	require.Equal(
		t,
		tailHandlerCancelGrace,
		boundedJoinElapsed(startedAt, startedAt.Add(2*tailHandlerCancelGrace)),
	)
	deadlineErr := tailTaskDeadlineResult(time.Second, context.Canceled)
	require.ErrorIs(t, deadlineErr, context.DeadlineExceeded)

	deadline := startedAt.Add(time.Second)
	graceDeadline := deadline.Add(tailHandlerCancelGrace)
	require.NoError(t, classifyTailTaskResult(
		time.Second,
		tailTaskResult{completedAt: deadline.Add(-time.Millisecond)},
		deadline,
		graceDeadline,
	))
	finalErr := finalTailTaskDeadlineResult(
		time.Second,
		make(chan tailTaskResult),
		deadline,
		graceDeadline,
	)
	require.ErrorIs(t, finalErr, context.DeadlineExceeded)
}

func TestTailTaskResultAfterParentCancellationPreservesNonNilResultAndClampsStageElapsed(t *testing.T) {
	t.Parallel()

	startedAt := time.Unix(100, 0)
	completedAt := startedAt.Add(10 * time.Millisecond)
	canceledAt := startedAt.Add(20 * time.Millisecond)
	parentErr := context.Canceled
	panicErr := &tailHandlerPanicError{}
	ordinaryErr := errors.New("ordinary handler error")
	require.Same(t, panicErr, tailTaskResultAfterParentCancellation(parentErr, tailTaskResult{err: panicErr}))
	require.Same(t, parentErr, tailTaskResultAfterParentCancellation(parentErr, tailTaskResult{}))
	require.Same(t, ordinaryErr, tailTaskResultAfterParentCancellation(
		parentErr,
		tailTaskResult{err: ordinaryErr},
	))

	metadata := &tailFailureMetadata{}
	metadata.setStageAt(TailFailureStageHandler, startedAt)
	metadata.freezeStageAt(canceledAt)
	execution := tailTaskExecutionAfterParentCancellation(
		parentErr,
		tailTaskResult{err: ordinaryErr, completedAt: completedAt},
		startedAt,
		canceledAt,
	)
	require.Same(t, ordinaryErr, execution.err)
	require.True(t, execution.handlerReturnedErr)
	require.Equal(t, completedAt, execution.observedAt)
	require.Equal(t, 10*time.Millisecond, execution.handlerElapsed)
	failure := newTailFailureFromExecution(tailTask{
		eventType:       "MESSAGE_CREATE",
		failureMetadata: metadata,
	}, execution)
	require.Equal(t, 10*time.Millisecond, failure.HandlerElapsed)
	require.Equal(t, failure.HandlerElapsed, failure.HandlerStageElapsed)
}

func TestAwaitTailTaskParentCancellationIsBoundedAndPreservesNonNilResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parentErr := context.Canceled
		panicErr := &tailHandlerPanicError{}
		panicResult := make(chan tailTaskResult, 1)
		go func() {
			time.Sleep(10 * time.Millisecond)
			panicResult <- tailTaskResult{err: panicErr, completedAt: time.Now()}
		}()
		startedAt := time.Now()
		require.Same(t, panicErr, awaitTailTaskParentCancellation(parentErr, panicResult))
		require.Equal(t, 10*time.Millisecond, time.Since(startedAt))

		ordinaryResult := make(chan tailTaskResult, 1)
		ordinaryErr := errors.New("ordinary handler error")
		go func() {
			time.Sleep(10 * time.Millisecond)
			ordinaryResult <- tailTaskResult{err: ordinaryErr, completedAt: time.Now()}
		}()
		startedAt = time.Now()
		require.Same(t, ordinaryErr, awaitTailTaskParentCancellation(parentErr, ordinaryResult))
		require.Equal(t, 10*time.Millisecond, time.Since(startedAt))

		startedAt = time.Now()
		err := awaitTailTaskParentCancellation(parentErr, make(chan tailTaskResult))
		require.ErrorIs(t, err, parentErr)
		var joinErr *tailHandlerJoinError
		require.ErrorAs(t, err, &joinErr)
		require.Equal(t, tailHandlerCancelGrace, time.Since(startedAt))
	})
}

func TestRunTailTaskPreservesPanicImmediatelyAfterParentCancellation(t *testing.T) {
	for _, timeout := range []time.Duration{0, 30 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			client := &Client{tailHandlerTimeout: timeout}
			started := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- client.runTailTask(parent, func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					panic("sensitive cancellation panic")
				})
			}()
			<-started
			cancel()

			err := <-done
			require.EqualError(t, err, "tail handler panic")
			require.NotContains(t, err.Error(), "sensitive cancellation panic")
			require.ErrorIs(t, parent.Err(), context.Canceled)
		})
	}
}

func TestRunTailTaskPreservesReturnedErrorImmediatelyAfterParentCancellation(t *testing.T) {
	for _, timeout := range []time.Duration{0, 30 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			client := &Client{tailHandlerTimeout: timeout}
			started := make(chan struct{})
			wantErr := errors.New("ordinary cancellation error")
			done := make(chan tailTaskExecution, 1)
			go func() {
				done <- client.runTailTaskExecution(parent, nil, func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return wantErr
				})
			}()
			<-started
			cancel()

			execution := <-done
			require.Same(t, wantErr, execution.err)
			require.True(t, execution.parentCancellation)
			require.True(t, execution.handlerReturnedErr)
			require.Equal(t, TailFailureJoinJoined, execution.joinOutcome)
			require.LessOrEqual(t, execution.joinElapsed, execution.handlerElapsed)
			require.ErrorIs(t, parent.Err(), context.Canceled)
		})
	}
}

func TestRunTailTaskTreatsPostDeadlineNilAsFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 5 * time.Second

		client := &Client{tailHandlerTimeout: timeout}
		startedAt := time.Now()
		execution := client.runTailTaskExecution(context.Background(), nil, func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		})
		require.Equal(t, timeout, time.Since(startedAt))
		require.ErrorIs(t, execution.err, context.DeadlineExceeded)
		var deadlineErr *tailHandlerDeadlineError
		require.ErrorAs(t, execution.err, &deadlineErr)
		require.True(t, deadlineErr.returnedNil)
		require.False(t, deadlineErr.detached)
		require.Equal(t, TailFailureJoinJoined, execution.joinOutcome)
		require.Zero(t, execution.joinElapsed)
		require.False(t, execution.forceFallback)
	})
}

func TestRunTailTaskPreservesCooperativePostDeadlineError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 5 * time.Second

		wantErr := errors.New("handler failed after deadline")
		client := &Client{tailHandlerTimeout: timeout}
		startedAt := time.Now()
		execution := client.runTailTaskExecution(context.Background(), nil, func(ctx context.Context) error {
			<-ctx.Done()
			return wantErr
		})
		require.Equal(t, timeout, time.Since(startedAt))
		require.ErrorIs(t, execution.err, wantErr)
		var deadlineErr *tailHandlerDeadlineError
		require.ErrorAs(t, execution.err, &deadlineErr)
		require.Same(t, wantErr, deadlineErr.cause)
		require.False(t, deadlineErr.returnedNil)
		require.False(t, deadlineErr.detached)
		require.Equal(t, TailFailureJoinJoined, execution.joinOutcome)
		require.Zero(t, execution.joinElapsed)
		require.False(t, execution.forceFallback)
	})
}

func TestAwaitTailTaskDeadlineHonorsBufferedPreDeadlineCompletion(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(-time.Millisecond)
	result := make(chan tailTaskResult, 1)
	wantErr := errors.New("completed before deadline")
	result <- tailTaskResult{
		err:         wantErr,
		completedAt: deadline.Add(-time.Millisecond),
	}
	client := &Client{tailHandlerTimeout: 10 * time.Millisecond}
	err := client.awaitTailTaskDeadline(
		result,
		deadline,
		deadline.Add(tailHandlerCancelGrace),
	)
	require.Same(t, wantErr, err)
	var deadlineErr *tailHandlerDeadlineError
	require.NotErrorAs(t, err, &deadlineErr)
}

func TestAwaitTailTaskDeadlineTreatsEqualAndLaterCompletionAsTimeout(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		offset time.Duration
	}{
		{name: "equal", offset: 0},
		{name: "later", offset: time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deadline := time.Now().Add(-time.Second)
			result := make(chan tailTaskResult, 1)
			wantErr := errors.New("completed at or after deadline")
			result <- tailTaskResult{
				err:         wantErr,
				completedAt: deadline.Add(tc.offset),
			}
			client := &Client{tailHandlerTimeout: 10 * time.Millisecond}
			err := client.awaitTailTaskDeadline(
				result,
				deadline,
				deadline.Add(tailHandlerCancelGrace),
			)
			require.ErrorIs(t, err, wantErr)
			var deadlineErr *tailHandlerDeadlineError
			require.ErrorAs(t, err, &deadlineErr)
			require.Same(t, wantErr, deadlineErr.cause)
			require.False(t, deadlineErr.returnedNil)
			require.False(t, deadlineErr.detached)
		})
	}
}

func TestAwaitTailTaskDeadlineFinalDrainUsesReadyResult(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(-tailHandlerCancelGrace)
	graceDeadline := deadline.Add(tailHandlerCancelGrace)
	for _, tc := range []struct {
		name         string
		completedAt  time.Time
		wantDetached bool
	}{
		{
			name:        "at local deadline",
			completedAt: deadline,
		},
		{
			name:        "just before grace deadline",
			completedAt: graceDeadline.Add(-time.Nanosecond),
		},
		{
			name:         "at grace deadline",
			completedAt:  graceDeadline,
			wantDetached: true,
		},
		{
			name:         "after grace deadline",
			completedAt:  graceDeadline.Add(time.Nanosecond),
			wantDetached: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := make(chan tailTaskResult, 1)
			wantErr := errors.New("completed with grace timer")
			result <- tailTaskResult{
				err:         wantErr,
				completedAt: tc.completedAt,
			}
			execution := finalTailTaskDeadlineExecution(
				10*time.Millisecond,
				result,
				deadline.Add(-10*time.Millisecond),
				deadline,
				graceDeadline,
			)
			var deadlineErr *tailHandlerDeadlineError
			require.ErrorAs(t, execution.err, &deadlineErr)
			require.Equal(t, tc.wantDetached, deadlineErr.detached)
			if tc.wantDetached {
				require.ErrorIs(t, execution.err, context.DeadlineExceeded)
				require.NotErrorIs(t, execution.err, wantErr)
				require.Equal(t, TailFailureJoinJoined, execution.joinOutcome)
				require.False(t, execution.forceFallback)
				return
			}
			require.ErrorIs(t, execution.err, wantErr)
			require.Same(t, wantErr, deadlineErr.cause)
			require.Equal(t, TailFailureJoinJoined, execution.joinOutcome)
			require.False(t, execution.forceFallback)
		})
	}
}

func TestAwaitTailTaskDeadlineFinalDrainAfterTimerExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deadline := time.Now()
		graceDeadline := deadline.Add(tailHandlerCancelGrace)
		result := make(chan tailTaskResult, 1)
		wantErr := errors.New("completed before grace timer")
		hookCalls := 0
		client := &Client{
			tailHandlerTimeout: 5 * time.Second,
			tailGraceTimerHook: func() {
				hookCalls++
				result <- tailTaskResult{
					err:         wantErr,
					completedAt: graceDeadline.Add(-time.Nanosecond),
				}
			},
		}

		execution := client.awaitTailTaskDeadlineExecution(
			result,
			deadline.Add(-5*time.Second),
			deadline,
			graceDeadline,
		)
		require.Equal(t, tailHandlerCancelGrace, time.Since(deadline))
		require.Equal(t, 1, hookCalls)
		require.ErrorIs(t, execution.err, wantErr)
		var deadlineErr *tailHandlerDeadlineError
		require.ErrorAs(t, execution.err, &deadlineErr)
		require.Same(t, wantErr, deadlineErr.cause)
		require.False(t, deadlineErr.detached)
		require.Equal(t, TailFailureJoinJoined, execution.joinOutcome)
		require.False(t, execution.forceFallback)
	})
}

func TestRunTailTaskReturnsEarlierParentDeadlinePromptly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			parentTimeout = 5 * time.Second
			localTimeout  = 30 * time.Second
		)

		parent, cancel := context.WithTimeout(context.Background(), parentTimeout)
		defer cancel()
		client := &Client{tailHandlerTimeout: localTimeout}
		started := make(chan struct{})
		release := make(chan struct{})
		finished := make(chan struct{})
		releaseHandler := sync.OnceFunc(func() { close(release) })
		defer releaseHandler()
		done := make(chan tailTaskExecution, 1)
		startedAt := time.Now()
		go func() {
			done <- client.runTailTaskExecution(parent, nil, func(context.Context) error {
				close(started)
				<-release
				close(finished)
				return nil
			})
		}()
		<-started

		execution := <-done
		require.Equal(t, parentTimeout+tailHandlerCancelGrace, time.Since(startedAt))
		require.ErrorIs(t, execution.err, context.DeadlineExceeded)
		var deadlineErr *tailHandlerDeadlineError
		require.NotErrorAs(t, execution.err, &deadlineErr)
		require.Equal(t, TailFailureJoinTimedOut, execution.joinOutcome)
		require.Equal(t, tailHandlerCancelGrace, execution.joinElapsed)
		require.True(t, execution.forceFallback)

		releaseHandler()
		<-finished
	})
}

func TestRunTailTaskPreservesParentCancellationBeforeDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			cancelAfter  = 5 * time.Second
			localTimeout = 30 * time.Second
		)

		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := &Client{tailHandlerTimeout: localTimeout}
		started := make(chan struct{})
		release := make(chan struct{})
		finished := make(chan struct{})
		releaseHandler := sync.OnceFunc(func() { close(release) })
		defer releaseHandler()
		done := make(chan tailTaskExecution, 1)
		startedAt := time.Now()
		go func() {
			done <- client.runTailTaskExecution(parent, nil, func(context.Context) error {
				close(started)
				<-release
				close(finished)
				return nil
			})
		}()
		<-started
		go func() {
			time.Sleep(cancelAfter)
			cancel()
		}()

		execution := <-done
		require.Equal(t, cancelAfter+tailHandlerCancelGrace, time.Since(startedAt))
		require.ErrorIs(t, execution.err, context.Canceled)
		var deadlineErr *tailHandlerDeadlineError
		require.NotErrorAs(t, execution.err, &deadlineErr)
		require.Equal(t, TailFailureJoinTimedOut, execution.joinOutcome)
		require.Equal(t, tailHandlerCancelGrace, execution.joinElapsed)
		require.True(t, execution.forceFallback)

		releaseHandler()
		<-finished
	})
}

func TestRunTailTaskPreservesLocalDeadlineAtParentCancellationBoundary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 5 * time.Second

		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := &Client{tailHandlerTimeout: timeout}
		started := make(chan struct{})
		release := make(chan struct{})
		finished := make(chan struct{})
		releaseHandler := sync.OnceFunc(func() { close(release) })
		defer releaseHandler()
		done := make(chan tailTaskExecution, 1)
		startedAt := time.Now()
		go func() {
			done <- client.runTailTaskExecution(parent, nil, func(context.Context) error {
				close(started)
				<-release
				close(finished)
				return nil
			})
		}()
		<-started
		go func() {
			time.Sleep(timeout)
			cancel()
		}()

		execution := <-done
		require.Equal(t, timeout+tailHandlerCancelGrace, time.Since(startedAt))
		require.ErrorIs(t, parent.Err(), context.Canceled)
		require.ErrorIs(t, execution.err, context.DeadlineExceeded)
		var deadlineErr *tailHandlerDeadlineError
		require.ErrorAs(t, execution.err, &deadlineErr)
		require.True(t, deadlineErr.detached)
		require.Equal(t, TailFailureJoinTimedOut, execution.joinOutcome)
		require.Equal(t, tailHandlerCancelGrace, execution.joinElapsed)
		require.True(t, execution.forceFallback)

		releaseHandler()
		<-finished
	})
}

func TestRunTailTaskPreservesDeadlineWhenParentCancelsDuringGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 5 * time.Second

		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := &Client{tailHandlerTimeout: timeout}
		started := make(chan struct{})
		release := make(chan struct{})
		finished := make(chan struct{})
		releaseHandler := sync.OnceFunc(func() { close(release) })
		defer releaseHandler()
		done := make(chan tailTaskExecution, 1)
		startedAt := time.Now()
		go func() {
			done <- client.runTailTaskExecution(parent, nil, func(context.Context) error {
				close(started)
				<-release
				close(finished)
				return nil
			})
		}()
		<-started
		time.Sleep(timeout)
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case execution := <-done:
			t.Fatalf("runTailTask returned before the fixed grace deadline: %v", execution.err)
		default:
		}

		execution := <-done
		require.Equal(t, timeout+tailHandlerCancelGrace, time.Since(startedAt))
		require.ErrorIs(t, execution.err, context.DeadlineExceeded)
		var deadlineErr *tailHandlerDeadlineError
		require.ErrorAs(t, execution.err, &deadlineErr)
		require.True(t, deadlineErr.detached)
		require.Equal(t, TailFailureJoinTimedOut, execution.joinOutcome)
		require.Equal(t, tailHandlerCancelGrace, execution.joinElapsed)
		require.True(t, execution.forceFallback)

		releaseHandler()
		<-finished
	})
}

func TestTailTaskMetadataIsNilSafe(t *testing.T) {
	t.Parallel()

	task := newMessageTailTask(
		"MESSAGE_UPDATE",
		nil,
		nil,
		&discordgo.Message{
			ID:        "m1",
			GuildID:   "g1",
			ChannelID: "c1",
			Author:    &discordgo.User{ID: "u1"},
		},
	)
	require.Equal(t, "MESSAGE_UPDATE", task.eventType)
	require.Equal(t, tailFailureClassOrdered, task.failureClass)
	require.Equal(t, "g1", task.guildID)
	require.Equal(t, "c1", task.channelID)
	require.Equal(t, "m1", task.messageID)
	require.Equal(t, "u1", task.userID)
	require.NotNil(t, task.failureMetadata)
	require.Nil(t, task.run)

	metadata := newTailFailureMetadata(tailTask{
		channelID: "c1",
		messageID: "m1",
	})
	metadataCtx := withTailFailureMetadata(context.Background(), metadata)
	EnrichTailFailureMetadata(metadataCtx, &discordgo.Message{
		ID:        "m1",
		GuildID:   "g-refetched",
		ChannelID: "c1",
		Author:    &discordgo.User{ID: "u-refetched"},
	})
	snapshot := metadata.snapshot(time.Now())
	require.Equal(t, "g-refetched", snapshot.guildID)
	require.Equal(t, "c1", snapshot.channelID)
	require.Equal(t, "m1", snapshot.messageID)
	require.Equal(t, "u-refetched", snapshot.userID)
	emptyCtx := context.Background()
	EnrichTailFailureMetadata(emptyCtx, &discordgo.Message{GuildID: "ignored"})
	EnrichTailFailureMetadata(emptyCtx, nil)
	require.Equal(t, emptyCtx, withTailFailureMetadata(emptyCtx, nil))

	require.Equal(t, "c2", newChannelTailTask(
		"CHANNEL_UPDATE",
		nil,
		nil,
		&discordgo.Channel{ID: "c2"},
	).channelID)
	require.Equal(t, "u2", newMemberTailTask(
		"GUILD_MEMBER_UPDATE",
		nil,
		nil,
		&discordgo.Member{User: &discordgo.User{ID: "u2"}},
	).userID)
}

func TestTailFailureStageContextIsSafeAndSnapshotsElapsedTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := SetTailFailureStage(context.Background(), TailFailureStageHandler)
		time.Sleep(2 * time.Second)
		UpdateTailFailureStage(ctx, TailFailureStageMessageUpdateRefetch)
		time.Sleep(3 * time.Second)

		metadata, ok := ctx.Value(tailFailureMetadataContextKey{}).(*tailFailureMetadata)
		require.True(t, ok)
		snapshot := metadata.snapshot(time.Now())
		require.Equal(t, TailFailureStageMessageUpdateRefetch, snapshot.handlerStage)
		require.Equal(t, 3*time.Second, snapshot.handlerStageElapsed)

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		UpdateTailFailureStage(cancelCtx, TailFailureStage("sensitive raw stage"))
		snapshot = metadata.snapshot(time.Now())
		require.Equal(t, TailFailureStageMessageUpdateRefetch, snapshot.handlerStage)
		require.Equal(t, 3*time.Second, snapshot.handlerStageElapsed)

		metadata.freezeStageAt(time.Now())
		UpdateTailFailureStage(ctx, TailFailureStageCanonicalWrite)
		metadata.freezeStageAt(time.Now())
		snapshot = metadata.snapshot(time.Now())
		require.Equal(t, TailFailureStageMessageUpdateRefetch, snapshot.handlerStage)

		ctx = SetTailFailureStage(context.Background(), TailFailureStage("sensitive raw stage"))
		metadata, ok = ctx.Value(tailFailureMetadataContextKey{}).(*tailFailureMetadata)
		require.True(t, ok)
		require.Equal(t, TailFailureStageUnknown, metadata.snapshot(time.Now()).handlerStage)
		var nilContext context.Context
		require.Nil(t, SetTailFailureStage(nilContext, TailFailureStageHandler))
		UpdateTailFailureStage(nilContext, TailFailureStageHandler)

		var nilMetadata *tailFailureMetadata
		nilMetadata.setStageAt(TailFailureStageHandler, time.Now())
		nilMetadata.freezeStageAt(time.Now())
		require.Equal(
			t,
			TailFailureStageUnknown,
			nilMetadata.snapshot(time.Now()).handlerStage,
		)
	})
}

func TestNormalizeTailFailureStageAllowsOnlySafeVocabulary(t *testing.T) {
	t.Parallel()

	for _, stage := range []TailFailureStage{
		TailFailureStageHandler,
		TailFailureStageMessageUpdateRefetch,
		TailFailureStageMessageBuild,
		TailFailureStageCanonicalWrite,
		TailFailureStageEventAppend,
		TailFailureStageStateUpdate,
		TailFailureStageCursorAdvance,
		TailFailureStageCanonicalDelete,
		TailFailureStageFailureResolution,
	} {
		require.Equal(t, stage, normalizeTailFailureStage(stage))
	}
	require.Equal(t, TailFailureStageUnknown, normalizeTailFailureStage("sensitive raw stage"))
}

func TestTailFailureReportingUsesSafeMetadata(t *testing.T) {
	t.Parallel()

	handler := &failureContinuationHandler{
		failureReported: make(chan struct{}),
		failures:        make(chan TailFailure, 1),
	}
	reportTailFailure(handler, newTailFailure(tailTask{
		eventType: "MESSAGE_CREATE",
		guildID:   "g1",
		channelID: "c1",
		messageID: "m1",
		userID:    "u1",
	}, context.Canceled))
	require.Equal(t, TailFailure{
		EventType:    "MESSAGE_CREATE",
		Kind:         "returned_error",
		GuildID:      "g1",
		ChannelID:    "c1",
		MessageID:    "m1",
		UserID:       "u1",
		HandlerStage: TailFailureStageUnknown,
		JoinOutcome:  TailFailureJoinNotRequired,
	}, <-handler.failures)
}

func TestTailFailureRecorderRequirements(t *testing.T) {
	t.Parallel()

	require.ErrorContains(t, recordTailFailure(nil, TailFailure{}), "recorder unavailable")
	require.ErrorContains(t, recordTailFailure(nil, TailFailure{MessageID: "m1"}), "recorder unavailable")
	reportTailFailure(nil, TailFailure{})

	wantErr := errors.New("ledger unavailable")
	recorder := &tailFailureRecorderStub{err: wantErr}
	require.ErrorIs(t, recordTailFailure(recorder, TailFailure{MessageID: "m1"}), wantErr)
	require.Equal(t, TailFailure{MessageID: "m1"}, recorder.failure)

	err := recordTailFailure(panicTailFailureRecorder{}, TailFailure{MessageID: "m1"})
	require.EqualError(t, err, "tail failure recorder panicked")
	require.NotContains(t, err.Error(), "sensitive recorder panic")
}

func TestRequestContextHonorsExistingDeadlineAndDisabledTimeout(t *testing.T) {
	t.Parallel()

	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := &Client{requestTimeout: 20 * time.Millisecond}
	reqCtx, reqCancel := client.requestContext(parent)
	reqCancel()
	parentDeadline, ok := parent.Deadline()
	require.True(t, ok)
	reqDeadline, ok := reqCtx.Deadline()
	require.True(t, ok)
	require.Equal(t, parentDeadline, reqDeadline)

	reqCtx, reqCancel = (&Client{}).requestContext(context.Background())
	defer reqCancel()
	_, ok = reqCtx.Deadline()
	require.False(t, ok)
}

func TestTailQueueAndWorkerSizing(t *testing.T) {
	client := &Client{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	workCh := make(chan tailTask)
	fatal := newTailFatalState()
	task := tailTask{run: func(context.Context) error { return nil }}
	client.enqueueTailTask(ctx, workCh, fatal, task)
	require.NoError(t, fatal.err())

	ctx = context.Background()
	fullWorkCh := make(chan tailTask)
	fatal = newTailFatalState()
	client.enqueueTailTask(ctx, fullWorkCh, fatal, task)
	require.ErrorContains(t, fatal.err(), "tail worker queue full")
	fatal.signal(errors.New("existing"))
	client.enqueueTailTask(ctx, fullWorkCh, fatal, task)
	require.ErrorContains(t, fatal.err(), "existing")
	require.ErrorContains(t, fatal.err(), "tail worker queue full")

	prev := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prev)
	require.Equal(t, 4, defaultTailWorkerCount())
	runtime.GOMAXPROCS(8)
	require.Equal(t, 8, defaultTailWorkerCount())
	runtime.GOMAXPROCS(32)
	require.Equal(t, 16, defaultTailWorkerCount())
	require.Equal(t, defaultTailWorkerCount()*32, defaultTailQueueSize())
}

func TestClientChannelMessagesTimesOut(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/channels/c1/messages", func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.requestTimeout = 20 * time.Millisecond

	start := time.Now()
	_, err = client.ChannelMessages(context.Background(), "c1", 100, "", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context deadline exceeded"))
	require.Less(t, time.Since(start), time.Second)
}

func TestUniqueChannels(t *testing.T) {
	channels := uniqueChannels([]*discordgo.Channel{
		{ID: "2"},
		{ID: "1"},
		{ID: "2"},
		nil,
	})
	require.Len(t, channels, 2)
	require.Equal(t, "1", channels[0].ID)
	require.Equal(t, "2", channels[1].ID)
}

func TestTailReceivesGatewayEvents(t *testing.T) {
	mux := http.NewServeMux()
	upgrader := websocket.Upgrader{}
	gatewayURL := ""
	mux.HandleFunc("/api/v10/gateway", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"url": gatewayURL})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	gatewayURL = "ws" + server.URL[len("http"):] + "/gateway"
	gatewayHandler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade gateway: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if err := conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		}); err != nil {
			t.Errorf("write hello: %v", err)
			return
		}
		_, _, err = conn.ReadMessage()
		if err != nil {
			t.Errorf("read identify: %v", err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "READY",
			"s":  1,
			"d": map[string]any{
				"session_id": "session",
				"user":       map[string]any{"id": "bot", "username": "bot"},
			},
		}); err != nil {
			t.Errorf("write ready: %v", err)
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		events := []map[string]any{
			{"op": 0, "t": "MESSAGE_CREATE", "s": 2, "d": map[string]any{"id": "m1", "guild_id": "g1", "channel_id": "c1", "content": "hello", "timestamp": now, "author": map[string]any{"id": "u1", "username": "user"}}},
			{"op": 0, "t": "MESSAGE_UPDATE", "s": 3, "d": map[string]any{"id": "m1", "guild_id": "g1", "channel_id": "c1", "content": "hello 2", "timestamp": now, "author": map[string]any{"id": "u1", "username": "user"}}},
			{"op": 0, "t": "MESSAGE_DELETE", "s": 4, "d": map[string]any{"id": "m1", "guild_id": "g1", "channel_id": "c1"}},
			{"op": 0, "t": "CHANNEL_CREATE", "s": 5, "d": map[string]any{"id": "c1", "guild_id": "g1", "name": "general", "type": 0}},
			{"op": 0, "t": "GUILD_MEMBER_ADD", "s": 6, "d": map[string]any{"guild_id": "g1", "user": map[string]any{"id": "u1", "username": "user"}, "roles": []string{}}},
			{"op": 0, "t": "GUILD_MEMBER_REMOVE", "s": 7, "d": map[string]any{"guild_id": "g1", "user": map[string]any{"id": "u1", "username": "user"}}},
		}
		for _, event := range events {
			if err := conn.WriteJSON(event); err != nil {
				t.Errorf("write event %v: %v", event["t"], err)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	mux.HandleFunc("/gateway", gatewayHandler)
	mux.HandleFunc("/gateway/", gatewayHandler)

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	require.Zero(t, discordSessionHandlerCount(client.session))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := &recordingHandler{}
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	require.NoError(t, client.Tail(ctx, handler))
	require.Equal(t, 1, handler.creates)
	require.Equal(t, 1, handler.updates)
	require.Equal(t, 1, handler.deletes)
	require.Equal(t, 1, handler.channels)
	require.Equal(t, 1, handler.memberUpserts)
	require.Equal(t, 1, handler.memberDeletes)
	require.Equal(t, 1, handler.ready)
	require.Zero(t, discordSessionHandlerCount(client.session))
}

func TestTailReportsScopedChannelAndMemberUpdateFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handler := &updateFailureHandler{
		failures: make(chan TailFailure, 2),
		reported: make(chan struct{}, 2),
	}
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "CHANNEL_UPDATE",
			"s":  2,
			"d": map[string]any{
				"id":       "c1",
				"guild_id": "g1",
				"name":     "renamed-channel",
				"type":     0,
			},
		}); err != nil {
			t.Errorf("write channel update: %v", err)
			return
		}
		select {
		case <-handler.reported:
		case <-ctx.Done():
			t.Error("channel update failure was not reported")
			return
		}

		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "GUILD_MEMBER_UPDATE",
			"s":  3,
			"d": map[string]any{
				"guild_id": "g1",
				"nick":     "renamed-member",
				"roles":    []string{"r1"},
				"user":     map[string]any{"id": "u1", "username": "test-user"},
			},
		}); err != nil {
			t.Errorf("write member update: %v", err)
			return
		}
		select {
		case <-handler.reported:
			cancel()
		case <-ctx.Done():
			t.Error("member update failure was not reported")
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 2

	require.NoError(t, client.Tail(ctx, handler))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.EqualValues(t, 1, handler.channelCalls.Load())
	require.EqualValues(t, 1, handler.memberCalls.Load())
	require.Zero(t, handler.recordCalls.Load())
	assertTailFailure(t, <-handler.failures, TailFailure{
		EventType: "CHANNEL_UPDATE",
		Kind:      "returned_error",
		GuildID:   "g1",
		ChannelID: "c1",
	}, TailFailureStageHandler, TailFailureJoinNotRequired, false)
	assertTailFailure(t, <-handler.failures, TailFailure{
		EventType: "GUILD_MEMBER_UPDATE",
		Kind:      "returned_error",
		GuildID:   "g1",
		UserID:    "u1",
	}, TailFailureStageHandler, TailFailureJoinNotRequired, false)
}

func TestTailContinuesAfterNonMessagePanicWithoutRecorder(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	handler := &nonMessagePanicContinuationHandler{
		cancel:          cancelTail,
		failureReported: make(chan struct{}),
		failures:        make(chan TailFailure, 1),
		laterHandled:    make(chan struct{}),
	}
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "CHANNEL_UPDATE",
			"s":  2,
			"d": map[string]any{
				"id":       "failed-channel",
				"guild_id": "g1",
				"name":     "failed-channel",
				"type":     0,
			},
		}); err != nil {
			t.Errorf("write failed channel update: %v", err)
			return
		}
		select {
		case <-handler.failureReported:
		case <-testCtx.Done():
			t.Error("non-message panic was not reported")
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "CHANNEL_UPDATE",
			"s":  3,
			"d": map[string]any{
				"id":       "later-channel",
				"guild_id": "g1",
				"name":     "later-channel",
				"type":     0,
			},
		}); err != nil {
			t.Errorf("write later channel update: %v", err)
			return
		}
		select {
		case <-handler.laterHandled:
		case <-testCtx.Done():
			t.Error("later non-message event was not handled")
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1

	require.NoError(t, client.Tail(tailCtx, handler))
	require.ErrorIs(t, tailCtx.Err(), context.Canceled)
	require.EqualValues(t, 2, handler.calls.Load())
	assertTailFailure(t, <-handler.failures, TailFailure{
		EventType: "CHANNEL_UPDATE",
		Kind:      "panic",
		GuildID:   "g1",
		ChannelID: "failed-channel",
	}, TailFailureStageHandler, TailFailureJoinNotRequired, false)
	select {
	case <-handler.laterHandled:
	default:
		t.Fatal("Tail returned before the later non-message event was handled")
	}
}

func TestTailContinuesAfterHandlerFailure(t *testing.T) {
	tests := []struct {
		name     string
		fail     func(context.Context) error
		wantKind string
	}{
		{
			name: "returned error",
			fail: func(context.Context) error {
				return errors.New("sensitive returned error")
			},
			wantKind: "returned_error",
		},
		{
			name: "panic",
			fail: func(context.Context) error {
				panic("sensitive panic value")
			},
			wantKind: "panic",
		},
		{
			name: "timeout",
			fail: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantKind: "timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			tailCtx, cancel := context.WithCancel(testCtx)
			defer cancel()

			handler := &failureContinuationHandler{
				fail:            tt.fail,
				cancel:          cancel,
				failureReported: make(chan struct{}),
				failures:        make(chan TailFailure, 1),
				recorded:        make(chan TailFailure, 1),
				laterHandled:    make(chan struct{}),
			}
			server := newTailTestGateway(t, func(conn *websocket.Conn) {
				now := time.Now().UTC().Format(time.RFC3339)
				if err := conn.WriteJSON(messageCreateEvent(2, "failed", now)); err != nil {
					t.Errorf("write failed event: %v", err)
					return
				}
				select {
				case <-handler.failureReported:
				case <-testCtx.Done():
					t.Error("tail failure was not reported")
					return
				}
				if err := conn.WriteJSON(messageCreateEvent(3, "later", now)); err != nil {
					t.Errorf("write later event: %v", err)
					return
				}
				select {
				case <-handler.laterHandled:
				case <-testCtx.Done():
					t.Error("later tail event was not handled")
				}
			})
			defer server.Close()

			restore := patchDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()

			client, err := New("token")
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			client.session.ShouldReconnectOnError = false
			client.tailWorkerCount = 1
			client.tailQueueSize = 1
			client.tailHandlerTimeout = 25 * time.Millisecond

			require.NoError(t, client.Tail(tailCtx, handler))
			require.ErrorIs(t, tailCtx.Err(), context.Canceled)
			failure := <-handler.failures
			joinOutcome := TailFailureJoinNotRequired
			if tt.wantKind == "timeout" {
				joinOutcome = TailFailureJoinJoined
			}
			assertTailFailure(t, failure, TailFailure{
				EventType: "MESSAGE_CREATE",
				Kind:      tt.wantKind,
				GuildID:   "g1",
				ChannelID: "c1",
				MessageID: "failed",
				UserID:    "u1",
			}, TailFailureStageHandler, joinOutcome, false)
			assertTailFailure(t, <-handler.recorded, TailFailure{
				EventType: "MESSAGE_CREATE",
				Kind:      tt.wantKind,
				GuildID:   "g1",
				ChannelID: "c1",
				MessageID: "failed",
				UserID:    "u1",
			}, TailFailureStageHandler, joinOutcome, false)
			require.EqualValues(t, 1, handler.recordCalls.Load())
			select {
			case <-handler.laterHandled:
			default:
				t.Fatal("Tail returned before the later event was handled")
			}
		})
	}
}

func TestTailMessageDeleteFailureRecordsExactlyOnce(t *testing.T) {
	tests := []struct {
		name        string
		fail        func(context.Context) error
		wantKind    string
		joinOutcome TailFailureJoinOutcome
	}{
		{
			name: "returned error",
			fail: func(context.Context) error {
				return errors.New("sensitive delete error")
			},
			wantKind:    "returned_error",
			joinOutcome: TailFailureJoinNotRequired,
		},
		{
			name: "panic",
			fail: func(context.Context) error {
				panic("sensitive delete panic")
			},
			wantKind:    "panic",
			joinOutcome: TailFailureJoinNotRequired,
		},
		{
			name: "cooperative timeout",
			fail: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantKind:    "timeout",
			joinOutcome: TailFailureJoinJoined,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelTest()
			tailCtx, cancelTail := context.WithCancel(testCtx)
			defer cancelTail()

			handler := &messageDeleteFailureHandler{
				fail:            tt.fail,
				cancel:          cancelTail,
				failures:        make(chan TailFailure, 1),
				recorded:        make(chan TailFailure, 1),
				failureReported: make(chan struct{}),
			}
			server := newTailTestGateway(t, func(conn *websocket.Conn) {
				if err := conn.WriteJSON(messageDeleteEvent(2, "deleted")); err != nil {
					t.Errorf("write delete event: %v", err)
					return
				}
				select {
				case <-handler.failureReported:
				case <-testCtx.Done():
					t.Error("delete failure was not reported")
				}
			})
			defer server.Close()

			restore := patchDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()

			client, err := New("token")
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			client.session.ShouldReconnectOnError = false
			client.tailWorkerCount = 1
			client.tailQueueSize = 1
			client.tailHandlerTimeout = 25 * time.Millisecond

			require.NoError(t, client.Tail(tailCtx, handler))
			require.ErrorIs(t, tailCtx.Err(), context.Canceled)
			expected := TailFailure{
				EventType: "MESSAGE_DELETE",
				Kind:      tt.wantKind,
				GuildID:   "g1",
				ChannelID: "c1",
				MessageID: "deleted",
			}
			assertTailFailure(
				t,
				<-handler.recorded,
				expected,
				TailFailureStageHandler,
				tt.joinOutcome,
				false,
			)
			assertTailFailure(
				t,
				<-handler.failures,
				expected,
				TailFailureStageHandler,
				tt.joinOutcome,
				false,
			)
			require.EqualValues(t, 1, handler.calls.Load())
			require.EqualValues(t, 1, handler.recordCalls.Load())
		})
	}
}

func TestTailPanicRecordsBeforeReportingAndContinuation(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	allowRecord := make(chan struct{})
	laterQueued := make(chan struct{})
	handler := &panicDurabilityHandler{
		panicValue:       "sensitive panic value",
		cancel:           cancelTail,
		recordingStarted: make(chan struct{}),
		allowRecord:      allowRecord,
		recorded:         make(chan TailFailure, 1),
		reported:         make(chan TailFailure, 1),
		laterHandled:     make(chan struct{}),
	}
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "failed", now)); err != nil {
			t.Errorf("write failed event: %v", err)
			return
		}
		select {
		case <-handler.recordingStarted:
		case <-testCtx.Done():
			t.Error("panic failure recording did not start")
			return
		}
		if err := conn.WriteJSON(messageCreateEvent(3, "later", now)); err != nil {
			t.Errorf("write later event: %v", err)
			return
		}
		close(laterQueued)
		<-tailCtx.Done()
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1

	done := make(chan error, 1)
	go func() {
		done <- client.Tail(tailCtx, handler)
	}()

	select {
	case <-handler.recordingStarted:
	case <-testCtx.Done():
		t.Fatal("panic failure recording did not start")
	}
	select {
	case <-laterQueued:
	case <-testCtx.Done():
		t.Fatal("later event was not queued")
	}
	select {
	case failure := <-handler.reported:
		t.Fatalf("panic was reported before persistence completed: %+v", failure)
	default:
	}
	select {
	case <-handler.laterHandled:
		t.Fatal("later event ran before panic persistence completed")
	default:
	}
	select {
	case err := <-done:
		t.Fatalf("Tail returned before panic persistence completed: %v", err)
	default:
	}

	close(allowRecord)
	select {
	case err = <-done:
	case <-testCtx.Done():
		t.Fatal("Tail did not continue after panic persistence completed")
	}
	require.NoError(t, err)
	require.ErrorIs(t, tailCtx.Err(), context.Canceled)

	wantFailure := TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "panic",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "failed",
		UserID:    "u1",
	}
	assertTailFailure(
		t,
		<-handler.recorded,
		wantFailure,
		TailFailureStageHandler,
		TailFailureJoinNotRequired,
		false,
	)
	assertTailFailure(
		t,
		<-handler.reported,
		wantFailure,
		TailFailureStageHandler,
		TailFailureJoinNotRequired,
		false,
	)
	require.Equal(t, []string{"record", "report", "later"}, handler.stepsSnapshot())
}

func TestTailPanicRecorderFailureIsGenericFatal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	const (
		sensitivePanicValue = "sensitive panic value"
		sensitiveRecordErr  = "sensitive recorder detail"
	)
	handler := &panicDurabilityHandler{
		panicValue:       sensitivePanicValue,
		recordErr:        errors.New(sensitiveRecordErr),
		recordingStarted: make(chan struct{}),
		recorded:         make(chan TailFailure, 1),
		reported:         make(chan TailFailure, 1),
		laterHandled:     make(chan struct{}),
	}
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "failed", now)); err != nil {
			t.Errorf("write failed event: %v", err)
			return
		}
		<-ctx.Done()
	})
	defer server.Close()
	defer cancel()
	defer cancel()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1

	err = client.Tail(ctx, handler)
	require.Error(t, err)
	require.True(t, IsFatalTailError(err))
	require.ErrorContains(t, err, "persist tail message failure")
	require.NotContains(t, err.Error(), sensitivePanicValue)
	require.NotContains(t, err.Error(), sensitiveRecordErr)
	require.EqualValues(t, 1, handler.calls.Load())
	require.Equal(t, "panic", (<-handler.recorded).Kind)
	select {
	case failure := <-handler.reported:
		t.Fatalf("panic recorder failure was reported instead of failing closed: %+v", failure)
	default:
	}
}

func TestTailMessageFailureWithoutRecorderFailsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handler := &messageFailureWithoutRecorderHandler{
		reported: make(chan TailFailure, 1),
	}
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "failed", now)); err != nil {
			t.Errorf("write failed event: %v", err)
			return
		}
		<-ctx.Done()
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1

	err = client.Tail(ctx, handler)
	require.Error(t, err)
	require.True(t, IsFatalTailError(err))
	require.ErrorContains(t, err, "persist tail message failure")
	require.NotContains(t, err.Error(), "sensitive returned error")
	require.EqualValues(t, 1, handler.calls.Load())
	select {
	case failure := <-handler.reported:
		t.Fatalf("failure was reported after recorder fail-closed: %+v", failure)
	default:
	}
}

func TestTailMessagePanicRecorderFailureAfterParentCancellationIsGenericFatal(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	const (
		sensitivePanicValue = "sensitive cancellation panic value"
		sensitiveRecordErr  = "sensitive cancellation recorder detail"
	)
	handler := &panicDurabilityHandler{
		panicValue:             sensitivePanicValue,
		panicAfterCancellation: true,
		handlerStarted:         make(chan struct{}),
		recordErr:              errors.New(sensitiveRecordErr),
		recordingStarted:       make(chan struct{}),
		recorded:               make(chan TailFailure, 1),
		reported:               make(chan TailFailure, 1),
	}
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "failed", now)); err != nil {
			t.Errorf("write failed event: %v", err)
			return
		}
		<-tailCtx.Done()
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1

	done := make(chan error, 1)
	go func() {
		done <- client.Tail(tailCtx, handler)
	}()
	select {
	case <-handler.handlerStarted:
	case <-testCtx.Done():
		t.Fatal("cancellation panic handler did not start")
	}
	cancelTail()
	select {
	case err = <-done:
	case <-testCtx.Done():
		t.Fatal("Tail did not return after cancellation panic")
	}

	require.Error(t, err)
	require.True(t, IsFatalTailError(err))
	require.ErrorContains(t, err, "persist tail message failure")
	require.NotContains(t, err.Error(), sensitivePanicValue)
	require.NotContains(t, err.Error(), sensitiveRecordErr)
	require.EqualValues(t, 1, handler.calls.Load())
	assertTailFailure(t, <-handler.recorded, TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "panic",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "failed",
		UserID:    "u1",
	}, TailFailureStageHandler, TailFailureJoinJoined, false)
	select {
	case failure := <-handler.reported:
		t.Fatalf("cancellation panic recorder failure was reported instead of failing closed: %+v", failure)
	default:
	}
}

func TestTailParentCancellationPreservesReturnedMessageFailureExactlyOnce(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	handler := &parentCancellationErrorHandler{
		started:  make(chan struct{}),
		failures: make(chan TailFailure, 1),
		recorded: make(chan TailFailure, 1),
	}
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "parent-race", now)); err != nil {
			t.Errorf("write parent race event: %v", err)
			return
		}
		select {
		case <-handler.started:
			cancelTail()
		case <-testCtx.Done():
			t.Error("parent race handler did not start")
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1
	client.tailHandlerTimeout = 30 * time.Second

	require.NoError(t, client.Tail(tailCtx, handler))
	require.ErrorIs(t, tailCtx.Err(), context.Canceled)
	require.EqualValues(t, 1, handler.calls.Load())
	require.EqualValues(t, 1, handler.recordCalls.Load())
	expected := TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "returned_error",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "parent-race",
		UserID:    "u1",
	}
	assertTailFailure(
		t,
		<-handler.recorded,
		expected,
		TailFailureStageHandler,
		TailFailureJoinJoined,
		false,
	)
	assertTailFailure(
		t,
		<-handler.failures,
		expected,
		TailFailureStageHandler,
		TailFailureJoinJoined,
		false,
	)
}

func TestTailEscalatesAfterConsecutiveHandlerFailures(t *testing.T) {
	tests := []struct {
		name     string
		fail     func(context.Context) error
		wantKind string
	}{
		{
			name: "returned error",
			fail: func(context.Context) error {
				return errors.New("persistent handler failure")
			},
			wantKind: "returned_error",
		},
		{
			name: "panic",
			fail: func(context.Context) error {
				panic("persistent handler panic")
			},
			wantKind: "panic",
		},
		{
			name: "deadline error",
			fail: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantKind: "timeout",
		},
		{
			name: "post-deadline nil",
			fail: func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			},
			wantKind: "timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			handler := &persistentFailureHandler{
				fail:     tt.fail,
				failures: make(chan TailFailure, defaultTailHandlerFailureLimit),
			}
			server := newTailTestGateway(t, func(conn *websocket.Conn) {
				now := time.Now().UTC().Format(time.RFC3339)
				for sequence := range defaultTailHandlerFailureLimit {
					messageID := fmt.Sprintf("failed-%d", sequence+1)
					if err := conn.WriteJSON(messageCreateEvent(sequence+2, messageID, now)); err != nil {
						t.Errorf("write failed event: %v", err)
						return
					}
					select {
					case failure := <-handler.failures:
						require.Equal(t, tt.wantKind, failure.Kind)
					case <-ctx.Done():
						t.Error("tail failure was not reported")
						return
					}
				}
			})
			defer server.Close()

			restore := patchDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()

			client, err := New("token")
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			client.session.ShouldReconnectOnError = false
			client.tailWorkerCount = 1
			client.tailQueueSize = defaultTailHandlerFailureLimit
			client.tailHandlerTimeout = 25 * time.Millisecond

			err = client.Tail(ctx, handler)
			require.ErrorContains(
				t,
				err,
				"tail handler circuit breaker opened after 3 consecutive failures",
			)
			require.NoError(t, ctx.Err())
			require.EqualValues(t, defaultTailHandlerFailureLimit, handler.calls.Load())
		})
	}
}

func TestTailMessageFailureCircuitIgnoresMemberSuccesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handler := &mixedFailureHandler{
		failures: make(chan TailFailure, defaultTailHandlerFailureLimit),
		members:  make(chan struct{}, defaultTailHandlerFailureLimit-1),
	}
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		for sequence := range defaultTailHandlerFailureLimit {
			if err := conn.WriteJSON(messageCreateEvent(sequence*2+2, fmt.Sprintf("failed-%d", sequence+1), now)); err != nil {
				t.Errorf("write failed event: %v", err)
				return
			}
			select {
			case <-handler.failures:
			case <-ctx.Done():
				t.Error("message failure was not reported")
				return
			}
			if sequence == defaultTailHandlerFailureLimit-1 {
				continue
			}
			if err := conn.WriteJSON(memberAddEvent(sequence*2 + 3)); err != nil {
				t.Errorf("write member event: %v", err)
				return
			}
			select {
			case <-handler.members:
			case <-ctx.Done():
				t.Error("member success was not observed")
				return
			}
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = defaultTailHandlerFailureLimit

	err = client.Tail(ctx, handler)
	require.ErrorContains(t, err, "tail handler circuit breaker opened after 3 consecutive failures")
	require.NoError(t, ctx.Err())
	require.EqualValues(t, defaultTailHandlerFailureLimit, handler.calls.Load())
}

func TestTailTimeoutStopsOrderedWorkAndReturnsAfterDurableReport(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	handler := &nonCooperativeFailureHandler{
		failures:         make(chan TailFailure, 1),
		recorded:         make(chan TailFailure, 1),
		failureReported:  make(chan struct{}),
		recordingStarted: make(chan struct{}),
		allowRecord:      make(chan struct{}),
		started:          make(chan string, 1),
		release:          make(chan struct{}),
		finished:         make(chan string, 1),
	}
	defer func() {
		close(handler.release)
		select {
		case <-handler.finished:
		case <-testCtx.Done():
			t.Error("detached handler did not finish after release")
		}
	}()
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "blocked-1", now)); err != nil {
			t.Errorf("write blocked event: %v", err)
			return
		}
		select {
		case <-handler.started:
		case <-testCtx.Done():
			t.Error("non-cooperative handler did not start")
			return
		}
		if err := conn.WriteJSON(messageCreateEvent(3, "must-not-run", now)); err != nil {
			t.Errorf("write later event: %v", err)
			return
		}
		select {
		case <-handler.failureReported:
		case <-testCtx.Done():
			t.Error("non-cooperative handler failure was not reported")
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = defaultTailHandlerFailureLimit
	client.tailHandlerTimeout = 25 * time.Millisecond

	started := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- client.Tail(tailCtx, handler)
	}()
	select {
	case <-handler.recordingStarted:
	case <-testCtx.Done():
		t.Fatal("timed-out failure recording did not start")
	}
	cancelTail()
	select {
	case err = <-done:
		t.Fatalf("Tail returned before the timed-out failure was durably recorded: %v", err)
	default:
	}
	close(handler.allowRecord)
	select {
	case err = <-done:
	case <-testCtx.Done():
		t.Fatal("Tail did not return after timed-out failure recording completed")
	}
	require.ErrorContains(t, err, "tail ordered handler timed out for MESSAGE_CREATE")
	require.Less(t, time.Since(started), 3*time.Second)
	require.ErrorIs(t, tailCtx.Err(), context.Canceled)
	require.EqualValues(t, 1, handler.calls.Load())
	require.EqualValues(t, 1, handler.recordCalls.Load())
	assertTailFailure(t, <-handler.recorded, TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "timeout",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "blocked-1",
		UserID:    "u1",
	}, TailFailureStageHandler, TailFailureJoinTimedOut, true)
	assertTailFailure(t, <-handler.failures, TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "timeout",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "blocked-1",
		UserID:    "u1",
	}, TailFailureStageHandler, TailFailureJoinTimedOut, true)
}

func TestTailLocalTimeoutIdentitySurvivesParentCancellationRace(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	allowRecord := make(chan struct{})
	close(allowRecord)
	handler := &nonCooperativeFailureHandler{
		failures:         make(chan TailFailure, 1),
		recorded:         make(chan TailFailure, 1),
		failureReported:  make(chan struct{}),
		recordingStarted: make(chan struct{}),
		allowRecord:      allowRecord,
		started:          make(chan string, 1),
		release:          make(chan struct{}),
		finished:         make(chan string, 1),
		cancelOnContext:  cancelTail,
	}
	defer func() {
		close(handler.release)
		select {
		case <-handler.finished:
		case <-testCtx.Done():
			t.Error("racing handler did not finish after release")
		}
	}()
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "racing-timeout", now)); err != nil {
			t.Errorf("write racing timeout event: %v", err)
			return
		}
		select {
		case <-handler.recordingStarted:
		case <-testCtx.Done():
			t.Error("racing timeout failure recording did not start")
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1
	client.tailHandlerTimeout = 25 * time.Millisecond

	err = client.Tail(tailCtx, handler)
	require.ErrorContains(t, err, "tail ordered handler timed out for MESSAGE_CREATE")
	require.ErrorIs(t, tailCtx.Err(), context.Canceled)
	require.EqualValues(t, 1, handler.calls.Load())
	require.EqualValues(t, 1, handler.recordCalls.Load())
	expected := TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "timeout",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "racing-timeout",
		UserID:    "u1",
	}
	assertTailFailure(
		t,
		<-handler.recorded,
		expected,
		TailFailureStageHandler,
		TailFailureJoinTimedOut,
		true,
	)
	assertTailFailure(
		t,
		<-handler.failures,
		expected,
		TailFailureStageHandler,
		TailFailureJoinTimedOut,
		true,
	)
}

func TestTailParentCancellationForcesFallbackWhenHandlerDoesNotJoin(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	allowRecord := make(chan struct{})
	close(allowRecord)
	handler := &nonCooperativeFailureHandler{
		failures:         make(chan TailFailure, 1),
		recorded:         make(chan TailFailure, 1),
		failureReported:  make(chan struct{}),
		recordingStarted: make(chan struct{}),
		allowRecord:      allowRecord,
		started:          make(chan string, 1),
		release:          make(chan struct{}),
		finished:         make(chan string, 1),
	}
	defer func() {
		close(handler.release)
		select {
		case <-handler.finished:
		case <-testCtx.Done():
			t.Error("parent-canceled handler did not finish after release")
		}
	}()
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "parent-canceled", now)); err != nil {
			t.Errorf("write parent-canceled event: %v", err)
			return
		}
		select {
		case <-handler.started:
			cancelTail()
		case <-testCtx.Done():
			t.Error("parent-canceled handler did not start")
			return
		}
		select {
		case <-handler.recordingStarted:
		case <-testCtx.Done():
			t.Error("parent-canceled fallback recording did not start")
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1
	client.tailHandlerTimeout = 30 * time.Second

	startedAt := time.Now()
	err = client.Tail(tailCtx, handler)
	require.ErrorContains(t, err, "tail handler did not stop after cancellation")
	require.Less(t, time.Since(startedAt), 2*time.Second)
	require.EqualValues(t, 1, handler.calls.Load())
	require.EqualValues(t, 1, handler.recordCalls.Load())
	expected := TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "timeout",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "parent-canceled",
		UserID:    "u1",
	}
	assertTailFailure(
		t,
		<-handler.recorded,
		expected,
		TailFailureStageHandler,
		TailFailureJoinTimedOut,
		true,
	)
	assertTailFailure(
		t,
		<-handler.failures,
		expected,
		TailFailureStageHandler,
		TailFailureJoinTimedOut,
		true,
	)
}

func TestTailParentCancellationWaitsForCooperativeHandlerJoin(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	handler := &shutdownJoinHandler{
		started:              make(chan struct{}),
		cancellationObserved: make(chan struct{}),
		allowJoin:            make(chan struct{}),
		finished:             make(chan struct{}),
	}
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "shutdown-join", now)); err != nil {
			t.Errorf("write shutdown join event: %v", err)
			return
		}
		<-handler.started
		<-tailCtx.Done()
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1
	client.tailHandlerTimeout = 30 * time.Second

	done := make(chan error, 1)
	go func() {
		done <- client.Tail(tailCtx, handler)
	}()
	select {
	case <-handler.started:
	case <-testCtx.Done():
		t.Fatal("shutdown join handler did not start")
	}
	cancelTail()
	select {
	case <-handler.cancellationObserved:
	case <-testCtx.Done():
		t.Fatal("shutdown join handler did not observe cancellation")
	}
	select {
	case err := <-done:
		t.Fatalf("Tail returned before the active handler joined: %v", err)
	default:
	}

	close(handler.allowJoin)
	select {
	case err = <-done:
	case <-testCtx.Done():
		t.Fatal("Tail did not return after the active handler joined")
	}
	require.NoError(t, err)
	select {
	case <-handler.finished:
	default:
		t.Fatal("Tail returned before the handler finished")
	}
}

func TestTailTimeoutSurfacesFailureRecorderError(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()

	allowRecord := make(chan struct{})
	close(allowRecord)
	handler := &nonCooperativeFailureHandler{
		failures:         make(chan TailFailure, 1),
		recorded:         make(chan TailFailure, 1),
		failureReported:  make(chan struct{}),
		recordingStarted: make(chan struct{}),
		allowRecord:      allowRecord,
		started:          make(chan string, 1),
		release:          make(chan struct{}),
		finished:         make(chan string, 1),
		recordErr:        errors.New("ledger unavailable"),
	}
	defer func() {
		close(handler.release)
		select {
		case <-handler.finished:
		case <-testCtx.Done():
			t.Error("detached handler did not finish after release")
		}
	}()
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "blocked-1", now)); err != nil {
			t.Errorf("write blocked event: %v", err)
			return
		}
		select {
		case <-handler.recordingStarted:
		case <-testCtx.Done():
			t.Error("non-cooperative handler failure recording did not start")
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1
	client.tailHandlerTimeout = 25 * time.Millisecond

	err = client.Tail(testCtx, handler)
	require.ErrorContains(t, err, "persist tail message failure")
	require.NotContains(t, err.Error(), "ledger unavailable")
	require.EqualValues(t, 1, handler.calls.Load())
	require.EqualValues(t, 1, handler.recordCalls.Load())
	select {
	case failure := <-handler.failures:
		t.Fatalf("recorder failure was reported instead of failing closed: %+v", failure)
	default:
	}
}

func TestTailAggregatesQueueOverflowWithPostDeadlinePersistenceFailure(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()

	handler := &postDeadlineNilRecordingHandler{
		recordingStarted: make(chan struct{}),
		allowRecord:      make(chan struct{}),
		recordErr:        errors.New("ledger unavailable"),
	}
	observerReady := make(chan struct{})
	overflowObserved := make(chan struct{})
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "post-deadline", now)); err != nil {
			t.Errorf("write post-deadline event: %v", err)
			return
		}
		select {
		case <-handler.recordingStarted:
		case <-testCtx.Done():
			t.Error("post-deadline failure recording did not start")
			return
		}
		select {
		case <-observerReady:
		case <-testCtx.Done():
			t.Error("queue observer was not installed")
			return
		}
		for sequence := 3; sequence < 13; sequence++ {
			if err := conn.WriteJSON(messageCreateEvent(sequence, fmt.Sprintf("queue-overflow-%d", sequence), now)); err != nil {
				t.Errorf("write queue-overflow event: %v", err)
				return
			}
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 0
	client.tailHandlerTimeout = 25 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		done <- client.Tail(testCtx, handler)
	}()
	select {
	case <-handler.recordingStarted:
	case <-testCtx.Done():
		t.Fatal("post-deadline failure recording did not start")
	}
	removeObserver := client.session.AddHandler(func(_ *discordgo.Session, event *discordgo.MessageCreate) {
		if event.ID == "queue-overflow-3" {
			close(overflowObserved)
		}
	})
	defer removeObserver()
	close(observerReady)
	select {
	case <-overflowObserved:
	case err := <-done:
		t.Fatalf("Tail returned before failure persistence completed: %v", err)
	case <-testCtx.Done():
		t.Fatal("tail queue did not overflow")
	}
	close(handler.allowRecord)
	select {
	case err = <-done:
	case <-testCtx.Done():
		t.Fatal("Tail did not return after failure persistence completed")
	}
	require.ErrorContains(t, err, "tail worker queue full")
	require.ErrorContains(t, err, "persist tail message failure")
	require.NotContains(t, err.Error(), "ledger unavailable")
	require.EqualValues(t, 1, handler.calls.Load())
}

func TestTailFailureCircuitResetsAfterSuccess(t *testing.T) {
	t.Parallel()

	circuit := &tailFailureCircuit{limit: 2}
	require.False(t, circuit.recordFailure())
	circuit.recordSuccess()
	require.False(t, circuit.recordFailure())
	require.True(t, circuit.recordFailure())
	require.False(t, circuit.recordFailure())
}

func TestTailMessageUpdateFailureUsesRefetchedMetadata(t *testing.T) {
	tests := []struct {
		name     string
		fail     func(context.Context) error
		wantKind string
	}{
		{
			name: "returned error",
			fail: func(context.Context) error {
				return errors.New("sensitive returned error")
			},
			wantKind: "returned_error",
		},
		{
			name: "panic",
			fail: func(context.Context) error {
				panic("sensitive panic value")
			},
			wantKind: "panic",
		},
		{
			name: "timeout",
			fail: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantKind: "timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			handler := &messageUpdateFailureHandler{
				fail:            tt.fail,
				cancel:          cancel,
				failureReported: make(chan struct{}),
				failures:        make(chan TailFailure, 1),
				recorded:        make(chan TailFailure, 1),
				updates:         make(chan *discordgo.Message, 1),
			}
			now := time.Now().UTC().Format(time.RFC3339)
			server := newTailTestGatewayWithRoutes(
				t,
				func(mux *http.ServeMux) {
					mux.HandleFunc("/api/v10/channels/c1/messages/m1", writeJSON(map[string]any{
						"id":         "m1",
						"guild_id":   "g-refetched",
						"channel_id": "c1",
						"content":    "full message",
						"timestamp":  now,
						"author":     map[string]any{"id": "u-refetched", "username": "test-user"},
					}))
				},
				func(conn *websocket.Conn) {
					if err := conn.WriteJSON(messageUpdateEvent(2, "m1", now)); err != nil {
						t.Errorf("write update event: %v", err)
						return
					}
					select {
					case <-handler.failureReported:
					case <-ctx.Done():
						t.Error("tail failure was not reported")
					}
				},
			)
			defer server.Close()

			restore := patchDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()

			client, err := New("token")
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			client.session.ShouldReconnectOnError = false
			client.tailWorkerCount = 1
			client.tailQueueSize = 1
			client.tailHandlerTimeout = 25 * time.Millisecond

			require.NoError(t, client.Tail(ctx, handler))
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			update := <-handler.updates
			require.Equal(t, "g-refetched", update.GuildID)
			require.Equal(t, "u-refetched", update.Author.ID)
			joinOutcome := TailFailureJoinNotRequired
			if tt.wantKind == "timeout" {
				joinOutcome = TailFailureJoinJoined
			}
			assertTailFailure(t, <-handler.failures, TailFailure{
				EventType: "MESSAGE_UPDATE",
				Kind:      tt.wantKind,
				GuildID:   "g-refetched",
				ChannelID: "c1",
				MessageID: "m1",
				UserID:    "u-refetched",
			}, TailFailureStageHandler, joinOutcome, false)
			assertTailFailure(t, <-handler.recorded, TailFailure{
				EventType: "MESSAGE_UPDATE",
				Kind:      tt.wantKind,
				GuildID:   "g-refetched",
				ChannelID: "c1",
				MessageID: "m1",
				UserID:    "u-refetched",
			}, TailFailureStageHandler, joinOutcome, false)
			require.EqualValues(t, 1, handler.recordCalls.Load())
		})
	}
}

func TestTailMessageUpdateRefetchTimeoutReportsRefetchStage(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	handler := &messageUpdateFailureHandler{
		fail: func(ctx context.Context) error {
			return ctx.Err()
		},
		cancel:          cancelTail,
		failureReported: make(chan struct{}),
		failures:        make(chan TailFailure, 1),
		recorded:        make(chan TailFailure, 1),
		updates:         make(chan *discordgo.Message, 1),
	}
	now := time.Now().UTC().Format(time.RFC3339)
	server := newTailTestGatewayWithRoutes(
		t,
		func(mux *http.ServeMux) {
			mux.HandleFunc("/api/v10/channels/c1/messages/m1", func(_ http.ResponseWriter, r *http.Request) {
				<-r.Context().Done()
			})
		},
		func(conn *websocket.Conn) {
			event := messageUpdateEvent(2, "m1", now)
			event["d"].(map[string]any)["guild_id"] = "g1"
			if err := conn.WriteJSON(event); err != nil {
				t.Errorf("write update event: %v", err)
				return
			}
			select {
			case <-handler.failureReported:
			case <-testCtx.Done():
				t.Error("refetch timeout was not reported")
			}
		},
	)
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1
	client.tailHandlerTimeout = 25 * time.Millisecond

	require.NoError(t, client.Tail(tailCtx, handler))
	expected := TailFailure{
		EventType: "MESSAGE_UPDATE",
		Kind:      "timeout",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m1",
	}
	assertTailFailure(
		t,
		<-handler.recorded,
		expected,
		TailFailureStageMessageUpdateRefetch,
		TailFailureJoinJoined,
		false,
	)
	assertTailFailure(
		t,
		<-handler.failures,
		expected,
		TailFailureStageMessageUpdateRefetch,
		TailFailureJoinJoined,
		false,
	)
	require.EqualValues(t, 1, handler.recordCalls.Load())
}

func TestTailMessageUpdateImmediateHTTPRefetchFailureStillInvokesHandlerAndRecordsOnce(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	handler := &messageUpdateFailureHandler{
		fail: func(context.Context) error {
			return nil
		},
		cancel:          cancelTail,
		failureReported: make(chan struct{}),
		failures:        make(chan TailFailure, 1),
		recorded:        make(chan TailFailure, 1),
		updates:         make(chan *discordgo.Message, 1),
	}
	var refetchCalls atomic.Int32
	now := time.Now().UTC().Format(time.RFC3339)
	server := newTailTestGatewayWithRoutes(
		t,
		func(mux *http.ServeMux) {
			mux.HandleFunc("/api/v10/channels/c1/messages/m1", func(w http.ResponseWriter, _ *http.Request) {
				refetchCalls.Add(1)
				http.Error(w, "refetch unavailable", http.StatusServiceUnavailable)
			})
		},
		func(conn *websocket.Conn) {
			event := messageUpdateEvent(2, "m1", now)
			event["d"].(map[string]any)["guild_id"] = "g1"
			if err := conn.WriteJSON(event); err != nil {
				t.Errorf("write update event: %v", err)
				return
			}
			select {
			case <-handler.failureReported:
			case <-testCtx.Done():
				t.Error("immediate refetch failure was not reported")
			}
		},
	)
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.session.MaxRestRetries = 0
	client.tailWorkerCount = 1
	client.tailQueueSize = 1

	require.NoError(t, client.Tail(tailCtx, handler))
	require.ErrorIs(t, tailCtx.Err(), context.Canceled)
	require.EqualValues(t, 1, refetchCalls.Load())
	update := <-handler.updates
	require.Equal(t, "m1", update.ID)
	require.Equal(t, "g1", update.GuildID)
	require.Equal(t, "c1", update.ChannelID)
	require.Empty(t, update.Content)
	expected := TailFailure{
		EventType: "MESSAGE_UPDATE",
		Kind:      "returned_error",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m1",
	}
	assertTailFailure(
		t,
		<-handler.recorded,
		expected,
		TailFailureStageMessageUpdateRefetch,
		TailFailureJoinNotRequired,
		false,
	)
	assertTailFailure(
		t,
		<-handler.failures,
		expected,
		TailFailureStageMessageUpdateRefetch,
		TailFailureJoinNotRequired,
		false,
	)
	require.EqualValues(t, 1, handler.recordCalls.Load())
}

func TestTailMessageUpdateSkipsRefetchForFilteredGuild(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	handler := &guildFilteringUpdateHandler{
		allowedGuild: "g-selected",
		filtered:     make(chan struct{}),
	}
	var refetchCalls atomic.Int32
	now := time.Now().UTC().Format(time.RFC3339)
	server := newTailTestGatewayWithRoutes(
		t,
		func(mux *http.ServeMux) {
			mux.HandleFunc("/api/v10/channels/c1/messages/m1", func(w http.ResponseWriter, _ *http.Request) {
				refetchCalls.Add(1)
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			})
		},
		func(conn *websocket.Conn) {
			event := messageUpdateEvent(2, "m1", now)
			event["d"].(map[string]any)["guild_id"] = "g-excluded"
			if err := conn.WriteJSON(event); err != nil {
				t.Errorf("write update event: %v", err)
				return
			}
			select {
			case <-handler.filtered:
				cancelTail()
			case <-testCtx.Done():
				t.Error("guild filter was not consulted")
			}
		},
	)
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()
	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.session.MaxRestRetries = 0
	client.tailWorkerCount = 1
	client.tailQueueSize = 1

	require.NoError(t, client.Tail(tailCtx, handler))
	require.Zero(t, refetchCalls.Load())
	require.Zero(t, handler.updates)
}

func TestTailMessageUpdateRejectsConflictingRefetchIdentity(t *testing.T) {
	tests := []struct {
		name string
		full map[string]any
	}{
		{
			name: "message id",
			full: map[string]any{
				"id":         "other-message",
				"guild_id":   "g1",
				"channel_id": "c1",
			},
		},
		{
			name: "channel id",
			full: map[string]any{
				"id":         "m1",
				"guild_id":   "g1",
				"channel_id": "other-channel",
			},
		},
		{
			name: "guild id",
			full: map[string]any{
				"id":         "m1",
				"guild_id":   "other-guild",
				"channel_id": "c1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			handler := &messageUpdateFailureHandler{
				fail: func(context.Context) error {
					return nil
				},
				cancel:          cancel,
				failureReported: make(chan struct{}),
				failures:        make(chan TailFailure, 1),
				recorded:        make(chan TailFailure, 1),
				updates:         make(chan *discordgo.Message, 1),
			}
			now := time.Now().UTC().Format(time.RFC3339)
			full := maps.Clone(tt.full)
			full["content"] = "full message"
			full["timestamp"] = now
			full["author"] = map[string]any{"id": "u-refetched", "username": "test-user"}
			server := newTailTestGatewayWithRoutes(
				t,
				func(mux *http.ServeMux) {
					mux.HandleFunc("/api/v10/channels/c1/messages/m1", writeJSON(full))
				},
				func(conn *websocket.Conn) {
					event := messageUpdateEvent(2, "m1", now)
					event["d"].(map[string]any)["guild_id"] = "g1"
					if err := conn.WriteJSON(event); err != nil {
						t.Errorf("write update event: %v", err)
						return
					}
					select {
					case <-handler.failureReported:
					case <-ctx.Done():
						t.Error("tail failure was not reported")
					}
				},
			)
			defer server.Close()

			restore := patchDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()

			client, err := New("token")
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			client.session.ShouldReconnectOnError = false
			client.tailWorkerCount = 1
			client.tailQueueSize = 1

			require.NoError(t, client.Tail(ctx, handler))
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			update := <-handler.updates
			require.Equal(t, "m1", update.ID)
			require.Equal(t, "g1", update.GuildID)
			require.Equal(t, "c1", update.ChannelID)
			assertTailFailure(t, <-handler.failures, TailFailure{
				EventType: "MESSAGE_UPDATE",
				Kind:      "returned_error",
				GuildID:   "g1",
				ChannelID: "c1",
				MessageID: "m1",
			}, TailFailureStageMessageUpdateRefetch, TailFailureJoinNotRequired, false)
			assertTailFailure(t, <-handler.recorded, TailFailure{
				EventType: "MESSAGE_UPDATE",
				Kind:      "returned_error",
				GuildID:   "g1",
				ChannelID: "c1",
				MessageID: "m1",
			}, TailFailureStageMessageUpdateRefetch, TailFailureJoinNotRequired, false)
			require.EqualValues(t, 1, handler.recordCalls.Load())
		})
	}
}

func TestValidateRefetchedMessageIdentity(t *testing.T) {
	t.Parallel()

	partial := &discordgo.Message{ID: "m1", GuildID: "g1", ChannelID: "c1"}
	tests := []struct {
		name    string
		full    *discordgo.Message
		wantErr string
	}{
		{
			name:    "message id",
			full:    &discordgo.Message{ID: "other", GuildID: "g1", ChannelID: "c1"},
			wantErr: "different message id",
		},
		{
			name:    "channel id",
			full:    &discordgo.Message{ID: "m1", GuildID: "g1", ChannelID: "other"},
			wantErr: "different channel id",
		},
		{
			name:    "guild id",
			full:    &discordgo.Message{ID: "m1", GuildID: "other", ChannelID: "c1"},
			wantErr: "different guild id",
		},
		{
			name: "matching",
			full: &discordgo.Message{ID: "m1", GuildID: "g1", ChannelID: "c1"},
		},
		{
			name: "missing optional guild",
			full: &discordgo.Message{ID: "m1", ChannelID: "c1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRefetchedMessageIdentity(partial, tt.full)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestTailReadyHandlerFailureRemainsTerminal(t *testing.T) {
	gatewayDone := make(chan struct{})
	server := newTailTestGateway(t, func(*websocket.Conn) {
		<-gatewayDone
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false

	readyErr := errors.New("ready failed")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = client.Tail(ctx, &readyFailureHandler{err: readyErr})
	close(gatewayDone)
	require.ErrorIs(t, err, readyErr)
}

func TestTailFailsFastWhenWorkerQueueFills(t *testing.T) {
	mux := http.NewServeMux()
	upgrader := websocket.Upgrader{}
	gatewayURL := ""
	mux.HandleFunc("/api/v10/gateway", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"url": gatewayURL})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	gatewayURL = "ws" + server.URL[len("http"):] + "/gateway"
	gatewayHandler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade gateway: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if err := conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		}); err != nil {
			t.Errorf("write hello: %v", err)
			return
		}
		_, _, err = conn.ReadMessage()
		if err != nil {
			t.Errorf("read identify: %v", err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "READY",
			"s":  1,
			"d": map[string]any{
				"session_id": "session",
				"user":       map[string]any{"id": "bot", "username": "bot"},
			},
		}); err != nil {
			t.Errorf("write ready: %v", err)
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		for i := range 4 {
			if err := conn.WriteJSON(map[string]any{
				"op": 0,
				"t":  "MESSAGE_CREATE",
				"s":  i + 2,
				"d": map[string]any{
					"id":         fmt.Sprintf("m%d", i),
					"guild_id":   "g1",
					"channel_id": "c1",
					"content":    "hello",
					"timestamp":  now,
					"author":     map[string]any{"id": "u1", "username": "user"},
				},
			}); err != nil {
				t.Errorf("write create %d: %v", i, err)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	mux.HandleFunc("/gateway", gatewayHandler)
	mux.HandleFunc("/gateway/", gatewayHandler)

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1
	client.tailHandlerTimeout = time.Second

	err = client.Tail(context.Background(), &slowHandler{sleep: 200 * time.Millisecond})
	require.ErrorContains(t, err, "tail worker queue full")
}

func TestTailDoesNotStartLaterOrderedHandlerCanceledAfterDequeue(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	handler := &dequeuedCancellationHandler{
		firstHandled: make(chan struct{}),
	}
	secondDequeued := make(chan struct{})
	server := newTailTestGateway(t, func(conn *websocket.Conn) {
		now := time.Now().UTC().Format(time.RFC3339)
		if err := conn.WriteJSON(messageCreateEvent(2, "first", now)); err != nil {
			t.Errorf("write first event: %v", err)
			return
		}
		select {
		case <-handler.firstHandled:
		case <-testCtx.Done():
			t.Error("first ordered handler did not run")
			return
		}
		if err := conn.WriteJSON(messageCreateEvent(3, "later", now)); err != nil {
			t.Errorf("write later event: %v", err)
			return
		}
		select {
		case <-secondDequeued:
		case <-testCtx.Done():
			t.Error("later ordered task was not dequeued")
		}
	})
	defer server.Close()

	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldReconnectOnError = false
	client.tailWorkerCount = 1
	client.tailQueueSize = 1
	client.tailTaskDequeuedHook = func(ctx context.Context) {
		if handler.dequeueCalls.Add(1) != 2 {
			return
		}
		close(secondDequeued)
		cancelTail()
		<-ctx.Done()
	}

	require.NoError(t, client.Tail(tailCtx, handler))
	require.ErrorIs(t, tailCtx.Err(), context.Canceled)
	require.EqualValues(t, 2, handler.dequeueCalls.Load())
	require.EqualValues(t, 1, handler.calls.Load())
	require.Zero(t, handler.laterCalls.Load())
}

func newTailTestGateway(t *testing.T, afterReady func(*websocket.Conn)) *httptest.Server {
	return newTailTestGatewayWithRoutes(t, nil, afterReady)
}

func newTailTestGatewayWithRoutes(
	t *testing.T,
	addRoutes func(*http.ServeMux),
	afterReady func(*websocket.Conn),
) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	if addRoutes != nil {
		addRoutes(mux)
	}
	upgrader := websocket.Upgrader{}
	mux.HandleFunc("/api/v10/gateway", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"url": "ws://" + r.Host + "/gateway"})
	})
	gatewayHandler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade gateway: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if err := conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		}); err != nil {
			t.Errorf("write hello: %v", err)
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Errorf("read identify: %v", err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "READY",
			"s":  1,
			"d": map[string]any{
				"session_id": "session",
				"user":       map[string]any{"id": "bot", "username": "bot"},
			},
		}); err != nil {
			t.Errorf("write ready: %v", err)
			return
		}
		afterReady(conn)
	}
	mux.HandleFunc("/gateway", gatewayHandler)
	mux.HandleFunc("/gateway/", gatewayHandler)
	return httptest.NewServer(mux)
}

func messageCreateEvent(sequence int, messageID, timestamp string) map[string]any {
	return map[string]any{
		"op": 0,
		"t":  "MESSAGE_CREATE",
		"s":  sequence,
		"d": map[string]any{
			"id":         messageID,
			"guild_id":   "g1",
			"channel_id": "c1",
			"content":    "safe test content",
			"timestamp":  timestamp,
			"author":     map[string]any{"id": "u1", "username": "test-user"},
		},
	}
}

func memberAddEvent(sequence int) map[string]any {
	return map[string]any{
		"op": 0,
		"t":  "GUILD_MEMBER_ADD",
		"s":  sequence,
		"d": map[string]any{
			"guild_id": "g1",
			"user":     map[string]any{"id": fmt.Sprintf("member-%d", sequence), "username": "test-member"},
			"roles":    []string{},
		},
	}
}

func messageUpdateEvent(sequence int, messageID, timestamp string) map[string]any {
	return map[string]any{
		"op": 0,
		"t":  "MESSAGE_UPDATE",
		"s":  sequence,
		"d": map[string]any{
			"id":         messageID,
			"channel_id": "c1",
			"content":    "",
			"timestamp":  timestamp,
		},
	}
}

func messageDeleteEvent(sequence int, messageID string) map[string]any {
	return map[string]any{
		"op": 0,
		"t":  "MESSAGE_DELETE",
		"s":  sequence,
		"d": map[string]any{
			"id":         messageID,
			"guild_id":   "g1",
			"channel_id": "c1",
		},
	}
}

func writeJSON(v any) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
}

func patchDiscordEndpoints(apiBase string) func() {
	oldDiscord := discordgo.EndpointDiscord
	oldAPI := discordgo.EndpointAPI
	oldGuilds := discordgo.EndpointGuilds
	oldChannels := discordgo.EndpointChannels
	oldUsers := discordgo.EndpointUsers
	oldGateway := discordgo.EndpointGateway
	oldUser := discordgo.EndpointUser
	oldUserGuilds := discordgo.EndpointUserGuilds
	oldGuild := discordgo.EndpointGuild
	oldGuildChannels := discordgo.EndpointGuildChannels
	oldGuildMembers := discordgo.EndpointGuildMembers
	oldChannelThreads := discordgo.EndpointChannelThreads
	oldChannelActiveThreads := discordgo.EndpointChannelActiveThreads
	oldChannelPublicArchivedThreads := discordgo.EndpointChannelPublicArchivedThreads
	oldChannelPrivateArchivedThreads := discordgo.EndpointChannelPrivateArchivedThreads
	oldChannelMessages := discordgo.EndpointChannelMessages
	oldChannelMessage := discordgo.EndpointChannelMessage

	discordgo.EndpointDiscord = apiBase[:len(apiBase)-len("api/v10/")]
	discordgo.EndpointAPI = apiBase
	discordgo.EndpointGuilds = apiBase + "guilds/"
	discordgo.EndpointChannels = apiBase + "channels/"
	discordgo.EndpointUsers = apiBase + "users/"
	discordgo.EndpointGateway = apiBase + "gateway"
	discordgo.EndpointUser = func(uID string) string { return discordgo.EndpointUsers + uID }
	discordgo.EndpointUserGuilds = func(uID string) string { return discordgo.EndpointUsers + uID + "/guilds" }
	discordgo.EndpointGuild = func(gID string) string { return discordgo.EndpointGuilds + gID }
	discordgo.EndpointGuildChannels = func(gID string) string { return discordgo.EndpointGuilds + gID + "/channels" }
	discordgo.EndpointGuildMembers = func(gID string) string { return discordgo.EndpointGuilds + gID + "/members" }
	discordgo.EndpointChannelThreads = func(cID string) string { return discordgo.EndpointChannels + cID + "/threads" }
	discordgo.EndpointChannelActiveThreads = func(cID string) string { return discordgo.EndpointChannelThreads(cID) + "/active" }
	discordgo.EndpointChannelPublicArchivedThreads = func(cID string) string { return discordgo.EndpointChannelThreads(cID) + "/archived/public" }
	discordgo.EndpointChannelPrivateArchivedThreads = func(cID string) string { return discordgo.EndpointChannelThreads(cID) + "/archived/private" }
	discordgo.EndpointChannelMessages = func(cID string) string { return discordgo.EndpointChannels + cID + "/messages" }
	discordgo.EndpointChannelMessage = func(cID, mID string) string { return discordgo.EndpointChannelMessages(cID) + "/" + mID }

	return func() {
		discordgo.EndpointDiscord = oldDiscord
		discordgo.EndpointAPI = oldAPI
		discordgo.EndpointGuilds = oldGuilds
		discordgo.EndpointChannels = oldChannels
		discordgo.EndpointUsers = oldUsers
		discordgo.EndpointGateway = oldGateway
		discordgo.EndpointUser = oldUser
		discordgo.EndpointUserGuilds = oldUserGuilds
		discordgo.EndpointGuild = oldGuild
		discordgo.EndpointGuildChannels = oldGuildChannels
		discordgo.EndpointGuildMembers = oldGuildMembers
		discordgo.EndpointChannelThreads = oldChannelThreads
		discordgo.EndpointChannelActiveThreads = oldChannelActiveThreads
		discordgo.EndpointChannelPublicArchivedThreads = oldChannelPublicArchivedThreads
		discordgo.EndpointChannelPrivateArchivedThreads = oldChannelPrivateArchivedThreads
		discordgo.EndpointChannelMessages = oldChannelMessages
		discordgo.EndpointChannelMessage = oldChannelMessage
	}
}

func discordSessionHandlerCount(session *discordgo.Session) int {
	handlers := reflect.ValueOf(session).Elem().FieldByName("handlers")
	total := 0
	for _, key := range handlers.MapKeys() {
		total += handlers.MapIndex(key).Len()
	}
	return total
}

func assertTailFailure(
	t *testing.T,
	actual TailFailure,
	expected TailFailure,
	stage TailFailureStage,
	joinOutcome TailFailureJoinOutcome,
	forceFallback bool,
) {
	t.Helper()

	require.Equal(t, expected.EventType, actual.EventType)
	require.Equal(t, expected.Kind, actual.Kind)
	require.Equal(t, expected.GuildID, actual.GuildID)
	require.Equal(t, expected.ChannelID, actual.ChannelID)
	require.Equal(t, expected.MessageID, actual.MessageID)
	require.Equal(t, expected.UserID, actual.UserID)
	require.Equal(t, stage, actual.HandlerStage)
	require.Equal(t, joinOutcome, actual.JoinOutcome)
	require.Equal(t, forceFallback, actual.ForceFallback)
	rendered := fmt.Sprintf("%+v", actual)
	require.NotContains(t, rendered, "sensitive")
	require.NotContains(t, rendered, "safe test content")
	require.GreaterOrEqual(t, actual.HandlerElapsed, time.Duration(0))
	require.GreaterOrEqual(t, actual.HandlerStageElapsed, time.Duration(0))
	require.LessOrEqual(t, actual.HandlerStageElapsed, actual.HandlerElapsed)
	require.GreaterOrEqual(t, actual.JoinElapsed, time.Duration(0))
	require.LessOrEqual(t, actual.JoinElapsed, tailHandlerCancelGrace)
	if joinOutcome == TailFailureJoinNotRequired {
		require.Zero(t, actual.JoinElapsed)
	}
	if joinOutcome == TailFailureJoinTimedOut {
		require.Equal(t, tailHandlerCancelGrace, actual.JoinElapsed)
	}
}

type recordingHandler struct {
	creates       int
	updates       int
	deletes       int
	channels      int
	memberUpserts int
	memberDeletes int
	ready         int
}

type gatewayOpenFailureHandler struct {
	recordingHandler
	recordCalls atomic.Int32
}

func (h *gatewayOpenFailureHandler) RecordTailFailure(TailFailure) error {
	h.recordCalls.Add(1)
	return nil
}

func (r *recordingHandler) OnTailReady(context.Context) error {
	r.ready++
	return nil
}

func (r *recordingHandler) OnMessageCreate(context.Context, *discordgo.Message) error {
	r.creates++
	return nil
}

func (r *recordingHandler) OnMessageUpdate(context.Context, *discordgo.Message) error {
	r.updates++
	return nil
}

func (r *recordingHandler) OnMessageDelete(context.Context, *discordgo.MessageDelete) error {
	r.deletes++
	return nil
}

func (r *recordingHandler) OnChannelUpsert(context.Context, *discordgo.Channel) error {
	r.channels++
	return nil
}

func (r *recordingHandler) OnMemberUpsert(context.Context, string, *discordgo.Member) error {
	r.memberUpserts++
	return nil
}

func (r *recordingHandler) OnMemberDelete(context.Context, string, string) error {
	r.memberDeletes++
	return nil
}

type updateFailureHandler struct {
	recordingHandler
	failures     chan TailFailure
	reported     chan struct{}
	channelCalls atomic.Int32
	memberCalls  atomic.Int32
	recordCalls  atomic.Int32
}

func (h *updateFailureHandler) OnTailFailure(failure TailFailure) {
	h.failures <- failure
	h.reported <- struct{}{}
}

func (h *updateFailureHandler) RecordTailFailure(TailFailure) error {
	h.recordCalls.Add(1)
	return nil
}

func (h *updateFailureHandler) OnChannelUpsert(context.Context, *discordgo.Channel) error {
	h.channelCalls.Add(1)
	return errors.New("channel update failed")
}

func (h *updateFailureHandler) OnMemberUpsert(context.Context, string, *discordgo.Member) error {
	h.memberCalls.Add(1)
	return errors.New("member update failed")
}

type nonMessagePanicContinuationHandler struct {
	recordingHandler
	cancel          context.CancelFunc
	failureReported chan struct{}
	failures        chan TailFailure
	laterHandled    chan struct{}
	failureOnce     sync.Once
	laterOnce       sync.Once
	calls           atomic.Int32
}

func (h *nonMessagePanicContinuationHandler) OnTailFailure(failure TailFailure) {
	h.failureOnce.Do(func() {
		h.failures <- failure
		close(h.failureReported)
	})
}

func (h *nonMessagePanicContinuationHandler) OnChannelUpsert(_ context.Context, channel *discordgo.Channel) error {
	h.calls.Add(1)
	if channel == nil {
		return nil
	}
	switch channel.ID {
	case "failed-channel":
		panic("sensitive non-message panic value")
	case "later-channel":
		h.laterOnce.Do(func() {
			close(h.laterHandled)
			h.cancel()
		})
	}
	return nil
}

type failureContinuationHandler struct {
	fail            func(context.Context) error
	cancel          context.CancelFunc
	failureReported chan struct{}
	failures        chan TailFailure
	recorded        chan TailFailure
	laterHandled    chan struct{}
	failureOnce     sync.Once
	laterOnce       sync.Once
	recordCalls     atomic.Int32
}

func (h *failureContinuationHandler) OnTailFailure(failure TailFailure) {
	h.failureOnce.Do(func() {
		h.failures <- failure
		close(h.failureReported)
	})
}

func (h *failureContinuationHandler) RecordTailFailure(failure TailFailure) error {
	h.recordCalls.Add(1)
	if h.recorded != nil {
		h.recorded <- failure
	}
	return nil
}

func (h *failureContinuationHandler) OnMessageCreate(ctx context.Context, msg *discordgo.Message) error {
	if msg == nil {
		return nil
	}
	switch msg.ID {
	case "failed":
		return h.fail(ctx)
	case "later":
		h.laterOnce.Do(func() {
			close(h.laterHandled)
			h.cancel()
		})
	}
	return nil
}

func (h *failureContinuationHandler) OnMessageUpdate(context.Context, *discordgo.Message) error {
	return nil
}

func (h *failureContinuationHandler) OnMessageDelete(context.Context, *discordgo.MessageDelete) error {
	return nil
}

func (h *failureContinuationHandler) OnChannelUpsert(context.Context, *discordgo.Channel) error {
	return nil
}

func (h *failureContinuationHandler) OnMemberUpsert(context.Context, string, *discordgo.Member) error {
	return nil
}

func (h *failureContinuationHandler) OnMemberDelete(context.Context, string, string) error {
	return nil
}

type messageDeleteFailureHandler struct {
	recordingHandler
	fail            func(context.Context) error
	cancel          context.CancelFunc
	failures        chan TailFailure
	recorded        chan TailFailure
	failureReported chan struct{}
	failureOnce     sync.Once
	calls           atomic.Int32
	recordCalls     atomic.Int32
}

func (h *messageDeleteFailureHandler) OnMessageDelete(
	ctx context.Context,
	_ *discordgo.MessageDelete,
) error {
	h.calls.Add(1)
	return h.fail(ctx)
}

func (h *messageDeleteFailureHandler) RecordTailFailure(failure TailFailure) error {
	h.recordCalls.Add(1)
	h.recorded <- failure
	return nil
}

func (h *messageDeleteFailureHandler) OnTailFailure(failure TailFailure) {
	h.failureOnce.Do(func() {
		h.failures <- failure
		close(h.failureReported)
		h.cancel()
	})
}

type panicDurabilityHandler struct {
	recordingHandler
	panicValue             any
	panicAfterCancellation bool
	handlerStarted         chan struct{}
	cancel                 context.CancelFunc
	recordingStarted       chan struct{}
	allowRecord            <-chan struct{}
	recorded               chan TailFailure
	reported               chan TailFailure
	laterHandled           chan struct{}
	recordErr              error
	handlerOnce            sync.Once
	recordOnce             sync.Once
	laterOnce              sync.Once
	calls                  atomic.Int32
	mu                     sync.Mutex
	steps                  []string
}

type messageFailureWithoutRecorderHandler struct {
	recordingHandler
	reported chan TailFailure
	calls    atomic.Int32
}

type shutdownJoinHandler struct {
	recordingHandler
	started              chan struct{}
	cancellationObserved chan struct{}
	allowJoin            chan struct{}
	finished             chan struct{}
	startOnce            sync.Once
	finishOnce           sync.Once
}

type parentCancellationErrorHandler struct {
	recordingHandler
	started     chan struct{}
	failures    chan TailFailure
	recorded    chan TailFailure
	startOnce   sync.Once
	calls       atomic.Int32
	recordCalls atomic.Int32
}

func (h *parentCancellationErrorHandler) OnMessageCreate(
	ctx context.Context,
	_ *discordgo.Message,
) error {
	h.calls.Add(1)
	h.startOnce.Do(func() {
		close(h.started)
	})
	<-ctx.Done()
	return errors.New("sensitive ordinary cancellation failure")
}

func (h *parentCancellationErrorHandler) RecordTailFailure(failure TailFailure) error {
	h.recordCalls.Add(1)
	h.recorded <- failure
	return nil
}

func (h *parentCancellationErrorHandler) OnTailFailure(failure TailFailure) {
	h.failures <- failure
}

func (h *shutdownJoinHandler) OnMessageCreate(ctx context.Context, _ *discordgo.Message) error {
	h.startOnce.Do(func() {
		close(h.started)
	})
	<-ctx.Done()
	close(h.cancellationObserved)
	<-h.allowJoin
	h.finishOnce.Do(func() {
		close(h.finished)
	})
	return nil
}

func (h *messageFailureWithoutRecorderHandler) OnMessageCreate(context.Context, *discordgo.Message) error {
	h.calls.Add(1)
	return errors.New("sensitive returned error")
}

func (h *messageFailureWithoutRecorderHandler) OnTailFailure(failure TailFailure) {
	h.reported <- failure
}

func (h *panicDurabilityHandler) RecordTailFailure(failure TailFailure) error {
	h.recordOnce.Do(func() {
		close(h.recordingStarted)
	})
	if h.allowRecord != nil {
		<-h.allowRecord
	}
	h.appendStep("record")
	h.recorded <- failure
	return h.recordErr
}

func (h *panicDurabilityHandler) OnTailFailure(failure TailFailure) {
	h.appendStep("report")
	h.reported <- failure
}

func (h *panicDurabilityHandler) OnMessageCreate(ctx context.Context, msg *discordgo.Message) error {
	h.calls.Add(1)
	if msg == nil {
		return nil
	}
	switch msg.ID {
	case "failed":
		if h.panicAfterCancellation {
			h.handlerOnce.Do(func() {
				close(h.handlerStarted)
			})
			<-ctx.Done()
		}
		panic(h.panicValue)
	case "later":
		h.laterOnce.Do(func() {
			h.appendStep("later")
			close(h.laterHandled)
			if h.cancel != nil {
				h.cancel()
			}
		})
	}
	return nil
}

func (h *panicDurabilityHandler) appendStep(step string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.steps = append(h.steps, step)
}

func (h *panicDurabilityHandler) stepsSnapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.steps...)
}

type persistentFailureHandler struct {
	recordingHandler
	fail     func(context.Context) error
	failures chan TailFailure
	calls    atomic.Int32
}

func (h *persistentFailureHandler) OnTailFailure(failure TailFailure) {
	h.failures <- failure
}

func (h *persistentFailureHandler) RecordTailFailure(TailFailure) error {
	return nil
}

func (h *persistentFailureHandler) OnMessageCreate(ctx context.Context, _ *discordgo.Message) error {
	h.calls.Add(1)
	if h.fail != nil {
		return h.fail(ctx)
	}
	return errors.New("persistent handler failure")
}

type postDeadlineNilRecordingHandler struct {
	recordingHandler
	recordingStarted chan struct{}
	allowRecord      chan struct{}
	recordErr        error
	recordOnce       sync.Once
	calls            atomic.Int32
}

func (h *postDeadlineNilRecordingHandler) OnMessageCreate(ctx context.Context, _ *discordgo.Message) error {
	h.calls.Add(1)
	<-ctx.Done()
	return nil
}

func (h *postDeadlineNilRecordingHandler) RecordTailFailure(TailFailure) error {
	h.recordOnce.Do(func() {
		close(h.recordingStarted)
	})
	<-h.allowRecord
	return h.recordErr
}

type mixedFailureHandler struct {
	recordingHandler
	failures chan TailFailure
	members  chan struct{}
	calls    atomic.Int32
}

func (h *mixedFailureHandler) OnTailFailure(failure TailFailure) {
	h.failures <- failure
}

func (h *mixedFailureHandler) RecordTailFailure(TailFailure) error {
	return nil
}

func (h *mixedFailureHandler) OnMessageCreate(context.Context, *discordgo.Message) error {
	h.calls.Add(1)
	return errors.New("persistent message failure")
}

func (h *mixedFailureHandler) OnMemberUpsert(context.Context, string, *discordgo.Member) error {
	h.members <- struct{}{}
	return nil
}

type nonCooperativeFailureHandler struct {
	recordingHandler
	failures         chan TailFailure
	recorded         chan TailFailure
	failureReported  chan struct{}
	recordingStarted chan struct{}
	allowRecord      chan struct{}
	started          chan string
	release          chan struct{}
	finished         chan string
	calls            atomic.Int32
	recordCalls      atomic.Int32
	failureOnce      sync.Once
	recordOnce       sync.Once
	recordErr        error
	record           func(TailFailure) error
	cancelOnContext  context.CancelFunc
}

func (h *nonCooperativeFailureHandler) OnTailFailure(failure TailFailure) {
	h.failures <- failure
	h.failureOnce.Do(func() {
		close(h.failureReported)
	})
}

func (h *nonCooperativeFailureHandler) RecordTailFailure(failure TailFailure) error {
	h.recordCalls.Add(1)
	h.recorded <- failure
	h.recordOnce.Do(func() {
		close(h.recordingStarted)
	})
	<-h.allowRecord
	if h.record != nil {
		return h.record(failure)
	}
	return h.recordErr
}

func (h *nonCooperativeFailureHandler) OnMessageCreate(ctx context.Context, msg *discordgo.Message) error {
	h.calls.Add(1)
	if msg != nil {
		select {
		case h.started <- msg.ID:
		default:
		}
	}
	if h.cancelOnContext != nil {
		<-ctx.Done()
		h.cancelOnContext()
	}
	<-h.release
	if msg != nil {
		h.finished <- msg.ID
	}
	return errors.New("blocked handler released")
}

type tailFailureRecorderStub struct {
	failure TailFailure
	err     error
}

type panicTailFailureRecorder struct{}

func (panicTailFailureRecorder) RecordTailFailure(TailFailure) error {
	panic("sensitive recorder panic")
}

func (s *tailFailureRecorderStub) RecordTailFailure(failure TailFailure) error {
	s.failure = failure
	return s.err
}

type readyFailureHandler struct {
	recordingHandler
	err error
}

func (h *readyFailureHandler) OnTailReady(context.Context) error {
	return h.err
}

type messageUpdateFailureHandler struct {
	recordingHandler
	fail            func(context.Context) error
	cancel          context.CancelFunc
	failureReported chan struct{}
	failures        chan TailFailure
	recorded        chan TailFailure
	updates         chan *discordgo.Message
	failureOnce     sync.Once
	recordCalls     atomic.Int32
}

type guildFilteringUpdateHandler struct {
	recordingHandler
	allowedGuild string
	filtered     chan struct{}
	filterOnce   sync.Once
}

func (h *guildFilteringUpdateHandler) TailAllowsGuild(guildID string) bool {
	allowed := guildID == h.allowedGuild
	if !allowed {
		h.filterOnce.Do(func() { close(h.filtered) })
	}
	return allowed
}

func (h *messageUpdateFailureHandler) OnTailFailure(failure TailFailure) {
	h.failureOnce.Do(func() {
		h.failures <- failure
		close(h.failureReported)
		h.cancel()
	})
}

func (h *messageUpdateFailureHandler) RecordTailFailure(failure TailFailure) error {
	h.recordCalls.Add(1)
	h.recorded <- failure
	return nil
}

func (h *messageUpdateFailureHandler) OnMessageUpdate(ctx context.Context, msg *discordgo.Message) error {
	h.updates <- msg
	return h.fail(ctx)
}

type slowHandler struct {
	sleep time.Duration
}

type dequeuedCancellationHandler struct {
	recordingHandler
	firstHandled chan struct{}
	firstOnce    sync.Once
	calls        atomic.Int32
	laterCalls   atomic.Int32
	dequeueCalls atomic.Int32
}

func (h *dequeuedCancellationHandler) OnMessageCreate(
	_ context.Context,
	msg *discordgo.Message,
) error {
	h.calls.Add(1)
	if msg != nil && msg.ID == "later" {
		h.laterCalls.Add(1)
	}
	if msg != nil && msg.ID == "first" {
		h.firstOnce.Do(func() {
			close(h.firstHandled)
		})
	}
	return nil
}

func (s *slowHandler) OnMessageCreate(context.Context, *discordgo.Message) error {
	time.Sleep(s.sleep)
	return nil
}

func (s *slowHandler) OnMessageUpdate(context.Context, *discordgo.Message) error {
	return nil
}

func (s *slowHandler) OnMessageDelete(context.Context, *discordgo.MessageDelete) error {
	return nil
}

func (s *slowHandler) OnChannelUpsert(context.Context, *discordgo.Channel) error {
	return nil
}

func (s *slowHandler) OnMemberUpsert(context.Context, string, *discordgo.Member) error {
	return nil
}

func (s *slowHandler) OnMemberDelete(context.Context, string, string) error {
	return nil
}
