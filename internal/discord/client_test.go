package discord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
	"github.com/openclaw/discrawl/internal/store"
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

func TestTailRemovesGatewayHandlersWhenOpenFails(t *testing.T) {
	oldGateway := discordgo.EndpointGateway
	discordgo.EndpointGateway = "://invalid-gateway"
	defer func() {
		discordgo.EndpointGateway = oldGateway
	}()

	client, err := New("token")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	require.Zero(t, discordSessionHandlerCount(client.session))

	require.Error(t, client.Tail(context.Background(), &recordingHandler{}))
	require.Zero(t, discordSessionHandlerCount(client.session))
}

func TestRunTailTaskRecoversPanics(t *testing.T) {
	t.Parallel()

	client := &Client{tailHandlerTimeout: 10 * time.Millisecond}
	err := client.runTailTask(context.Background(), func(context.Context) error {
		panic("boom")
	})
	require.ErrorContains(t, err, "tail handler panic: boom")

	client.tailHandlerTimeout = 0
	err = client.runTailTask(context.Background(), func(context.Context) error {
		panic("again")
	})
	require.ErrorContains(t, err, "tail handler panic: again")
}

func TestTailTaskResultAfterParentCancellationPreservesOnlyPanic(t *testing.T) {
	t.Parallel()

	parentErr := context.Canceled
	panicErr := &tailHandlerPanicError{value: "sensitive panic value"}
	require.Same(t, panicErr, tailTaskResultAfterParentCancellation(parentErr, tailTaskResult{err: panicErr}))
	require.Same(t, parentErr, tailTaskResultAfterParentCancellation(parentErr, tailTaskResult{}))
	require.Same(t, parentErr, tailTaskResultAfterParentCancellation(
		parentErr,
		tailTaskResult{err: errors.New("ordinary handler error")},
	))
}

func TestAwaitTailTaskParentCancellationIsBoundedAndPreservesImmediatePanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parentErr := context.Canceled
		panicErr := &tailHandlerPanicError{value: "sensitive panic value"}
		panicResult := make(chan tailTaskResult, 1)
		go func() {
			time.Sleep(10 * time.Millisecond)
			panicResult <- tailTaskResult{err: panicErr, completedAt: time.Now()}
		}()
		startedAt := time.Now()
		require.Same(t, panicErr, awaitTailTaskParentCancellation(parentErr, panicResult))
		require.Equal(t, 10*time.Millisecond, time.Since(startedAt))

		ordinaryResult := make(chan tailTaskResult, 1)
		go func() {
			time.Sleep(10 * time.Millisecond)
			ordinaryResult <- tailTaskResult{err: errors.New("ordinary handler error"), completedAt: time.Now()}
		}()
		startedAt = time.Now()
		require.Same(t, parentErr, awaitTailTaskParentCancellation(parentErr, ordinaryResult))
		require.Equal(t, 10*time.Millisecond, time.Since(startedAt))

		startedAt = time.Now()
		require.Same(t, parentErr, awaitTailTaskParentCancellation(parentErr, make(chan tailTaskResult)))
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
			require.ErrorContains(t, err, "tail handler panic: sensitive cancellation panic")
			require.ErrorIs(t, parent.Err(), context.Canceled)
		})
	}
}

func TestRunTailTaskTreatsPostDeadlineNilAsFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 5 * time.Second

		client := &Client{tailHandlerTimeout: timeout}
		startedAt := time.Now()
		err := client.runTailTask(context.Background(), func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		})
		require.Equal(t, timeout, time.Since(startedAt))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		var deadlineErr *tailHandlerDeadlineError
		require.ErrorAs(t, err, &deadlineErr)
		require.True(t, deadlineErr.returnedNil)
		require.False(t, deadlineErr.detached)
		require.True(t, deadlineErr.requiresSynchronousRecord())
	})
}

func TestRunTailTaskPreservesCooperativePostDeadlineError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 5 * time.Second

		wantErr := errors.New("handler failed after deadline")
		client := &Client{tailHandlerTimeout: timeout}
		startedAt := time.Now()
		err := client.runTailTask(context.Background(), func(ctx context.Context) error {
			<-ctx.Done()
			return wantErr
		})
		require.Equal(t, timeout, time.Since(startedAt))
		require.ErrorIs(t, err, wantErr)
		var deadlineErr *tailHandlerDeadlineError
		require.ErrorAs(t, err, &deadlineErr)
		require.Same(t, wantErr, deadlineErr.cause)
		require.False(t, deadlineErr.returnedNil)
		require.False(t, deadlineErr.detached)
		require.False(t, deadlineErr.requiresSynchronousRecord())
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
			err := finalTailTaskDeadlineResult(
				10*time.Millisecond,
				result,
				deadline,
				graceDeadline,
			)
			var deadlineErr *tailHandlerDeadlineError
			require.ErrorAs(t, err, &deadlineErr)
			require.Equal(t, tc.wantDetached, deadlineErr.detached)
			if tc.wantDetached {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.NotErrorIs(t, err, wantErr)
				require.True(t, deadlineErr.requiresSynchronousRecord())
				return
			}
			require.ErrorIs(t, err, wantErr)
			require.Same(t, wantErr, deadlineErr.cause)
			require.False(t, deadlineErr.requiresSynchronousRecord())
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

		err := client.awaitTailTaskDeadline(result, deadline, graceDeadline)
		require.Equal(t, tailHandlerCancelGrace, time.Since(deadline))
		require.Equal(t, 1, hookCalls)
		require.ErrorIs(t, err, wantErr)
		var deadlineErr *tailHandlerDeadlineError
		require.ErrorAs(t, err, &deadlineErr)
		require.Same(t, wantErr, deadlineErr.cause)
		require.False(t, deadlineErr.detached)
		require.False(t, deadlineErr.requiresSynchronousRecord())
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
		done := make(chan error, 1)
		startedAt := time.Now()
		go func() {
			done <- client.runTailTask(parent, func(context.Context) error {
				close(started)
				<-release
				close(finished)
				return nil
			})
		}()
		<-started

		err := <-done
		require.Equal(t, parentTimeout+tailHandlerCancelGrace, time.Since(startedAt))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		var deadlineErr *tailHandlerDeadlineError
		require.NotErrorAs(t, err, &deadlineErr)

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
		done := make(chan error, 1)
		startedAt := time.Now()
		go func() {
			done <- client.runTailTask(parent, func(context.Context) error {
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

		err := <-done
		require.Equal(t, cancelAfter+tailHandlerCancelGrace, time.Since(startedAt))
		require.ErrorIs(t, err, context.Canceled)
		var deadlineErr *tailHandlerDeadlineError
		require.NotErrorAs(t, err, &deadlineErr)

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
		done := make(chan error, 1)
		startedAt := time.Now()
		go func() {
			done <- client.runTailTask(parent, func(context.Context) error {
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

		err := <-done
		require.Equal(t, timeout+tailHandlerCancelGrace, time.Since(startedAt))
		require.ErrorIs(t, parent.Err(), context.Canceled)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		var deadlineErr *tailHandlerDeadlineError
		require.ErrorAs(t, err, &deadlineErr)
		require.True(t, deadlineErr.detached)
		require.True(t, deadlineErr.requiresSynchronousRecord())

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
		done := make(chan error, 1)
		startedAt := time.Now()
		go func() {
			done <- client.runTailTask(parent, func(context.Context) error {
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
		case err := <-done:
			t.Fatalf("runTailTask returned before the fixed grace deadline: %v", err)
		default:
		}

		err := <-done
		require.Equal(t, timeout+tailHandlerCancelGrace, time.Since(startedAt))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		var deadlineErr *tailHandlerDeadlineError
		require.ErrorAs(t, err, &deadlineErr)
		require.True(t, deadlineErr.detached)
		require.True(t, deadlineErr.requiresSynchronousRecord())

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
	require.Equal(t, tailTask{
		eventType:    "MESSAGE_UPDATE",
		failureClass: tailFailureClassOrdered,
		guildID:      "g1",
		channelID:    "c1",
		messageID:    "m1",
		userID:       "u1",
	}, task)

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
	guildID, channelID, messageID, userID := metadata.snapshot()
	require.Equal(t, "g-refetched", guildID)
	require.Equal(t, "c1", channelID)
	require.Equal(t, "m1", messageID)
	require.Equal(t, "u-refetched", userID)
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
		EventType: "MESSAGE_CREATE",
		Kind:      "returned_error",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m1",
		UserID:    "u1",
	}, <-handler.failures)
}

func TestTailFailureRecorderRequirements(t *testing.T) {
	t.Parallel()

	require.NoError(t, recordTailFailure(nil, TailFailure{}))
	require.ErrorContains(t, recordTailFailure(nil, TailFailure{MessageID: "m1"}), "recorder unavailable")
	reportTailFailure(nil, TailFailure{})

	wantErr := errors.New("ledger unavailable")
	recorder := &tailFailureRecorderStub{err: wantErr}
	require.ErrorIs(t, recordTailFailure(recorder, TailFailure{MessageID: "m1"}), wantErr)
	require.Equal(t, TailFailure{MessageID: "m1"}, recorder.failure)
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
	require.Equal(t, TailFailure{
		EventType: "CHANNEL_UPDATE",
		Kind:      "returned_error",
		GuildID:   "g1",
		ChannelID: "c1",
	}, <-handler.failures)
	require.Equal(t, TailFailure{
		EventType: "GUILD_MEMBER_UPDATE",
		Kind:      "returned_error",
		GuildID:   "g1",
		UserID:    "u1",
	}, <-handler.failures)
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
	require.Equal(t, TailFailure{
		EventType: "CHANNEL_UPDATE",
		Kind:      "panic",
		GuildID:   "g1",
		ChannelID: "failed-channel",
	}, <-handler.failures)
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
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			handler := &failureContinuationHandler{
				fail:            tt.fail,
				cancel:          cancel,
				failureReported: make(chan struct{}),
				failures:        make(chan TailFailure, 1),
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
				case <-ctx.Done():
					t.Error("tail failure was not reported")
					return
				}
				if err := conn.WriteJSON(messageCreateEvent(3, "later", now)); err != nil {
					t.Errorf("write later event: %v", err)
					return
				}
				select {
				case <-handler.laterHandled:
				case <-ctx.Done():
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

			require.NoError(t, client.Tail(ctx, handler))
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			failure := <-handler.failures
			require.Equal(t, TailFailure{
				EventType: "MESSAGE_CREATE",
				Kind:      tt.wantKind,
				GuildID:   "g1",
				ChannelID: "c1",
				MessageID: "failed",
				UserID:    "u1",
			}, failure)
			select {
			case <-handler.laterHandled:
			default:
				t.Fatal("Tail returned before the later event was handled")
			}
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
	require.Equal(t, wantFailure, <-handler.recorded)
	require.Equal(t, wantFailure, <-handler.reported)
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
	require.ErrorContains(t, err, "persist tail handler panic failure")
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
	require.ErrorContains(t, err, "persist tail handler panic failure")
	require.NotContains(t, err.Error(), sensitivePanicValue)
	require.NotContains(t, err.Error(), sensitiveRecordErr)
	require.EqualValues(t, 1, handler.calls.Load())
	require.Equal(t, TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "panic",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "failed",
		UserID:    "u1",
	}, <-handler.recorded)
	select {
	case failure := <-handler.reported:
		t.Fatalf("cancellation panic recorder failure was reported instead of failing closed: %+v", failure)
	default:
	}
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
	st, err := store.Open(testCtx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	handler := &nonCooperativeFailureHandler{
		failures:         make(chan TailFailure, 1),
		recorded:         make(chan TailFailure, 1),
		failureReported:  make(chan struct{}),
		recordingStarted: make(chan struct{}),
		allowRecord:      make(chan struct{}),
		started:          make(chan string, 1),
		release:          make(chan struct{}),
		finished:         make(chan string, 1),
		record: func(failure TailFailure) error {
			return st.RecordFailure(context.Background(), store.FailureRef{
				Operation: "tail_message",
				Source:    "discord",
				GuildID:   failure.GuildID,
				ChannelID: failure.ChannelID,
				MessageID: failure.MessageID,
			}, context.DeadlineExceeded)
		},
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
	require.Equal(t, TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "timeout",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "blocked-1",
		UserID:    "u1",
	}, <-handler.recorded)
	require.Equal(t, TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "timeout",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "blocked-1",
		UserID:    "u1",
	}, <-handler.failures)
	report, err := st.ListFailures(context.Background(), store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "g1", report.Failures[0].GuildID)
	require.Equal(t, "c1", report.Failures[0].ChannelID)
	require.Equal(t, "blocked-1", report.Failures[0].MessageID)
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
	client.tailQueueSize = 1
	client.tailHandlerTimeout = 25 * time.Millisecond

	err = client.Tail(testCtx, handler)
	require.ErrorContains(t, err, "tail ordered handler timed out for MESSAGE_CREATE")
	require.ErrorContains(t, err, "persist timed-out tail failure: ledger unavailable")
	require.EqualValues(t, 1, handler.calls.Load())
}

func TestTailAggregatesQueueOverflowWithPostDeadlinePersistenceFailure(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()

	handler := &postDeadlineNilRecordingHandler{
		recordingStarted: make(chan struct{}),
		allowRecord:      make(chan struct{}),
		recordErr:        errors.New("ledger unavailable"),
	}
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
	select {
	case err := <-done:
		t.Fatalf("Tail returned before failure persistence completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(handler.allowRecord)
	select {
	case err = <-done:
	case <-testCtx.Done():
		t.Fatal("Tail did not return after failure persistence completed")
	}
	require.ErrorContains(t, err, "tail worker queue full")
	require.ErrorContains(t, err, "persist timed-out tail failure: ledger unavailable")
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
			require.Equal(t, TailFailure{
				EventType: "MESSAGE_UPDATE",
				Kind:      tt.wantKind,
				GuildID:   "g-refetched",
				ChannelID: "c1",
				MessageID: "m1",
				UserID:    "u-refetched",
			}, <-handler.failures)
		})
	}
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
			require.Equal(t, TailFailure{
				EventType: "MESSAGE_UPDATE",
				Kind:      "returned_error",
				GuildID:   "g1",
				ChannelID: "c1",
				MessageID: "m1",
			}, <-handler.failures)
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

type recordingHandler struct {
	creates       int
	updates       int
	deletes       int
	channels      int
	memberUpserts int
	memberDeletes int
	ready         int
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
}

func (h *updateFailureHandler) OnTailFailure(failure TailFailure) {
	h.failures <- failure
	h.reported <- struct{}{}
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
	laterHandled    chan struct{}
	failureOnce     sync.Once
	laterOnce       sync.Once
}

func (h *failureContinuationHandler) OnTailFailure(failure TailFailure) {
	h.failureOnce.Do(func() {
		h.failures <- failure
		close(h.failureReported)
	})
}

func (h *failureContinuationHandler) RecordTailFailure(TailFailure) error {
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
	failureOnce      sync.Once
	recordOnce       sync.Once
	recordErr        error
	record           func(TailFailure) error
}

func (h *nonCooperativeFailureHandler) OnTailFailure(failure TailFailure) {
	h.failures <- failure
	h.failureOnce.Do(func() {
		close(h.failureReported)
	})
}

func (h *nonCooperativeFailureHandler) RecordTailFailure(failure TailFailure) error {
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

func (h *nonCooperativeFailureHandler) OnMessageCreate(_ context.Context, msg *discordgo.Message) error {
	h.calls.Add(1)
	if msg != nil {
		select {
		case h.started <- msg.ID:
		default:
		}
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
	updates         chan *discordgo.Message
	failureOnce     sync.Once
}

func (h *messageUpdateFailureHandler) OnTailFailure(failure TailFailure) {
	h.failureOnce.Do(func() {
		h.failures <- failure
		close(h.failureReported)
		h.cancel()
	})
}

func (h *messageUpdateFailureHandler) RecordTailFailure(TailFailure) error {
	return nil
}

func (h *messageUpdateFailureHandler) OnMessageUpdate(ctx context.Context, msg *discordgo.Message) error {
	h.updates <- msg
	return h.fail(ctx)
}

type slowHandler struct {
	sleep time.Duration
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
