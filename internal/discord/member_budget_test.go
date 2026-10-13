package discord

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestMemberBudgetHonorsRetryAfterAndSeparatesWait(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("X-Ratelimit-Remaining", "0")
			w.Header().Set("X-Ratelimit-Reset-After", "0.05")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = fmt.Fprint(w, `{"retry_after":0.05,"global":false}`)
			return
		}
		writeJSON([]map[string]any{{"user": map[string]any{"id": "1"}}})(w, r)
	}))
	defer server.Close()
	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()
	client, err := New("synthetic")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	client.session.ShouldRetryOnRateLimit = true
	started := time.Now()
	result, err := client.GuildMemberEnumeration(t.Context(), "g")
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.EqualValues(t, 2, calls.Load())
	require.Equal(t, 1, result.RateLimits)
	require.Equal(t, 1, result.Pages)
	require.GreaterOrEqual(t, result.ProviderWait, 50*time.Millisecond)
	require.Positive(t, result.CallsElapsed)
	require.LessOrEqual(t, result.ProviderWait+result.CallsElapsed, time.Since(started))
}

func TestMemberBudgetCancellationDuringProviderWaitMakesNoExtraRequest(t *testing.T) {
	for _, mode := range []string{"bucket", "429"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = fmt.Fprint(w, `{"retry_after":5,"global":false}`)
			}))
			defer server.Close()
			restore := patchDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()
			client, err := New("synthetic")
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			client.session.ShouldRetryOnRateLimit = true
			if mode == "bucket" {
				bucket := client.session.Ratelimiter.GetBucket(discordgo.EndpointGuildMembers("g"))
				bucket.Lock()
				require.NoError(t, bucket.Release(http.Header{"X-Ratelimit-Remaining": []string{"0"}, "X-Ratelimit-Reset-After": []string{"5"}}))
			}
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			started := time.Now()
			result, err := client.GuildMemberEnumeration(ctx, "g")
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Less(t, time.Since(started), time.Second)
			require.False(t, result.Complete)
			require.Nil(t, result.Members)
			require.Positive(t, result.ProviderWait)
			if mode == "bucket" {
				require.Zero(t, calls.Load())
			} else {
				require.EqualValues(t, 1, calls.Load())
				require.Equal(t, 1, result.RateLimits)
			}
		})
	}
}

func TestMemberProgressContainsOnlyCountsAndCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		after, _ := strconv.Atoi(r.URL.Query().Get("after"))
		page := []map[string]any{}
		if after < 25000 {
			for i := range 1000 {
				page = append(page, map[string]any{"user": map[string]any{"id": strconv.Itoa(after + i + 1)}})
			}
		}
		writeJSON(page)(w, r)
	}))
	defer server.Close()
	restore := patchDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()
	client, err := New("synthetic")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	var progress []MemberProgress
	ctx := WithMemberProgress(t.Context(), func(p MemberProgress) { progress = append(progress, p) })
	result, err := client.GuildMemberEnumeration(ctx, "g")
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.Equal(t, 26, result.Pages)
	require.Len(t, progress, 1)
	require.Equal(t, 25, progress[0].Pages)
	require.Equal(t, 25000, progress[0].Members)
	require.Equal(t, "25000", progress[0].LastUserID)
}
