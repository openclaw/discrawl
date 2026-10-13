package discord

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemberEnumerationTerminalBoundaryAndInterruptedPages(t *testing.T) {
	for _, mode := range []string{"complete", "429", "interrupted", "backward"} {
		t.Run(mode, func(t *testing.T) {
			page := make([]map[string]any, 1000)
			for i := range page {
				page[i] = map[string]any{"user": map[string]any{"id": strconv.Itoa(i + 1)}, "roles": []string{}}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				if call == 1 {
					assert.Empty(t, r.URL.Query().Get("after"))
					writeJSON(page)(w, r)
					return
				}
				assert.Equal(t, "1000", r.URL.Query().Get("after"))
				switch mode {
				case "429":
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = fmt.Fprint(w, `{"retry_after":1,"global":false,"message":"fixture"}`)
				case "interrupted":
					cancel()
				case "backward":
					writeJSON([]map[string]any{{"user": map[string]any{"id": "999"}, "roles": []string{}}})(w, r)
				case "complete":
					writeJSON([]map[string]any{{"user": map[string]any{"id": "1001"}, "roles": []string{}}})(w, r)
				}
			}))
			defer server.Close()
			restore := patchDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()
			client, err := New("synthetic")
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			client.session.ShouldRetryOnRateLimit = false
			result, err := client.GuildMemberEnumeration(ctx, "g")
			require.EqualValues(t, 2, calls.Load())
			if mode == "complete" {
				require.NoError(t, err)
				require.True(t, result.Complete)
				require.Equal(t, 2, result.Pages)
				require.Equal(t, "1001", result.LastUserID)
				require.Len(t, result.Members, 1001)
			} else {
				require.Error(t, err)
				require.False(t, result.Complete)
				require.Nil(t, result.Members)
				if mode == "backward" {
					require.Equal(t, 2, result.Pages)
				} else {
					require.Equal(t, 1, result.Pages)
				}
				require.Equal(t, "1000", result.LastUserID)
			}
		})
	}
}
