package headlinemetrics

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

const sampleTime = "2026-09-15T12:00:00Z"

func TestCollectDiscordPublicCountsAndUnknowns(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		members *float64
		online  *float64
	}{
		{"zero is valid", 200, `{"guild":{"id":"123"},"approximate_member_count":20,"approximate_presence_count":0}`, new(20.0), new(0.0)},
		{"missing presence", 200, `{"guild":{"id":"123"},"approximate_member_count":12}`, new(12.0), nil},
		{"null members", 200, `{"guild":{"id":"123"},"approximate_member_count":null,"approximate_presence_count":3}`, nil, new(3.0)},
		{"negative count", 200, `{"guild":{"id":"123"},"approximate_member_count":-1,"approximate_presence_count":3}`, nil, new(3.0)},
		{"fractional count", 200, `{"guild":{"id":"123"},"approximate_member_count":1.5}`, nil, nil},
		{"unrepresentable count", 200, `{"guild":{"id":"123"},"approximate_member_count":9007199254740993,"approximate_presence_count":1}`, nil, new(1.0)},
		{"non-guild invite", 200, `{"approximate_member_count":20,"approximate_presence_count":5}`, nil, nil},
		{"malformed", 200, `{"guild":`, nil, nil},
		{"trailing JSON", 200, `{"guild":{"id":"123"}} {}`, nil, nil},
		{"oversized", 200, strings.Repeat(" ", 2*1024*1024+1), nil, nil},
		{"expired invite", 404, `{}`, nil, nil},
		{"rate limited", 429, `{"retry_after":3600}`, nil, nil},
		{"unavailable", 503, `{}`, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			body := &closedBody{Reader: strings.NewReader(tc.body)}
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				require.Equal(t, http.MethodGet, req.Method)
				require.Equal(t, "https://discord.com/api/v10/invites/clawd?with_counts=true", req.URL.String())
				require.Empty(t, req.Header.Get("Authorization"))
				require.Empty(t, req.Header.Get("Cookie"))
				return &http.Response{StatusCode: tc.status, Body: body, Header: http.Header{"Retry-After": []string{"3600"}}}, nil
			})}
			rows, err := collectDiscord(t.Context(), Config{Targets: []Target{{Entity: "openclaw", Target: "clawd"}}}, sampleTime, client)
			require.Len(t, rows, 2)
			require.Equal(t, tc.members, rows[0].Value)
			require.Equal(t, tc.online, rows[1].Value)
			require.Equal(t, "members", rows[0].Metric)
			require.Equal(t, "online", rows[1].Metric)
			require.Equal(t, "discord_invite_approximate", rows[1].Provenance)
			require.Equal(t, "counter", rows[1].Kind)
			require.Equal(t, sampleTime, rows[1].ObservedAt)
			require.Equal(t, 1, calls)
			require.True(t, body.closed)
			if tc.members == nil || tc.online == nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type closedBody struct {
	io.Reader
	closed bool
}

func (b *closedBody) Close() error { b.closed = true; return nil }

func TestCollectDiscordIgnoresCredentialsAndRefusesRedirects(t *testing.T) {
	t.Setenv("DISCORD_BOT_TOKEN", "fixture-not-to-be-read")
	old := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = old })
	requested := []string{}
	http.DefaultTransport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requested = append(requested, req.URL.String())
		require.Equal(t, "discord.com", req.URL.Host)
		require.Empty(t, req.Header.Get("Authorization"))
		require.Empty(t, req.Header.Get("Cookie"))
		if strings.HasSuffix(req.URL.Path, "/clawd") {
			return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": []string{"https://elsewhere.invalid/private"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"guild":{"id":"456"},"approximate_member_count":50,"approximate_presence_count":7}`))}, nil
	})
	rows, err := CollectDiscord(t.Context(), Config{CookieJar: "/never/read", TokenEnv: "DISCORD_BOT_TOKEN", Targets: []Target{{"openclaw", "clawd"}, {"hermes", "nousresearch"}}}, sampleTime)
	require.Error(t, err)
	require.Len(t, requested, 2)
	require.Len(t, rows, 4)
	require.Nil(t, rows[0].Value)
	require.Equal(t, new(50.0), rows[2].Value)
	require.Equal(t, "hermes", rows[2].Entity)
}

func TestCollectDiscordInvalidTargetAndTransportFailure(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("fixture transport failure")
	})}
	rows, err := collectDiscord(t.Context(), Config{Targets: []Target{{"bad", "../private"}, {"valid", "clawd"}}}, sampleTime, client)
	require.Error(t, err)
	require.Len(t, rows, 4)
	require.Equal(t, 1, calls)
	for _, row := range rows {
		require.Nil(t, row.Value)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = collectDiscord(ctx, Config{Targets: []Target{{"valid", "clawd"}}}, sampleTime, &http.Client{})
	require.Error(t, err)
}
