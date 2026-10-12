package inferencemetrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/embed"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type trackedBody struct {
	reader io.Reader
	read   int
	closed bool
	chunk  int
}

func (b *trackedBody) Read(p []byte) (int, error) {
	if b.chunk > 0 && len(p) > b.chunk {
		p = p[:b.chunk]
	}
	n, err := b.reader.Read(p)
	b.read += n
	return n, err
}
func (b *trackedBody) Close() error { b.closed = true; return nil }

func listener(t *testing.T) (*net.UDPConn, netip.AddrPort) {
	t.Helper()
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c, c.LocalAddr().(*net.UDPAddr).AddrPort()
}

func readPacket(t *testing.T, c *net.UDPConn) string {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(time.Second)))
	p := make([]byte, 4097)
	n, err := c.Read(p)
	require.NoError(t, err)
	require.LessOrEqual(t, n, 4096)
	return string(p[:n])
}

func providerFixture(t *testing.T, o *Observer) embed.Provider {
	t.Helper()
	p, err := embed.NewProvider(embed.Config{Provider: "openai", Model: embed.DefaultOpenAIModel, Dimensions: 2}, embed.WithAPIKey("synthetic-test-key"), o.Option())
	require.NoError(t, err)
	return o.Wrap(p)
}

func TestAttemptPreservesProviderResultsAndUnknownUsage(t *testing.T) {
	for _, tc := range []struct {
		name, payload, outcome string
		known                  bool
		tokens                 string
		priced                 bool
		failure                bool
	}{
		{"known", `{"model":"text-embedding-3-small","data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":100,"total_tokens":100}}`, "success", true, "100", true, false},
		{"zero", `{"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":0}}`, "success", true, "0", true, false},
		{"unknown", `{"data":[{"index":0,"embedding":[1,2]}]}`, "success", false, "", false, false},
		{"null", `{"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":null}}`, "success", false, "", false, false},
		{"string", `{"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":"123"}}`, "success", false, "", false, false},
		{"negative", `{"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":-1}}`, "success", false, "", false, false},
		{"fractional", `{"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":1.5}}`, "success", false, "", false, false},
		{"overflow", `{"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":9007199254740992}}`, "success", false, "", false, false},
		{"tier", `{"service_tier":"priority","data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":100}}`, "success", true, "100", false, false},
		{"tier-null", `{"service_tier":null,"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":100}}`, "success", true, "100", false, false},
		{"invalid-json", `{"data":[`, "invalid_response", false, "", false, true},
		{"invalid-vector", `{"data":[{"index":0,"embedding":[]}],"usage":{"prompt_tokens":4}}`, "invalid_response", true, "4", true, true},
		{"different-model", `{"model":"text-embedding-3-large","data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":4}}`, "success", true, "4", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			udp, addr := listener(t)
			body := &trackedBody{reader: strings.NewReader(tc.payload), chunk: 1}
			o := &Observer{address: addr, model: embed.DefaultOpenAIModel, transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "Bearer synthetic-test-key", r.Header.Get("Authorization"))
				req, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Contains(t, string(req), "private fixture input")
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
			})}
			got, err := providerFixture(t, o).Embed(Query(t.Context()), []string{"private fixture input"})
			if tc.failure {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, [][]float32{{1, 2}}, got.Vectors)
			}
			require.True(t, body.closed)
			packet := readPacket(t, udp)
			require.Contains(t, packet, "operation:embedding_query")
			require.Contains(t, packet, "outcome:"+tc.outcome)
			require.Equal(t, 1, strings.Count(packet, "claw.inference.requests:1|c"))
			require.Contains(t, packet, "claw.inference.duration:")
			require.Contains(t, packet, "claw.inference.last_event:")
			if tc.known {
				require.Contains(t, packet, "claw.inference.input_tokens:"+tc.tokens+"|c")
				require.Contains(t, packet, "claw.inference.usage_known:1|c")
			} else {
				require.NotContains(t, packet, "input_tokens:")
				require.NotContains(t, packet, "usage_known:")
			}
			if tc.priced {
				require.Contains(t, packet, "claw.inference.cost_known:1|c")
			} else {
				require.NotContains(t, packet, "cost_usd:")
				require.NotContains(t, packet, "cost_known:")
			}
			if tc.name == "different-model" {
				require.Contains(t, packet, "model:text-embedding-3-large")
			}
			if tc.name == "known" {
				require.Contains(t, packet, "claw.inference.cost_usd:0.000002000000|c")
			}
			for _, secret := range []string{"synthetic-test-key", "private fixture input", "embedding\"", "api.openai.com", "/v1/"} {
				require.NotContains(t, packet, secret)
			}
		})
	}
}

func TestHTTPErrorLimitAndRetryAttempts(t *testing.T) {
	udp, addr := listener(t)
	var bodies []*trackedBody
	o := &Observer{address: addr, model: "text-embedding-3-small"}
	o.transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		b := &trackedBody{reader: strings.NewReader(strings.Repeat("private-error", 1000))}
		bodies = append(bodies, b)
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": []string{"17"}}, Body: b}, nil
	})
	p := providerFixture(t, o)
	for range 2 {
		_, err := p.Embed(t.Context(), []string{"input"})
		var httpErr *embed.HTTPError
		require.ErrorAs(t, err, &httpErr)
		require.Equal(t, 429, httpErr.StatusCode)
		require.Len(t, httpErr.Body, 4096)
		require.Equal(t, "17", httpErr.Header.Get("Retry-After"))
		packet := readPacket(t, udp)
		require.Contains(t, packet, "outcome:http_error")
		require.NotContains(t, packet, "private-error")
		require.NotContains(t, packet, "usage_known")
	}
	require.Len(t, bodies, 2)
	for _, b := range bodies {
		require.Equal(t, 4096, b.read)
		require.True(t, b.closed)
	}
}

func TestRedirectCountsHTTPAttemptsAndEmptyInputCountsNone(t *testing.T) {
	udp, addr := listener(t)
	calls := 0
	o := &Observer{address: addr, model: "text-embedding-3-small"}
	o.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{"Location": []string{"https://api.openai.com/v1/embeddings?fixture=1"}}, Body: io.NopCloser(strings.NewReader("redirect")), Request: r}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":8}}`)), Request: r}, nil
	})
	p := providerFixture(t, o)
	_, err := p.Embed(t.Context(), nil)
	require.NoError(t, err)
	require.Zero(t, calls)
	_, err = p.Embed(t.Context(), []string{"input"})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Contains(t, readPacket(t, udp), "outcome:http_error")
	require.Contains(t, readPacket(t, udp), "claw.inference.input_tokens:8|c")
}

type cancelReader struct {
	ctx   context.Context
	ready chan struct{}
}

func (b *cancelReader) Read([]byte) (int, error) {
	close(b.ready)
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (b *cancelReader) Close() error { return nil }

func TestTransportAndBodyCancellation(t *testing.T) {
	for _, phase := range []string{"transport", "body"} {
		t.Run(phase, func(t *testing.T) {
			udp, addr := listener(t)
			ready := make(chan struct{})
			o := &Observer{address: addr, model: "fixture", transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if phase == "transport" {
					close(ready)
					<-r.Context().Done()
					return nil, r.Context().Err()
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: &cancelReader{ctx: r.Context(), ready: ready}}, nil
			})}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p := providerFixture(t, o)
			done := make(chan error, 1)
			go func() { _, err := p.Embed(ctx, []string{"input"}); done <- err }()
			<-ready
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("measurement blocked cancellation")
			}
			packet := readPacket(t, udp)
			require.Contains(t, packet, "outcome:transport_error")
			require.NotContains(t, packet, "usage_known")
		})
	}
}

func TestUnavailableUDPAndConcurrentCalls(t *testing.T) {
	udp, addr := listener(t)
	require.NoError(t, udp.Close())
	o := &Observer{address: addr, model: "fixture", transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":1}}`))}, nil
	})}
	p := providerFixture(t, o)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			result, err := p.Embed(t.Context(), []string{"input"})
			if err != nil || len(result.Vectors) != 1 {
				t.Errorf("unavailable telemetry changed provider result: %v", err)
			}
		})
	}
	wg.Wait()
}

func TestOptInScopeAndTags(t *testing.T) {
	for _, endpoint := range []string{"", "bad", "localhost:18125", "192.0.2.1:18125", "127.0.0.1:0"} {
		t.Setenv("CLAW_INFERENCE_STATSD", endpoint)
		require.Nil(t, FromEnvironment(embed.Config{}))
	}
	t.Setenv("CLAW_INFERENCE_STATSD", "127.0.0.1:18125")
	for _, cfg := range []embed.Config{{Provider: "ollama"}, {Provider: "openai_compatible", BaseURL: embed.DefaultOpenAIBaseURL}, {Provider: "openai", BaseURL: "https://example.invalid/v1"}} {
		require.Nil(t, FromEnvironment(cfg))
	}
	o := FromEnvironment(embed.Config{})
	require.NotNil(t, o)
	require.Equal(t, embed.DefaultOpenAIModel, o.model)
	require.Equal(t, embed.DefaultOpenAIModel, o.model)
	o = FromEnvironment(embed.Config{Model: "text-embedding-3-large"})
	require.NotNil(t, o)
	require.Equal(t, "text-embedding-3-large", o.model)
	for _, m := range []string{"private:tag|#injected", strings.Repeat("a", 81), "🌎"} {
		require.Equal(t, "other", modelTag(m))
	}
}

func TestBodyReadErrorAndNoReadAhead(t *testing.T) {
	udp, addr := listener(t)
	readErr := errors.New("fixture read interruption")
	o := &Observer{address: addr, model: "fixture", transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(errorReader{readErr})}, nil
	})}
	_, err := providerFixture(t, o).Embed(t.Context(), []string{"input"})
	require.ErrorIs(t, err, readErr)
	require.Contains(t, readPacket(t, udp), "outcome:transport_error")
	// A valid first JSON value followed by megabytes must retain Crawlkit's
	// original decoder read-ahead only, never drain the response for telemetry.
	content := `{"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":7}}` + strings.Repeat(" ", 1<<20)
	var reads []int
	for _, enabled := range []bool{false, true} {
		body := &trackedBody{reader: strings.NewReader(content)}
		base := roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
		})
		var p embed.Provider
		if enabled {
			o.transport = base
			p = providerFixture(t, o)
		} else {
			p, err = embed.NewProvider(embed.Config{Dimensions: 2}, embed.WithAPIKey("fixture"), embed.WithHTTPClient(&http.Client{Transport: base}))
			require.NoError(t, err)
		}
		_, err = p.Embed(t.Context(), []string{"input"})
		require.NoError(t, err)
		require.True(t, body.closed)
		reads = append(reads, body.read)
	}
	require.Equal(t, reads[0], reads[1])
	require.Less(t, reads[1], len(content))
	require.Contains(t, readPacket(t, udp), "claw.inference.input_tokens:7|c")
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestDurationIncludesBodyRead(t *testing.T) {
	udp, addr := listener(t)
	o := &Observer{address: addr, model: "fixture", transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(delayedReader{strings.NewReader(`{"data":[{"index":0,"embedding":[1,2]}]}`)})}, nil
	})}
	_, err := providerFixture(t, o).Embed(t.Context(), []string{"input"})
	require.NoError(t, err)
	packet := readPacket(t, udp)
	var duration float64
	for line := range strings.SplitSeq(packet, "\n") {
		if strings.HasPrefix(line, "claw.inference.duration:") {
			_, err = fmt.Sscanf(line, "claw.inference.duration:%f|ms", &duration)
			require.NoError(t, err)
		}
	}
	require.GreaterOrEqual(t, duration, 10.0)
}

type delayedReader struct{ io.Reader }

func (r delayedReader) Read(p []byte) (int, error) {
	time.Sleep(10 * time.Millisecond)
	return r.Reader.Read(p)
}
