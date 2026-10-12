package cli

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

type inferenceFixtureTransport func(*http.Request) (*http.Response, error)

func (f inferenceFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestNativeInferenceMeasurementDocumentAndQueryWiring(t *testing.T) {
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = udp.Close() })
	t.Setenv("CLAW_INFERENCE_STATSD", udp.LocalAddr().String())
	t.Setenv("DISCRAWL_TEST_INFERENCE_KEY", "synthetic-fixture")
	previous := http.DefaultTransport
	http.DefaultTransport = inferenceFixtureTransport(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "https://api.openai.com/v1/embeddings", r.URL.String())
		require.Equal(t, "Bearer synthetic-fixture", r.Header.Get("Authorization"))
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"model":"text-embedding-3-small","data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":3}}`))}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = previous })
	cfg := config.Default()
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai"
	cfg.Search.Embeddings.Model = "text-embedding-3-small"
	cfg.Search.Embeddings.APIKeyEnv = "DISCRAWL_TEST_INFERENCE_KEY"
	p, err := newEmbeddingProvider(cfg.Search.Embeddings)
	require.NoError(t, err)
	_, err = p.Embed(t.Context(), []string{"synthetic document"})
	require.NoError(t, err)
	r := &runtime{ctx: t.Context(), cfg: cfg}
	_, err = r.semanticSearchOptions(store.SearchOptions{Query: "synthetic query"})
	require.NoError(t, err)
	for _, operation := range []string{"embedding_document", "embedding_query"} {
		require.NoError(t, udp.SetReadDeadline(time.Now().Add(time.Second)))
		packet := make([]byte, 4096)
		n, err := udp.Read(packet)
		require.NoError(t, err)
		require.Contains(t, string(packet[:n]), "operation:"+operation)
		require.Contains(t, string(packet[:n]), "claw.inference.input_tokens:3|c")
	}
}
