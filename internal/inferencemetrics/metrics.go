// Package inferencemetrics observes opt-in OpenAI embedding attempts. It has no
// archive, logging, credential, retry or background-worker dependencies.
package inferencemetrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openclaw/crawlkit/embed"
)

type (
	contextKey   struct{}
	operationKey struct{}
)

// Query marks the one native query-embedding call site. All other embedding
// calls are document work; arbitrary caller strings never become metric tags.
func Query(ctx context.Context) context.Context { return context.WithValue(ctx, operationKey{}, true) }

type Observer struct {
	address   netip.AddrPort
	model     string
	transport http.RoundTripper
}

// FromEnvironment returns nil unless this is an explicitly enabled direct
// public OpenAI provider. Numeric loopback only avoids DNS, remote export and
// destination injection. Invalid telemetry configuration never breaks a model.
func FromEnvironment(cfg embed.Config) *Observer {
	endpoint := os.Getenv("CLAW_INFERENCE_STATSD")
	address, err := netip.ParseAddrPort(endpoint)
	if err != nil || !address.Addr().IsLoopback() || address.Port() == 0 {
		return nil
	}
	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if provider != "" && provider != embed.ProviderOpenAI || base != "" && base != embed.DefaultOpenAIBaseURL {
		return nil
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		model = embed.DefaultOpenAIModel
	}
	return &Observer{address: address, model: modelTag(model), transport: http.DefaultTransport}
}

func modelTag(model string) string {
	if len(model) == 0 || len(model) > 80 {
		return "other"
	}
	for _, c := range model {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
		default:
			return "other"
		}
	}
	return model
}

func (o *Observer) Option() embed.Option                 { return embed.WithHTTPClient(&http.Client{Transport: o}) }
func (o *Observer) Wrap(p embed.Provider) embed.Provider { return &provider{Provider: p} }

type provider struct {
	embed.Provider
}
type call struct {
	mu        sync.Mutex
	pending   *attempt
	operation string
}

func (p *provider) Embed(ctx context.Context, inputs []string) (embed.EmbeddingBatch, error) {
	op := "embedding_document"
	if query, _ := ctx.Value(operationKey{}).(bool); query {
		op = "embedding_query"
	}
	c := &call{operation: op}
	result, err := p.Provider.Embed(context.WithValue(ctx, contextKey{}, c), inputs)
	c.mu.Lock()
	a := c.pending
	c.pending = nil
	c.mu.Unlock()
	if a != nil {
		a.emit(err)
	}
	return result, err
}

func (o *Observer) RoundTrip(req *http.Request) (*http.Response, error) {
	c, _ := req.Context().Value(contextKey{}).(*call)
	// Only the provider's public embeddings endpoint is in scope. Redirected
	// destinations keep the normal HTTP behavior but are never measured/priced.
	if c == nil || req.URL.Scheme != "https" || req.URL.Host != "api.openai.com" || req.URL.Path != "/v1/embeddings" {
		return o.transport.RoundTrip(req)
	}
	a := &attempt{observer: o, operation: c.operation}
	c.mu.Lock()
	previous := c.pending
	c.pending = a
	c.mu.Unlock()
	if previous != nil {
		previous.emit(nil)
	}
	a.start = time.Now()
	resp, err := o.transport.RoundTrip(req)
	a.mu.Lock()
	if err != nil {
		a.readFailed = true
		a.end = time.Now()
	}
	if resp != nil {
		a.status = resp.StatusCode
	}
	a.mu.Unlock()
	if err == nil && resp != nil && resp.Body != nil {
		resp.Body = &body{ReadCloser: resp.Body, attempt: a}
	}
	return resp, err
}

type attempt struct {
	mu         sync.Mutex
	observer   *Observer
	operation  string
	start, end time.Time
	status     int
	readFailed bool
	usage      usageScanner
	once       sync.Once
}
type body struct {
	io.ReadCloser
	attempt *attempt
}

func (b *body) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.attempt.mu.Lock()
	if b.attempt.status >= 200 && b.attempt.status < 300 {
		b.attempt.usage.feed(p[:n])
	}
	b.attempt.end = time.Now()
	if err != nil && err != io.EOF {
		b.attempt.readFailed = true
	}
	b.attempt.mu.Unlock()
	return n, err
}

func (b *body) Close() error {
	err := b.ReadCloser.Close()
	b.attempt.mu.Lock()
	if b.attempt.end.IsZero() {
		b.attempt.end = time.Now()
	}
	b.attempt.mu.Unlock()
	return err
}

func (a *attempt) emit(providerErr error) {
	a.once.Do(func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.end.IsZero() {
			a.end = time.Now()
		}
		outcome := "success"
		switch {
		case a.status != 0 && (a.status < 200 || a.status >= 300):
			outcome = "http_error"
		case a.readFailed || a.status == 0 || errors.Is(providerErr, context.Canceled) || errors.Is(providerErr, context.DeadlineExceeded):
			outcome = "transport_error"
		case providerErr != nil:
			outcome = "invalid_response"
		}
		tier := a.usage.tier
		if tier == "" {
			tier = "default"
		} // Embeddings sends no service-tier override.
		switch tier {
		case "default", "priority", "flex", "unknown":
		default:
			tier = "other"
		}
		model := a.observer.model
		if a.usage.model != "" {
			model = modelTag(a.usage.model)
		}
		tags := fmt.Sprintf("|#service:discrawl,workload:discrawl,operation:%s,provider:openai,model:%s,tier:%s,outcome:%s", a.operation, model, tier, outcome)
		var packet strings.Builder
		metric := func(name, value, kind string) {
			fmt.Fprintf(&packet, "claw.inference.%s:%s|%s%s\n", name, value, kind, tags)
		}
		metric("requests", "1", "c")
		metric("duration", strconv.FormatFloat(float64(a.end.Sub(a.start))/float64(time.Millisecond), 'f', 3, 64), "ms")
		metric("last_event", strconv.FormatInt(a.end.Unix(), 10), "g")
		var syntaxError *json.SyntaxError
		var typeError *json.UnmarshalTypeError
		decoded := !errors.As(providerErr, &syntaxError) && !errors.As(providerErr, &typeError)
		if (outcome == "success" || outcome == "invalid_response") && decoded && a.usage.done && a.usage.input != nil {
			input := *a.usage.input
			metric("input_tokens", strconv.FormatUint(input, 10), "c")
			metric("usage_known", "1", "c")
			if a.usage.cached != nil && *a.usage.cached <= input {
				metric("cached_tokens", strconv.FormatUint(*a.usage.cached, 10), "c")
			}
			if model == embed.DefaultOpenAIModel && tier == "default" {
				metric("cost_usd", strconv.FormatFloat(float64(input)*0.02/1e6, 'f', 12, 64), "c")
				metric("cost_known", "1", "c")
			}
		}
		a.observer.send(packet.String())
	})
}

func (o *Observer) send(packet string) {
	if len(packet) > 4096 {
		return
	}
	// One bounded local UDP write; no queue, goroutine, retry or shutdown work.
	conn, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(o.address))
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	if conn.SetWriteDeadline(time.Now().Add(2*time.Millisecond)) != nil {
		return
	}
	_, _ = conn.Write([]byte(packet))
}
