package acquisition_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/acquisition"
)

func TestClientRejectsUntrustedStateAndDoesNotRedirectPublisher(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"wrong profile", `{"profile":"other","sequence":1,"completedSequence":1}`, 200},
		{"negative revision", `{"profile":"discord-acquisition/v1","revision":-1,"sequence":1,"completedSequence":1}`, 200},
		{"future completion", `{"profile":"discord-acquisition/v1","sequence":1,"completedSequence":2}`, 200},
		{"missing completion", `{"profile":"discord-acquisition/v1","sequence":1}`, 200},
		{"unknown field", `{"profile":"discord-acquisition/v1","sequence":1,"completedSequence":1,"unexpected":true}`, 200},
		{"trailing document", `{"profile":"discord-acquisition/v1","sequence":1,"completedSequence":1} {}`, 200},
		{"truncated JSON", `{"profile":`, 200},
		{"oversized response", strings.Repeat(" ", 128<<10+1), 200},
		{"error body private", `private-error-detail`, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client, err := acquisition.NewClient(server.URL, "fixture", "publisher")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = client.State(t.Context(), "200"); err == nil || strings.Contains(err.Error(), "private-error-detail") {
				t.Fatalf("invalid state accepted or private error leaked: %v", err)
			}
		})
	}
	called := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; t.Error("publisher followed redirect") }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client, err := acquisition.NewClient(redirect.URL, "fixture", "publisher")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.State(t.Context(), "200"); err == nil || called {
		t.Fatal("redirect did not fail closed")
	}
	for _, tc := range []struct{ origin, source, token string }{
		{"http://archive.example.test", "fixture", "publisher"},
		{"https://archive.example.test/path", "fixture", "publisher"},
		{"https://archive.example.test?query=1", "fixture", "publisher"},
		{"https://user@archive.example.test", "fixture", "publisher"},
		{"https://archive.example.test", "../other", "publisher"},
		{"https://archive.example.test", "fixture", "bad token"},
	} {
		if _, err := acquisition.NewClient(tc.origin, tc.source, tc.token); err == nil {
			t.Fatal("unsafe origin/source/credential accepted")
		}
	}
}

func TestClientRejectsUnboundRunLookupAndPageReceipts(t *testing.T) {
	state := acquisition.State{Revision: 4, Sequence: 20}
	run := acquisition.Run{ID: "run"}
	valid := retainedMessage("123", time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC))
	for _, tc := range []struct {
		name     string
		invoke   func(*acquisition.Client) error
		response any
	}{
		{"wrong run receipt", func(c *acquisition.Client) error { return c.Begin(context.Background(), run) }, map[string]any{"runId": "other", "status": "accepted"}},
		{"lookup stale revision", func(c *acquisition.Client) error {
			_, err := c.Messages(context.Background(), run, state, []string{"123"})
			return err
		}, map[string]any{"revision": 3, "sequence": 20, "messages": []remote.Change{valid}}},
		{"lookup outside selection", func(c *acquisition.Client) error {
			_, err := c.Messages(context.Background(), run, state, []string{"999"})
			return err
		}, map[string]any{"revision": 4, "sequence": 20, "messages": []remote.Change{valid}}},
		{"lookup duplicate row", func(c *acquisition.Client) error {
			_, err := c.Messages(context.Background(), run, state, []string{"123"})
			return err
		}, map[string]any{"revision": 4, "sequence": 20, "messages": []remote.Change{valid, valid}}},
		{"page hash differs", func(c *acquisition.Client) error {
			_, err := c.Commit(context.Background(), state, acquisition.Page{RunID: "run", ID: "page"})
			return err
		}, acquisition.Receipt{RunID: "run", PageID: "page", Revision: 5, Sequence: 21, SHA256: strings.Repeat("0", 64)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(tc.response) }))
			defer server.Close()
			c, err := acquisition.NewClient(server.URL, "fixture", "publisher")
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.invoke(c); err == nil {
				t.Fatal("unbound API response accepted")
			}
		})
	}
}
