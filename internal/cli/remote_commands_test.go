package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/control"
	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
)

type fakeRemoteClient struct{}

func (fakeRemoteClient) Archives(context.Context) ([]crawlremote.Archive, error) {
	return []crawlremote.Archive{{ID: "discrawl/openclaw", App: "discrawl", Slug: "openclaw"}}, nil
}

func (fakeRemoteClient) Query(context.Context, string, string, crawlremote.QueryRequest) (crawlremote.QueryResult, error) {
	return crawlremote.QueryResult{}, nil
}

func (fakeRemoteClient) Status(context.Context, string, string) (crawlremote.Status, error) {
	return crawlremote.Status{}, nil
}

func (fakeRemoteClient) Whoami(context.Context) (crawlremote.Identity, error) {
	return crawlremote.Identity{Login: "alice", Org: "openclaw"}, nil
}

type queryRemoteClient struct {
	last crawlremote.QueryRequest
}

func (queryRemoteClient) Archives(context.Context) ([]crawlremote.Archive, error) { return nil, nil }

func (c *queryRemoteClient) Query(_ context.Context, _ string, _ string, req crawlremote.QueryRequest) (crawlremote.QueryResult, error) {
	c.last = req
	return crawlremote.QueryResult{Values: []map[string]any{{
		"message_id":      "m1",
		"guild_id":        "g1",
		"channel_id":      "c1",
		"channel_name":    "general",
		"author_id":       "u1",
		"author_username": "alice",
		"content":         "hello from remote",
		"created_at":      "2026-05-27T17:00:00Z",
	}}}, nil
}

func (queryRemoteClient) Status(context.Context, string, string) (crawlremote.Status, error) {
	return crawlremote.Status{}, nil
}

func (queryRemoteClient) Whoami(context.Context) (crawlremote.Identity, error) {
	return crawlremote.Identity{}, nil
}

func TestRemoteCommandHelpers(t *testing.T) {
	if got, err := singleRemoteGuild([]string{"g1"}); err != nil || got != "g1" {
		t.Fatalf("single guild = %q err=%v", got, err)
	}
	if _, err := singleRemoteGuild([]string{"g1", "g2"}); err == nil || !strings.Contains(err.Error(), "one guild") {
		t.Fatalf("multi guild err = %v", err)
	}
	value := map[string]any{"name": 123, "created_at": "2026-05-27T17:00:00.123Z"}
	if got := remoteString(value, "name"); got != "123" {
		t.Fatalf("remote string = %q", got)
	}
	if got := remoteTime(value, "created_at"); got.IsZero() {
		t.Fatalf("remote time is zero")
	}
	if got := remoteString(nil, "missing"); got != "" {
		t.Fatalf("nil remote string = %q", got)
	}
	counts := []control.Count{control.NewCount("messages", "Messages", 42)}
	if got := countValue(counts, "messages"); got != 42 {
		t.Fatalf("count = %d", got)
	}
	if got := countValue(counts, "guilds"); got != 0 {
		t.Fatalf("missing count = %d", got)
	}
}

func TestRuntimeRemoteArchivesAndWhoamiUseConfiguredClient(t *testing.T) {
	var out bytes.Buffer
	r := &runtime{
		ctx:    context.Background(),
		stdout: &out,
		json:   true,
		cfg: config.Config{Remote: crawlremote.Config{
			Mode:     crawlremote.ModeCloud,
			Endpoint: "https://remote.example.test",
			Archive:  "discrawl/openclaw",
		}},
		newRemote: func(config.Config) (remoteArchiveClient, error) {
			return fakeRemoteClient{}, nil
		},
	}
	if err := r.runRemoteArchives(); err != nil {
		t.Fatalf("archives: %v", err)
	}
	if !strings.Contains(out.String(), "discrawl/openclaw") {
		t.Fatalf("archives output = %s", out.String())
	}
	out.Reset()
	if err := r.runRemoteWhoami(nil); err != nil {
		t.Fatalf("whoami: %v", err)
	}
	if !strings.Contains(out.String(), "alice") {
		t.Fatalf("whoami output = %s", out.String())
	}
}

func TestRunRemoteDispatchesConfiguredCommands(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.Remote.Mode = crawlremote.ModeCloud
	cfg.Remote.Endpoint = "https://remote.example.test"
	cfg.Remote.Archive = "discrawl/openclaw"
	if err := config.Write(cfgPath, cfg); err != nil {
		t.Fatalf("write config: %v", err)
	}

	var out bytes.Buffer
	r := &runtime{
		ctx:        context.Background(),
		configPath: cfgPath,
		stdout:     &out,
		json:       true,
		newRemote: func(config.Config) (remoteArchiveClient, error) {
			return fakeRemoteClient{}, nil
		},
	}
	if err := r.runRemote([]string{"archives"}); err != nil {
		t.Fatalf("remote archives: %v", err)
	}
	if !strings.Contains(out.String(), "discrawl/openclaw") {
		t.Fatalf("archives output = %s", out.String())
	}
	out.Reset()
	if err := r.runRemote([]string{"whoami"}); err != nil {
		t.Fatalf("remote whoami: %v", err)
	}
	if !strings.Contains(out.String(), "alice") {
		t.Fatalf("whoami output = %s", out.String())
	}
	if err := r.runRemote([]string{"unknown"}); err == nil || !strings.Contains(err.Error(), "unknown remote subcommand") {
		t.Fatalf("unknown err = %v", err)
	}
	out.Reset()
	if err := r.runRemote(nil); err != nil {
		t.Fatalf("remote help: %v", err)
	}
	if !strings.Contains(out.String(), "discrawl remote") {
		t.Fatalf("help output = %s", out.String())
	}
	if err := r.runRemote([]string{"status", "extra"}); err == nil || !strings.Contains(err.Error(), "takes no arguments") {
		t.Fatalf("status args err = %v", err)
	}
	if err := r.runCloud([]string{"unknown"}); err == nil || !strings.Contains(err.Error(), "unknown cloud subcommand") {
		t.Fatalf("cloud unknown err = %v", err)
	}
	out.Reset()
	if err := r.runCloud(nil); err != nil {
		t.Fatalf("cloud help err = %v", err)
	}
	if !strings.Contains(out.String(), "discrawl cloud") {
		t.Fatalf("cloud help output = %s", out.String())
	}
}

func TestRemoteReadCommandsValidateAndQuery(t *testing.T) {
	client := &queryRemoteClient{}
	r := &runtime{
		ctx: context.Background(),
		cfg: config.Config{Remote: crawlremote.Config{
			Mode:     crawlremote.ModeCloud,
			Endpoint: "https://remote.example.test",
			Archive:  "discrawl/openclaw",
		}},
		newRemote: func(config.Config) (remoteArchiveClient, error) {
			return client, nil
		},
	}

	for _, tc := range []struct {
		name string
		run  func() error
		want string
	}{
		{name: "search dm", run: func() error {
			_, err := r.runRemoteSearch(store.SearchOptions{Query: "hello"}, "fts", true)
			return err
		}, want: "--dm"},
		{name: "search semantic", run: func() error {
			_, err := r.runRemoteSearch(store.SearchOptions{Query: "hello"}, "semantic", false)
			return err
		}, want: "fts mode"},
		{name: "search author", run: func() error {
			_, err := r.runRemoteSearch(store.SearchOptions{Query: "hello", Author: "alice"}, "fts", false)
			return err
		}, want: "--author"},
		{name: "messages unsupported window", run: func() error {
			_, err := r.runRemoteMessages(store.MessageListOptions{Since: time.Now()}, false)
			return err
		}, want: "currently supports"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}

	results, err := r.runRemoteSearch(store.SearchOptions{Query: "hello", GuildIDs: []string{"g1"}, Channel: "c1", Limit: 3}, "fts", false)
	if err != nil {
		t.Fatalf("remote search: %v", err)
	}
	if len(results) != 1 || results[0].Content != "hello from remote" || results[0].CreatedAt.IsZero() {
		t.Fatalf("search results = %#v", results)
	}
	if client.last.Name != "discrawl.messages.search" || client.last.Limit != 3 || client.last.Args["guild_id"] != "g1" {
		t.Fatalf("search request = %#v", client.last)
	}

	messages, err := r.runRemoteMessages(store.MessageListOptions{Channel: "c1"}, false)
	if err != nil {
		t.Fatalf("remote messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Source != "remote" || messages[0].Content != "hello from remote" {
		t.Fatalf("messages = %#v", messages)
	}
	if client.last.Name != "discrawl.messages.list" || client.last.Limit != 500 {
		t.Fatalf("messages request = %#v", client.last)
	}
}

func TestRemoteLoginValidationAndClientErrors(t *testing.T) {
	r := &runtime{
		ctx:        context.Background(),
		configPath: filepath.Join(t.TempDir(), "missing.toml"),
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
	}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "too many args", args: []string{"one", "two"}, want: "at most one"},
		{name: "invalid timeout", args: []string{"--endpoint", "https://remote.example.test", "--timeout", "0s"}, want: "invalid --timeout"},
		{name: "invalid poll", args: []string{"--endpoint", "https://remote.example.test", "--poll-interval", "0s"}, want: "invalid --poll-interval"},
		{name: "missing endpoint", args: nil, want: "requires --endpoint"},
		{name: "empty github token env", args: []string{"--endpoint", "https://remote.example.test", "--github-token-env", "DISCRAWL_EMPTY_GITHUB_TOKEN"}, want: "DISCRAWL_EMPTY_GITHUB_TOKEN is empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "empty github token env" {
				t.Setenv("DISCRAWL_EMPTY_GITHUB_TOKEN", "")
			}
			err := r.runRemoteLogin(tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}

	cfg := config.Default()
	cfg.Remote.Auth.TokenSource = "none"
	cfg.Remote.Endpoint = ""
	if _, err := (&runtime{cfg: cfg}).remoteClient(false); err == nil || !strings.Contains(err.Error(), "remote.endpoint") {
		t.Fatalf("missing endpoint err = %v", err)
	}
	cfg.Remote.Endpoint = "https://remote.example.test"
	if _, err := (&runtime{cfg: cfg}).remoteClient(true); err == nil || !strings.Contains(err.Error(), "remote.archive") {
		t.Fatalf("missing archive err = %v", err)
	}
	cfg.Remote.Archive = "discrawl/openclaw"
	if _, err := (&runtime{cfg: cfg}).remoteClient(true); err == nil || !strings.Contains(err.Error(), "remote token") {
		t.Fatalf("missing token err = %v", err)
	}

	loginRuntime := &runtime{configPath: filepath.Join(t.TempDir(), "config.toml"), stdout: &bytes.Buffer{}}
	if err := loginRuntime.finishRemoteLogin(config.Default(), "github-oauth", crawlremote.LoginPollResult{Status: "pending"}); err == nil || !strings.Contains(err.Error(), "pending") {
		t.Fatalf("pending login err = %v", err)
	}
	if err := loginRuntime.finishRemoteLogin(config.Default(), "github-oauth", crawlremote.LoginPollResult{Status: "complete"}); err == nil || !strings.Contains(err.Error(), "without token") {
		t.Fatalf("empty token err = %v", err)
	}
}

func TestRuntimeWithConfigFallbacks(t *testing.T) {
	missing := &runtime{configPath: filepath.Join(t.TempDir(), "missing.toml")}
	called := false
	if err := missing.withConfig(func() error {
		called = true
		if missing.cfg.DBPath == "" {
			t.Fatalf("default config not normalized")
		}
		return nil
	}); err != nil || !called {
		t.Fatalf("missing config fallback err=%v called=%v", err, called)
	}

	badPath := filepath.Join(t.TempDir(), "bad.toml")
	if err := os.WriteFile(badPath, []byte("[bad"), 0o600); err != nil {
		t.Fatalf("write bad config: %v", err)
	}
	if err := (&runtime{configPath: badPath}).withConfig(func() error { return nil }); err == nil {
		t.Fatalf("expected invalid config error")
	}
}

func TestPollRemoteLoginStates(t *testing.T) {
	t.Run("pending then complete", func(t *testing.T) {
		polls := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			polls++
			w.Header().Set("Content-Type", "application/json")
			if polls == 1 {
				_ = json.NewEncoder(w).Encode(crawlremote.LoginPollResult{Status: "pending"})
				return
			}
			_ = json.NewEncoder(w).Encode(crawlremote.LoginPollResult{Status: "complete", Token: "session-token", Login: "alice"})
		}))
		defer server.Close()

		client, err := crawlremote.NewClientFromConfig(crawlremote.Config{Endpoint: server.URL}, crawlremote.Options{})
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		result, err := pollRemoteLogin(context.Background(), client, "login-1", "secret", time.Second, time.Millisecond)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		if result.Token != "session-token" || polls != 2 {
			t.Fatalf("result=%#v polls=%d", result, polls)
		}
	})

	for _, tc := range []struct {
		name   string
		result crawlremote.LoginPollResult
		want   string
	}{
		{name: "complete missing token", result: crawlremote.LoginPollResult{Status: "complete"}, want: "without token"},
		{name: "error with message", result: crawlremote.LoginPollResult{Status: "error", Error: "denied"}, want: "denied"},
		{name: "error without message", result: crawlremote.LoginPollResult{Status: "error"}, want: "remote login failed"},
		{name: "unknown status", result: crawlremote.LoginPollResult{Status: "confused"}, want: `status "confused"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(tc.result)
			}))
			defer server.Close()

			client, err := crawlremote.NewClientFromConfig(crawlremote.Config{Endpoint: server.URL}, crawlremote.Options{})
			if err != nil {
				t.Fatalf("client: %v", err)
			}
			_, err = pollRemoteLogin(context.Background(), client, "login-1", "secret", time.Second, time.Millisecond)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSendIngestRowsBatchesAndFinalizes(t *testing.T) {
	var requests []crawlremote.IngestRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body crawlremote.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requests = append(requests, body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{RowsAccepted: int64(len(body.Rows)), Complete: body.Final})
	}))
	defer server.Close()

	client, err := crawlremote.NewClientFromConfig(crawlremote.Config{Endpoint: server.URL}, crawlremote.Options{
		TokenProvider: crawlremote.StaticToken("publish-token"),
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	rows := make([][]any, discrawlCloudBatchSize+1)
	for i := range rows {
		rows[i] = []any{i}
	}
	accepted, err := sendIngestRows(context.Background(), client, "discrawl/openclaw", crawlremote.IngestManifest{App: "discrawl"}, "messages", []string{"id"}, rows, true)
	if err != nil {
		t.Fatalf("send ingest: %v", err)
	}
	if accepted != int64(len(rows)) || len(requests) != 2 {
		t.Fatalf("accepted=%d requests=%d", accepted, len(requests))
	}
	if requests[0].Cursor != "" || requests[0].Final || len(requests[0].Rows) != discrawlCloudBatchSize {
		t.Fatalf("first request = %#v", requests[0])
	}
	if requests[1].Cursor != "250" || !requests[1].Final || len(requests[1].Rows) != 1 {
		t.Fatalf("second request = %#v", requests[1])
	}
}
