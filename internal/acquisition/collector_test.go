package acquisition_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/acquisition"
)

type pageProvider struct {
	channel *discordgo.Channel
	pages   map[string][]*discordgo.Message
	allowed bool
}

func (p *pageProvider) Channel(context.Context, string) (*discordgo.Channel, error) {
	return p.channel, nil
}

func (p *pageProvider) CanReadHistory(context.Context, *discordgo.Channel) (bool, error) {
	return p.allowed, nil
}

func (p *pageProvider) ChannelMessages(_ context.Context, _ string, limit int, before, after string) ([]*discordgo.Message, error) {
	if limit != 100 || after != "" {
		panic("collector changed descending provider pagination")
	}
	return p.pages[before], nil
}

func snowflakeAt(at time.Time) string {
	return strconv.FormatUint(uint64(at.UnixMilli()-1420070400000)<<22, 10)
}
func textCell(s string) remote.ChangeValue { return remote.ChangeValue{Kind: "text", Value: &s} }
func retainedMessage(id string, at time.Time) remote.Change {
	row := []remote.ChangeValue{textCell(id), textCell("100"), textCell("200"), textCell("42"), {Kind: "integer", Value: new("0")}, textCell(at.Format(time.RFC3339Nano)), {Kind: "null"}, {Kind: "null"}, textCell("retained"), textCell("retained normalized"), {Kind: "null"}, {Kind: "integer", Value: new("0")}, {Kind: "integer", Value: new("0")}, textCell(`{"original":"full raw evidence"}`), textCell(at.Format(time.RFC3339Nano)), textCell(`[{"text":"enriched"}]`), {Kind: "integer", Value: new("9")}}
	return remote.Change{Table: "messages", Operation: "upsert", Key: []remote.ChangeValue{textCell(id)}, Row: row}
}

func TestCollectBoundedRestartPreservesRetainedRowsAndLostFinalACK(t *testing.T) {
	since := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	horizon := since.Add(10 * time.Hour)
	newest, old := snowflakeAt(since.Add(3*time.Hour)), snowflakeAt(since.Add(2*time.Hour))
	retained := retainedMessage(old, since.Add(2*time.Hour))
	state := acquisition.State{Profile: "discord-acquisition/v1", Sequence: 10, CompletedSequence: 10, Before: snowflakeAt(horizon)}
	var mu sync.Mutex
	var pages []acquisition.Page
	begins := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer publisher" {
			t.Error("publisher credential changed")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/apps/discrawl/archives/fixture/acquisition/state":
			if r.URL.Query().Get("channel") != "200" {
				t.Error("state channel lost")
			}
			_ = json.NewEncoder(w).Encode(state)
		case "/v1/apps/discrawl/archives/fixture/acquisition/runs":
			var run acquisition.Run
			if err := json.NewDecoder(r.Body).Decode(&run); err != nil {
				t.Error(err)
				return
			}
			if run.ExpectedRevision != state.Revision || run.GuildID != "100" || run.ChannelID != "200" || !run.Horizon.Equal(horizon) || !run.Since.Equal(since) {
				t.Error("run selection changed")
			}
			begins++
			state.ActiveRun = &run
			_ = json.NewEncoder(w).Encode(map[string]string{"runId": run.ID, "status": "accepted"})
		case "/v1/apps/discrawl/archives/fixture/acquisition/messages":
			var req struct {
				RunID            string   `json:"runId"`
				ExpectedRevision int64    `json:"expectedRevision"`
				IDs              []string `json:"ids"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				return
			}
			if req.RunID != state.ActiveRun.ID || req.ExpectedRevision != state.Revision || !reflect.DeepEqual(req.IDs, []string{newest, old}) {
				t.Error("retained lookup not scoped to exact active page")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": state.Revision, "sequence": state.Sequence, "messages": []remote.Change{retained}})
		case "/v1/apps/discrawl/archives/fixture/acquisition/pages":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			var page acquisition.Page
			if err = json.Unmarshal(body, &page); err != nil {
				t.Error(err)
				return
			}
			if page.ExpectedRevision != state.Revision || page.Before != state.Before || page.RunID != state.ActiveRun.ID {
				t.Error("page did not resume pinned state")
			}
			pages = append(pages, page)
			state.Revision++
			state.Sequence++
			state.Before = page.NextBefore
			if page.Terminal != "" {
				state.CompletedSequence = state.Sequence
				state.CompletedThrough = &horizon
				state.ActiveRun = nil
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			sum := sha256.Sum256(body)
			_ = json.NewEncoder(w).Encode(acquisition.Receipt{RunID: page.RunID, PageID: page.ID, SHA256: hex.EncodeToString(sum[:]), Revision: state.Revision, Sequence: state.Sequence})
		default:
			t.Error("unexpected API route", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	api, err := acquisition.NewClient(server.URL, "fixture", "publisher")
	if err != nil {
		t.Fatal(err)
	}
	provider := &pageProvider{channel: &discordgo.Channel{ID: "200", GuildID: "100"}, allowed: true, pages: map[string][]*discordgo.Message{
		snowflakeAt(horizon): {{ID: newest, ChannelID: "200", Timestamp: since.Add(3 * time.Hour), Content: "new\x00content", Author: &discordgo.User{ID: "43"}}, {ID: old, ChannelID: "200", Timestamp: since.Add(2 * time.Hour), Content: "retained", Author: &discordgo.User{ID: "42"}}},
		old:                  {},
	}}
	first, err := acquisition.Collect(t.Context(), api, provider, acquisition.Options{Guild: "100", Channel: "200", Since: since, Horizon: horizon, MaxPages: 1})
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "bounded" || first.Pages != 1 || first.Messages != 2 {
		t.Fatalf("first result: %+v", first)
	}
	resumed, err := acquisition.Collect(t.Context(), api, provider, acquisition.Options{Guild: "100", Channel: "200", MaxPages: 2})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Status != "complete" || resumed.RunID != first.RunID || !resumed.Through.Equal(horizon) {
		t.Fatalf("lost-ACK resume: %+v", resumed)
	}
	mu.Lock()
	defer mu.Unlock()
	if begins != 1 || len(pages) != 2 || pages[1].Terminal != "empty" {
		t.Fatalf("unexpected restart: begins=%d pages=%+v", begins, pages)
	}
	if !reflect.DeepEqual(pages[0].Changes[1], retained) {
		t.Fatal("retained enrichment/raw fields changed")
	}
	if *pages[0].Changes[0].Row[8].Value != "new\x00content" {
		t.Fatal("new full message was lost")
	}
}

func TestCollectRefusesInvalidProviderPagesBeforeWrite(t *testing.T) {
	since := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	horizon := since.Add(time.Hour)
	id := snowflakeAt(since.Add(30 * time.Minute))
	good := func() *discordgo.Message {
		return &discordgo.Message{ID: id, ChannelID: "200", Timestamp: since.Add(30 * time.Minute), Content: "body"}
	}
	cases := []struct {
		name     string
		messages []*discordgo.Message
		allowed  bool
	}{
		{"permission denied", nil, false},
		{"nil message", []*discordgo.Message{nil}, true},
		{"wrong channel", []*discordgo.Message{{ID: id, ChannelID: "201"}}, true},
		{"wrong guild", []*discordgo.Message{{ID: id, ChannelID: "200", GuildID: "101"}}, true},
		{"invalid snowflake", []*discordgo.Message{{ID: "01", ChannelID: "200"}}, true},
		{"cursor repeated", []*discordgo.Message{{ID: snowflakeAt(horizon), ChannelID: "200", Timestamp: horizon}}, true},
		{"creation time differs", []*discordgo.Message{{ID: id, ChannelID: "200", Timestamp: since}}, true},
		{"out of order", []*discordgo.Message{good(), good()}, true},
		{"oversized page", make([]*discordgo.Message, 101), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := acquisition.Run{ID: "active", GuildID: "100", ChannelID: "200", Since: since, Horizon: horizon}
			writes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes++
					t.Error("unqualified provider page reached write API")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(acquisition.State{Profile: "discord-acquisition/v1", Sequence: 10, CompletedSequence: 10, ActiveRun: &run, Before: snowflakeAt(horizon)})
			}))
			defer server.Close()
			api, err := acquisition.NewClient(server.URL, "fixture", "publisher")
			if err != nil {
				t.Fatal(err)
			}
			provider := &pageProvider{channel: &discordgo.Channel{ID: "200", GuildID: "100"}, allowed: tc.allowed, pages: map[string][]*discordgo.Message{snowflakeAt(horizon): tc.messages}}
			if _, err := acquisition.Collect(t.Context(), api, provider, acquisition.Options{Guild: "100", Channel: "200", MaxPages: 1}); err == nil || writes != 0 {
				t.Fatal("invalid provider evidence accepted")
			}
		})
	}
}
