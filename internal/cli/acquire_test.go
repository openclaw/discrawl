package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/acquisition"
)

func TestAcquireHelpDoesNotOpenArchive(t *testing.T) {
	root := t.TempDir()
	config := filepath.Join(root, "absent", "config.toml")
	var out bytes.Buffer
	if err := Run(t.Context(), []string{"--config", config, "acquire", "--help"}, &out, &out); err != nil || !strings.Contains(out.String(), "PostgreSQL-owned") {
		t.Fatal("acquire help failed", err)
	}
	if _, err := os.Stat(filepath.Dir(config)); !os.IsNotExist(err) {
		t.Fatal("acquire help initialized local state")
	}
}

// The CrawlRemote integration harness supplies a real PostgreSQL-backed API.
// Only Discord is a local HTTP fixture; Run is the normal production CLI path.
func TestAcquirePostgresHTTP(t *testing.T) {
	origin := os.Getenv("CRAWL_TEST_DISCORD_ACQUISITION_ORIGIN")
	if origin == "" {
		t.Skip("requires the CrawlRemote PostgreSQL integration harness")
	}
	token := os.Getenv("CRAWL_TEST_DISCORD_ACQUISITION_TOKEN")
	if token == "" {
		t.Fatal("missing isolated publisher token")
	}
	const channel = "9007199254740993"
	at := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	id := func(h int) string {
		return strconv.FormatUint(uint64(at.Add(time.Duration(h)*time.Hour).UnixMilli()-1420070400000)<<22, 10)
	}
	newest, protected := id(3), id(2)
	var pages atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bot fixture-bot" {
			t.Error("native bot credential missing")
			http.Error(w, "unauthorized", 401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/channels/" + channel:
			fmt.Fprintf(w, `{"id":%q,"guild_id":"100","type":0}`, channel)
		case "/users/@me":
			ioString(w, `{"id":"bot"}`)
		case "/guilds/100":
			ioString(w, `{"id":"100","owner_id":"bot","roles":[]}`)
		case "/guilds/100/members/bot":
			ioString(w, `{"user":{"id":"bot"},"roles":[]}`)
		case "/channels/" + channel + "/messages":
			pages.Add(1)
			if r.URL.Query().Get("limit") != "100" || r.URL.Query().Get("after") != "" {
				t.Error("native pagination changed")
			}
			if r.URL.Query().Get("before") == protected {
				ioString(w, `[]`)
				return
			}
			fmt.Fprintf(w, `[{"id":%q,"channel_id":%q,"timestamp":"2026-11-19T03:00:00Z","content":"new\u0000text","author":{"id":"42","username":"fixture"},"flags":32768,"components":[{"type":10,"content":"component text"}]},{"id":%q,"channel_id":%q,"timestamp":"2026-11-19T02:00:00Z","edited_timestamp":"2026-11-19T04:00:00Z","content":"newer retained text"}]`, newest, channel, protected, channel)
		default:
			t.Errorf("unexpected native route %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer provider.Close()
	oldChannels, oldUsers, oldGuilds := discordgo.EndpointChannels, discordgo.EndpointUsers, discordgo.EndpointGuilds
	discordgo.EndpointChannels = provider.URL + "/channels/"
	discordgo.EndpointUsers = provider.URL + "/users/"
	discordgo.EndpointGuilds = provider.URL + "/guilds/"
	defer func() {
		discordgo.EndpointChannels = oldChannels
		discordgo.EndpointUsers = oldUsers
		discordgo.EndpointGuilds = oldGuilds
	}()
	t.Setenv("DISCRAWL_TEST_PUBLISHER", token)
	t.Setenv("DISCRAWL_TEST_BOT", "fixture-bot")
	root := t.TempDir()
	config := filepath.Join(root, "must-remain-absent", "config.toml")
	args := []string{"--config", config, "acquire", "--api-url", origin, "--source", "acquisition", "--guild", "100", "--channel", channel, "--publisher-token-env", "DISCRAWL_TEST_PUBLISHER", "--discord-token-env", "DISCRAWL_TEST_BOT", "--timeout", "20s"}
	invoke := func(extra ...string) acquisition.Result {
		t.Helper()
		var out, stderr bytes.Buffer
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
		defer cancel()
		if err := Run(ctx, append(append([]string{}, args...), extra...), &out, &stderr); err != nil {
			t.Fatalf("CLI acquisition: %v (%d diagnostic bytes)", err, stderr.Len())
		}
		var result acquisition.Result
		if json.Unmarshal(out.Bytes(), &result) != nil {
			t.Fatal("invalid CLI result")
		}
		return result
	}
	first := invoke("--since", "2026-11-19T00:00:00Z", "--horizon", "2026-11-20T00:00:00Z", "--max-pages", "1")
	if first.Status != "bounded" || first.Pages != 1 {
		t.Fatalf("first bounded collection: %+v", first)
	}
	second := invoke("--max-pages", "10")
	if second.Status != "complete" || second.RunID != first.RunID || second.Through.Format(time.RFC3339) != "2026-11-20T00:00:00Z" || pages.Load() != 2 {
		t.Fatalf("resume: %+v native pages=%d", second, pages.Load())
	}
	if _, err := os.Stat(filepath.Dir(config)); !os.IsNotExist(err) {
		t.Fatal("API collector created local archive/config")
	}
}

func ioString(w http.ResponseWriter, s string) { _, _ = w.Write([]byte(s)) }
