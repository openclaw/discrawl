package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bwmarrin/discordgo"
)

func TestAcquireServiceStopsAtAPIWithoutLocalFallback(t *testing.T) {
	unit, err := os.ReadFile("../../operations/discrawl-acquire@.service")
	if err != nil {
		t.Fatal(err)
	}
	var command []string
	for line := range strings.SplitSeq(string(unit), "\n") {
		if value, ok := strings.CutPrefix(line, "ExecStart="); ok {
			command = strings.Fields(value)
		}
	}
	if len(command) < 7 || strings.Join(command[:6], " ") != "/usr/bin/flock --no-fork --nonblock --conflict-exit-code=75 /var/lib/discrawl/%i/acquisition.lock /usr/local/bin/discrawl" {
		t.Fatal("service must preserve the source acquisition lock")
	}
	for _, arg := range command[6:] {
		if arg == "sync" || arg == "delivery" || arg == "--since" || arg == "--horizon" || arg == "--config" || strings.Contains(arg, "sqlite") {
			t.Fatalf("service bypasses API-owned progress: %s", arg)
		}
	}
	for _, boundary := range []string{"EnvironmentFile=/etc/discrawl/acquire-%i.env", "EnvironmentFile=/etc/discrawl/provider-%i.env", "--max-pages 20 --timeout 90s", "TimeoutStartSec=120s", "TimeoutStopSec=30s", "KillMode=control-group", "MemoryMax=512M"} {
		if !strings.Contains(string(unit), boundary) {
			t.Fatalf("missing service boundary: %s", boundary)
		}
	}
	root := t.TempDir()
	config := filepath.Join(root, "invalid.toml")
	if err := os.WriteFile(config, []byte("not valid TOML ["), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DISCRAWL_CONFIG", config)
	t.Setenv("XDG_DATA_HOME", filepath.Join(root, "data"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(root, "cache"))
	t.Setenv("DISCRAWL_PUBLISHER_TOKEN", "fixture-publisher")
	t.Setenv("DISCORD_BOT_TOKEN", "fixture-bot")
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("API failure or missing checkpoint must not query Discord")
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer provider.Close()
	oldChannels, oldUsers, oldGuilds := discordgo.EndpointChannels, discordgo.EndpointUsers, discordgo.EndpointGuilds
	discordgo.EndpointChannels, discordgo.EndpointUsers, discordgo.EndpointGuilds = provider.URL+"/channels/", provider.URL+"/users/", provider.URL+"/guilds/"
	defer func() {
		discordgo.EndpointChannels, discordgo.EndpointUsers, discordgo.EndpointGuilds = oldChannels, oldUsers, oldGuilds
	}()
	for _, tc := range []struct {
		name, response, want string
		status               int
	}{
		{"API failure", "", "acquisition API returned HTTP 503", http.StatusServiceUnavailable},
		{"missing checkpoint", `{"profile":"discord-acquisition/v1","revision":0,"sequence":1,"completedSequence":1,"completedThrough":null,"before":""}`, "first acquisition needs explicit UTC millisecond since before horizon", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method != http.MethodGet || r.URL.Path != "/v1/apps/discrawl/archives/fixture/acquisition/state" || r.URL.RawQuery != "channel=200" || r.Header.Get("Authorization") != "Bearer fixture-publisher" {
					t.Error("service lost its source, channel or publisher credential")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.response))
			}))
			defer api.Close()
			vars := map[string]string{"DISCRAWL_ORIGIN": api.URL, "DISCRAWL_SOURCE": "fixture", "DISCRAWL_GUILD_ID": "100", "DISCRAWL_CHANNEL_ID": "200"}
			args := append([]string(nil), command[6:]...)
			for i, arg := range args {
				args[i] = os.Expand(arg, func(key string) string { return vars[key] })
			}
			var out bytes.Buffer
			err := Run(t.Context(), args, &out, &out)
			if err == nil || !strings.Contains(err.Error(), tc.want) || requests.Load() != 1 {
				t.Fatalf("service did not stop at API boundary: requests=%d err=%v", requests.Load(), err)
			}
		})
	}
	for _, path := range []string{"data", "cache"} {
		if _, err := os.Stat(filepath.Join(root, path)); !os.IsNotExist(err) {
			t.Fatalf("service entered local archive runtime: %s: %v", path, err)
		}
	}
}
