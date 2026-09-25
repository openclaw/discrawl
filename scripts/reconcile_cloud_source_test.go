//go:build ignore

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/openclaw/discrawl/internal/syncer"
	"github.com/stretchr/testify/require"
)

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestReconciliationEntryPointLoadsPrivatePlanAndUsesNativeDiscordClient(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "runtime.db")
	configPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(configPath, cfg))
	db, err := store.Open(context.Background(), cfg.DBPath)
	require.NoError(t, err)
	require.NoError(t, db.UpsertGuild(context.Background(), store.GuildRecord{ID: "1", Name: "fixture", RawJSON: `{}`}))
	require.NoError(t, db.Close())
	plan := syncer.ReconcilePlan{Archive: "fixture", Members: []syncer.ReconcileMember{{GuildID: "1", UserID: "3"}}}
	raw, err := json.Marshal(plan)
	require.NoError(t, err)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err = writer.Write(raw)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	t.Setenv("DISCRAWL_CLOUD_RECONCILE_PLAN", base64.StdEncoding.EncodeToString(compressed.Bytes()))
	t.Setenv("DISCRAWL_CLOUD_ARCHIVE", "fixture")
	t.Setenv("DISCRAWL_GUILD_ID", "1")
	t.Setenv("CONFIG", configPath)
	t.Setenv("DB", cfg.DBPath)
	t.Setenv("DISCORD_BOT_TOKEN", "fixture-bot-token")
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	calls := 0
	http.DefaultTransport = fixtureTransport(func(req *http.Request) (*http.Response, error) {
		if req.Header.Get("Authorization") != "Bot fixture-bot-token" {
			return nil, fmt.Errorf("unexpected fixture authorization")
		}
		calls++
		var body string
		switch {
		case strings.HasSuffix(req.URL.Path, "/guilds/1"):
			body = `{"id":"1"}`
		case strings.HasSuffix(req.URL.Path, "/guilds/1/members/3"):
			body = `{"user":{"id":"3","username":"fixture","bot":true},"roles":["7"],"joined_at":"2026-09-19T00:00:00Z"}`
		default:
			return nil, fmt.Errorf("unexpected fixture request")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})
	require.NoError(t, run())
	require.Equal(t, 2, calls)
	db, err = store.Open(context.Background(), cfg.DBPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	var count int
	require.NoError(t, db.DB().QueryRowContext(context.Background(), "select count(*) from members where guild_id='1' and user_id='3' and bot=1").Scan(&count))
	require.Equal(t, 1, count)
	t.Setenv("DISCRAWL_GUILD_ID", "other")
	require.Error(t, run())
	require.Equal(t, 2, calls)
	// No token is written to the config or source records.
	content, err := os.ReadFile(configPath)
	require.NoError(t, err)
	require.NotContains(t, string(content), "fixture-bot-token")
}
