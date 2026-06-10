package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/crawlkit/control"
	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/config"
	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/discorddesktop"
	"github.com/openclaw/discrawl/internal/media"
	"github.com/openclaw/discrawl/internal/report"
	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/openclaw/discrawl/internal/syncer"
	"github.com/zalando/go-keyring"
)

func TestHelpAndVersion(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, Run(context.Background(), []string{"help"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "discrawl")

	out.Reset()
	require.NoError(t, Run(context.Background(), []string{"--version"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "0.8.0")

	err := Run(context.Background(), []string{"bogus"}, &out, &bytes.Buffer{})
	require.Equal(t, 2, ExitCode(err))
	require.Equal(t, 1, ExitCode(context.Canceled))
	require.Equal(t, 7, ExitCode(&cliError{code: 7, err: errors.New("custom")}))
}

func TestCommandValidationEdges(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Discord.TokenSource = "none"
	require.NoError(t, config.Write(cfgPath, cfg))
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	cases := [][]string{
		{"--config", cfgPath, "--bogus"},
		{"--config", cfgPath, "search"},
		{"--config", cfgPath, "search", "--mode", "bogus", "term"},
		{"--config", cfgPath, "messages"},
		{"--config", cfgPath, "messages", "--hours", "-1", "--channel", "general"},
		{"--config", cfgPath, "messages", "--hours", "1", "--days", "1", "--channel", "general"},
		{"--config", cfgPath, "messages", "--all", "--last", "1", "--channel", "general"},
		{"--config", cfgPath, "messages", "--dm", "--sync", "--channel", "alice"},
		{"--config", cfgPath, "dms", "--hours", "-1"},
		{"--config", cfgPath, "dms", "--limit", "1", "--last", "1", "--with", "alice"},
		{"--config", cfgPath, "mentions"},
		{"--config", cfgPath, "mentions", "--days", "-1", "--target", "u1"},
		{"--config", cfgPath, "mentions", "--type", "channel", "--target", "u1"},
		{"--config", cfgPath, "digest", "--since", "-1d"},
		{"--config", cfgPath, "analytics", "wat"},
		{"--config", cfgPath, "analytics", "quiet", "extra"},
		{"--config", cfgPath, "analytics", "trends", "--weeks", "-1"},
		{"--config", cfgPath, "channels"},
		{"--config", cfgPath, "channels", "wat"},
		{"--config", cfgPath, "channels", "show"},
		{"--config", cfgPath, "status", "extra"},
		{"--config", cfgPath, "report", "extra"},
		{"--config", cfgPath, "wiretap", "extra"},
		{"--config", cfgPath, "wiretap", "--max-file-bytes", "0"},
		{"--config", cfgPath, "sync", "--source", "bogus"},
		{"--config", cfgPath, "sync", "--since", "not-time"},
		{"--config", cfgPath, "sync", "--no-update", "--update", "force"},
		{"--config", cfgPath, "cloud", "publish", "--bogus"},
		{"--config", cfgPath, "cloud", "publish", "extra"},
		{"--config", cfgPath, "cloud", "publish", "--json"},
		{"--config", cfgPath, "cloud", "publish", "--remote", "https://remote.example"},
		{"--config", cfgPath, "publish", "--remote", ""},
		{"--config", cfgPath, "subscribe"},
		{"--config", cfgPath, "update", "extra"},
		{"--config", cfgPath, "sql", "--confirm", "select 1"},
		{"--config", cfgPath, "sql", "--unsafe", "select 1"},
		{"--config", cfgPath, "members"},
		{"--config", cfgPath, "members", "wat"},
	}
	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		err := Run(ctx, args, &stdout, &stderr)
		require.Error(t, err, args)
	}
}

func TestOutputBranches(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	values := []any{
		syncRunStats{
			Source:  "both",
			Discord: &syncer.SyncStats{Guilds: 1, Channels: 2, Threads: 3, Members: 4, Messages: 5},
			Wiretap: &discorddesktop.Stats{
				Path:                  "/tmp/discord",
				FilesVisited:          1,
				FilesScanned:          2,
				FilesSkipped:          3,
				FilesUnchanged:        4,
				CacheFilesFastSkipped: 5,
				JSONObjects:           6,
				Guilds:                7,
				Channels:              8,
				Messages:              9,
				DMMessages:            10,
				DMChannels:            11,
				GuildMessages:         12,
				SkippedMessages:       13,
				SkippedChannels:       14,
				Checkpoints:           15,
				FullCache:             true,
				DryRun:                true,
			},
		},
		syncer.SyncStats{Guilds: 1, Channels: 2, Threads: 3, Members: 4, Messages: 5},
		discorddesktop.Stats{Path: "/tmp/discord", FilesVisited: 1, FullCache: true, DryRun: true},
		store.EmbeddingDrainStats{
			Processed:        3,
			Succeeded:        2,
			Failed:           1,
			Requeued:         4,
			RateLimited:      true,
			RemainingBacklog: 5,
			Provider:         "openai",
			Model:            "model",
			InputVersion:     "v1",
		},
		[]store.DirectMessageConversationRow{{
			ChannelID:      "c1",
			Name:           "Alice",
			MessageCount:   2,
			AuthorCount:    1,
			FirstMessageAt: now.Add(-time.Hour),
			LastMessageAt:  now,
		}},
		store.MemberProfile{
			Member: store.MemberRow{
				GuildID:     "g1",
				UserID:      "u1",
				Username:    "peter",
				DisplayName: "Peter",
				JoinedAt:    now,
				XHandle:     "steipete",
				GitHubLogin: "steipete",
				Website:     "https://steipete.me",
				Pronouns:    "he/him",
				Location:    "Vienna",
				Bio:         "Maintainer",
				URLs:        []string{"https://example.com"},
			},
			MessageCount:   1,
			FirstMessageAt: now.Add(-time.Hour),
			LastMessageAt:  now,
			RecentMessages: []store.MessageRow{{ChannelName: "general", CreatedAt: now, Content: "hello"}},
		},
		report.Digest{
			Since:       now.Add(-24 * time.Hour),
			Until:       now,
			WindowLabel: "1d",
			Channels: []report.ChannelDigest{{
				ChannelID:     "c1",
				ChannelName:   "general",
				Kind:          "text",
				GuildID:       "g1",
				Messages:      3,
				Replies:       1,
				ActiveAuthors: 2,
				TopPosters:    []report.RankedCount{{Name: "Peter", Count: 2}},
				TopMentions:   []report.RankedCount{{Count: 1}},
			}},
			Totals: report.DigestTotals{Messages: 3, Replies: 1, Channels: 1, ActiveAuthors: 2},
		},
		report.Quiet{
			Since: now.Add(-24 * time.Hour),
			Until: now,
			Channels: []report.QuietChannel{{
				ChannelID:   "c1",
				ChannelName: "general",
				Kind:        "text",
				LastMessage: "",
				DaysSilent:  -1,
			}},
			Totals: report.QuietTotals{Channels: 1},
		},
		report.Trends{
			Since: now.AddDate(0, 0, -14),
			Until: now,
			Weeks: 2,
			Rows: []report.TrendsRow{{
				ChannelID:   "c1",
				ChannelName: "general",
				Kind:        "text",
				GuildID:     "g1",
				Weekly: []report.WeeklyCount{
					{WeekStart: now.AddDate(0, 0, -14), Messages: 1},
					{WeekStart: now.AddDate(0, 0, -7), Messages: 2},
				},
			}},
		},
		map[string]any{"b": 2, "a": 1},
	}
	for _, value := range values {
		var out bytes.Buffer
		require.NoError(t, printHuman(&out, value))
		require.NotEmpty(t, out.String())
	}

	var plain bytes.Buffer
	require.NoError(t, printPlain(&plain, report.Quiet{Channels: []report.QuietChannel{{ChannelID: "c1", ChannelName: "general", Kind: "text", GuildID: "g1", LastMessage: "now", DaysSilent: 0}}}))
	require.NoError(t, printPlain(&plain, report.Trends{Rows: []report.TrendsRow{{GuildID: "g1", ChannelID: "c1", ChannelName: "general", Kind: "text", Weekly: []report.WeeklyCount{{WeekStart: now, Messages: 2}}}}}))
	require.Error(t, printPlain(io.Discard, struct{}{}))
	require.Error(t, printHuman(io.Discard, struct{}{}))
	require.Equal(t, "this is a profile field with a very l...", trimForTable("this is a profile field with a very long text value"))
}

func TestStatusSearchSQLAndListings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"bio":"Maintainer","github":"steipete","website":"https://steipete.me"}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "panic locked database",
		NormalizedContent: "panic locked database",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g2", Name: "Other Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c2", GuildID: "g2", Kind: "text", Name: "random", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m-other",
		GuildID:           "g2",
		ChannelID:         "c2",
		ChannelName:       "random",
		AuthorID:          "u2",
		AuthorName:        "Outside",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
		Content:           "outside default guild",
		NormalizedContent: "outside default guild",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano),
		Content:           "",
		NormalizedContent: "",
		RawJSON:           `{"author":{"username":"Peter"}}`,
	}))
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m3",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         time.Now().UTC().Add(2 * time.Second).Format(time.RFC3339Nano),
			Content:           "",
			NormalizedContent: "trace.txt stack trace line one",
			HasAttachments:    true,
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
		Mentions: []store.MentionEventRecord{{
			MessageID:  "m3",
			GuildID:    "g1",
			ChannelID:  "c1",
			AuthorID:   "u1",
			TargetType: "user",
			TargetID:   "u2",
			TargetName: "Shadow",
			EventAt:    time.Now().UTC().Add(2 * time.Second).Format(time.RFC3339Nano),
		}},
	}}))
	require.NoError(t, s.Close())

	tests := [][]string{
		{"--config", cfgPath, "status"},
		{"--config", cfgPath, "search", "panic"},
		{"--config", cfgPath, "search", "panic", "--limit", "1"},
		{"--config", cfgPath, "search", "stack"},
		{"--config", cfgPath, "search", "--include-empty", "Peter"},
		{"--config", cfgPath, "messages", "--channel", "general", "--days", "7", "--all"},
		{"--config", cfgPath, "messages", "--channel", "general", "--days", "7", "--all", "--include-empty"},
		{"--config", cfgPath, "mentions", "--target", "Shadow", "--limit", "10"},
		{"--config", cfgPath, "sql", "select count(*) as total from messages"},
		{"--config", cfgPath, "members", "list"},
		{"--config", cfgPath, "members", "search", "Maintainer"},
		{"--config", cfgPath, "members", "show", "u1"},
		{"--config", cfgPath, "channels", "list"},
		{"--config", cfgPath, "report"},
	}
	for _, args := range tests {
		var out bytes.Buffer
		require.NoError(t, Run(ctx, args, &out, &bytes.Buffer{}))
		require.NotEmpty(t, out.String())
	}

	for _, args := range [][]string{
		{"--config", cfgPath, "metadata", "--json"},
		{"--config", cfgPath, "status", "--json"},
	} {
		var out bytes.Buffer
		require.NoError(t, Run(ctx, args, &out, &bytes.Buffer{}))
		var payload map[string]any
		require.NoError(t, json.Unmarshal(out.Bytes(), &payload))
		require.NotEmpty(t, payload)
	}

	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "--json", "tui", "--limit", "5"}, &out, &bytes.Buffer{}))
	var rows []map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &rows))
	require.NotEmpty(t, rows)
	require.Equal(t, "panic locked database", rows[0]["title"])
	require.Equal(t, "discord", rows[0]["source"])
	require.Equal(t, "message", rows[0]["kind"])
	require.Equal(t, "Guild", rows[0]["scope"])
	require.Equal(t, "general", rows[0]["container"])
	require.Equal(t, "https://discord.com/channels/g1/c1/m1", rows[0]["url"])
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	require.Equal(t, before, after, "tui --json should not mutate the database")
}

func TestTUIHelpReturnsUsage(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	require.NoError(t, Run(context.Background(), []string{"tui", "--help"}, &stdout, &stderr))
	require.Contains(t, stdout.String(), "Usage of tui:")
	require.Contains(t, stdout.String(), "-limit")
	require.Contains(t, stdout.String(), "right-click")
	require.Contains(t, stdout.String(), "#              jump")
	require.Empty(t, stderr.String())
}

func TestControlStatusIncludesShareAndFileSizes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "discrawl.db")
	require.NoError(t, os.WriteFile(dbPath, []byte("db"), 0o600))
	require.NoError(t, os.WriteFile(dbPath+"-wal", []byte("wal"), 0o600))
	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Share.Remote = "https://github.com/openclaw/discrawl-share.git"
	cfg.Share.RepoPath = filepath.Join(dir, "share")
	status := store.Status{
		DBPath:       dbPath,
		MessageCount: 5,
		ChannelCount: 2,
	}

	out := controlStatus(filepath.Join(dir, "config.toml"), cfg, status, true)
	require.Equal(t, int64(2), out.DatabaseBytes)
	require.Equal(t, int64(3), out.WALBytes)
	require.Zero(t, fileSize(filepath.Join(dir, "missing.db")))
	require.NotNil(t, out.Share)
	require.True(t, out.Share.Enabled)
	require.True(t, out.Share.NeedsUpdate)
	require.Contains(t, out.Summary, "5 messages")
}

func TestFormattingAndTUISourceBranches(t *testing.T) {
	require.Equal(t, "-", formatDaysSilent(-1))
	require.Equal(t, "4", formatDaysSilent(4))
	require.Equal(t, "0", formatWindowDuration(0))
	require.Equal(t, "2d", formatWindowDuration(48*time.Hour))
	require.Equal(t, "3h", formatWindowDuration(3*time.Hour))
	require.Equal(t, "1h30m0s", formatWindowDuration(90*time.Minute))
	require.Equal(t, 6*time.Hour, mustDuration("bogus"))
	require.Equal(t, 15*time.Minute, mustDuration("15m"))

	cfg := config.Default()
	cfg.DBPath = "/tmp/discrawl.db"
	r := &runtime{cfg: cfg}
	require.Equal(t, "local", r.archiveSourceKind())
	require.Equal(t, cfg.DBPath, r.archiveSourceLocation())
	guilds, err := r.resolveTUIGuilds(false, "", "")
	require.NoError(t, err)
	require.Empty(t, guilds)

	r.cfg.DefaultGuildID = "guild-one"
	guilds, err = r.resolveTUIGuilds(false, "", "")
	require.NoError(t, err)
	require.Equal(t, []string{"guild-one"}, guilds)

	r.cfg.Share.Remote = "https://github.com/openclaw/discrawl-share.git"
	require.Equal(t, "remote", r.archiveSourceKind())
	require.Equal(t, r.cfg.Share.Remote, r.archiveSourceLocation())
}

func TestWiretapImportsDesktopDirectMessages(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	desktopPath := filepath.Join(dir, "discord")
	require.NoError(t, os.MkdirAll(filepath.Join(desktopPath, "IndexedDB"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(desktopPath, "IndexedDB", "000001.log"), []byte(`{"id":"111111111111111111","type":1,"recipients":[{"id":"222222222222222222","username":"alice","global_name":"Alice"}]}
{"id":"333333333333333333","channel_id":"111111111111111111","content":"secret DM launch plan","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222222","username":"alice","global_name":"Alice"}}`), 0o600))

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Desktop.Path = desktopPath
	cfg.Discord.TokenSource = "none"
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "wiretap"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "messages=1")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "launch"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "secret DM launch plan")
	require.Contains(t, out.String(), "@me")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "dms"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "Alice")
	require.Contains(t, out.String(), "111111111111111111")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "dms", "--with", "Alice", "--last", "1"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "secret DM launch plan")
	require.Contains(t, out.String(), "@me")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--dm", "launch"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "secret DM launch plan")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "messages", "--dm", "--channel", "Alice", "--last", "1"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "secret DM launch plan")
}

func TestDiscordTUIRowsIncludePaneMetadata(t *testing.T) {
	rows := discordTUIRows([]store.MessageRow{{
		MessageID:       "m1",
		GuildID:         "@me",
		GuildName:       "Discord Direct Messages",
		ChannelID:       "c1",
		ChannelName:     "Vincent K",
		AuthorID:        "u1",
		AuthorName:      "Peter",
		Content:         "hello from desktop",
		DisplayContent:  "hello from Vincent",
		CreatedAt:       time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC),
		ReplyToMessage:  "m0",
		HasAttachments:  true,
		AttachmentNames: "trace.txt",
		AttachmentText:  "stack trace line one",
		Pinned:          true,
	}})
	require.Len(t, rows, 1)
	require.Equal(t, "hello from Vincent", rows[0].Title)
	require.Contains(t, rows[0].Detail, "hello from Vincent")
	require.Contains(t, rows[0].Detail, "Attachments")
	require.Contains(t, rows[0].Detail, "stack trace line one")
	require.Equal(t, "hello from Vincent", rows[0].Text)
	require.Equal(t, "Direct messages", rows[0].Scope)
	require.Equal(t, "Vincent K", rows[0].Container)
	require.Contains(t, rows[0].Tags, "dm")
	require.Equal(t, "true", rows[0].Fields["attachments"])
	require.Equal(t, "trace.txt", rows[0].Fields["attachment_names"])
	require.Equal(t, "true", rows[0].Fields["pinned"])
	require.Equal(t, "m0", rows[0].Fields["reply_to"])
	require.Equal(t, "@me", rows[0].Fields["guild_id"])

	rows = discordTUIRows([]store.MessageRow{{
		MessageID: "m2",
		GuildID:   "g1",
		ChannelID: "c2",
		AuthorID:  "439223656200273932",
		Content:   "desktop-only author",
		CreatedAt: time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC),
		Source:    "discord_desktop",
	}})
	require.Equal(t, "user:439223...3932", rows[0].Author)
	require.Equal(t, "DM c2", discordContainerLabel(store.MessageRow{GuildID: "@me", ChannelID: "c2"}))
	require.Contains(t, rows[0].Tags, "discord_desktop")
}

func TestParseMessageWindow(t *testing.T) {
	rt := &runtime{now: func() time.Time {
		return time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	}}

	since, before, err := rt.parseMessageWindow(6, 0, "", "")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 4, 24, 6, 0, 0, 0, time.UTC), since)
	require.True(t, before.IsZero())

	since, before, err = rt.parseMessageWindow(0, 2, "", "")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC), since)
	require.True(t, before.IsZero())

	since, before, err = rt.parseMessageWindow(0, 0, "2026-04-20T10:00:00Z", "2026-04-21T10:00:00Z")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC), since)
	require.Equal(t, time.Date(2026, 4, 21, 10, 0, 0, 0, time.UTC), before)

	_, _, err = rt.parseMessageWindow(0, 0, "bad", "")
	require.Equal(t, 2, ExitCode(err))
	_, _, err = rt.parseMessageWindow(0, 0, "", "bad")
	require.Equal(t, 2, ExitCode(err))
}

func TestWiretapAndSearchWorkWithoutConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	home := filepath.Join(dir, "home")
	desktopPath := filepath.Join(dir, "discord")
	require.NoError(t, os.MkdirAll(filepath.Join(desktopPath, "IndexedDB"), 0o755))
	require.NoError(t, os.MkdirAll(home, 0o755))
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	require.NoError(t, os.WriteFile(filepath.Join(desktopPath, "IndexedDB", "000001.log"), []byte(`{"id":"111111111111111112","type":1,"recipients":[{"id":"222222222222222223","username":"alice","global_name":"Alice"}]}
{"id":"333333333333333334","channel_id":"111111111111111112","content":"local-only DM import","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222223","username":"alice","global_name":"Alice"}}`), 0o600))

	cfgPath := filepath.Join(dir, "missing.toml")
	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "wiretap", "--path", desktopPath}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "messages=1")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "local-only"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "local-only DM import")
	require.Contains(t, out.String(), "@me")
}

func TestSyncWiretapSourceImportsDesktopMessages(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	desktopPath := filepath.Join(dir, "discord")
	require.NoError(t, os.MkdirAll(filepath.Join(desktopPath, "IndexedDB"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(desktopPath, "IndexedDB", "000001.log"), []byte(`{"id":"111111111111111117","type":1,"recipients":[{"id":"222222222222222228","username":"alice","global_name":"Alice"}]}
{"id":"333333333333333339","channel_id":"111111111111111117","content":"sync wiretap import","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222228","username":"alice","global_name":"Alice"}}`), 0o600))

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Desktop.Path = desktopPath
	cfg.Discord.TokenSource = "none"
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "sync", "--source", "wiretap"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "dm_messages=1")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "sync wiretap"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "sync wiretap import")
	require.Contains(t, out.String(), "@me")
}

func TestParseSyncSources(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		name    string
		discord bool
		wiretap bool
	}{
		{"", "both", true, true},
		{"both", "both", true, true},
		{"key", "discord", true, false},
		{"discord", "discord", true, false},
		{"wiretap", "wiretap", false, true},
		{"key+wiretap", "both", true, true},
	} {
		sources, err := parseSyncSources(tc.raw)
		require.NoError(t, err)
		require.Equal(t, tc.name, sources.name)
		require.Equal(t, tc.discord, sources.discord)
		require.Equal(t, tc.wiretap, sources.wiretap)
	}
	_, err := parseSyncSources("nope")
	require.Error(t, err)
}

func TestFetchSyncMediaScopesWiretapToDMs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := seedCLIStore(t, filepath.Join(dir, "discrawl.db"))
	defer func() { _ = s.Close() }()
	require.NoError(t, addCLIAttachment(ctx, s, ""))
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: store.DirectMessageGuildID, Name: "Discord Direct Messages", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "dm-c1", GuildID: store.DirectMessageGuildID, Kind: "dm", Name: "Alice", RawJSON: `{}`}))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "dm1",
			GuildID:           store.DirectMessageGuildID,
			ChannelID:         "dm-c1",
			ChannelName:       "Alice",
			AuthorID:          "u2",
			AuthorName:        "Alice",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "dm attachment",
			NormalizedContent: "dm attachment private.png",
			HasAttachments:    true,
			RawJSON:           `{}`,
		},
		Attachments: []store.AttachmentRecord{{
			AttachmentID: "a-dm",
			MessageID:    "dm1",
			GuildID:      store.DirectMessageGuildID,
			ChannelID:    "dm-c1",
			AuthorID:     "u2",
			Filename:     "private.png",
			ContentType:  "image/png",
		}},
	}}))
	cfg := config.Default()
	rt := &runtime{ctx: ctx, cfg: cfg, store: s, now: time.Now}

	stats, err := rt.fetchSyncMedia(syncSources{name: "wiretap", wiretap: true}, syncer.SyncOptions{GuildIDs: []string{"g1"}}, filepath.Join(dir, "cache"), nil)
	require.NoError(t, err)
	require.Equal(t, &media.FetchStats{Attachments: 1, Skipped: 1}, stats)

	guildRows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m100"})
	require.NoError(t, err)
	require.Len(t, guildRows, 1)
	require.Empty(t, guildRows[0].FetchStatus)
	dmRows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "dm1"})
	require.NoError(t, err)
	require.Len(t, dmRows, 1)
	require.Equal(t, "no_url", dmRows[0].FetchStatus)
}

func TestFetchSyncMediaHonorsSince(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := seedCLIStore(t, filepath.Join(dir, "discrawl.db"))
	defer func() { _ = s.Close() }()
	cutoff := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{
		{
			Record: store.MessageRecord{
				ID:                "m-old",
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "general",
				AuthorID:          "u1",
				AuthorName:        "Peter",
				MessageType:       0,
				CreatedAt:         cutoff.Add(-time.Hour).Format(time.RFC3339Nano),
				Content:           "old attachment",
				NormalizedContent: "old attachment old.png",
				HasAttachments:    true,
				RawJSON:           `{}`,
			},
			Attachments: []store.AttachmentRecord{{
				AttachmentID: "a-old",
				MessageID:    "m-old",
				GuildID:      "g1",
				ChannelID:    "c1",
				AuthorID:     "u1",
				Filename:     "old.png",
				ContentType:  "image/png",
			}},
		},
		{
			Record: store.MessageRecord{
				ID:                "m-new",
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "general",
				AuthorID:          "u1",
				AuthorName:        "Peter",
				MessageType:       0,
				CreatedAt:         cutoff.Add(time.Hour).Format(time.RFC3339Nano),
				Content:           "new attachment",
				NormalizedContent: "new attachment new.png",
				HasAttachments:    true,
				RawJSON:           `{}`,
			},
			Attachments: []store.AttachmentRecord{{
				AttachmentID: "a-new",
				MessageID:    "m-new",
				GuildID:      "g1",
				ChannelID:    "c1",
				AuthorID:     "u1",
				Filename:     "new.png",
				ContentType:  "image/png",
			}},
		},
	}))
	cfg := config.Default()
	rt := &runtime{ctx: ctx, cfg: cfg, store: s, now: time.Now}

	stats, err := rt.fetchSyncMedia(syncSources{name: "discord", discord: true}, syncer.SyncOptions{GuildIDs: []string{"g1"}, Since: cutoff}, filepath.Join(dir, "cache"), nil)
	require.NoError(t, err)
	require.Equal(t, &media.FetchStats{Attachments: 1, Skipped: 1}, stats)

	oldRows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m-old"})
	require.NoError(t, err)
	require.Len(t, oldRows, 1)
	require.Empty(t, oldRows[0].FetchStatus)
	newRows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m-new"})
	require.NoError(t, err)
	require.Len(t, newRows, 1)
	require.Equal(t, "no_url", newRows[0].FetchStatus)
}

func TestReadCommandsAutoImportStaleShare(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourceDB := filepath.Join(dir, "source.db")
	source := seedCLIStore(t, sourceDB)
	defer func() { _ = source.Close() }()

	workRepo := filepath.Join(dir, "work")
	remoteRepo := filepath.Join(dir, "remote.git")
	opts := share.Options{RepoPath: workRepo, Branch: "main"}
	_, err := share.Export(ctx, source, opts)
	require.NoError(t, err)
	runGit(t, workRepo, "config", "user.name", "discrawl test")
	runGit(t, workRepo, "config", "user.email", "discrawl@example.com")
	committed, err := share.Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	runGit(t, dir, "init", "--bare", remoteRepo)
	runGit(t, workRepo, "remote", "add", "origin", remoteRepo)
	runGit(t, workRepo, "push", "-u", "origin", "main")

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "reader.db")
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "reader-share")
	cfg.Share.StaleAfter = "15m"
	cfg.Share.AutoUpdate = true
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "automatic updates work")

	reader, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	lastImport, err := reader.GetSyncState(ctx, share.LastImportSyncScope)
	require.NoError(t, err)
	require.NotEmpty(t, lastImport)
}

func TestAttachmentsCommandListsAndFetchesMedia(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	body := []byte("png-ish")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/file.png" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	dbPath := filepath.Join(dir, "discrawl.db")
	s := seedCLIStore(t, dbPath)
	require.NoError(t, addCLIAttachment(ctx, s, server.URL+"/file.png"))
	require.NoError(t, s.Close())

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.Share.Remote = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "attachments", "--all"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "file.png")
	require.Contains(t, out.String(), "image/png")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "attachments", "--author", "Peter", "--all"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "file.png")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "attachments", "fetch", "--all"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "fetched=1")

	check, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = check.Close() }()
	rows, err := check.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m100"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	path, err := media.LocalPath(cfg.CacheDir, rows[0].MediaPath)
	require.NoError(t, err)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, body, got)

	require.NoError(t, os.Remove(path))
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "attachments", "--missing", "--all"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "file.png")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "attachments", "fetch", "--missing", "--all"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "fetched=1")
	got, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestAttachmentsDMCommandsSkipShareAutoUpdate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "discrawl.db")
	s := seedCLIStore(t, dbPath)
	require.NoError(t, addCLIDMAttachment(ctx, s))
	require.NoError(t, s.Close())

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.Share.Remote = filepath.Join(dir, "missing-remote.git")
	cfg.Share.RepoPath = filepath.Join(dir, "share")
	cfg.Share.AutoUpdate = true
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "attachments", "--dm", "--all"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "private.png")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "attachments", "fetch", "--dm", "--all"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "skipped=1")
}

func TestAttachmentFlagParsing(t *testing.T) {
	t.Parallel()

	rt := &runtime{
		now: func() time.Time { return time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC) },
	}
	opts, limit, err := rt.parseAttachmentListFlags("attachments", []string{
		"--channel", "general",
		"--author=Peter",
		"--message", "m1",
		"--filename", "file",
		"--type", "image",
		"--hours", "2",
		"--before", "2026-05-15T13:00:00Z",
		"--missing",
		"--guilds", "g1,g2",
		"--all",
	}, 20)
	require.NoError(t, err)
	require.Zero(t, limit)
	require.Equal(t, []string{"g1", "g2"}, opts.GuildIDs)
	require.Equal(t, "general", opts.Channel)
	require.Equal(t, "Peter", opts.Author)
	require.Equal(t, "m1", opts.MessageID)
	require.Equal(t, "file", opts.Filename)
	require.Equal(t, "image", opts.ContentType)
	require.True(t, opts.MissingOnly)
	require.Equal(t, time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC), opts.Since)

	_, _, err = rt.parseAttachmentListFlags("attachments", []string{"--hours", "1", "--since", "2026-05-15T10:00:00Z"}, 20)
	require.Error(t, err)
	_, _, err = rt.parseAttachmentListFlags("attachments", []string{"positional"}, 20)
	require.Error(t, err)
	_, _, err = rt.parseAttachmentListFlags("attachments", []string{"--limit", "-1"}, 20)
	require.Error(t, err)
	_, _, err = rt.parseAttachmentListFlags("attachments", []string{"--since", "bad"}, 20)
	require.Error(t, err)
	_, _, err = rt.parseAttachmentListFlags("attachments", []string{"--before", "bad"}, 20)
	require.Error(t, err)
	_, _, err = rt.parseAttachmentListFlags("attachments", []string{"--dm", "--guild", "g1"}, 20)
	require.Error(t, err)

	require.Equal(t, []string{"--channel", "general", "tail"}, stripFlags([]string{"--force", "--max-bytes", "10", "--channel", "general", "tail"}, map[string]struct{}{"force": {}, "max-bytes": {}}))
	fs := flag.NewFlagSet("attachments fetch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	force := fs.Bool("force", false, "")
	maxBytes := fs.Int64("max-bytes", 0, "")
	require.NoError(t, parseKnown(fs, []string{"--force", "--max-bytes=10", "--channel", "general"}, attachmentListFlagNames()))
	require.True(t, *force)
	require.Equal(t, int64(10), *maxBytes)
}

func TestFilterMissingAttachmentMediaKeepsInvalidAndMissingRows(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	existingPath := "attachments/aa/file.png"
	fullPath, err := media.LocalPath(cacheDir, existingPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o755))
	require.NoError(t, os.WriteFile(fullPath, []byte("cached"), 0o600))

	rows := filterMissingAttachmentMedia(cacheDir, []store.AttachmentRow{
		{AttachmentID: "empty"},
		{AttachmentID: "invalid", MediaPath: "../bad"},
		{AttachmentID: "missing", MediaPath: "attachments/bb/missing.png"},
		{AttachmentID: "existing", MediaPath: existingPath},
	})
	require.Equal(t, []string{"empty", "invalid", "missing"}, attachmentIDs(rows))
}

func attachmentIDs(rows []store.AttachmentRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.AttachmentID)
	}
	return out
}

func TestReadCommandsCanDisableAutoImportWithEnv(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourceDB := filepath.Join(dir, "source.db")
	source := seedCLIStore(t, sourceDB)
	defer func() { _ = source.Close() }()

	workRepo := filepath.Join(dir, "work")
	remoteRepo := filepath.Join(dir, "remote.git")
	opts := share.Options{RepoPath: workRepo, Branch: "main"}
	_, err := share.Export(ctx, source, opts)
	require.NoError(t, err)
	runGit(t, workRepo, "config", "user.name", "discrawl test")
	runGit(t, workRepo, "config", "user.email", "discrawl@example.com")
	committed, err := share.Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	runGit(t, dir, "init", "--bare", remoteRepo)
	runGit(t, workRepo, "remote", "add", "origin", remoteRepo)
	runGit(t, workRepo, "push", "-u", "origin", "main")

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "reader.db")
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "reader-share")
	cfg.Share.StaleAfter = "15m"
	cfg.Share.AutoUpdate = true
	require.NoError(t, config.Write(cfgPath, cfg))

	t.Setenv("DISCRAWL_NO_AUTO_UPDATE", "1")
	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.NotContains(t, out.String(), "automatic updates work")

	reader, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	lastImport, err := reader.GetSyncState(ctx, share.LastImportSyncScope)
	require.NoError(t, err)
	require.Empty(t, lastImport)
}

func TestSubscribeNoMediaPersistsShareMediaOptOut(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "subscribe", "--no-import", "--no-media", "https://github.com/example/archive.git"}, &out, &bytes.Buffer{}))

	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	require.False(t, cfg.ShareMediaEnabled())
}

func TestSubscribeCloudDoesNotCreateLocalDB(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "archive", "discrawl.db")

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"subscribe-cloud",
		"--endpoint", "https://remote.example.test",
		"--archive", "openclaw/discord",
		"--db", dbPath,
	}, &out, &bytes.Buffer{}))

	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	require.Equal(t, "cloud", cfg.Remote.Mode)
	require.Equal(t, "https://remote.example.test", cfg.Remote.Endpoint)
	require.Equal(t, "openclaw/discord", cfg.Remote.Archive)
	require.Equal(t, config.DefaultRemoteTokenEnv, cfg.Remote.TokenEnv)
	require.Equal(t, "none", cfg.Discord.TokenSource)
	require.NoFileExists(t, dbPath)
	require.NoDirExists(t, filepath.Dir(dbPath))
}

func TestCloudStatusJSONUsesRemoteWithoutLocalDB(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "discrawl.db")
	tokenEnv := "DISCRAWL_TEST_REMOTE_TOKEN"
	t.Setenv(tokenEnv, "test-token")
	seen := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		seen = true
		assert.Equal(t, http.MethodGet, req.Method)
		assert.Equal(t, "Bearer test-token", req.Header.Get("Authorization"))
		assert.Equal(t, "/v1/apps/discrawl/archives/openclaw%2Fdiscord/status", req.URL.EscapedPath())
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"app": "discrawl",
			"archive": "openclaw/discord",
			"mode": "cloud",
			"last_sync_at": "2026-05-27T10:00:00Z",
			"last_ingest_at": "2026-05-27T10:05:00Z",
			"counts": [
				{"id": "guilds", "label": "Guilds", "value": 2},
				{"id": "messages", "label": "Messages", "value": 42}
			],
			"warnings": ["readonly"]
		}`)
	}))
	defer server.Close()

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Discord.TokenSource = "none"
	cfg.Remote.Mode = "cloud"
	cfg.Remote.Endpoint = server.URL
	cfg.Remote.Archive = "openclaw/discord"
	cfg.Remote.TokenEnv = tokenEnv
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "status", "--json"}, &out, &bytes.Buffer{}))
	require.True(t, seen)
	require.NoFileExists(t, dbPath)

	var status control.Status
	require.NoError(t, json.Unmarshal(out.Bytes(), &status))
	require.Equal(t, "discrawl", status.AppID)
	require.Equal(t, "current", status.State)
	require.Empty(t, status.DatabasePath)
	require.NotNil(t, status.Remote)
	require.True(t, status.Remote.Enabled)
	require.Equal(t, "cloud", status.Remote.Mode)
	require.Equal(t, server.URL, status.Remote.Endpoint)
	require.Equal(t, "openclaw/discord", status.Remote.Archive)
	require.Equal(t, "2026-05-27T10:00:00Z", status.Remote.LastSyncAt)
	require.Equal(t, "2026-05-27T10:05:00Z", status.Remote.LastIngestAt)
	require.Len(t, status.Databases, 1)
	require.Equal(t, "cloudflare-d1", status.Databases[0].Kind)
	require.Equal(t, "openclaw/discord", status.Databases[0].Archive)
	require.Equal(t, int64(42), status.Counts[1].Value)
	require.Equal(t, []string{"readonly"}, status.Warnings)
}

func TestCloudSearchAndMessagesUseRemoteWithoutLocalDB(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "discrawl.db")
	tokenEnv := "DISCRAWL_TEST_REMOTE_TOKEN"
	t.Setenv(tokenEnv, "test-token")
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, http.MethodPost, req.Method)
		assert.Equal(t, "Bearer test-token", req.Header.Get("Authorization"))
		assert.Equal(t, "/v1/apps/discrawl/archives/openclaw%2Fdiscord/query", req.URL.EscapedPath())
		var body crawlremote.QueryRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		seen[body.Name] = true
		assert.Equal(t, "openclaw/discord", body.Archive)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(crawlremote.QueryResult{
			Values: []map[string]any{{
				"message_id":      "m1",
				"guild_id":        "g1",
				"channel_id":      "c1",
				"channel_name":    "general",
				"author_id":       "u1",
				"author_username": "Alice",
				"content":         "worker-backed message",
				"created_at":      "2026-05-27T17:00:00Z",
			}},
			Stats: crawlremote.QueryStats{ServedBy: "d1"},
		})
	}))
	defer server.Close()

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Discord.TokenSource = "none"
	cfg.Remote.Mode = "cloud"
	cfg.Remote.Endpoint = server.URL
	cfg.Remote.Archive = "openclaw/discord"
	cfg.Remote.TokenEnv = tokenEnv
	require.NoError(t, config.Write(cfgPath, cfg))

	var searchOut bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "--json", "search", "worker"}, &searchOut, &bytes.Buffer{}))
	require.Contains(t, searchOut.String(), "worker-backed message")
	require.True(t, seen["discrawl.messages.search"])
	require.NoFileExists(t, dbPath)

	var messagesOut bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "--json", "messages", "--channel", "c1"}, &messagesOut, &bytes.Buffer{}))
	require.Contains(t, messagesOut.String(), "worker-backed message")
	require.True(t, seen["discrawl.messages.list"])
	require.NoFileExists(t, dbPath)
}

func TestRemoteLoginStoresKeyringToken(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	keyring.MockInit()

	var pollSecretHash string
	var pollSecret string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/v1/auth/github/start":
			var body crawlremote.LoginStartRequest
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			pollSecretHash = body.PollSecretHash
			_ = json.NewEncoder(w).Encode(crawlremote.LoginStartResult{LoginID: "login-1", URL: server.URL + "/authorize"})
		case "/v1/auth/github/poll":
			var body crawlremote.LoginPollRequest
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			assert.Equal(t, "login-1", body.LoginID)
			pollSecret = body.PollSecret
			_ = json.NewEncoder(w).Encode(crawlremote.LoginPollResult{Status: "complete", Token: "session-token", Org: "openclaw", Login: "alice"})
		case "/v1/whoami":
			assert.Equal(t, "Bearer session-token", req.Header.Get("Authorization"))
			_ = json.NewEncoder(w).Encode(crawlremote.Identity{Owner: "openclaw", Org: "openclaw", Login: "alice", Auth: "github"})
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	cfgPath := filepath.Join(dir, "config.toml")
	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"--json",
		"remote", "login",
		"--endpoint", server.URL,
		"--no-browser",
		"--timeout", "1s",
		"--poll-interval", "1ms",
	}, &out, &bytes.Buffer{}))
	require.NotEmpty(t, pollSecretHash)
	require.NotEmpty(t, pollSecret)
	require.Equal(t, pollSecretHash, crawlremote.LoginPollSecretHash(pollSecret))

	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	require.Equal(t, "keyring", cfg.Remote.Auth.TokenSource)
	require.NotEmpty(t, cfg.Remote.Auth.KeyringService)
	require.NotEmpty(t, cfg.Remote.Auth.KeyringAccount)
	stored, err := keyring.Get(cfg.Remote.Auth.KeyringService, cfg.Remote.Auth.KeyringAccount)
	require.NoError(t, err)
	require.Equal(t, "session-token", stored)

	var whoami bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "--json", "whoami"}, &whoami, &bytes.Buffer{}))
	require.Contains(t, whoami.String(), `"login": "alice"`)
}

func TestRemoteLoginWithGitHubTokenEnvStoresKeyringToken(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	keyring.MockInit()
	t.Setenv("DISCRAWL_TEST_GITHUB_TOKEN", "github-token")

	var sawToken string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/v1/auth/github/token":
			var body crawlremote.GitHubTokenLoginRequest
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			sawToken = body.Token
			_ = json.NewEncoder(w).Encode(crawlremote.LoginPollResult{Status: "complete", Token: "session-token", Org: "openclaw", Login: "alice"})
		case "/v1/whoami":
			assert.Equal(t, "Bearer session-token", req.Header.Get("Authorization"))
			_ = json.NewEncoder(w).Encode(crawlremote.Identity{Owner: "openclaw", Org: "openclaw", Login: "alice", Auth: "github"})
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	cfgPath := filepath.Join(dir, "config.toml")
	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"--json",
		"remote", "login",
		"--endpoint", server.URL,
		"--github-token-env", "DISCRAWL_TEST_GITHUB_TOKEN",
	}, &out, &bytes.Buffer{}))
	require.Equal(t, "github-token", sawToken)
	require.Contains(t, out.String(), `"login_method": "github-token"`)

	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	stored, err := keyring.Get(cfg.Remote.Auth.KeyringService, cfg.Remote.Auth.KeyringAccount)
	require.NoError(t, err)
	require.Equal(t, "session-token", stored)

	var whoami bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "--json", "whoami"}, &whoami, &bytes.Buffer{}))
	require.Contains(t, whoami.String(), `"login": "alice"`)
}

func TestCloudPublishSendsNonDMRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "publisher.db")
	require.NoError(t, config.Write(cfgPath, cfg))
	publisher := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, addCLIDMAttachment(ctx, publisher))
	require.NoError(t, publisher.Close())

	tokenEnv := "DISCRAWL_TEST_PUBLISH_TOKEN"
	t.Setenv(tokenEnv, "publish-token")
	seenTables := map[string]crawlremote.IngestRequest{}
	var sawSQLitePart bool
	var sawSQLiteManifest bool
	var sqliteBundlePart []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "Bearer publish-token", req.Header.Get("Authorization"))
		if req.Method == http.MethodPut && req.URL.EscapedPath() == "/v1/apps/discrawl/archives/discrawl%2Fopenclaw/sqlite" {
			uploadKind := req.Header.Get("X-Crawl-Sqlite-Upload")
			payload, err := io.ReadAll(req.Body)
			if err != nil {
				t.Errorf("read sqlite upload: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			switch uploadKind {
			case "bundle-part":
				sawSQLitePart = true
				assert.Equal(t, "application/gzip", req.Header.Get("Content-Type"))
				assert.NotEmpty(t, req.Header.Get("X-Crawl-Content-Sha256"))
				assert.True(t, bytes.HasPrefix(payload, []byte{0x1f, 0x8b}), "sqlite bundle part should be gzip")
				sqliteBundlePart = append(sqliteBundlePart[:0], payload...)
				_ = json.NewEncoder(w).Encode(crawlremote.SQLiteUploadResult{
					App:      "discrawl",
					Archive:  "discrawl/openclaw",
					Complete: false,
					Object:   &crawlremote.SQLiteObject{Key: "v1/discrawl/discrawl%2Fopenclaw/sqlite/chunks/current.db.gz.part-0000", Size: int64(len(payload))},
				})
			case "bundle-manifest":
				sawSQLiteManifest = true
				var manifest crawlremote.SQLiteBundleManifest
				if err := json.Unmarshal(payload, &manifest); err != nil {
					t.Errorf("decode sqlite bundle manifest: %v", err)
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				assert.Equal(t, crawlremote.SQLiteGzipChunkedBundleFormat, manifest.Format)
				assert.Equal(t, crawlremote.SQLiteGzipCompression, manifest.Compression.Algorithm)
				assert.Equal(t, int64(1), manifest.Counts["messages"])
				assert.Equal(t, false, manifest.Privacy["includes_private_messages"])
				assert.Equal(t, false, manifest.Privacy["includes_raw_json"])
				assert.Equal(t, "@me", manifest.Privacy["excludes_guild_id"])
				reader, err := gzip.NewReader(bytes.NewReader(sqliteBundlePart))
				if err != nil {
					t.Errorf("open sqlite bundle gzip: %v", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				decompressed, err := io.ReadAll(reader)
				if closeErr := reader.Close(); err == nil {
					err = closeErr
				}
				if err != nil {
					t.Errorf("read sqlite bundle gzip: %v", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				uploadPath := filepath.Join(dir, "uploaded-cloud.db")
				if err := os.WriteFile(uploadPath, decompressed, 0o600); err != nil {
					t.Errorf("write sqlite upload: %v", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				uploadDB, err := sql.Open("sqlite", uploadPath)
				if err != nil {
					t.Errorf("open sqlite upload: %v", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				defer func() { _ = uploadDB.Close() }()
				var dmMessages int
				if err := uploadDB.QueryRowContext(ctx, "select count(*) from messages where guild_id = ?", store.DirectMessageGuildID).Scan(&dmMessages); err != nil {
					t.Errorf("count dm messages: %v", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				assert.Zero(t, dmMessages)
				var tableCount int
				if err := uploadDB.QueryRowContext(ctx, "select count(*) from sqlite_master where type = 'table' and name in ('guilds', 'channels', 'members', 'messages')").Scan(&tableCount); err != nil {
					t.Errorf("count cloud tables: %v", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				assert.Equal(t, 4, tableCount)
				_ = json.NewEncoder(w).Encode(crawlremote.SQLiteBundleUploadResult{
					App:      "discrawl",
					Archive:  "discrawl/openclaw",
					Complete: true,
					Bundle:   &crawlremote.SQLiteBundle{Key: "v1/discrawl/discrawl%2Fopenclaw/sqlite/current.manifest.json", Manifest: &manifest},
				})
			default:
				http.Error(w, "missing sqlite bundle upload kind", http.StatusBadRequest)
			}
			return
		}
		assert.Equal(t, http.MethodPost, req.Method)
		assert.Equal(t, "/v1/apps/discrawl/archives/discrawl%2Fopenclaw/ingest", req.URL.EscapedPath())
		var body crawlremote.IngestRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		assert.Equal(t, "discrawl", body.Manifest.App)
		assert.Equal(t, "discrawl/openclaw", body.Manifest.Archive)
		for idx, column := range body.Columns {
			if column == "guild_id" {
				for _, row := range body.Rows {
					assert.NotEqual(t, store.DirectMessageGuildID, row[idx])
				}
			}
		}
		seenTables[body.Table] = body
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{Table: body.Table, RowsAccepted: int64(len(body.Rows)), Complete: body.Final})
	}))
	defer server.Close()

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"--json",
		"cloud", "publish",
		"--remote", server.URL,
		"--archive", "discrawl/openclaw",
		"--token-env", tokenEnv,
	}, &out, &bytes.Buffer{}))

	require.Len(t, seenTables, 4)
	require.Len(t, seenTables["guilds"].Rows, 1)
	require.Len(t, seenTables["channels"].Rows, 1)
	require.Len(t, seenTables["messages"].Rows, 1)
	require.True(t, seenTables["messages"].Final)
	require.True(t, sawSQLitePart)
	require.True(t, sawSQLiteManifest)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &payload))
	require.InDelta(t, float64(1), payload["guilds"], 0)
	require.InDelta(t, float64(1), payload["messages"], 0)
	require.Equal(t, false, payload["sqlite_only"])
	require.NotNil(t, payload["sqlite_bundle"])
}

func TestCloudPublishSQLiteOnlySkipsD1Ingest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "publisher.db")
	require.NoError(t, config.Write(cfgPath, cfg))
	publisher := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, publisher.Close())

	tokenEnv := "DISCRAWL_TEST_PUBLISH_TOKEN"
	t.Setenv(tokenEnv, "publish-token")
	var sawSQLitePart bool
	var sawSQLiteManifest bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "Bearer publish-token", req.Header.Get("Authorization"))
		if req.Method != http.MethodPut || req.URL.EscapedPath() != "/v1/apps/discrawl/archives/discrawl%2Fopenclaw/sqlite" {
			http.Error(w, "sqlite-only publish should not ingest D1 rows", http.StatusBadRequest)
			return
		}
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch req.Header.Get("X-Crawl-Sqlite-Upload") {
		case "bundle-part":
			sawSQLitePart = true
			assert.True(t, bytes.HasPrefix(payload, []byte{0x1f, 0x8b}), "sqlite bundle part should be gzip")
			_ = json.NewEncoder(w).Encode(crawlremote.SQLiteUploadResult{
				App:      "discrawl",
				Archive:  "discrawl/openclaw",
				Complete: false,
				Object:   &crawlremote.SQLiteObject{Key: "v1/discrawl/discrawl%2Fopenclaw/sqlite/chunks/current.db.gz.part-0000", Size: int64(len(payload))},
			})
		case "bundle-manifest":
			sawSQLiteManifest = true
			var manifest crawlremote.SQLiteBundleManifest
			if err := json.Unmarshal(payload, &manifest); err != nil {
				t.Errorf("decode sqlite bundle manifest: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			assert.Equal(t, int64(1), manifest.Counts["messages"])
			_ = json.NewEncoder(w).Encode(crawlremote.SQLiteBundleUploadResult{
				App:      "discrawl",
				Archive:  "discrawl/openclaw",
				Complete: true,
				Bundle:   &crawlremote.SQLiteBundle{Key: "v1/discrawl/discrawl%2Fopenclaw/sqlite/current.manifest.json", Manifest: &manifest},
			})
		default:
			http.Error(w, "missing sqlite bundle upload kind", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"--json",
		"cloud", "publish",
		"--sqlite-only",
		"--remote", server.URL,
		"--archive", "discrawl/openclaw",
		"--token-env", tokenEnv,
	}, &out, &bytes.Buffer{}))
	require.True(t, sawSQLitePart)
	require.True(t, sawSQLiteManifest)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &payload))
	require.Equal(t, true, payload["sqlite_only"])
	require.InDelta(t, float64(1), payload["messages"], 0)
	require.NotNil(t, payload["sqlite_bundle"])
}

func TestCloudSQLiteExportHelpers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	s := seedCLIStore(t, sourcePath)
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"private":"ignored"}`,
	}))
	require.NoError(t, addCLIDMAttachment(ctx, s))

	require.Empty(t, sqlPlaceholders(0))
	require.Equal(t, "?,?,?", sqlPlaceholders(3))

	snapshotPath, cleanup, err := sqliteSnapshotPath(ctx, s.DB())
	require.NoError(t, err)
	require.FileExists(t, snapshotPath)
	cleanup()
	require.NoFileExists(t, snapshotPath)

	exportPath := filepath.Join(dir, "cloud.db")
	require.NoError(t, writeCloudSQLiteExport(ctx, s.DB(), exportPath))
	require.NoError(t, s.Close())

	sum, err := cloudFileSHA256(exportPath)
	require.NoError(t, err)
	require.Len(t, sum, 64)
	_, err = cloudFileSHA256(filepath.Join(dir, "missing.db"))
	require.Error(t, err)

	cloudDB, err := sql.Open("sqlite", exportPath)
	require.NoError(t, err)
	defer func() { _ = cloudDB.Close() }()

	var guilds, channels, members, messages int
	require.NoError(t, cloudDB.QueryRowContext(ctx, "select count(*) from guilds").Scan(&guilds))
	require.NoError(t, cloudDB.QueryRowContext(ctx, "select count(*) from channels").Scan(&channels))
	require.NoError(t, cloudDB.QueryRowContext(ctx, "select count(*) from members").Scan(&members))
	require.NoError(t, cloudDB.QueryRowContext(ctx, "select count(*) from messages").Scan(&messages))
	require.Equal(t, 1, guilds)
	require.Equal(t, 1, channels)
	require.Equal(t, 1, members)
	require.Equal(t, 1, messages)

	var dmRows int
	require.NoError(t, cloudDB.QueryRowContext(ctx, "select count(*) from messages where guild_id = ?", store.DirectMessageGuildID).Scan(&dmRows))
	require.Zero(t, dmRows)

	var authorUsername string
	require.NoError(t, cloudDB.QueryRowContext(ctx, "select author_username from messages where message_id = 'm100'").Scan(&authorUsername))
	require.Equal(t, "Peter", authorUsername)

	var rawJSONColumns int
	require.NoError(t, cloudDB.QueryRowContext(ctx, "select count(*) from pragma_table_info('messages') where name = 'raw_json'").Scan(&rawJSONColumns))
	require.Zero(t, rawJSONColumns)
}

func TestCopyCloudSQLiteRowsErrorsAndBytes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source, err := sql.Open("sqlite", filepath.Join(dir, "source.db"))
	require.NoError(t, err)
	defer func() { _ = source.Close() }()
	out, err := sql.Open("sqlite", filepath.Join(dir, "out.db"))
	require.NoError(t, err)
	defer func() { _ = out.Close() }()

	_, err = source.ExecContext(ctx, `create table source_rows(value blob)`)
	require.NoError(t, err)
	_, err = source.ExecContext(ctx, `insert into source_rows(value) values(x'68656c6c6f')`)
	require.NoError(t, err)
	_, err = out.ExecContext(ctx, `create table copied(value text)`)
	require.NoError(t, err)

	require.NoError(t, copyCloudSQLiteRows(ctx, source, out, "copied", []string{"value"}, `select value from source_rows`))
	var value string
	require.NoError(t, out.QueryRowContext(ctx, `select value from copied`).Scan(&value))
	require.Equal(t, "hello", value)

	err = copyCloudSQLiteRows(ctx, source, out, "copied", []string{"value"}, `select missing from source_rows`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "query sqlite cloud export copied")
}

func TestShareCommandsPublishSubscribeAndUpdate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "publisher.db")
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "publisher-share")
	require.NoError(t, config.Write(cfgPath, cfg))
	publisher := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, publisher.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"publish",
		"--repo", cfg.Share.RepoPath,
		"--remote", remoteRepo,
		"--readme", filepath.Join(cfg.Share.RepoPath, "README.md"),
		"--no-commit",
	}, &out, &bytes.Buffer{}))
	require.FileExists(t, filepath.Join(cfg.Share.RepoPath, share.ManifestName))
	require.FileExists(t, filepath.Join(cfg.Share.RepoPath, "README.md"))
	err := Run(ctx, []string{
		"--config", cfgPath,
		"publish",
		"--repo", filepath.Join(dir, "filtered-share"),
		"--public-only",
		"--readme", filepath.Join(dir, "filtered-share", "README.md"),
		"--no-commit",
	}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Equal(t, 2, ExitCode(err))
	require.ErrorContains(t, err, "publish --readme is not supported with share filters")

	runGit(t, cfg.Share.RepoPath, "config", "user.name", "discrawl test")
	runGit(t, cfg.Share.RepoPath, "config", "user.email", "discrawl@example.com")
	committed, err := share.Commit(ctx, share.Options{RepoPath: cfg.Share.RepoPath, Remote: remoteRepo, Branch: "main"}, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, share.Push(ctx, share.Options{RepoPath: cfg.Share.RepoPath, Remote: remoteRepo, Branch: "main"}))

	readerCfgPath := filepath.Join(dir, "reader.toml")
	require.NoError(t, Run(ctx, []string{
		"--config", readerCfgPath,
		"subscribe",
		"--repo", filepath.Join(dir, "reader-share"),
		"--no-import",
		remoteRepo,
	}, &bytes.Buffer{}, &bytes.Buffer{}))
	readerCfg, err := config.Load(readerCfgPath)
	require.NoError(t, err)
	require.Equal(t, remoteRepo, readerCfg.Share.Remote)
	require.Equal(t, "none", readerCfg.Discord.TokenSource)

	readerCfg.DBPath = filepath.Join(dir, "reader.db")
	require.NoError(t, config.Write(readerCfgPath, readerCfg))
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "update"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "generated_at")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "automatic updates work")
}

func TestFilteredPublishRemovesGeneratedReadme(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "publisher.db")
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "publisher-share")
	require.NoError(t, config.Write(cfgPath, cfg))
	publisher := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, publisher.Close())

	require.NoError(t, os.MkdirAll(cfg.Share.RepoPath, 0o755))
	runGit(t, cfg.Share.RepoPath, "init")
	runGit(t, cfg.Share.RepoPath, "checkout", "-B", "main")
	runGit(t, cfg.Share.RepoPath, "config", "user.name", "discrawl test")
	runGit(t, cfg.Share.RepoPath, "config", "user.email", "discrawl@example.com")
	runGit(t, cfg.Share.RepoPath, "remote", "add", "origin", remoteRepo)

	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"publish",
		"--repo", cfg.Share.RepoPath,
		"--remote", remoteRepo,
		"--readme", filepath.Join(cfg.Share.RepoPath, "README.md"),
		"--push",
	}, &bytes.Buffer{}, &bytes.Buffer{}))
	require.FileExists(t, filepath.Join(cfg.Share.RepoPath, "README.md"))
	out, err := exec.CommandContext(ctx, "git", "-C", cfg.Share.RepoPath, "ls-tree", "--name-only", "HEAD", "README.md").Output()
	require.NoError(t, err)
	require.Equal(t, "README.md\n", string(out))

	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"publish",
		"--repo", cfg.Share.RepoPath,
		"--remote", remoteRepo,
		"--public-only",
		"--push",
		"--message", "test: filtered snapshot",
	}, &bytes.Buffer{}, &bytes.Buffer{}))
	require.NoFileExists(t, filepath.Join(cfg.Share.RepoPath, "README.md"))
	out, err = exec.CommandContext(ctx, "git", "-C", cfg.Share.RepoPath, "ls-tree", "--name-only", "HEAD", "README.md").Output()
	require.NoError(t, err)
	require.Empty(t, string(out))
}

func TestShareCommandsRoundTripEmbeddings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	cfgPath := filepath.Join(dir, "publisher.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "publisher.db")
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "publisher-share")
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	publisher := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, insertCLIEmbedding(ctx, publisher, "m100", "openai_compatible", "local-model", []float32{1, 0}))
	require.NoError(t, publisher.Close())
	require.NoError(t, os.MkdirAll(cfg.Share.RepoPath, 0o755))
	runGit(t, cfg.Share.RepoPath, "init")
	runGit(t, cfg.Share.RepoPath, "checkout", "-B", "main")
	runGit(t, cfg.Share.RepoPath, "config", "user.name", "discrawl test")
	runGit(t, cfg.Share.RepoPath, "config", "user.email", "discrawl@example.com")
	runGit(t, cfg.Share.RepoPath, "remote", "add", "origin", remoteRepo)

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"publish",
		"--repo", cfg.Share.RepoPath,
		"--remote", remoteRepo,
		"--with-embeddings",
		"--push",
	}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "embeddings=[")

	readerCfgPath := filepath.Join(dir, "reader.toml")
	readerCfg := config.Default()
	readerCfg.DBPath = filepath.Join(dir, "reader.db")
	readerCfg.Search.Embeddings.Enabled = true
	readerCfg.Search.Embeddings.Provider = "openai_compatible"
	readerCfg.Search.Embeddings.Model = "local-model"
	readerCfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(readerCfgPath, readerCfg))

	out.Reset()
	require.NoError(t, Run(ctx, []string{
		"--config", readerCfgPath,
		"subscribe",
		"--repo", filepath.Join(dir, "reader-share"),
		"--with-embeddings",
		remoteRepo,
	}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "imported=true")
	require.Contains(t, out.String(), "embeddings=[")

	reader, err := store.Open(ctx, readerCfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	_, rows, err := reader.ReadOnlyQuery(ctx, "select provider, model, count(*) from message_embeddings group by provider, model")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"openai_compatible", "local-model", "1"}}, rows)
}

func TestSubscribeGitOnlyModeNeedsNoDiscordCredentials(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	publisherDB := filepath.Join(dir, "publisher.db")
	publisher := seedCLIStore(t, publisherDB)
	defer func() { _ = publisher.Close() }()
	publisherRepo := filepath.Join(dir, "publisher-share")
	opts := share.Options{RepoPath: publisherRepo, Remote: remoteRepo, Branch: "main"}
	_, err := share.Export(ctx, publisher, opts)
	require.NoError(t, err)
	runGit(t, publisherRepo, "config", "user.name", "discrawl test")
	runGit(t, publisherRepo, "config", "user.email", "discrawl@example.com")
	committed, err := share.Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, share.Push(ctx, opts))

	cfgPath := filepath.Join(dir, "reader.toml")
	readerDB := filepath.Join(dir, "reader.db")
	readerCfg := config.Default()
	readerCfg.DBPath = readerDB
	require.NoError(t, config.Write(cfgPath, readerCfg))
	t.Setenv(config.DefaultTokenEnv, "")
	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"subscribe",
		"--repo", filepath.Join(dir, "reader-share"),
		"--stale-after", "1m",
		remoteRepo,
	}, &out, &bytes.Buffer{}))

	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	require.Equal(t, "none", cfg.Discord.TokenSource)
	require.True(t, cfg.Share.AutoUpdate)
	require.Equal(t, "1m", cfg.Share.StaleAfter)

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "automatic updates work")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "doctor"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "disabled (git share mode)")

	err = Run(ctx, []string{"--config", cfgPath, "sync", "--all"}, &out, &bytes.Buffer{})
	require.Equal(t, 4, ExitCode(err))
	require.Contains(t, err.Error(), "discord token disabled")
}

func TestShareUpdateImportsNewRemoteSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	publisherDB := filepath.Join(dir, "publisher.db")
	publisher := seedCLIStore(t, publisherDB)
	defer func() { _ = publisher.Close() }()
	publisherRepo := filepath.Join(dir, "publisher-share")
	opts := share.Options{RepoPath: publisherRepo, Remote: remoteRepo, Branch: "main"}
	publishSnapshot(t, ctx, publisher, opts, "test: old snapshot")

	readerCfgPath := filepath.Join(dir, "reader.toml")
	readerCfg := config.Default()
	readerCfg.DBPath = filepath.Join(dir, "reader.db")
	readerCfg.Share.Remote = remoteRepo
	readerCfg.Share.RepoPath = filepath.Join(dir, "reader-share")
	readerCfg.Share.AutoUpdate = true
	readerCfg.Share.StaleAfter = "15m"
	readerCfg.Discord.TokenSource = "none"
	require.NoError(t, config.Write(readerCfgPath, readerCfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "update"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "imported=true")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "update"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "imported=false")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "automatic updates work")

	require.NoError(t, publisher.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m200",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "newer git snapshot arrived",
		NormalizedContent: "newer git snapshot arrived",
		RawJSON:           `{}`,
	}))
	publishSnapshot(t, ctx, publisher, opts, "test: new snapshot")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "update"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "imported=true")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "search", "newer snapshot"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "newer git snapshot arrived")
}

func TestSyncSkipsGitShareByDefaultAndCanImportBeforeLiveDiscord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	publisherDB := filepath.Join(dir, "publisher.db")
	publisher := seedCLIStore(t, publisherDB)
	defer func() { _ = publisher.Close() }()
	publisherRepo := filepath.Join(dir, "publisher-share")
	opts := share.Options{RepoPath: publisherRepo, Remote: remoteRepo, Branch: "main"}
	publishSnapshot(t, ctx, publisher, opts, "test: git snapshot")

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "reader.db")
	cfg.DefaultGuildID = "g1"
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "reader-share")
	cfg.Share.AutoUpdate = true
	cfg.Share.StaleAfter = "15m"
	cfg.Desktop.Path = filepath.Join(dir, "empty-discord")
	require.NoError(t, os.MkdirAll(cfg.Desktop.Path, 0o755))
	require.NoError(t, config.Write(cfgPath, cfg))

	hybrid := &hybridSyncService{}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) {
			return &fakeDiscordClient{guilds: []*discordgo.UserGuild{{ID: "g1"}}, self: &discordgo.User{ID: "bot"}}, nil
		},
		newSyncer: func(_ syncer.Client, s *store.Store, _ *slog.Logger) syncService {
			hybrid.store = s
			return hybrid
		},
	}

	require.NoError(t, rt.dispatch([]string{"sync", "--all"}))
	require.False(t, hybrid.sawGitMessage)

	reader, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	rows, err := reader.ListMessages(ctx, store.MessageListOptions{Channel: "general", IncludeEmpty: true})
	require.NoError(t, err)
	contents := make([]string, 0, len(rows))
	for _, row := range rows {
		contents = append(contents, row.Content)
	}
	require.NotContains(t, contents, "automatic updates work")
	require.Contains(t, contents, "live discord filled the delta")
	require.NoError(t, reader.Close())

	hybrid.sawGitMessage = false
	require.NoError(t, rt.dispatch([]string{"sync", "--all", "--update=auto"}))
	require.True(t, hybrid.sawGitMessage)

	reader, err = store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	rows, err = reader.ListMessages(ctx, store.MessageListOptions{Channel: "general", IncludeEmpty: true})
	require.NoError(t, err)
	contents = contents[:0]
	for _, row := range rows {
		contents = append(contents, row.Content)
	}
	require.Contains(t, contents, "automatic updates work")
	require.Contains(t, contents, "live discord filled the delta")
}

func TestSyncLockSerializesConcurrentRuns(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock is currently a no-op on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "discrawl.db")
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))

	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		cfg:        cfg,
	}
	firstRelease, err := acquireSyncLock(ctx, filepath.Join(dir, ".discrawl-sync.lock"))
	require.NoError(t, err)
	defer func() { _ = firstRelease() }()

	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	rt.ctx = waitCtx
	err = rt.withSyncLock(func() error { return nil })
	require.ErrorIs(t, err, context.DeadlineExceeded)

	waitCtx, cancel = context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	rt.ctx = waitCtx
	err = rt.dispatch([]string{"update"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestReadCommandsDoNotWaitForSyncLock(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "discrawl.db")
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))

	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())

	firstRelease, err := acquireSyncLock(ctx, filepath.Join(dir, ".discrawl-sync.lock"))
	require.NoError(t, err)
	defer func() { _ = firstRelease() }()

	for _, args := range [][]string{
		{"--config", cfgPath, "search", "automatic"},
		{"--config", cfgPath, "messages", "--channel", "general", "--last", "1"},
		{"--config", cfgPath, "sql", "select count(*) as total from messages"},
	} {
		runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var out bytes.Buffer
		err := Run(runCtx, args, &out, &bytes.Buffer{})
		cancel()
		require.NoError(t, err, args)
		require.NotEmpty(t, out.String(), args)
	}
}

func TestMessagesSyncFailsFastWhenTailOwnsSyncLock(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLock(ctx, lockPath)
	require.NoError(t, err)
	defer func() { _ = release() }()
	releaseToken := holdSyncLockToken(t, ctx, lockPath, testSyncLockToken())
	defer releaseToken()
	writeSyncLockMetadata(t, lockPath, "tail", os.Getpid())

	rt, fakeSync := messagesSyncTestRuntime(ctx, cfgPath)
	err = rt.dispatch([]string{"messages", "--channel", "general", "--last", "1", "--sync"})
	require.Error(t, err)
	require.Equal(t, 2, ExitCode(err))
	require.Contains(t, err.Error(), "tail already owns live sync; omit --sync while tail is running")
	require.Zero(t, fakeSync.syncCalls)
}

func TestMessagesSyncFailsFastDuringTailLockMetadataStartup(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLock(ctx, lockPath)
	require.NoError(t, err)
	defer func() { _ = release() }()
	releaseToken := holdSyncLockToken(t, ctx, lockPath, testSyncLockToken())
	defer releaseToken()
	require.NoError(t, writeSyncLockMetadataFiles(lockPath, fmt.Appendf(nil, "pid=%d\n", os.Getpid())))
	go func() {
		time.Sleep(25 * time.Millisecond)
		body := fmt.Sprintf("pid=%d\noperation=tail\ntoken=%s\nstarted_at=2026-03-08T12:00:00Z\nupdated_at=2026-03-08T12:00:00Z\nphase=locked\n", os.Getpid(), testSyncLockToken())
		_ = writeSyncLockMetadataFiles(lockPath, []byte(body))
	}()

	rt, fakeSync := messagesSyncTestRuntime(ctx, cfgPath)
	err = rt.dispatch([]string{"messages", "--channel", "general", "--last", "1", "--sync"})
	require.Error(t, err)
	require.Equal(t, 2, ExitCode(err))
	require.Contains(t, err.Error(), "tail already owns live sync; omit --sync while tail is running")
	require.Zero(t, fakeSync.syncCalls)
}

func TestMessagesSyncIgnoresStaleTailLockMetadata(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	writeSyncLockMetadata(t, filepath.Join(dir, ".discrawl-sync.lock"), "tail", os.Getpid())

	rt, fakeSync := messagesSyncTestRuntime(ctx, cfgPath)
	require.NoError(t, rt.dispatch([]string{"messages", "--channel", "general", "--last", "1", "--sync"}))
	require.Equal(t, 1, fakeSync.syncCalls)
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "automatic updates work")
}

func TestMessagesSyncTreatsStaleTailMetadataHeldByNonTailAsWriter(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLock(ctx, lockPath)
	require.NoError(t, err)
	defer func() { _ = release() }()
	writeSyncLockMetadata(t, lockPath, "tail", os.Getpid())

	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	rt, fakeSync := messagesSyncTestRuntime(waitCtx, cfgPath)
	err = rt.dispatch([]string{"messages", "--channel", "general", "--last", "1", "--sync"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotContains(t, err.Error(), "tail already owns live sync")
	require.Zero(t, fakeSync.syncCalls)
}

func TestMessagesSyncWaitsDuringTailStartup(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLockWithMetadata(ctx, lockPath, syncLockMetadataBody("tail-starting", "locked", time.Now().UTC(), time.Now().UTC(), testSyncLockToken()))
	require.NoError(t, err)
	defer func() { _ = release() }()

	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	rt, fakeSync := messagesSyncTestRuntime(waitCtx, cfgPath)
	err = rt.dispatch([]string{"messages", "--channel", "general", "--last", "1", "--sync"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotContains(t, err.Error(), "tail already owns live sync")
	require.Zero(t, fakeSync.syncCalls)
}

func TestMessagesSyncWaitsForNonTailSyncLockOwner(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLock(ctx, lockPath)
	require.NoError(t, err)
	defer func() { _ = release() }()
	writeSyncLockMetadata(t, lockPath, "sync", os.Getpid())

	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	rt, fakeSync := messagesSyncTestRuntime(waitCtx, cfgPath)
	err = rt.dispatch([]string{"messages", "--channel", "general", "--last", "1", "--sync"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, fakeSync.syncCalls)
}

func TestMessagesSyncWaitsForLegacySyncLockMetadata(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLock(ctx, lockPath)
	require.NoError(t, err)
	defer func() { _ = release() }()
	require.NoError(t, os.Remove(syncLockMetadataPath(lockPath)))
	require.NoError(t, os.WriteFile(lockPath, fmt.Appendf(nil, "pid=%d\n", os.Getpid()), 0o600))

	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	rt, fakeSync := messagesSyncTestRuntime(waitCtx, cfgPath)
	err = rt.dispatch([]string{"messages", "--channel", "general", "--last", "1", "--sync"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, fakeSync.syncCalls)
}

func TestMessagesSyncWaitsForMalformedSyncLockMetadata(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLock(ctx, lockPath)
	require.NoError(t, err)
	defer func() { _ = release() }()
	require.NoError(t, writeSyncLockMetadataFiles(lockPath, []byte("pid\noperation=tail\n")))

	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	rt, fakeSync := messagesSyncTestRuntime(waitCtx, cfgPath)
	err = rt.dispatch([]string{"messages", "--channel", "general", "--last", "1", "--sync"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, fakeSync.syncCalls)
}

func TestMessagesSyncPreservesCancellationWhileWaitingForSyncLock(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLock(ctx, lockPath)
	require.NoError(t, err)
	defer func() { _ = release() }()
	writeSyncLockMetadata(t, lockPath, "sync", os.Getpid())

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	rt, fakeSync := messagesSyncTestRuntime(canceledCtx, cfgPath)
	err = rt.dispatch([]string{"messages", "--channel", "general", "--last", "1", "--sync"})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, fakeSync.syncCalls)
}

func TestPlainMessagesStillReadsWhileTailOwnsSyncLock(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLock(ctx, lockPath)
	require.NoError(t, err)
	defer func() { _ = release() }()
	releaseToken := holdSyncLockToken(t, ctx, lockPath, testSyncLockToken())
	defer releaseToken()
	writeSyncLockMetadata(t, lockPath, "tail", os.Getpid())

	runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var out bytes.Buffer
	err = Run(runCtx, []string{"--config", cfgPath, "messages", "--channel", "general", "--last", "1"}, &out, &bytes.Buffer{})
	require.NoError(t, err)
	require.Contains(t, out.String(), "automatic updates work")
}

func TestMessagesSyncFalseStillReadsWhileTailOwnsSyncLock(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLock(ctx, lockPath)
	require.NoError(t, err)
	defer func() { _ = release() }()
	releaseToken := holdSyncLockToken(t, ctx, lockPath, testSyncLockToken())
	defer releaseToken()
	writeSyncLockMetadata(t, lockPath, "tail", os.Getpid())

	runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var out bytes.Buffer
	err = Run(runCtx, []string{"--config", cfgPath, "messages", "--channel", "general", "--last", "1", "--sync=false"}, &out, &bytes.Buffer{})
	require.NoError(t, err)
	require.Contains(t, out.String(), "automatic updates work")
}

func TestMessagesRepeatedSyncFalseStillReadsWhileTailOwnsSyncLock(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	release, err := acquireSyncLock(ctx, lockPath)
	require.NoError(t, err)
	defer func() { _ = release() }()
	releaseToken := holdSyncLockToken(t, ctx, lockPath, testSyncLockToken())
	defer releaseToken()
	writeSyncLockMetadata(t, lockPath, "tail", os.Getpid())

	runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var out bytes.Buffer
	err = Run(runCtx, []string{"--config", cfgPath, "messages", "--channel", "general", "--last", "1", "--sync", "--sync=false"}, &out, &bytes.Buffer{})
	require.NoError(t, err)
	require.Contains(t, out.String(), "automatic updates work")
}

func TestTailOpenFailureDoesNotPublishActiveTailOwner(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	fakeSync := &fakeSyncService{tailErr: errors.New("gateway open failed")}
	rt := tailTestRuntime(ctx, cfgPath, fakeSync)
	err := rt.dispatch([]string{"tail"})
	require.ErrorContains(t, err, "gateway open failed")
	require.Zero(t, fakeSync.tailReadyCalls)

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	owner, ok := readSyncLockOwner(lockPath)
	require.True(t, ok)
	require.Equal(t, "tail-starting", owner.Operation)
	require.False(t, rt.activeTailOwnsSyncLock(lockPath))
	require.Empty(t, syncLockOwnerFiles(t, lockPath))
}

func TestTailReadyPromotesOnceAndCleansUpOnCancellation(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock timing is flaky on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, s.Close())
	t.Setenv(config.DefaultTokenEnv, "env-token")

	fakeSync := &fakeSyncService{callTailReady: true, tailErr: context.Canceled}
	rt := tailTestRuntime(ctx, cfgPath, fakeSync)
	err := rt.dispatch([]string{"tail"})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, fakeSync.tailReadyCalls)

	lockPath := filepath.Join(dir, ".discrawl-sync.lock")
	owner, ok := readSyncLockOwner(lockPath)
	require.True(t, ok)
	require.Equal(t, "tail", owner.Operation)
	require.False(t, rt.activeTailOwnsSyncLock(lockPath))
	require.Empty(t, syncLockOwnerFiles(t, lockPath))
}

func TestSyncLockHelperEdges(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	lockPath := filepath.Join(dir, ".discrawl-sync.lock")

	require.False(t, validSyncLockToken(""))
	require.False(t, validSyncLockToken("not-hex-not-hex-not-hex-not-hex"))
	require.True(t, validSyncLockToken(testSyncLockToken()))
	require.False(t, syncLockTokenHeld(lockPath, testSyncLockToken()))

	require.NoError(t, os.WriteFile(lockPath, []byte("pid=bad\n"), 0o600))
	_, ok := readSyncLockOwner(lockPath)
	require.False(t, ok)

	require.NoError(t, writeSyncLockMetadataFiles(lockPath, []byte("pid=123\noperation=legacy\n")))
	owner, ok := readSyncLockOwner(lockPath)
	require.True(t, ok)
	require.Equal(t, "legacy", owner.Operation)

	require.NoError(t, writeSyncLockMetadataSidecar(lockPath, []byte("pid=123\noperation=sidecar\nphase=current\n")))
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	err := syncLockErr(canceledCtx, lockPath)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "phase=current")

	rt := &runtime{}
	require.NoError(t, rt.activateTailSyncLock())
}

func TestReadCommandsMigrateOlderLocalStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "discrawl.db")
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))

	s := seedCLIStore(t, cfg.DBPath)
	_, err := s.DB().ExecContext(ctx, `pragma user_version = 1`)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "automatic updates work")

	reader, err := store.OpenReadOnly(ctx, cfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	var version int
	require.NoError(t, reader.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, 3, version)
}

func TestReadOnlyCommandsMigrateOlderLocalStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "discrawl.db")
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))

	s := seedCLIStore(t, cfg.DBPath)
	_, err := s.DB().ExecContext(ctx, `pragma user_version = 1`)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "status"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "messages=1")

	reader, err := store.OpenReadOnly(ctx, cfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	var version int
	require.NoError(t, reader.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, 3, version)
}

func seedCLIStore(t *testing.T, path string) *store.Store {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, path)
	require.NoError(t, err)
	now := time.Now().UTC()
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m100",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         now.Format(time.RFC3339Nano),
		Content:           "automatic updates work",
		NormalizedContent: "automatic updates work",
		RawJSON:           `{}`,
	}))
	return s
}

func writeTestConfig(t *testing.T, dir string) (config.Config, string) {
	t.Helper()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "discrawl.db")
	cfg.DefaultGuildID = "g1"
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))
	return cfg, cfgPath
}

func testSyncLockToken() string {
	return fmt.Sprintf("%032x", 1)
}

func writeSyncLockMetadata(t *testing.T, path, operation string, pid int) {
	t.Helper()
	body := fmt.Sprintf("pid=%d\noperation=%s\ntoken=%s\nstarted_at=2026-03-08T12:00:00Z\nupdated_at=2026-03-08T12:00:00Z\nphase=locked\n", pid, operation, testSyncLockToken())
	require.NoError(t, writeSyncLockMetadataFiles(path, []byte(body)))
}

func holdSyncLockToken(t *testing.T, ctx context.Context, lockPath, token string) func() {
	t.Helper()
	now := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)
	tokenPath := syncLockTokenPath(lockPath, token)
	release, err := acquireSyncLockWithMetadata(ctx, tokenPath, syncLockMetadataBody("tail-token", "locked", now, now, token))
	require.NoError(t, err)
	return func() {
		_ = release()
		_ = os.Remove(tokenPath)
	}
}

func messagesSyncTestRuntime(ctx context.Context, cfgPath string) (*runtime, *fakeSyncService) {
	fakeSync := &fakeSyncService{}
	out := &bytes.Buffer{}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}
	return rt, fakeSync
}

func tailTestRuntime(ctx context.Context, cfgPath string, fakeSync *fakeSyncService) *runtime {
	return &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}
}

func syncLockOwnerFiles(t *testing.T, lockPath string) []string {
	t.Helper()
	files, err := filepath.Glob(lockPath + ".*.owner*")
	require.NoError(t, err)
	return files
}

func addCLIAttachment(ctx context.Context, s *store.Store, url string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m100",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "automatic updates work",
			NormalizedContent: "automatic updates work file.png",
			HasAttachments:    true,
			RawJSON:           `{"author":{"username":"Peter","global_name":"Peter"}}`,
		},
		Attachments: []store.AttachmentRecord{{
			AttachmentID: "a-cli",
			MessageID:    "m100",
			GuildID:      "g1",
			ChannelID:    "c1",
			AuthorID:     "u1",
			Filename:     "file.png",
			ContentType:  "image/png",
			Size:         7,
			URL:          url,
		}},
	}})
}

func addCLIDMAttachment(ctx context.Context, s *store.Store) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := s.UpsertGuild(ctx, store.GuildRecord{ID: store.DirectMessageGuildID, Name: "Discord Direct Messages", RawJSON: `{}`}); err != nil {
		return err
	}
	if err := s.UpsertChannel(ctx, store.ChannelRecord{ID: "dm-c1", GuildID: store.DirectMessageGuildID, Kind: "dm", Name: "Alice", RawJSON: `{}`}); err != nil {
		return err
	}
	return s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "dm1",
			GuildID:           store.DirectMessageGuildID,
			ChannelID:         "dm-c1",
			ChannelName:       "Alice",
			AuthorID:          "u2",
			AuthorName:        "Alice",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "dm attachment",
			NormalizedContent: "dm attachment private.png",
			HasAttachments:    true,
			RawJSON:           `{}`,
		},
		Attachments: []store.AttachmentRecord{{
			AttachmentID: "a-dm",
			MessageID:    "dm1",
			GuildID:      store.DirectMessageGuildID,
			ChannelID:    "dm-c1",
			AuthorID:     "u2",
			Filename:     "private.png",
			ContentType:  "image/png",
		}},
	}})
}

func publishSnapshot(t *testing.T, ctx context.Context, s *store.Store, opts share.Options, message string) {
	t.Helper()
	_, err := share.Export(ctx, s, opts)
	require.NoError(t, err)
	runGit(t, opts.RepoPath, "config", "user.name", "discrawl test")
	runGit(t, opts.RepoPath, "config", "user.email", "discrawl@example.com")
	committed, err := share.Commit(ctx, opts, message)
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, share.Push(ctx, opts))
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	// #nosec G204 -- fixed git argv in test setup.
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}

func TestEmbedCommandDrainsBoundedBacklog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/embeddings", r.URL.Path)
		var req struct {
			Input []string `json:"input"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Len(t, req.Input, 1)
		_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[1,2]}]}`))
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	for _, id := range []string{"m1", "m2"} {
		require.NoError(t, s.UpsertMessageWithOptions(ctx, store.MessageRecord{
			ID:                id,
			GuildID:           "g1",
			ChannelID:         "c1",
			MessageType:       0,
			CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
			Content:           "hello",
			NormalizedContent: "hello",
			RawJSON:           `{}`,
		}, store.WriteOptions{EnqueueEmbedding: true}))
	}
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "embed", "--limit", "1"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "processed=1")
	require.Contains(t, out.String(), "succeeded=1")
	require.Contains(t, out.String(), "remaining_backlog=1")
	require.Contains(t, out.String(), "provider=openai_compatible")

	s, err = store.Open(ctx, dbPath)
	require.NoError(t, err)
	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from message_embeddings")
	require.NoError(t, err)
	require.Equal(t, "1", rows[0][0])
	require.NoError(t, s.Close())

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "embed", "--rebuild", "--limit", "1"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "processed=1")
	require.Contains(t, out.String(), "succeeded=1")
	require.Contains(t, out.String(), "remaining_backlog=1")
	require.Contains(t, out.String(), "requeued=2")
}

func TestSearchSemanticCommandUsesStoredEmbeddings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		assert.Equal(t, "/embeddings", r.URL.Path)
		var req struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "local-model", req.Model)
		assert.Equal(t, []string{"cats"}, req.Input)
		_, _ = w.Write([]byte(`{"model":"local-model","data":[{"index":0,"embedding":[1,0]}]}`))
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.DefaultMode = "semantic"
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	base := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Alice",
		MessageType:       0,
		CreatedAt:         base.Format(time.RFC3339Nano),
		Content:           "database migration discussion",
		NormalizedContent: "database migration discussion",
		RawJSON:           `{"author":{"username":"Alice"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u2",
		AuthorName:        "Bob",
		MessageType:       0,
		CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "cats in semantic search",
		NormalizedContent: "cats in semantic search",
		RawJSON:           `{"author":{"username":"Bob"}}`,
	}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "m1", "openai_compatible", "local-model", []float32{1, 0}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "m2", "openai_compatible", "local-model", []float32{0.8, 0.2}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--limit", "1", "cats"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "database migration discussion")
	require.NotContains(t, out.String(), "cats in semantic search")
	require.Equal(t, 1, requests)

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--mode", "semantic", "--channel", "general", "--author", "Alice", "cats"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "database migration discussion")
	require.NotContains(t, out.String(), "cats in semantic search")
	require.Equal(t, 2, requests)
}

func TestSearchSemanticCommandUsesTurboVecBackend(t *testing.T) {
	python := os.Getenv("DISCRAWL_TEST_TURBOVEC_PYTHON")
	if python == "" {
		t.Skip("set DISCRAWL_TEST_TURBOVEC_PYTHON to run the real turbovec bridge")
	}
	t.Setenv("CRAWLKIT_TURBOVEC_PYTHON", python)

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		assert.Equal(t, "/embeddings", r.URL.Path)
		var req struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "local-model", req.Model)
		assert.Equal(t, []string{"cats"}, req.Input)
		_, _ = w.Write([]byte(`{"model":"local-model","data":[{"index":0,"embedding":[1,0,0,0,0,0,0,0]}]}`))
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.DefaultMode = "semantic"
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	cfg.Search.Embeddings.VectorBackend = "turbovec"
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	base := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "best",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Alice",
		MessageType:       0,
		CreatedAt:         base.Format(time.RFC3339Nano),
		Content:           "best vector match",
		NormalizedContent: "best vector match",
		RawJSON:           `{"author":{"username":"Alice"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "second",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u2",
		AuthorName:        "Bob",
		MessageType:       0,
		CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "second vector match",
		NormalizedContent: "second vector match",
		RawJSON:           `{"author":{"username":"Bob"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "other",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u3",
		AuthorName:        "Carol",
		MessageType:       0,
		CreatedAt:         base.Add(2 * time.Minute).Format(time.RFC3339Nano),
		Content:           "orthogonal vector",
		NormalizedContent: "orthogonal vector",
		RawJSON:           `{"author":{"username":"Carol"}}`,
	}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "best", "openai_compatible", "local-model", []float32{1, 0, 0, 0, 0, 0, 0, 0}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "second", "openai_compatible", "local-model", []float32{0.8, 0.2, 0, 0, 0, 0, 0, 0}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "other", "openai_compatible", "local-model", []float32{0, 1, 0, 0, 0, 0, 0, 0}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--limit", "2", "cats"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "best vector match")
	require.Contains(t, out.String(), "second vector match")
	require.NotContains(t, out.String(), "orthogonal vector")
	require.Equal(t, 1, requests)
}

func TestSearchHybridCommandFusesResults(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/embeddings", r.URL.Path)
		var req struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "local-model", req.Model)
		assert.Equal(t, []string{"panic"}, req.Input)
		_, _ = w.Write([]byte(`{"model":"local-model","data":[{"index":0,"embedding":[1,0]}]}`))
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.DefaultMode = "hybrid"
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	base := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m3",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Alice",
		MessageType:       0,
		CreatedAt:         base.Format(time.RFC3339Nano),
		Content:           "panic stack trace",
		NormalizedContent: "panic stack trace",
		RawJSON:           `{"author":{"username":"Alice"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u2",
		AuthorName:        "Bob",
		MessageType:       0,
		CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "database worker stalled",
		NormalizedContent: "database worker stalled",
		RawJSON:           `{"author":{"username":"Bob"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u3",
		AuthorName:        "Carol",
		MessageType:       0,
		CreatedAt:         base.Add(2 * time.Minute).Format(time.RFC3339Nano),
		Content:           "panic database lock",
		NormalizedContent: "panic database lock",
		RawJSON:           `{"author":{"username":"Carol"}}`,
	}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "m1", "openai_compatible", "local-model", []float32{0.9, 0.1}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "m2", "openai_compatible", "local-model", []float32{1, 0}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "m3", "openai_compatible", "local-model", []float32{0, 1}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--limit", "3", "panic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "panic database lock")
	require.Contains(t, out.String(), "database worker stalled")
	require.Contains(t, out.String(), "panic stack trace")
}

func TestSearchSemanticCommandErrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	err = Run(ctx, []string{"--config", cfgPath, "search", "--mode", "bogus", "cats"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Equal(t, 2, ExitCode(err))
	require.ErrorContains(t, err, `unsupported search mode "bogus"`)

	err = Run(ctx, []string{"--config", cfgPath, "search", "--mode", "semantic", "cats"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Equal(t, 1, ExitCode(err))
	require.ErrorContains(t, err, "embeddings are disabled")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer server.Close()
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	err = Run(ctx, []string{"--config", cfgPath, "search", "--mode", "semantic", "cats"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Equal(t, 1, ExitCode(err))
	require.ErrorContains(t, err, "embedding query failed")
}

func TestSearchHybridCommandFallsBackToFTS(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Alice",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "panic exact match",
		NormalizedContent: "panic exact match",
		RawJSON:           `{"author":{"username":"Alice"}}`,
	}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--mode", "hybrid", "panic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "panic exact match")

	okRequests := 0
	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		okRequests++
		_, _ = w.Write([]byte(`{"model":"local-model","data":[{"index":0,"embedding":[1,0]}]}`))
	}))
	defer okServer.Close()
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = okServer.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--mode", "hybrid", "panic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "panic exact match")
	require.Equal(t, 0, okRequests)

	s, err = store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, insertCLIEmbedding(ctx, s, "m1", "openai_compatible", "local-model", []float32{1, 0}))
	require.NoError(t, s.Close())

	failedRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failedRequests++
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer server.Close()
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--mode", "hybrid", "panic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "panic exact match")
	require.Equal(t, 1, failedRequests)
}

func insertCLIEmbedding(ctx context.Context, s *store.Store, messageID, provider, model string, vector []float32) error {
	blob, err := store.EncodeEmbeddingVector(vector)
	if err != nil {
		return err
	}
	_, err = s.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values(?, ?, ?, ?, ?, ?, ?)
	`, messageID, provider, model, store.EmbeddingInputVersion, len(vector), blob, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

type fakeDiscordClient struct {
	guilds []*discordgo.UserGuild
	self   *discordgo.User
}

func (f *fakeDiscordClient) Close() error { return nil }
func (f *fakeDiscordClient) Self(context.Context) (*discordgo.User, error) {
	return f.self, nil
}

func (f *fakeDiscordClient) Guilds(context.Context) ([]*discordgo.UserGuild, error) {
	return f.guilds, nil
}

func (f *fakeDiscordClient) Guild(context.Context, string) (*discordgo.Guild, error) {
	return &discordgo.Guild{}, nil
}

func (f *fakeDiscordClient) GuildChannels(context.Context, string) ([]*discordgo.Channel, error) {
	return nil, nil
}

func (f *fakeDiscordClient) ThreadsActive(context.Context, string) ([]*discordgo.Channel, error) {
	return nil, nil
}

func (f *fakeDiscordClient) GuildThreadsActive(context.Context, string) ([]*discordgo.Channel, error) {
	return nil, nil
}

func (f *fakeDiscordClient) ThreadsArchived(context.Context, string, bool) ([]*discordgo.Channel, error) {
	return nil, nil
}

func (f *fakeDiscordClient) GuildMembers(context.Context, string) ([]*discordgo.Member, error) {
	return nil, nil
}

func (f *fakeDiscordClient) ChannelMessages(context.Context, string, int, string, string) ([]*discordgo.Message, error) {
	return nil, nil
}

func (f *fakeDiscordClient) ChannelMessage(context.Context, string, string) (*discordgo.Message, error) {
	return nil, nil
}

func (f *fakeDiscordClient) Tail(context.Context, discordclient.EventHandler) error {
	return nil
}

type fakeSyncService struct {
	discovered            []*discordgo.UserGuild
	lastSync              syncer.SyncOptions
	syncCalls             int
	lastTail              []string
	lastRepair            time.Duration
	attachmentTextEnabled bool
	callTailReady         bool
	tailReadyCalls        int
	tailReady             func(context.Context) error
	tailErr               error
}

func (f *fakeSyncService) DiscoverGuilds(context.Context) ([]*discordgo.UserGuild, error) {
	return f.discovered, nil
}

func (f *fakeSyncService) Sync(_ context.Context, opts syncer.SyncOptions) (syncer.SyncStats, error) {
	f.syncCalls++
	f.lastSync = opts
	return syncer.SyncStats{Guilds: len(opts.GuildIDs), Messages: 3}, nil
}

func (f *fakeSyncService) RunTail(ctx context.Context, guildIDs []string, repairEvery time.Duration) error {
	f.lastTail = guildIDs
	f.lastRepair = repairEvery
	if f.callTailReady && f.tailReady != nil {
		f.tailReadyCalls++
		if err := f.tailReady(ctx); err != nil {
			return err
		}
	}
	if f.tailErr != nil {
		return f.tailErr
	}
	return nil
}

func (f *fakeSyncService) SetTailReadyCallback(fn func(context.Context) error) {
	f.tailReady = fn
}

func (f *fakeSyncService) SetAttachmentTextEnabled(enabled bool) {
	f.attachmentTextEnabled = enabled
}

type hybridSyncService struct {
	store         *store.Store
	sawGitMessage bool
}

func (f *hybridSyncService) DiscoverGuilds(context.Context) ([]*discordgo.UserGuild, error) {
	return []*discordgo.UserGuild{{ID: "g1"}}, nil
}

func (f *hybridSyncService) Sync(ctx context.Context, opts syncer.SyncOptions) (syncer.SyncStats, error) {
	rows, err := f.store.ListMessages(ctx, store.MessageListOptions{Channel: "general", IncludeEmpty: true})
	if err != nil {
		return syncer.SyncStats{}, err
	}
	for _, row := range rows {
		if row.Content == "automatic updates work" {
			f.sawGitMessage = true
			break
		}
	}
	if err := f.store.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}); err != nil {
		return syncer.SyncStats{}, err
	}
	if err := f.store.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}); err != nil {
		return syncer.SyncStats{}, err
	}
	if err := f.store.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m-live",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "live discord filled the delta",
		NormalizedContent: "live discord filled the delta",
		RawJSON:           `{}`,
	}); err != nil {
		return syncer.SyncStats{}, err
	}
	return syncer.SyncStats{Guilds: len(opts.GuildIDs), Messages: 1}, nil
}

func (f *hybridSyncService) RunTail(context.Context, []string, time.Duration) error {
	return nil
}

func TestRuntimeInitSyncTailAndDoctor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	fakeDiscord := &fakeDiscordClient{
		guilds: []*discordgo.UserGuild{{ID: "g1"}, {ID: "g2"}},
		self:   &discordgo.User{ID: "bot"},
	}
	fakeSync := &fakeSyncService{discovered: fakeDiscord.guilds}

	newRuntime := func() *runtime {
		return &runtime{
			ctx:        ctx,
			configPath: cfgPath,
			stdout:     &bytes.Buffer{},
			stderr:     &bytes.Buffer{},
			logger:     discardLogger(),
			openStore:  store.Open,
			newDiscord: func(config.Config) (discordClient, error) { return fakeDiscord, nil },
			newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
				return fakeSync
			},
		}
	}

	rt := newRuntime()
	require.NoError(t, rt.runInit([]string{"--db", dbPath, "--with-embeddings", "--guild", "g2"}))

	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	require.Equal(t, []string{"g1", "g2"}, cfg.GuildIDs)
	require.Equal(t, "g2", cfg.DefaultGuildID)
	require.True(t, cfg.Search.Embeddings.Enabled)
	cfg.Desktop.Path = filepath.Join(dir, "empty-discord")
	require.NoError(t, os.MkdirAll(cfg.Desktop.Path, 0o755))
	require.NoError(t, config.Write(cfgPath, cfg))

	rt = newRuntime()
	require.NoError(t, rt.withServices(true, func() error { return rt.runSync([]string{"--guilds", "g2"}) }))
	require.Equal(t, []string{"g2"}, fakeSync.lastSync.GuildIDs)
	require.True(t, fakeSync.lastSync.LatestOnly)
	require.True(t, fakeSync.lastSync.SkipMembers)
	require.True(t, fakeSync.attachmentTextEnabled)

	rt = newRuntime()
	require.NoError(t, rt.withServices(true, func() error { return rt.runSync([]string{"--all"}) }))
	require.Nil(t, fakeSync.lastSync.GuildIDs)
	require.True(t, fakeSync.lastSync.LatestOnly)
	require.True(t, fakeSync.lastSync.SkipMembers)

	rt = newRuntime()
	require.NoError(t, rt.withServices(true, func() error { return rt.runSync([]string{"--guilds", "g2", "--all-channels"}) }))
	require.Equal(t, []string{"g2"}, fakeSync.lastSync.GuildIDs)
	require.False(t, fakeSync.lastSync.LatestOnly)
	require.False(t, fakeSync.lastSync.SkipMembers)

	rt = newRuntime()
	require.NoError(t, rt.withServices(true, func() error { return rt.runTail([]string{"--repair-every", "30s"}) }))
	require.Equal(t, []string{"g2"}, fakeSync.lastTail)
	require.Equal(t, 30*time.Second, fakeSync.lastRepair)

	rt = newRuntime()
	var out bytes.Buffer
	rt.stdout = &out
	require.NoError(t, rt.runDoctor(nil))
	require.Contains(t, out.String(), "discord_auth=ok")
}

func TestSyncModeDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		full           bool
		allChannels    bool
		since          string
		channels       string
		defaultLatest  bool
		latestOnly     bool
		skipMembers    bool
		explicitLatest bool
		explicitSkip   bool
	}{
		{name: "routine", defaultLatest: true, latestOnly: true, skipMembers: true},
		{name: "all channels", allChannels: true},
		{name: "full", full: true},
		{name: "since", since: "2026-04-27T20:00:00Z"},
		{name: "channels", channels: "c1"},
		{name: "explicit latest", allChannels: true, explicitLatest: true, latestOnly: true},
		{name: "explicit skip members", allChannels: true, explicitSkip: true, skipMembers: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			defaultLatest := defaultLatestSyncMode(tt.full, tt.allChannels, tt.since, tt.channels)
			require.Equal(t, tt.defaultLatest, defaultLatest)
			require.Equal(t, tt.latestOnly, syncLatestOnly(tt.explicitLatest, defaultLatest))
			require.Equal(t, tt.skipMembers, syncSkipsMembers(tt.explicitSkip, defaultLatest))
		})
	}
}

func TestDoctorChecksEnabledLocalEmbeddingProvider(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/embed", r.URL.Path)
		_, _ = w.Write([]byte(`{"model":"nomic-embed-text","embeddings":[[1,2,3]]}`))
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "ollama"
	cfg.Search.Embeddings.Model = "nomic-embed-text"
	cfg.Search.Embeddings.BaseURL = server.URL
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.runDoctor(nil))
	require.Contains(t, out.String(), "embeddings=ok")
	require.Contains(t, out.String(), "embeddings_provider=ollama")
	require.Contains(t, out.String(), "embeddings_probe=ok")
}

func TestDoctorReportsEmbeddingProviderWarningsNonFatally(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv("OPENAI_API_KEY", "")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai"
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.runDoctor(nil))
	require.Contains(t, out.String(), "embeddings=warning")
	require.Contains(t, out.String(), "embeddings_warning=embedding provider \"openai\" requires API key env OPENAI_API_KEY")
}

func TestDoctorReportsUnsupportedEmbeddingProviderNonFatally(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "bogus"
	cfg.Search.Embeddings.Model = "custom"
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.runDoctor(nil))
	require.Contains(t, out.String(), "embeddings=warning")
	require.Contains(t, out.String(), "embeddings_warning=unsupported embedding provider \"bogus\"")
}

func TestRuntimeConfiguresAttachmentTextOnSyncer(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Sync.AttachmentText = nil
	require.NoError(t, config.Write(cfgPath, cfg))

	fakeSync := &fakeSyncService{}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}
	require.NoError(t, rt.withServices(true, func() error { return nil }))
	require.True(t, fakeSync.attachmentTextEnabled)

	cfg.Sync.AttachmentText = new(false)
	require.NoError(t, config.Write(cfgPath, cfg))
	require.NoError(t, rt.withServices(true, func() error { return nil }))
	require.False(t, fakeSync.attachmentTextEnabled)
}

func TestSQLRejectsMutationsByDefaultAndAllowsUnsafeConfirm(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.Close())

	err = Run(ctx, []string{"--config", cfgPath, "sql", "delete from messages"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "only read-only sql is allowed")

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "sql", "--unsafe", "--confirm", "select count(*) as total from messages"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "total")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "sql", "--unsafe", "--confirm", "delete from messages"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "rows_affected=1")

	s, err = store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from messages")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestCommandUsageBranches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	cases := []struct {
		args []string
		want string
	}{
		{[]string{"--config", cfgPath, "sql", "--confirm", "select 1"}, "--confirm requires --unsafe"},
		{[]string{"--config", cfgPath, "sql", "--unsafe", "delete from messages"}, "--unsafe requires --confirm"},
		{[]string{"--config", cfgPath, "search"}, "search requires a query"},
		{[]string{"--config", cfgPath, "search", "--dm", "--guild", "g1", "panic"}, "use either --dm or --guild/--guilds"},
		{[]string{"--config", cfgPath, "messages", "--dm", "--guild", "g1"}, "use either --dm or --guild/--guilds"},
		{[]string{"--config", cfgPath, "messages", "--dm", "--sync"}, "messages --sync is not supported with --dm"},
		{[]string{"--config", cfgPath, "dms", "extra"}, "dms takes flags only"},
		{[]string{"--config", cfgPath, "wiretap", "extra"}, "wiretap takes flags only"},
		{[]string{"--config", cfgPath, "wiretap", "--max-file-bytes", "0"}, "--max-file-bytes must be positive"},
		{[]string{"--config", cfgPath, "wiretap", "--watch-every", "1ms"}, "--watch-every must be at least 1s"},
		{[]string{"--config", cfgPath, "members"}, "members requires a subcommand"},
		{[]string{"--config", cfgPath, "members", "search"}, "members search requires a query"},
		{[]string{"--config", cfgPath, "members", "bogus"}, `unknown members subcommand "bogus"`},
		{[]string{"--config", cfgPath, "channels"}, "channels requires a subcommand"},
		{[]string{"--config", cfgPath, "channels", "bogus"}, `unknown channels subcommand "bogus"`},
		{[]string{"--config", cfgPath, "status", "extra"}, "status takes no arguments"},
		{[]string{"--config", cfgPath, "report", "extra"}, "report takes no positional arguments"},
		{[]string{"--config", cfgPath, "embed"}, "embeddings are disabled"},
		{[]string{"--config", cfgPath, "embed", "--limit", "0"}, "--limit must be positive"},
		{[]string{"--config", cfgPath, "embed", "--batch-size", "0"}, "--batch-size must be positive"},
		{[]string{"--config", cfgPath, "publish", "extra"}, "publish takes no positional arguments"},
		{[]string{"--config", cfgPath, "update", "extra"}, "update takes no positional arguments"},
		{[]string{"--config", cfgPath, "subscribe"}, "subscribe requires one remote"},
		{[]string{"--config", cfgPath, "subscribe", "one", "two"}, "subscribe requires one remote"},
	}
	for _, tc := range cases {
		err := Run(ctx, tc.args, &bytes.Buffer{}, &bytes.Buffer{})
		require.Equal(t, 2, ExitCode(err), tc.args)
		require.ErrorContains(t, err, tc.want, tc.args)
	}
}

func TestCommandHelpDoesNotOpenConfigOrStore(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"--config", filepath.Join(t.TempDir(), "missing.toml"), "help", "search"},
		{"--config", filepath.Join(t.TempDir(), "missing.toml"), "search", "--help"},
		{"--config", filepath.Join(t.TempDir(), "missing.toml"), "messages", "--help"},
		{"--config", filepath.Join(t.TempDir(), "missing.toml"), "sql", "--help"},
	} {
		var stdout, stderr bytes.Buffer
		require.NoError(t, Run(context.Background(), args, &stdout, &stderr), "args=%v", args)
		require.Contains(t, stdout.String(), "Usage:", "args=%v", args)
		require.Empty(t, stderr.String(), "args=%v", args)
	}

	err := Run(context.Background(), []string{"help", "wat"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Error(t, err)
	require.Contains(t, err.Error(), `unknown help topic "wat"`)
}

func TestHelpers(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"a", "b"}, csvList("a,b,a"))
	require.Equal(t, "x", (&cliError{code: 2, err: assertErr("x")}).Error())
	mode, err := syncShareUpdateMode([]string{"--all"})
	require.NoError(t, err)
	require.Equal(t, shareUpdateNever, mode)
	mode, err = syncShareUpdateMode([]string{"--update=auto"})
	require.NoError(t, err)
	require.Equal(t, shareUpdateAuto, mode)
	mode, err = syncShareUpdateMode([]string{"--update", "force"})
	require.NoError(t, err)
	require.Equal(t, shareUpdateForce, mode)
	_, err = syncShareUpdateMode([]string{"--update"})
	require.Error(t, err)
	require.Equal(t, 2, ExitCode(usageErr(assertErr("x"))))
	require.Equal(t, 4, ExitCode(authErr(assertErr("x"))))
	require.Equal(t, 5, ExitCode(dbErr(assertErr("x"))))
	require.Equal(t, 3, ExitCode(configErr(assertErr("x"))))
	require.Equal(t, 1, ExitCode(assertErr("x")))
	require.True(t, hybridSemanticUnavailable(store.ErrNoCompatibleEmbeddings))
	require.True(t, hybridSemanticUnavailable(assertErr("semantic query embedding missing")))
	require.False(t, hybridSemanticUnavailable(assertErr("other")))
	opts, err := shareOptionsFromFlags("~/share", "git@example.com:org/archive.git", "")
	require.NoError(t, err)
	require.Equal(t, "git@example.com:org/archive.git", opts.Remote)
	require.Equal(t, "main", opts.Branch)
	var out bytes.Buffer
	require.NoError(t, printHuman(&out, syncer.SyncStats{Guilds: 1}))
	require.Contains(t, out.String(), "guilds=1")
	require.Contains(t, formatTime(time.Unix(1, 0).UTC()), "1970")
	require.Equal(t, "x", firstNonEmpty("", "x", "y"))
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestRuntimeHelpersAndSubcommands(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "dm1", GuildID: store.DirectMessageGuildID, Kind: "dm", Name: "Alice", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u1", Username: "peter", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	base := time.Date(2026, 3, 8, 10, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{
		{
			Record: store.MessageRecord{
				ID:                "m1",
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "general",
				AuthorID:          "u1",
				AuthorName:        "peter",
				CreatedAt:         base.Format(time.RFC3339Nano),
				Content:           "hello <@u1> in <#c1>",
				NormalizedContent: "hello <@u1> in <#c1>",
				RawJSON:           `{"author":{"username":"peter"}}`,
			},
			Mentions: []store.MentionEventRecord{{
				MessageID:  "m1",
				GuildID:    "g1",
				ChannelID:  "c1",
				AuthorID:   "u1",
				TargetType: "user",
				TargetID:   "u1",
				TargetName: "peter",
				EventAt:    base.Format(time.RFC3339Nano),
			}},
		},
		{
			Record: store.MessageRecord{
				ID:                "dm-msg",
				GuildID:           store.DirectMessageGuildID,
				ChannelID:         "dm1",
				ChannelName:       "Alice",
				AuthorID:          "u2",
				AuthorName:        "Alice",
				CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
				Content:           "private hello",
				NormalizedContent: "private hello",
				RawJSON:           `{"source":"discord_desktop"}`,
			},
		},
	}))
	require.NoError(t, s.Close())

	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.withServices(false, func() error {
		require.Equal(t, []string{"g1"}, rt.resolveSyncGuilds("", ""))
		require.Nil(t, rt.resolveSearchGuilds("", ""))
		require.NoError(t, rt.runMembers([]string{"show", "u1"}))
		require.NoError(t, rt.runMembers([]string{"search", "pet"}))
		require.NoError(t, rt.runMembers([]string{"list"}))
		rt.now = func() time.Time { return time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC) }
		require.NoError(t, rt.runMessages([]string{"--channel", "#general", "--hours", "6", "--last", "1"}))
		require.NoError(t, rt.runMessages([]string{"--channel", "#general", "--days", "7", "--all"}))
		require.NoError(t, rt.runMessages([]string{"--channel", "#general", "--days", "7", "--all", "--include-empty"}))
		require.NoError(t, rt.runMessages([]string{"--channel", "#general", "--since", "2026-03-08T00:00:00Z", "--before", "2026-03-09T00:00:00Z", "--limit", "1"}))
		require.NoError(t, rt.runMessages([]string{"--dm", "--channel", "Alice", "--last", "1"}))
		require.NoError(t, rt.runDirectMessages([]string{"--list"}))
		require.NoError(t, rt.runDirectMessages([]string{"--with", "Alice", "--search", "private", "--limit", "1"}))
		require.NoError(t, rt.runDirectMessages([]string{"--with", "Alice", "--since", "2026-03-08T00:00:00Z", "--before", "2026-03-09T00:00:00Z", "--all"}))
		require.NoError(t, rt.runMentions([]string{"--channel", "#general", "--target", "u2"}))
		require.NoError(t, rt.runMentions([]string{"--channel", "#general", "--days", "7", "--type", "user"}))
		require.NoError(t, rt.runDigest([]string{"--since", "12h", "--channel", "general", "--top-n", "2"}))
		require.NoError(t, rt.runReport([]string{"--readme", filepath.Join(dir, "README.md")}))
		require.NoError(t, rt.runSearch([]string{"--include-empty", "Peter"}))
		require.NoError(t, rt.runChannels([]string{"show", "c1"}))
		require.NoError(t, rt.runChannels([]string{"list"}))
		require.NoError(t, rt.runStatus(nil))
		require.NoError(t, rt.runAnalytics([]string{}))
		require.NoError(t, rt.runTUI([]string{"--json", "--limit", "1", "--include-empty"}))
		require.NoError(t, rt.runAnalytics([]string{"quiet", "--since", "1d"}))
		require.NoError(t, rt.runAnalytics([]string{"trends", "--weeks", "1", "--channel", "general"}))
		return nil
	}))
}

func TestRunInitWritesDiscoveredGuildConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	fakeSync := &fakeSyncService{discovered: []*discordgo.UserGuild{{ID: "g1"}, {ID: "g2"}}}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}

	require.NoError(t, rt.runInit([]string{"--db", dbPath, "--guild", "g2", "--with-embeddings"}))
	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	require.Equal(t, dbPath, cfg.DBPath)
	require.Equal(t, []string{"g1", "g2"}, cfg.GuildIDs)
	require.Equal(t, "g2", cfg.DefaultGuildID)
	require.True(t, cfg.Search.Embeddings.Enabled)
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "g2")
}

func TestRunInitRejectsUnknownDefaultGuild(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	fakeSync := &fakeSyncService{discovered: []*discordgo.UserGuild{{ID: "g1"}}}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}

	err := rt.runInit([]string{"--db", dbPath, "--guild", "missing"})
	require.Equal(t, 2, ExitCode(err))
	require.ErrorContains(t, err, "guild missing is not accessible")
}

func TestRunMembersShowUsesDefaultGuildForAmbiguousQuery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "same",
		DisplayName: "Same",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"github":"steipete"}`,
	}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g2",
		UserID:      "u2",
		Username:    "same",
		DisplayName: "Same",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"github":"other"}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Same",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.withServices(false, func() error {
		return rt.runMembers([]string{"show", "same"})
	}))
	require.Contains(t, out.String(), "guild=g1")
	require.Contains(t, out.String(), "github=steipete")
}

func TestRunMembersShowReturnsListWhenStillAmbiguous(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u1", Username: "same", DisplayName: "Same", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g2", UserID: "u2", Username: "same", DisplayName: "Same", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.withServices(false, func() error {
		return rt.runMembers([]string{"show", "same"})
	}))
	require.Contains(t, out.String(), "GUILD")
	require.Contains(t, out.String(), "u1")
	require.Contains(t, out.String(), "u2")
}

func TestRunMessagesSyncTargetsResolvedChannel(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.Close())

	fakeSync := &fakeSyncService{}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}
	rt.now = func() time.Time { return time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC) }

	require.NoError(t, rt.withServices(true, func() error {
		return rt.runMessages([]string{"--channel", "#general", "--hours", "6", "--last", "1", "--sync"})
	}))
	require.Equal(t, []string{"g1"}, fakeSync.lastSync.GuildIDs)
	require.Equal(t, []string{"c1"}, fakeSync.lastSync.ChannelIDs)
}

func TestRunMessagesSyncFallsBackToGuildSyncForUnknownChannel(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	fakeSync := &fakeSyncService{}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}

	require.NoError(t, rt.withServices(true, func() error {
		return rt.runMessages([]string{"--channel", "new-channel", "--days", "1", "--sync"})
	}))
	require.Equal(t, []string{"g1"}, fakeSync.lastSync.GuildIDs)
	require.Empty(t, fakeSync.lastSync.ChannelIDs)
}

func TestRunMentionsValidation(t *testing.T) {
	t.Parallel()

	rt := &runtime{stderr: &bytes.Buffer{}}
	rt.now = func() time.Time { return time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC) }

	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"extra"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--hours", "-1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--days", "-1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--hours", "1", "--days", "1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--hours", "1", "--since", "2026-03-01T00:00:00Z"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--limit", "-1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--last", "-1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--all", "--last", "1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--limit", "1", "--last", "1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--since", "bad"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--before", "bad"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--hours", "-1", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--hours", "1", "--days", "1", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--hours", "1", "--since", "2026-03-01T00:00:00Z", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--last", "-1", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--last", "1", "--limit", "20", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--last", "1", "--all", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMentions([]string{"--days", "-1", "--target", "u1"})))
	require.Equal(t, 2, ExitCode(rt.runMentions([]string{"--days", "1", "--since", "2026-03-01T00:00:00Z", "--target", "u1"})))
	require.Equal(t, 2, ExitCode(rt.runMentions([]string{"--since", "bad", "--target", "u1"})))
	require.Equal(t, 2, ExitCode(rt.runMentions([]string{"--type", "nope", "--target", "u1"})))
	require.Equal(t, 2, ExitCode(rt.runMentions([]string{})))
}

func TestPrintJSONAndPlain(t *testing.T) {
	t.Parallel()

	rt := &runtime{stdout: &bytes.Buffer{}, json: true}
	require.NoError(t, rt.print(map[string]any{"ok": true}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "\"ok\": true")

	rt = &runtime{stdout: &bytes.Buffer{}, plain: true}
	require.NoError(t, rt.print([]store.ChannelRow{{GuildID: "g1", ID: "c1", Kind: "text", Name: "general"}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "general")

	rt = &runtime{stdout: &bytes.Buffer{}}
	require.NoError(t, rt.print([]store.SearchResult{{GuildID: "g1", ChannelName: "general", AuthorName: "Peter", Content: "hello"}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "hello")

	rt = &runtime{stdout: &bytes.Buffer{}}
	require.NoError(t, rt.print([]store.MentionRow{{GuildID: "g1", ChannelName: "general", AuthorName: "Peter", TargetType: "user", TargetName: "Shadow", Content: "hello"}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "Shadow")

	rt = &runtime{stdout: &bytes.Buffer{}, plain: true}
	require.NoError(t, rt.print([]store.MemberRow{{GuildID: "g1", UserID: "u1", Username: "peter"}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "peter")

	rt = &runtime{stdout: &bytes.Buffer{}, plain: true}
	require.NoError(t, rt.print([]store.SearchResult{{GuildID: "g1", ChannelID: "c1", AuthorID: "u1", Content: "hello"}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "hello")

	rt = &runtime{stdout: &bytes.Buffer{}, plain: true}
	require.NoError(t, rt.print([]store.MessageRow{{GuildID: "g1", ChannelID: "c1", AuthorID: "u1", MessageID: "m1", Content: "hello", CreatedAt: time.Unix(1, 0).UTC()}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "m1")

	rt = &runtime{stdout: &bytes.Buffer{}, plain: true}
	require.NoError(t, rt.print([]store.MentionRow{{GuildID: "g1", ChannelID: "c1", AuthorID: "u1", TargetType: "user", TargetID: "u2", Content: "hello", CreatedAt: time.Unix(1, 0).UTC()}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "u2")

	rt = &runtime{stdout: &bytes.Buffer{}}
	require.NoError(t, rt.print(struct{ OK bool }{OK: true}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "\"OK\": true")
}

func TestWithServicesErrors(t *testing.T) {
	t.Parallel()

	rt := &runtime{ctx: context.Background(), configPath: filepath.Join(t.TempDir(), "missing.toml"), stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}}
	err := rt.withServices(false, func() error { return nil })
	require.Equal(t, 3, ExitCode(err))

	cfgPath := filepath.Join(t.TempDir(), "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "discrawl.db")
	require.NoError(t, config.Write(cfgPath, cfg))
	rt = &runtime{
		ctx:        context.Background(),
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		openStore: func(context.Context, string) (*store.Store, error) {
			return nil, assertErr("db")
		},
	}
	err = rt.withServices(false, func() error { return nil })
	require.Equal(t, 5, ExitCode(err))

	rt = &runtime{
		ctx:        context.Background(),
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) { return nil, assertErr("auth") },
	}
	err = rt.withServices(true, func() error { return nil })
	require.Equal(t, 4, ExitCode(err))
}

func TestCommandUsageErrors(t *testing.T) {
	t.Parallel()

	rt := &runtime{}
	require.Equal(t, 2, ExitCode(rt.runMembers(nil)))
	require.Equal(t, 2, ExitCode(rt.runMembers([]string{"nope"})))
	require.Equal(t, 2, ExitCode(rt.runMessages(nil)))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--days", "-1"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--days", "1", "--since", "2026-03-01T00:00:00Z"})))
	require.Equal(t, 2, ExitCode(rt.runSync([]string{"--all", "--guild", "g1"})))
	require.Equal(t, 2, ExitCode(rt.runSync([]string{"--update", "bogus"})))
	require.Equal(t, 2, ExitCode(rt.runSync([]string{"--update=force", "--no-update"})))
	require.Equal(t, 2, ExitCode(rt.runChannels(nil)))
	require.Equal(t, 2, ExitCode(rt.runStatus([]string{"extra"})))
	require.NoError(t, (&runtime{stdout: &bytes.Buffer{}}).runDoctor(nil))
}

func TestRunSQLReadsStdin(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()
	file, err := os.CreateTemp(dir, "stdin.sql")
	require.NoError(t, err)
	_, err = file.WriteString("select 1 as one")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	file, err = os.Open(file.Name())
	require.NoError(t, err)
	os.Stdin = file

	rt := &runtime{ctx: ctx, configPath: cfgPath, stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}, logger: discardLogger()}
	require.NoError(t, rt.withServices(false, func() error { return rt.runSQL([]string{"-"}) }))
}
