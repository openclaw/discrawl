package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/openclaw/crawlkit/control"
	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
)

func (r *runtime) runMetadata(args []string) error {
	fs := flag.NewFlagSet("metadata", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("metadata takes flags only"))
	}
	if *jsonOut {
		r.json = true
	}
	cfg := config.Default()
	manifest := control.NewManifest("discrawl", "Discord Crawl", "discrawl")
	manifest.Description = "Local-first Discord archive crawler."
	manifest.Branding = control.Branding{SymbolName: "bubble.left.and.bubble.right.fill", AccentColor: "#5865f2", BundleIdentifier: "com.hnc.Discord"}
	manifest.Paths = control.Paths{
		DefaultConfig:   config.ResolvePath(""),
		ConfigEnv:       config.DefaultConfigEnv,
		DefaultDatabase: cfg.DBPath,
		DefaultCache:    cfg.CacheDir,
		DefaultLogs:     cfg.LogDir,
		DefaultShare:    cfg.Share.RepoPath,
	}
	manifest.Capabilities = []string{"metadata", "status", "diagnostics", "doctor", "sync", "tap", "tui", "git-share", "cloud-remote", "cloud-publish", "sql", "embeddings"}
	manifest.Privacy = control.Privacy{ContainsPrivateMessages: true, ExportsSecrets: false, LocalOnlyScopes: []string{"discord", "desktop-cache", "sqlite", "git-share"}}
	manifest.Commands = map[string]control.Command{
		"status":          {Title: "Status", Argv: []string{"discrawl", "status", "--json"}, JSON: true},
		"diagnostics":     {Title: "Diagnostics", Argv: []string{"discrawl", "diagnostics", "--json"}, JSON: true},
		"check-update":    {Title: "Check for updates", Argv: []string{"discrawl", "check-update", "--json"}, JSON: true},
		"doctor":          {Title: "Doctor", Argv: []string{"discrawl", "doctor", "--json"}, JSON: true},
		"sync":            {Title: "Sync", Argv: []string{"discrawl", "--json", "sync"}, JSON: true, Mutates: true},
		"tap":             {Title: "Import desktop cache", Argv: []string{"discrawl", "--json", "tap"}, JSON: true, Mutates: true},
		"cache-import":    {Title: "Import desktop cache", Argv: []string{"discrawl", "--json", "cache-import"}, JSON: true, Mutates: true},
		"wiretap":         {Title: "Legacy desktop cache import", Argv: []string{"discrawl", "--json", "wiretap"}, JSON: true, Mutates: true, Legacy: true, Deprecated: true},
		"tui":             {Title: "Terminal browser", Argv: []string{"discrawl", "tui"}},
		"tui-json":        {Title: "Terminal browser rows", Argv: []string{"discrawl", "tui", "--json"}, JSON: true},
		"publish":         {Title: "Publish share", Argv: []string{"discrawl", "--json", "publish"}, JSON: true, Mutates: true},
		"cloud-publish":   {Title: "Publish cloud archive", Argv: []string{"discrawl", "--json", "cloud", "publish"}, JSON: true, Mutates: true},
		"subscribe":       {Title: "Subscribe share", Argv: []string{"discrawl", "--json", "subscribe"}, JSON: true, Mutates: true},
		"subscribe-cloud": {Title: "Subscribe cloud archive", Argv: []string{"discrawl", "--json", "subscribe-cloud"}, JSON: true, Mutates: true},
		"remote-status":   {Title: "Remote status", Argv: []string{"discrawl", "--json", "remote", "status"}, JSON: true},
		"remote-archives": {Title: "Remote archives", Argv: []string{"discrawl", "--json", "remote", "archives"}, JSON: true},
		"remote-login":    {Title: "Remote GitHub login", Argv: []string{"discrawl", "--json", "remote", "login"}, JSON: true, Mutates: true},
		"whoami":          {Title: "Remote identity", Argv: []string{"discrawl", "--json", "whoami"}, JSON: true},
		"update":          {Title: "Update share", Argv: []string{"discrawl", "--json", "update"}, JSON: true, Mutates: true},
	}
	return r.print(manifest)
}

func controlStatus(configPath string, cfg config.Config, status store.Status, shareNeedsUpdate bool) control.Status {
	counts := []control.Count{
		control.NewCount("guilds", "Guilds", int64(status.GuildCount)),
		control.NewCount("channels", "Channels", int64(status.ChannelCount)),
		control.NewCount("threads", "Threads", int64(status.ThreadCount)),
		control.NewCount("messages", "Messages", int64(status.MessageCount)),
		control.NewCount("members", "Members", int64(status.MemberCount)),
		control.NewCount("embedding_backlog", "Embedding backlog", int64(status.EmbeddingBacklog)),
	}
	out := control.NewStatus("discrawl", fmt.Sprintf("%d messages across %d channels", status.MessageCount, status.ChannelCount))
	out.State = "current"
	out.ConfigPath = configPath
	out.DatabasePath = status.DBPath
	out.Counts = counts
	if !status.LastSyncAt.IsZero() {
		out.LastSyncAt = status.LastSyncAt.UTC().Format(time.RFC3339)
	}
	db := control.SQLiteDatabase("primary", "Discord archive", "archive", status.DBPath, true, counts)
	out.DatabaseBytes = db.Bytes
	out.WALBytes = fileSize(status.DBPath + "-wal")
	out.Databases = []control.Database{db}
	out.Share = &control.Share{
		Enabled:     cfg.ShareEnabled(),
		RepoPath:    cfg.Share.RepoPath,
		Remote:      cfg.Share.Remote,
		Branch:      cfg.Share.Branch,
		NeedsUpdate: shareNeedsUpdate,
	}
	return out
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
