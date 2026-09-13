package discorddesktop

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

const (
	DirectMessageGuildID   = "@me"
	DirectMessageGuildName = "Discord Direct Messages"
	defaultMaxFileBytes    = 64 << 20
	maxObjectBytes         = 4 << 20
	cacheSniffBytes        = 1 << 20
	checkpointEveryFiles   = 256
)

var (
	channelRouteRE     = regexp.MustCompile(`/channels/(@me|[0-9]{12,24})/([0-9]{12,24})`)
	apiMessagesRouteRE = regexp.MustCompile(`/api/v[0-9]+/channels/[0-9]{12,24}/messages`)
)

type Options struct {
	Path         string
	MaxFileBytes int64
	DryRun       bool
	FullCache    bool
	Now          func() time.Time
}

type Stats struct {
	Path                  string    `json:"path"`
	FilesVisited          int       `json:"files_visited"`
	FilesScanned          int       `json:"files_scanned"`
	FilesSkipped          int       `json:"files_skipped"`
	FilesUnchanged        int       `json:"files_unchanged"`
	CacheFilesFastSkipped int       `json:"cache_files_fast_skipped"`
	BytesScanned          int64     `json:"bytes_scanned"`
	JSONObjects           int       `json:"json_objects"`
	Guilds                int       `json:"guilds"`
	Channels              int       `json:"channels"`
	Messages              int       `json:"messages"`
	DMMessages            int       `json:"dm_messages"`
	DMChannels            int       `json:"dm_channels"`
	GuildMessages         int       `json:"guild_messages"`
	SkippedMessages       int       `json:"skipped_messages"`
	SkippedChannels       int       `json:"skipped_channels"`
	Checkpoints           int       `json:"checkpoints"`
	DryRun                bool      `json:"dry_run,omitempty"`
	FullCache             bool      `json:"full_cache,omitempty"`
	StartedAt             time.Time `json:"started_at"`
	FinishedAt            time.Time `json:"finished_at"`
}

func DefaultPath() string {
	home, _ := os.UserHomeDir()
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "discord")
	case "windows":
		if appData := strings.TrimSpace(os.Getenv("APPDATA")); appData != "" {
			return filepath.Join(appData, "discord")
		}
		return filepath.Join(home, "AppData", "Roaming", "discord")
	default:
		if configHome := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); configHome != "" {
			return filepath.Join(configHome, "discord")
		}
		return filepath.Join(home, ".config", "discord")
	}
}

func Import(ctx context.Context, st *store.Store, opts Options) (Stats, error) {
	if st == nil && !opts.DryRun {
		return Stats{}, errors.New("store is required")
	}
	state, err := loadScanState(ctx, st, opts)
	if err != nil {
		return Stats{}, err
	}
	if opts.FullCache {
		stats, snap, err := scanFullCache(ctx, opts, state)
		if err != nil {
			return stats, err
		}
		stats.DryRun = opts.DryRun
		if opts.DryRun {
			return stats, nil
		}
		if err := writeSnapshot(ctx, st, snap, len(state.previous) == 0); err != nil {
			return stats, err
		}
		if err := saveFileIndex(ctx, st, state.current); err != nil {
			return stats, err
		}
		stats.Checkpoints = 1
		if err := saveCoverageStats(ctx, st, stats); err != nil {
			return stats, err
		}
		return stats, nil
	}
	stats, err := scanAndImport(ctx, st, opts, state)
	if err != nil {
		return stats, err
	}
	stats.DryRun = opts.DryRun
	if !opts.DryRun {
		if err := saveCoverageStats(ctx, st, stats); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func saveCoverageStats(ctx context.Context, st *store.Store, stats Stats) error {
	return st.SetWiretapImportStats(ctx, store.WiretapImportStats{
		FilesScanned:    stats.FilesScanned,
		Messages:        stats.Messages,
		Channels:        stats.Channels,
		SkippedMessages: stats.SkippedMessages,
		SkippedChannels: stats.SkippedChannels,
		StartedAt:       stats.StartedAt,
		FinishedAt:      stats.FinishedAt,
	})
}
