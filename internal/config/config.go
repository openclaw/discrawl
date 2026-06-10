package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	crawlconfig "github.com/openclaw/crawlkit/config"
	crawlremote "github.com/openclaw/crawlkit/remote"
)

const (
	DefaultConfigEnv           = "DISCRAWL_CONFIG"
	DefaultTokenEnv            = "DISCORD_BOT_TOKEN"
	DefaultRemoteTokenEnv      = "DISCRAWL_REMOTE_TOKEN"
	DefaultTokenKeyringService = "discrawl"
	DefaultTokenKeyringAccount = "discord_bot_token"
)

type Config struct {
	Version        int                `toml:"version"`
	GuildID        string             `toml:"guild_id,omitempty"`
	DefaultGuildID string             `toml:"default_guild_id,omitempty"`
	GuildIDs       []string           `toml:"guild_ids,omitempty"`
	DBPath         string             `toml:"db_path"`
	CacheDir       string             `toml:"cache_dir"`
	LogDir         string             `toml:"log_dir"`
	Discord        DiscordConfig      `toml:"discord"`
	Desktop        DesktopConfig      `toml:"desktop"`
	Sync           SyncConfig         `toml:"sync"`
	Search         SearchConfig       `toml:"search"`
	Share          ShareConfig        `toml:"share"`
	Remote         crawlremote.Config `toml:"remote"`
}

type DiscordConfig struct {
	TokenSource         string `toml:"token_source"`
	TokenEnv            string `toml:"token_env"`
	TokenKeyringService string `toml:"token_keyring_service"`
	TokenKeyringAccount string `toml:"token_keyring_account"`
}

type DesktopConfig struct {
	Path         string `toml:"path"`
	MaxFileBytes int64  `toml:"max_file_bytes"`
	FullCache    bool   `toml:"full_cache"`
}

type SyncConfig struct {
	Source             string `toml:"source"`
	Concurrency        int    `toml:"concurrency"`
	RepairEvery        string `toml:"repair_every"`
	FullHistory        bool   `toml:"full_history"`
	AttachmentText     *bool  `toml:"attachment_text"`
	AttachmentMedia    *bool  `toml:"attachment_media"`
	MaxAttachmentBytes int64  `toml:"max_attachment_bytes"`
}

type SearchConfig struct {
	DefaultMode string           `toml:"default_mode"`
	Embeddings  EmbeddingsConfig `toml:"embeddings"`
}

type ShareConfig struct {
	Remote     string            `toml:"remote,omitempty"`
	RepoPath   string            `toml:"repo_path,omitempty"`
	Branch     string            `toml:"branch,omitempty"`
	AutoUpdate bool              `toml:"auto_update"`
	StaleAfter string            `toml:"stale_after"`
	Media      *bool             `toml:"media"`
	Filter     ShareFilterConfig `toml:"filter"`
}

type ShareFilterConfig struct {
	PublicOnly        bool     `toml:"public_only"`
	IncludeChannelIDs []string `toml:"include_channel_ids,omitempty"`
	ExcludeChannelIDs []string `toml:"exclude_channel_ids,omitempty"`
}

type EmbeddingsConfig struct {
	Enabled        bool   `toml:"enabled"`
	Provider       string `toml:"provider"`
	Model          string `toml:"model"`
	BaseURL        string `toml:"base_url"`
	APIKeyEnv      string `toml:"api_key_env"`
	BatchSize      int    `toml:"batch_size"`
	MaxInputChars  int    `toml:"max_input_chars"`
	RequestTimeout string `toml:"request_timeout"`
	VectorBackend  string `toml:"vector_backend"`
}

type TokenResolution struct {
	Token  string
	Source string
	Path   string
}

var appConfig = crawlconfig.App{
	Name:          "discrawl",
	ConfigEnv:     DefaultConfigEnv,
	LegacyBaseDir: "~/.discrawl",
	PlatformDirs:  true,
}

func Default() Config {
	home, _ := os.UserHomeDir()
	paths, err := appConfig.DefaultPaths()
	if err != nil {
		base := filepath.Join(home, ".discrawl")
		paths = crawlconfig.Paths{
			DBPath:   filepath.Join(base, "discrawl.db"),
			CacheDir: filepath.Join(base, "cache"),
			LogDir:   filepath.Join(base, "logs"),
			ShareDir: filepath.Join(base, "share"),
		}
	}
	return Config{
		Version:        1,
		DBPath:         paths.DBPath,
		CacheDir:       paths.CacheDir,
		LogDir:         paths.LogDir,
		DefaultGuildID: "",
		Discord: DiscordConfig{
			TokenSource:         "env",
			TokenEnv:            DefaultTokenEnv,
			TokenKeyringService: DefaultTokenKeyringService,
			TokenKeyringAccount: DefaultTokenKeyringAccount,
		},
		Desktop: DesktopConfig{
			Path:         defaultDiscordDesktopPath(home),
			MaxFileBytes: 64 << 20,
		},
		Sync: SyncConfig{
			Source:             "both",
			Concurrency:        defaultSyncConcurrency(),
			RepairEvery:        "6h",
			FullHistory:        true,
			AttachmentText:     new(true),
			AttachmentMedia:    new(false),
			MaxAttachmentBytes: 100 << 20,
		},
		Search: SearchConfig{
			DefaultMode: "fts",
			Embeddings: EmbeddingsConfig{
				Enabled:        false,
				Provider:       "openai",
				Model:          "text-embedding-3-small",
				APIKeyEnv:      "OPENAI_API_KEY",
				BatchSize:      64,
				MaxInputChars:  12000,
				RequestTimeout: "2m",
				VectorBackend:  "exact",
			},
		},
		Share: ShareConfig{
			RepoPath:   paths.ShareDir,
			Branch:     "main",
			AutoUpdate: true,
			StaleAfter: "15m",
			Media:      new(true),
		},
		Remote: crawlremote.Config{
			Mode:     crawlremote.ModeLocal,
			TokenEnv: DefaultRemoteTokenEnv,
		},
	}
}

func defaultSyncConcurrency() int {
	workers := runtime.GOMAXPROCS(0) * 2
	switch {
	case workers < 8:
		return 8
	case workers > 32:
		return 32
	default:
		return workers
	}
}

func ResolvePath(flagPath string) string {
	path, err := appConfig.ResolveConfigPath(flagPath)
	if err != nil {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, ".discrawl", "config.toml")
	}
	return path
}

func Load(path string) (Config, error) {
	cfg := Default()
	expanded, err := ExpandPath(path)
	if err != nil {
		return Config{}, err
	}
	if err := crawlconfig.LoadTOML(expanded, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.Normalize(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func Write(path string, cfg Config) error {
	if err := cfg.Normalize(); err != nil {
		return err
	}
	expanded, err := ExpandPath(path)
	if err != nil {
		return err
	}
	return crawlconfig.WriteTOML(expanded, cfg, 0o600)
}

func (c *Config) Normalize() error {
	if c.Version == 0 {
		c.Version = 1
	}
	if c.DefaultGuildID == "" && c.GuildID != "" {
		c.DefaultGuildID = c.GuildID
	}
	if c.DBPath == "" || c.CacheDir == "" || c.LogDir == "" {
		def := Default()
		if c.DBPath == "" {
			c.DBPath = def.DBPath
		}
		if c.CacheDir == "" {
			c.CacheDir = def.CacheDir
		}
		if c.LogDir == "" {
			c.LogDir = def.LogDir
		}
	}
	c.Discord.TokenSource = strings.ToLower(strings.TrimSpace(c.Discord.TokenSource))
	c.Discord.TokenEnv = strings.TrimSpace(c.Discord.TokenEnv)
	c.Discord.TokenKeyringService = strings.TrimSpace(c.Discord.TokenKeyringService)
	c.Discord.TokenKeyringAccount = strings.TrimSpace(c.Discord.TokenKeyringAccount)
	if c.Discord.TokenSource == "" {
		c.Discord.TokenSource = "env"
	}
	if c.Discord.TokenEnv == "" {
		c.Discord.TokenEnv = DefaultTokenEnv
	}
	if c.Discord.TokenKeyringService == "" {
		c.Discord.TokenKeyringService = DefaultTokenKeyringService
	}
	if c.Discord.TokenKeyringAccount == "" {
		c.Discord.TokenKeyringAccount = DefaultTokenKeyringAccount
	}
	if c.Desktop.Path == "" {
		c.Desktop.Path = defaultDiscordDesktopPath(homeDir())
	}
	if c.Desktop.MaxFileBytes <= 0 {
		c.Desktop.MaxFileBytes = 64 << 20
	}
	if c.Sync.Concurrency <= 0 {
		c.Sync.Concurrency = defaultSyncConcurrency()
	}
	c.Sync.Source = strings.ToLower(strings.TrimSpace(c.Sync.Source))
	if c.Sync.Source == "" {
		c.Sync.Source = "both"
	}
	if c.Sync.RepairEvery == "" {
		c.Sync.RepairEvery = "6h"
	}
	if c.Sync.AttachmentText == nil {
		c.Sync.AttachmentText = new(true)
	}
	if c.Sync.AttachmentMedia == nil {
		c.Sync.AttachmentMedia = new(false)
	}
	if c.Sync.MaxAttachmentBytes <= 0 {
		c.Sync.MaxAttachmentBytes = 100 << 20
	}
	if c.Search.DefaultMode == "" {
		c.Search.DefaultMode = "fts"
	}
	c.Search.Embeddings.Provider = strings.ToLower(strings.TrimSpace(c.Search.Embeddings.Provider))
	c.Search.Embeddings.Model = strings.TrimSpace(c.Search.Embeddings.Model)
	c.Search.Embeddings.BaseURL = strings.TrimRight(strings.TrimSpace(c.Search.Embeddings.BaseURL), "/")
	c.Search.Embeddings.APIKeyEnv = strings.TrimSpace(c.Search.Embeddings.APIKeyEnv)
	c.Search.Embeddings.RequestTimeout = strings.TrimSpace(c.Search.Embeddings.RequestTimeout)
	c.Search.Embeddings.VectorBackend = strings.ToLower(strings.TrimSpace(c.Search.Embeddings.VectorBackend))
	if c.Search.Embeddings.Provider == "" {
		c.Search.Embeddings.Provider = "openai"
	}
	if c.Search.Embeddings.Model == "" {
		switch strings.ToLower(strings.TrimSpace(c.Search.Embeddings.Provider)) {
		case "ollama", "llamacpp":
			c.Search.Embeddings.Model = "nomic-embed-text"
		default:
			c.Search.Embeddings.Model = "text-embedding-3-small"
		}
	}
	if c.Search.Embeddings.APIKeyEnv == "" && c.Search.Embeddings.Provider == "openai" {
		c.Search.Embeddings.APIKeyEnv = "OPENAI_API_KEY"
	}
	if (c.Search.Embeddings.Provider == "ollama" || c.Search.Embeddings.Provider == "llamacpp") && c.Search.Embeddings.APIKeyEnv == "OPENAI_API_KEY" {
		c.Search.Embeddings.APIKeyEnv = ""
	}
	if c.Search.Embeddings.BatchSize <= 0 {
		c.Search.Embeddings.BatchSize = 64
	}
	if c.Share.RepoPath == "" {
		c.Share.RepoPath = Default().Share.RepoPath
	}
	if c.Share.Branch == "" {
		c.Share.Branch = "main"
	}
	if c.Share.StaleAfter == "" {
		c.Share.StaleAfter = "15m"
	}
	if c.Share.Media == nil {
		c.Share.Media = new(true)
	}
	c.Share.Filter.IncludeChannelIDs = uniqueStrings(c.Share.Filter.IncludeChannelIDs)
	c.Share.Filter.ExcludeChannelIDs = uniqueStrings(c.Share.Filter.ExcludeChannelIDs)
	if c.Remote.TokenEnv == "" {
		c.Remote.TokenEnv = DefaultRemoteTokenEnv
	}
	c.Remote.Normalize()
	if c.Search.Embeddings.MaxInputChars <= 0 {
		c.Search.Embeddings.MaxInputChars = 12000
	}
	if c.Search.Embeddings.RequestTimeout == "" {
		c.Search.Embeddings.RequestTimeout = "2m"
	}
	if c.Search.Embeddings.VectorBackend == "" {
		c.Search.Embeddings.VectorBackend = "exact"
	}
	if c.Search.Embeddings.VectorBackend != "exact" && c.Search.Embeddings.VectorBackend != "turbovec" {
		return fmt.Errorf("unsupported search.embeddings.vector_backend %q", c.Search.Embeddings.VectorBackend)
	}
	if timeout, err := time.ParseDuration(c.Search.Embeddings.RequestTimeout); err != nil {
		return fmt.Errorf("parse search.embeddings.request_timeout: %w", err)
	} else if timeout <= 0 {
		return errors.New("search.embeddings.request_timeout must be positive")
	}
	c.GuildIDs = uniqueStrings(c.GuildIDs)
	return nil
}

func defaultDiscordDesktopPath(home string) string {
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

func homeDir() string {
	home, _ := os.UserHomeDir()
	return home
}

func (c Config) EffectiveDefaultGuildID() string {
	if c.DefaultGuildID != "" {
		return c.DefaultGuildID
	}
	if len(c.GuildIDs) == 1 {
		return c.GuildIDs[0]
	}
	return ""
}

func (c Config) SearchGuildDefaults() []string {
	return nil
}

func (c Config) AttachmentTextEnabled() bool {
	return c.Sync.AttachmentText == nil || *c.Sync.AttachmentText
}

func (c Config) AttachmentMediaEnabled() bool {
	return c.Sync.AttachmentMedia != nil && *c.Sync.AttachmentMedia
}

func (c Config) ShareMediaEnabled() bool {
	return c.Share.Media == nil || *c.Share.Media
}

func (c Config) ShareEnabled() bool {
	return strings.TrimSpace(c.Share.Remote) != ""
}

func (c Config) RemoteEnabled() bool {
	return c.Remote.Enabled()
}

func (c Config) RemoteCloudReadOnly() bool {
	return strings.EqualFold(strings.TrimSpace(c.Remote.Mode), crawlremote.ModeCloud)
}

func EnsureRuntimeDirs(cfg Config) error {
	return crawlconfig.EnsureRuntimeDirs(crawlconfig.RuntimeConfig{
		DBPath:   cfg.DBPath,
		CacheDir: cfg.CacheDir,
		LogDir:   cfg.LogDir,
	})
}

func ExpandPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("empty path")
	}
	return filepath.Clean(os.ExpandEnv(crawlconfig.ExpandHome(path))), nil
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}
