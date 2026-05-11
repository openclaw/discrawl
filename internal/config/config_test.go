package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zalando/go-keyring"
)

func TestNormalizeFillsDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	require.NoError(t, cfg.Normalize())
	require.Equal(t, 1, cfg.Version)
	require.Equal(t, "env", cfg.Discord.TokenSource)
	require.Equal(t, DefaultTokenEnv, cfg.Discord.TokenEnv)
	require.Equal(t, DefaultTokenKeyringService, cfg.Discord.TokenKeyringService)
	require.Equal(t, DefaultTokenKeyringAccount, cfg.Discord.TokenKeyringAccount)
	require.Equal(t, defaultSyncConcurrency(), cfg.Sync.Concurrency)
	require.GreaterOrEqual(t, cfg.Sync.Concurrency, 8)
	require.LessOrEqual(t, cfg.Sync.Concurrency, 32)
	require.NotNil(t, cfg.Sync.AttachmentText)
	require.True(t, *cfg.Sync.AttachmentText)
	require.Equal(t, "fts", cfg.Search.DefaultMode)
	require.Equal(t, "main", cfg.Share.Branch)
	require.Equal(t, "15m", cfg.Share.StaleAfter)
	require.True(t, Default().Share.AutoUpdate)
	require.False(t, cfg.ShareEnabled())
	cfg.Share.Remote = "git@example.com:org/archive.git"
	require.True(t, cfg.ShareEnabled())
	require.Equal(t, "openai", cfg.Search.Embeddings.Provider)
	require.Equal(t, "text-embedding-3-small", cfg.Search.Embeddings.Model)
	require.Empty(t, cfg.Search.Embeddings.BaseURL)
	require.Equal(t, "OPENAI_API_KEY", cfg.Search.Embeddings.APIKeyEnv)
	require.Equal(t, 64, cfg.Search.Embeddings.BatchSize)
	require.Equal(t, 12000, cfg.Search.Embeddings.MaxInputChars)
	require.Equal(t, "2m", cfg.Search.Embeddings.RequestTimeout)
}

func TestDefaultPathsUseXDGDirs(t *testing.T) {
	// Explicit XDG env vars define the candidate new-install paths on every
	// platform, including macOS.
	home := t.TempDir()
	configHome := filepath.Join(home, "xdg-config")
	dataHome := filepath.Join(home, "xdg-data")
	cacheHome := filepath.Join(home, "xdg-cache")
	stateHome := filepath.Join(home, "xdg-state")
	setTestHome(t, home)
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_CACHE_HOME", cacheHome)
	t.Setenv("XDG_STATE_HOME", stateHome)

	cfg := Default()
	require.Equal(t, filepath.Join(dataHome, "discrawl", "discrawl.db"), cfg.DBPath)
	require.Equal(t, filepath.Join(cacheHome, "discrawl"), cfg.CacheDir)
	require.Equal(t, filepath.Join(stateHome, "discrawl", "logs"), cfg.LogDir)
	require.Equal(t, filepath.Join(dataHome, "discrawl", "share"), cfg.Share.RepoPath)
	require.Equal(t, filepath.Join(configHome, "discrawl", "config.toml"), ResolvePath(""))
}

func TestDefaultPathsUseXDGFallbacks(t *testing.T) {
	// With no XDG env vars, use the adrg/xdg platform defaults.
	home := t.TempDir()
	configHome, dataHome, cacheHome, stateHome := defaultXDGTestDirs(home)
	setTestHome(t, home)
	clearXDGEnv(t)

	cfg := Default()
	require.Equal(t, filepath.Join(dataHome, "discrawl", "discrawl.db"), cfg.DBPath)
	require.Equal(t, filepath.Join(cacheHome, "discrawl"), cfg.CacheDir)
	require.Equal(t, filepath.Join(stateHome, "discrawl", "logs"), cfg.LogDir)
	require.Equal(t, filepath.Join(dataHome, "discrawl", "share"), cfg.Share.RepoPath)
	require.Equal(t, filepath.Join(configHome, "discrawl", "config.toml"), ResolvePath(""))
}

func TestDefaultPathsIgnoreRelativeXDGDirs(t *testing.T) {
	// Relative XDG env values are invalid per the spec and should fall back.
	home := t.TempDir()
	configHome, dataHome, cacheHome, stateHome := defaultXDGTestDirs(home)
	setTestHome(t, home)
	t.Setenv("XDG_CONFIG_HOME", "relative-config")
	t.Setenv("XDG_DATA_HOME", "relative-data")
	t.Setenv("XDG_CACHE_HOME", "relative-cache")
	t.Setenv("XDG_STATE_HOME", "relative-state")

	cfg := Default()
	require.Equal(t, filepath.Join(dataHome, "discrawl", "discrawl.db"), cfg.DBPath)
	require.Equal(t, filepath.Join(cacheHome, "discrawl"), cfg.CacheDir)
	require.Equal(t, filepath.Join(stateHome, "discrawl", "logs"), cfg.LogDir)
	require.Equal(t, filepath.Join(configHome, "discrawl", "config.toml"), ResolvePath(""))
}

func TestDefaultPathsPreferExistingLegacyInstallPaths(t *testing.T) {
	// Existing ~/.discrawl installs should continue to run without migration.
	home := t.TempDir()
	legacy := filepath.Join(home, ".discrawl")
	require.NoError(t, os.MkdirAll(filepath.Join(legacy, "cache"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(legacy, "logs"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(legacy, "share"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "discrawl.db"), nil, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "config.toml"), nil, 0o600))
	setTestHome(t, home)
	clearXDGEnv(t)

	cfg := Default()
	require.Equal(t, filepath.Join(legacy, "discrawl.db"), cfg.DBPath)
	require.Equal(t, filepath.Join(legacy, "cache"), cfg.CacheDir)
	require.Equal(t, filepath.Join(legacy, "logs"), cfg.LogDir)
	require.Equal(t, filepath.Join(legacy, "share"), cfg.Share.RepoPath)
	require.Equal(t, filepath.Join(legacy, "config.toml"), ResolvePath(""))
}

func TestDefaultPathsKeepLegacyInstallWithXDGEnv(t *testing.T) {
	// XDG env vars are often ambient desktop state. They should not force an
	// existing ~/.discrawl install to migrate before the new paths exist.
	home := t.TempDir()
	legacy := filepath.Join(home, ".discrawl")
	configHome := filepath.Join(home, "xdg-config")
	dataHome := filepath.Join(home, "xdg-data")
	cacheHome := filepath.Join(home, "xdg-cache")
	stateHome := filepath.Join(home, "xdg-state")
	require.NoError(t, os.MkdirAll(filepath.Join(legacy, "cache"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(legacy, "logs"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(legacy, "share"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "discrawl.db"), nil, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "config.toml"), nil, 0o600))
	setTestHome(t, home)
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_CACHE_HOME", cacheHome)
	t.Setenv("XDG_STATE_HOME", stateHome)

	cfg := Default()
	require.Equal(t, filepath.Join(legacy, "discrawl.db"), cfg.DBPath)
	require.Equal(t, filepath.Join(legacy, "cache"), cfg.CacheDir)
	require.Equal(t, filepath.Join(legacy, "logs"), cfg.LogDir)
	require.Equal(t, filepath.Join(legacy, "share"), cfg.Share.RepoPath)
	require.Equal(t, filepath.Join(legacy, "config.toml"), ResolvePath(""))
}

func TestDefaultPathsMixLegacyAndNewLocations(t *testing.T) {
	// Fallback is path-by-path so partial migration does not hide newer data.
	home := t.TempDir()
	_, dataHome, _, stateHome := defaultXDGTestDirs(home)
	legacy := filepath.Join(home, ".discrawl")
	newDBPath := filepath.Join(dataHome, "discrawl", "discrawl.db")
	require.NoError(t, os.MkdirAll(filepath.Dir(newDBPath), 0o755))
	require.NoError(t, os.WriteFile(newDBPath, nil, 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(legacy, "cache"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(legacy, "share"), 0o755))
	setTestHome(t, home)
	clearXDGEnv(t)

	cfg := Default()
	require.Equal(t, newDBPath, cfg.DBPath)
	require.Equal(t, filepath.Join(legacy, "cache"), cfg.CacheDir)
	require.Equal(t, filepath.Join(stateHome, "discrawl", "logs"), cfg.LogDir)
	require.Equal(t, filepath.Join(legacy, "share"), cfg.Share.RepoPath)
}

func TestDefaultPathsPreferNewConfigOverLegacy(t *testing.T) {
	// Once the new config exists, it becomes the default source of truth.
	home := t.TempDir()
	configHome, _, _, _ := defaultXDGTestDirs(home)
	legacyConfig := filepath.Join(home, ".discrawl", "config.toml")
	newConfig := filepath.Join(configHome, "discrawl", "config.toml")
	require.NoError(t, os.MkdirAll(filepath.Dir(legacyConfig), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(newConfig), 0o755))
	require.NoError(t, os.WriteFile(legacyConfig, nil, 0o600))
	require.NoError(t, os.WriteFile(newConfig, nil, 0o600))
	setTestHome(t, home)
	t.Setenv("XDG_CONFIG_HOME", "")

	require.Equal(t, newConfig, ResolvePath(""))
}

func setTestHome(t *testing.T, home string) {
	t.Helper()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("LOCALAPPDATA", filepath.Join(home, "AppData", "Local"))
	t.Setenv("APPDATA", filepath.Join(home, "AppData", "Roaming"))
}

func clearXDGEnv(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("XDG_CACHE_HOME", "")
	t.Setenv("XDG_STATE_HOME", "")
}

func defaultXDGTestDirs(home string) (configHome, dataHome, cacheHome, stateHome string) {
	switch runtime.GOOS {
	case "darwin":
		appSupport := filepath.Join(home, "Library", "Application Support")
		return appSupport, appSupport, filepath.Join(home, "Library", "Caches"), appSupport
	case "windows":
		localAppData := filepath.Join(home, "AppData", "Local")
		return localAppData, localAppData, filepath.Join(localAppData, "cache"), localAppData
	default:
		return filepath.Join(home, ".config"),
			filepath.Join(home, ".local", "share"),
			filepath.Join(home, ".cache"),
			filepath.Join(home, ".local", "state")
	}
}

func TestDefaultSyncConcurrencyBounds(t *testing.T) {
	old := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(old) })

	runtime.GOMAXPROCS(1)
	require.Equal(t, 8, defaultSyncConcurrency())

	runtime.GOMAXPROCS(8)
	require.Equal(t, 16, defaultSyncConcurrency())

	runtime.GOMAXPROCS(100)
	require.Equal(t, 32, defaultSyncConcurrency())
}

func TestResolveDiscordTokenFromEnv(t *testing.T) {
	cfg := Default()
	t.Setenv(DefaultTokenEnv, "Bot env-token")

	token, err := ResolveDiscordToken(cfg)
	require.NoError(t, err)
	require.Equal(t, "env-token", token.Token)
	require.Equal(t, "env", token.Source)
}

func TestResolveDiscordTokenFallsBackToKeyring(t *testing.T) {
	cfg := Default()
	t.Setenv(DefaultTokenEnv, "")
	stubDiscordTokenKeyring(t, func(service, account string) (string, error) {
		require.Equal(t, DefaultTokenKeyringService, service)
		require.Equal(t, DefaultTokenKeyringAccount, account)
		return "Bot keyring-token", nil
	})

	token, err := ResolveDiscordToken(cfg)
	require.NoError(t, err)
	require.Equal(t, "keyring-token", token.Token)
	require.Equal(t, "keyring", token.Source)
	require.Equal(t, "discrawl/discord_bot_token", token.Path)
}

func TestResolveDiscordTokenFromKeyringSource(t *testing.T) {
	cfg := Default()
	cfg.Discord.TokenSource = "keyring"
	cfg.Discord.TokenKeyringService = " custom-service "
	cfg.Discord.TokenKeyringAccount = " custom-account "
	t.Setenv(DefaultTokenEnv, "ignored-env-token")
	stubDiscordTokenKeyring(t, func(service, account string) (string, error) {
		require.Equal(t, "custom-service", service)
		require.Equal(t, "custom-account", account)
		return "custom-keyring-token", nil
	})

	token, err := ResolveDiscordToken(cfg)
	require.NoError(t, err)
	require.Equal(t, "custom-keyring-token", token.Token)
	require.Equal(t, "keyring", token.Source)
	require.Equal(t, "custom-service/custom-account", token.Path)
}

func TestResolveDiscordTokenFromCustomEnv(t *testing.T) {
	cfg := Default()
	cfg.Discord.TokenEnv = "DISCRAWL_TEST_DISCORD_TOKEN"
	t.Setenv("DISCRAWL_TEST_DISCORD_TOKEN", "custom-env-token")

	token, err := ResolveDiscordToken(cfg)
	require.NoError(t, err)
	require.Equal(t, "custom-env-token", token.Token)
	require.Equal(t, "DISCRAWL_TEST_DISCORD_TOKEN", token.Path)
}

func TestResolveDiscordTokenRequiresEnvValue(t *testing.T) {
	cfg := Default()
	t.Setenv(DefaultTokenEnv, "")
	stubDiscordTokenKeyring(t, func(_, _ string) (string, error) {
		return "", keyring.ErrNotFound
	})

	_, err := ResolveDiscordToken(cfg)
	require.ErrorContains(t, err, `discord token not found in environment variable "DISCORD_BOT_TOKEN" or keyring item "discrawl"/"discord_bot_token"`)
	require.ErrorIs(t, err, keyring.ErrNotFound)
}

func TestResolveDiscordTokenDisabled(t *testing.T) {
	cfg := Default()
	cfg.Discord.TokenSource = "none"
	t.Setenv(DefaultTokenEnv, "env-token")

	_, err := ResolveDiscordToken(cfg)
	require.ErrorContains(t, err, "discord token disabled")
}

func TestResolveDiscordTokenRejectsUnsupportedSource(t *testing.T) {
	cfg := Default()
	cfg.Discord.TokenSource = "legacy"

	_, err := ResolveDiscordToken(cfg)
	require.ErrorContains(t, err, `unsupported discord token_source "legacy"`)
}

func TestWriteAndLoadRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.toml")
	cfg := Default()
	cfg.DefaultGuildID = "g1"
	cfg.GuildIDs = []string{"g1", "g2"}
	require.NoError(t, Write(path, cfg))

	loaded, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "g1", loaded.EffectiveDefaultGuildID())
	require.Equal(t, []string{"g1", "g2"}, loaded.GuildIDs)
	require.NotNil(t, loaded.Sync.AttachmentText)
	require.True(t, *loaded.Sync.AttachmentText)
}

func TestWriteRejectsNonPositiveEmbeddingTimeout(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Search.Embeddings.RequestTimeout = "0s"
	err := Write(filepath.Join(t.TempDir(), "config.toml"), cfg)
	require.ErrorContains(t, err, "must be positive")

	cfg.Search.Embeddings.RequestTimeout = "not-a-duration"
	err = cfg.Normalize()
	require.ErrorContains(t, err, "parse search.embeddings.request_timeout")
}

func TestNormalizeEmbeddingProviderDefaults(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Search.Embeddings.Provider = "OLLAMA"
	require.NoError(t, cfg.Normalize())
	require.Equal(t, "ollama", cfg.Search.Embeddings.Provider)
	require.Equal(t, "text-embedding-3-small", cfg.Search.Embeddings.Model)
	require.Empty(t, cfg.Search.Embeddings.APIKeyEnv)
	require.Empty(t, cfg.Search.Embeddings.BaseURL)
	require.Equal(t, "2m", cfg.Search.Embeddings.RequestTimeout)

	cfg = Config{}
	cfg.Search.Embeddings.Provider = "llamacpp"
	require.NoError(t, cfg.Normalize())
	require.Equal(t, "nomic-embed-text", cfg.Search.Embeddings.Model)
	require.Empty(t, cfg.Search.Embeddings.APIKeyEnv)

	cfg = Config{}
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.BaseURL = " http://127.0.0.1:9999/v1/ "
	require.NoError(t, cfg.Normalize())
	require.Equal(t, "openai_compatible", cfg.Search.Embeddings.Provider)
	require.Equal(t, "http://127.0.0.1:9999/v1", cfg.Search.Embeddings.BaseURL)
	require.Equal(t, "text-embedding-3-small", cfg.Search.Embeddings.Model)
	require.Empty(t, cfg.Search.Embeddings.APIKeyEnv)

	cfg = Config{}
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.APIKeyEnv = "OPENAI_API_KEY"
	require.NoError(t, cfg.Normalize())
	require.Equal(t, "OPENAI_API_KEY", cfg.Search.Embeddings.APIKeyEnv)
}

func TestLoadLegacyEmbeddingConfigDefaults(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
db_path = "/tmp/discrawl.db"
cache_dir = "/tmp/discrawl-cache"
log_dir = "/tmp/discrawl-logs"

[search.embeddings]
enabled = true
provider = "openai"
model = "text-embedding-3-small"
api_key_env = "OPENAI_API_KEY"
batch_size = 64
`), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.True(t, cfg.Search.Embeddings.Enabled)
	require.Equal(t, "openai", cfg.Search.Embeddings.Provider)
	require.Empty(t, cfg.Search.Embeddings.BaseURL)
	require.Equal(t, 12000, cfg.Search.Embeddings.MaxInputChars)
	require.Equal(t, "2m", cfg.Search.Embeddings.RequestTimeout)
}

func TestNormalizeRejectsInvalidEmbeddingTimeout(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Search.Embeddings.RequestTimeout = "0s"
	require.ErrorContains(t, cfg.Normalize(), "must be positive")

	cfg = Default()
	cfg.Search.Embeddings.RequestTimeout = "soon"
	require.ErrorContains(t, cfg.Normalize(), "parse search.embeddings.request_timeout")
}

func TestAttachmentTextExplicitFalseSurvivesNormalize(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Sync.AttachmentText = new(false)
	require.NoError(t, cfg.Normalize())
	require.False(t, cfg.AttachmentTextEnabled())
}

func TestExpandPath(t *testing.T) {
	t.Parallel()

	path, err := ExpandPath("~/discrawl-test")
	require.NoError(t, err)
	require.Contains(t, path, "discrawl-test")
}

func TestResolvePath(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, "env.toml")
	// ResolvePath reads process-wide home/XDG state through adrg/xdg; isolate
	// the test so host XDG variables or Windows known-folder env vars do not
	// leak into the fallback assertion below.
	setTestHome(t, dir)
	t.Setenv(DefaultConfigEnv, envPath)
	require.Equal(t, "flag.toml", ResolvePath("flag.toml"))
	require.Equal(t, envPath, ResolvePath(""))
	t.Setenv(DefaultConfigEnv, "")
	clearXDGEnv(t)
	configHome, _, _, _ := defaultXDGTestDirs(dir)
	require.Equal(t, filepath.Join(configHome, "discrawl", "config.toml"), ResolvePath(""))
	_, err := ExpandPath("")
	require.ErrorContains(t, err, "empty path")
}

func TestEffectiveDefaultGuildAndDirs(t *testing.T) {
	t.Parallel()

	require.Equal(t, "explicit", Config{DefaultGuildID: "explicit", GuildIDs: []string{"g1"}}.EffectiveDefaultGuildID())
	require.Empty(t, Config{GuildIDs: []string{"g1", "g2"}}.EffectiveDefaultGuildID())
	require.Equal(t, []string{"a", "b"}, uniqueStrings([]string{" a ", "", "b", "a"}))
	require.Equal(t, "token", NormalizeBotToken(" token "))
	require.Nil(t, uniqueStrings(nil))

	cfg := Default()
	cfg.GuildIDs = []string{"g1"}
	require.Equal(t, "g1", cfg.EffectiveDefaultGuildID())
	require.Nil(t, cfg.SearchGuildDefaults())

	cfg.CacheDir = filepath.Join(t.TempDir(), "cache")
	cfg.LogDir = filepath.Join(t.TempDir(), "logs")
	cfg.DBPath = filepath.Join(t.TempDir(), "db", "discrawl.db")
	require.NoError(t, EnsureRuntimeDirs(cfg))
}

func TestResolvePathUsesEnv(t *testing.T) {
	t.Setenv(DefaultConfigEnv, "/tmp/custom.toml")
	require.Equal(t, "/tmp/custom.toml", ResolvePath(""))
	require.Equal(t, "/tmp/flag.toml", ResolvePath("/tmp/flag.toml"))
}

func TestConfigErrorsAndBackupFallback(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(DefaultTokenEnv, "")
	stubDiscordTokenKeyring(t, func(_, _ string) (string, error) {
		return "", keyring.ErrNotFound
	})

	_, err := ExpandPath("")
	require.Error(t, err)

	bad := filepath.Join(dir, "bad.toml")
	require.NoError(t, os.WriteFile(bad, []byte("not = [toml"), 0o600))
	_, err = Load(bad)
	require.Error(t, err)

	cfg := Default()
	_, err = ResolveDiscordToken(cfg)
	require.Error(t, err)
}

func stubDiscordTokenKeyring(t *testing.T, get func(service, account string) (string, error)) {
	t.Helper()
	old := discordTokenKeyringGet
	discordTokenKeyringGet = get
	t.Cleanup(func() {
		discordTokenKeyringGet = old
	})
}
