package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
)

func TestMessageSyncOptionsNumericChannelID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	cfg.Sync.Concurrency = 7
	require.NoError(t, config.Write(cfgPath, cfg))

	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.withServices(false, func() error {
		opts, err := rt.messageSyncOptions("1456744319972282449", "", "")
		require.NoError(t, err)
		require.Equal(t, []string{"g1"}, opts.GuildIDs)
		require.Equal(t, []string{"1456744319972282449"}, opts.ChannelIDs)
		require.Equal(t, 7, opts.Concurrency)
		return nil
	}))
}

func TestMessageSyncOptionsRequiresScopeWithoutDefaults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.withServices(false, func() error {
		_, err := rt.messageSyncOptions("", "", "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "--channel or --guild")
		return nil
	}))
}

func TestMessageSyncOptionsErrorsWhenChannelCannotResolve(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.withServices(false, func() error {
		_, err := rt.messageSyncOptions("ghost-town", "", "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot resolve channel")
		return nil
	}))
}

func TestSyncMessagesQueryRequiresDiscordServices(t *testing.T) {
	t.Parallel()

	err := (&runtime{}).syncMessagesQuery("general", "", "")
	require.Error(t, err)
	require.Equal(t, 2, ExitCode(err))
}

func TestHasBoolFlag(t *testing.T) {
	t.Parallel()

	require.True(t, hasBoolFlag([]string{"--sync"}, "--sync"))
	require.True(t, hasBoolFlag([]string{"--sync=true"}, "--sync"))
	require.False(t, hasBoolFlag([]string{"--other"}, "--sync"))
	require.True(t, boolFlagEnabled([]string{"--sync"}, "--sync"))
	require.True(t, boolFlagEnabled([]string{"--sync=true"}, "--sync"))
	require.False(t, boolFlagEnabled([]string{"--sync=false"}, "--sync"))
	require.False(t, boolFlagEnabled([]string{"--sync", "--sync=false"}, "--sync"))
	require.True(t, boolFlagEnabled([]string{"--sync=false", "--sync"}, "--sync"))
	require.False(t, boolFlagEnabled([]string{"--other"}, "--sync"))
}

func TestIsDiscordID(t *testing.T) {
	t.Parallel()

	require.True(t, isDiscordID("1456744319972282449"))
	require.False(t, isDiscordID("145674431997228244x"))
	require.False(t, isDiscordID("short"))
}

func TestMessageSyncOptionsAddsGuildForMatchedChannel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g42", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.Close())

	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.withServices(false, func() error {
		opts, err := rt.messageSyncOptions("#general", "", "")
		require.NoError(t, err)
		require.Equal(t, []string{"g42"}, opts.GuildIDs)
		require.Equal(t, []string{"c1"}, opts.ChannelIDs)
		return nil
	}))
}
