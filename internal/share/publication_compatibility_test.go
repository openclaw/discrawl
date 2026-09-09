package share

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

// Run with the binary built from released v0.14.0 commit
// d6f7772937cf3a48043836d9b0caf2f83186d6a3. No historical planner is copied here.
func TestReleasedReaderPublicationCompatibility(t *testing.T) {
	binary := os.Getenv("DISCRAWL_RELEASED_READER")
	if binary == "" {
		t.Skip("set DISCRAWL_RELEASED_READER to an exact v0.14.0 source build")
	}
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "publisher.db")
	cfg.CacheDir = filepath.Join(dir, "publisher-cache")
	cfg.LogDir = filepath.Join(dir, "publisher-logs")
	cfg.Share.RepoPath = filepath.Join(dir, "publisher-share")
	cfg.Share.Remote = filepath.Join(dir, "remote.git")
	testGitRun(t, t.Context(), dir, "init", "--bare", cfg.Share.Remote)
	cfg.Discord.TokenSource = "none"
	cfgPath := filepath.Join(dir, "publisher.toml")
	require.NoError(t, config.Write(cfgPath, cfg))
	src := seedStore(t, cfg.DBPath)
	require.NoError(t, src.Close())
	oldCLI := func(cfgPath string, args ...string) ([]byte, error) {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), binary, append([]string{"--config", cfgPath}, args...)...)
		// No inherited credentials, config, live HOME, or implicit remote updates.
		cmd.Env = []string{
			"PATH=" + os.Getenv("PATH"), "HOME=" + dir,
			"XDG_CONFIG_HOME=" + filepath.Join(dir, "config"), "XDG_DATA_HOME=" + filepath.Join(dir, "data"),
			"XDG_CACHE_HOME=" + filepath.Join(dir, "cache"), "XDG_STATE_HOME=" + filepath.Join(dir, "state"),
			"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull, "DISCRAWL_NO_AUTO_UPDATE=1",
		}
		return cmd.CombinedOutput()
	}
	out, err := oldCLI(cfgPath, "--version")
	require.NoError(t, err, "%s", out)
	require.Contains(t, string(out), "0.14.0")
	out, err = oldCLI(cfgPath, "publish", "--no-media")
	require.NoError(t, err, "%s", out)
	initial, err := ReadManifest(cfg.Share.RepoPath)
	require.NoError(t, err)
	for _, table := range initial.Tables {
		for _, name := range table.Files {
			require.NotContains(t, name, ".generations")
		}
	}
	reader := config.Default()
	reader.DBPath = filepath.Join(dir, "reader.db")
	reader.CacheDir = filepath.Join(dir, "reader-cache")
	reader.LogDir = filepath.Join(dir, "reader-logs")
	reader.Share.RepoPath = filepath.Join(dir, "reader-share")
	reader.Discord.TokenSource = "none"
	readerPath := filepath.Join(dir, "reader.toml")
	require.NoError(t, config.Write(readerPath, reader))
	out, err = oldCLI(readerPath, "subscribe", "--no-auto-update", "--no-media", cfg.Share.RepoPath)
	require.NoError(t, err, "%s", out)
	dst, err := store.Open(t.Context(), reader.DBPath)
	require.NoError(t, err)
	require.NoError(t, dst.UpsertMessages(t.Context(), []store.MessageMutation{
		compatibilityMessage("local-public", "g1", "destination public"),
		compatibilityMessage("local-dm", "@me", "destination DM"),
	}))
	require.NoError(t, dst.Close())
	src, err = store.Open(t.Context(), cfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = src.Close() }()
	opts := Options{RepoPath: cfg.Share.RepoPath, Branch: "main", Producer: fixtureProducer()}
	configureGitUser(t, cfg.Share.RepoPath)
	originalShardLimit := maxShardBytes
	t.Cleanup(func() { maxShardBytes = originalShardLimit })
	for _, step := range []string{"unchanged", "edit", "append"} {
		if step == "edit" {
			_, err := src.DB().ExecContext(t.Context(), `update messages set content = 'edited publication', normalized_content = 'edited publication' where id = 'm1'`)
			require.NoError(t, err)
		}
		if step == "append" {
			maxShardBytes = 1
			var messages []store.MessageMutation
			for i := range 1025 {
				messages = append(messages, compatibilityMessage("new-"+strconv.Itoa(i), "g1", "appended publication"))
			}
			require.NoError(t, src.UpsertMessages(t.Context(), messages))
		}
		manifest, err := Export(t.Context(), src, opts)
		require.NoError(t, err, step)
		require.Equal(t, producerName, manifest.Files["producer"])
		_, err = Commit(t.Context(), opts, "test: "+step)
		require.NoError(t, err, step)
		out, err = oldCLI(readerPath, "update", "--no-media")
		require.NoError(t, err, "%s: %s", step, out)
		require.NotContains(t, string(out), "requires forced")
		dst, err := store.Open(t.Context(), reader.DBPath)
		require.NoError(t, err)
		checkCompatibilitySentinels(t, dst)
		var content string
		require.NoError(t, dst.DB().QueryRowContext(t.Context(), `select content from messages where id = 'm1'`).Scan(&content))
		if step != "unchanged" {
			require.Equal(t, "edited publication", content)
		}
		if step == "append" {
			var count int
			require.NoError(t, dst.DB().QueryRowContext(t.Context(), `select count(*) from messages where id like 'new-%'`).Scan(&count))
			require.Equal(t, 1025, count)
			require.Greater(t, len(tableEntry(t, manifest, "messages").Files), 1)
		}
		require.NoError(t, dst.Close())
		t.Logf("released reader: %s publication consumed without force, both sentinels retained", step)
	}
	// A genuine shard removal must still refuse merge instead of deleting local rows.
	_, err = src.DB().ExecContext(t.Context(), `delete from messages where id like 'new-%'`)
	require.NoError(t, err)
	_, err = Export(t.Context(), src, opts)
	require.NoError(t, err)
	_, err = Commit(t.Context(), opts, "test: removed shard")
	require.NoError(t, err)
	out, err = oldCLI(readerPath, "update", "--no-media")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(string(out)), "replac")
	dst, err = store.Open(t.Context(), reader.DBPath)
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	checkCompatibilitySentinels(t, dst)
	var count int
	require.NoError(t, dst.DB().QueryRowContext(t.Context(), `select count(*) from messages where id like 'new-%'`).Scan(&count))
	require.Equal(t, 1025, count, "refused removal must not mutate the subscriber")
	t.Log("released reader: genuine removed shard refused without mutating subscriber")
}

func compatibilityMessage(id, guild, content string) store.MessageMutation {
	return store.MessageMutation{Record: store.MessageRecord{
		ID: id, GuildID: guild, ChannelID: "c1", AuthorID: "u1", AuthorName: "Fixture",
		Content: content, NormalizedContent: content, RawJSON: `{}`,
		CreatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}}
}

func checkCompatibilitySentinels(t *testing.T, dst *store.Store) {
	t.Helper()
	for id, want := range map[string]string{"local-public": "destination public", "local-dm": "destination DM"} {
		var content string
		require.NoError(t, dst.DB().QueryRowContext(t.Context(), `select content from messages where id = ?`, id).Scan(&content))
		require.Equal(t, want, content)
	}
}
