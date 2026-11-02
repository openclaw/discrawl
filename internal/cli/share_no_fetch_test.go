package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestUpdateNoFetchImportsOwnedCheckoutWithUnavailableRemote(t *testing.T) {
	ctx, dir := t.Context(), t.TempDir()
	remote := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remote)
	publisher := seedCLIStore(t, filepath.Join(dir, "publisher.db"))
	t.Cleanup(func() { _ = publisher.Close() })
	opts := share.Options{RepoPath: filepath.Join(dir, "publisher-share"), Remote: remote, Branch: "main"}
	publishSnapshot(t, ctx, publisher, opts, "first snapshot")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "reader.db")
	cfg.Share.RepoPath = filepath.Join(dir, "reader-share")
	cfg.Share.Remote, cfg.Share.Branch, cfg.Share.AutoUpdate = remote, "main", false
	cfg.Discord.TokenSource = "none"
	cfgPath := filepath.Join(dir, "reader.toml")
	require.NoError(t, config.Write(cfgPath, cfg))
	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "update"}, &out, &bytes.Buffer{}))
	require.NoError(t, publisher.UpsertMessage(ctx, store.MessageRecord{
		ID: "external-next", GuildID: "g1", ChannelID: "c1", AuthorID: "u1",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), Content: "external snapshot row",
		NormalizedContent: "external snapshot row", RawJSON: `{}`,
	}))
	publishSnapshot(t, ctx, publisher, opts, "second snapshot")
	// The external owner refreshes before handing the immutable checkout to the
	// import-only command. Its real scheduler holds its lock around both steps.
	runGit(t, cfg.Share.RepoPath, "pull", "--ff-only", "origin", "main")
	runGit(t, cfg.Share.RepoPath, "config", "crawlkit.snapshot-cache-owner", "fixture-owner")
	require.NoError(t, os.Rename(remote, remote+"-offline"))
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "update", "--no-fetch"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "fetched=false")
	reader, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	_, rows, err := reader.ReadOnlyQuery(ctx, "select count(*) from messages where id = 'external-next'")
	require.NoError(t, err)
	require.Equal(t, "1", rows[0][0])
	require.NoError(t, reader.Close())
	cfg.Share.StaleAfter = "1ns"
	require.NoError(t, config.Write(cfgPath, cfg))
	for _, args := range [][]string{{"update"}, {"update", "--remote", "https://example.invalid/new.git"}, {"sync", "--update=auto"}, {"sync", "--update=force"}} {
		err := Run(ctx, append([]string{"--config", cfgPath}, args...), &out, &bytes.Buffer{})
		require.Error(t, err)
		require.ErrorContains(t, err, "external snapshot owner")
	}
	err = Run(ctx, []string{"--config", cfgPath, "update", "--no-fetch", "--force", "--ref", "HEAD"}, &out, &bytes.Buffer{})
	require.ErrorContains(t, err, "cannot be combined with --ref")
}
