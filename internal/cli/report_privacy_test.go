package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPublishedReportCLIAndPublishExcludeDirectMessages(t *testing.T) {
	dir := t.TempDir()
	s := seedCLIStore(t, filepath.Join(dir, "archive.db"))
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.UpsertChannel(t.Context(), store.ChannelRecord{
		ID: "dm-channel", GuildID: store.DirectMessageGuildID, Name: "dm-only-label", Kind: "dm", RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertMessage(t.Context(), store.MessageRecord{
		ID: "dm-message", GuildID: store.DirectMessageGuildID, ChannelID: "dm-channel",
		AuthorID: "dm-author", CreatedAt: "2099-01-01T00:00:00Z",
		Content: "synthetic direct message", NormalizedContent: "synthetic direct message", RawJSON: `{}`,
	}))
	var out bytes.Buffer
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "archive.db")
	r := &runtime{ctx: t.Context(), cfg: cfg, store: s, stdout: &out, stderr: &bytes.Buffer{}}

	require.NoError(t, r.runReport(nil))
	require.Contains(t, out.String(), "dm-only-label")
	require.Contains(t, out.String(), "2099-01-01")
	out.Reset()
	require.NoError(t, r.runReport([]string{"--published"}))
	require.NotContains(t, out.String(), "dm-only-label")
	require.NotContains(t, out.String(), "2099-01-01")
	require.Contains(t, out.String(), "general")

	localReadme := filepath.Join(dir, "LOCAL.md")
	require.NoError(t, r.runReport([]string{"--readme", localReadme}))
	body, err := os.ReadFile(localReadme)
	require.NoError(t, err)
	require.Contains(t, string(body), "dm-only-label")
	sharedReadme := filepath.Join(dir, "SHARED.md")
	require.NoError(t, r.runReport([]string{"--published", "--readme", sharedReadme}))
	body, err = os.ReadFile(sharedReadme)
	require.NoError(t, err)
	require.NotContains(t, string(body), "dm-only-label")

	repo := filepath.Join(dir, "share")
	remote := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remote)
	publishReadme := filepath.Join(repo, "README.md")
	require.NoError(t, r.runPublish([]string{"--repo", repo, "--remote", remote, "--no-media", "--no-commit", "--readme", publishReadme}))
	body, err = os.ReadFile(publishReadme)
	require.NoError(t, err)
	require.NotContains(t, string(body), "dm-only-label")
	require.NotContains(t, string(body), "2099-01-01")
	require.Contains(t, string(body), "general")

	r.cfg.Share.Filter.PublicOnly = true
	require.NoError(t, r.runReport(nil), "share filters must not change the local default")
	require.ErrorContains(t, r.runReport([]string{"--published", "--readme", sharedReadme}), "not supported with share filters")
	require.ErrorContains(t, r.runPublish([]string{"--repo", repo, "--remote", remote, "--no-commit", "--readme", publishReadme}), "not supported with share filters")
	require.Contains(t, commandUsage["report"], "--published")
}
