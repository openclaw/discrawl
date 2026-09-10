package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/report"
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

	out.Reset()
	require.NoError(t, r.runReport([]string{"--published", "--field-notes", "--readme", sharedReadme}))
	metadata, err := os.ReadFile(filepath.Join(dir, report.FieldNotesJSONPath))
	require.NoError(t, err)
	var notes report.FieldNotes
	require.NoError(t, json.Unmarshal(metadata, &notes))
	require.Equal(t, "published-non-dm", notes.Scope)
	require.Equal(t, "unknown", notes.Coverage)
	require.NotContains(t, string(metadata), "dm-only-label")
	require.NotContains(t, string(metadata), "general")
	require.NotContains(t, string(metadata), "2099-01-01")
	markdown, err := os.ReadFile(filepath.Join(dir, report.FieldNotesMarkdownPath))
	require.NoError(t, err)
	require.Contains(t, string(markdown), notes.GeneratedAt.Format("2006-01-02T15:04:05Z07:00"))
	require.NotContains(t, string(markdown), "general")
	body, err = os.ReadFile(sharedReadme)
	require.NoError(t, err)
	require.Contains(t, string(body), string(bytes.TrimSpace(markdown)))
	for _, args := range [][]string{
		{"--field-notes"},
		{"--field-notes", "--readme", sharedReadme},
		{"--field-notes", "--published"},
	} {
		require.ErrorContains(t, r.runReport(args), "requires --published and --readme")
	}

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
	require.ErrorContains(t, r.runReport([]string{"--published", "--field-notes", "--readme", sharedReadme}), "not supported with share filters")
	unchanged, err := os.ReadFile(filepath.Join(dir, report.FieldNotesJSONPath))
	require.NoError(t, err)
	require.Equal(t, metadata, unchanged)
	require.ErrorContains(t, r.runPublish([]string{"--repo", repo, "--remote", remote, "--no-commit", "--readme", publishReadme}), "not supported with share filters")
	require.Contains(t, commandUsage["report"], "--published")
}

func TestFilteredPublishRemovesFieldNotesPreservesDocs(t *testing.T) {
	for _, readmeMode := range []string{"activity-and-notes", "notes-only", "missing", "custom"} {
		t.Run(readmeMode, func(t *testing.T) {
			dir := t.TempDir()
			remote := filepath.Join(dir, "remote.git")
			repo := filepath.Join(dir, "publisher")
			runGit(t, dir, "init", "--bare", remote)
			s := seedCLIStore(t, filepath.Join(dir, "fixture.db"))
			t.Cleanup(func() { _ = s.Close() })
			cfg := config.Default()
			cfg.DBPath = filepath.Join(dir, "fixture.db")
			r := &runtime{ctx: t.Context(), cfg: cfg, store: s, stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}}
			require.NoError(t, r.runPublish([]string{"--repo", repo, "--remote", remote, "--no-media"}))
			runGit(t, repo, "config", "user.name", "Fixture")
			runGit(t, repo, "config", "user.email", "fixture@example.invalid")
			runGit(t, repo, "config", "commit.gpgsign", "false")
			readme := filepath.Join(repo, "README.md")
			require.NoError(t, r.runReport([]string{"--published", "--field-notes", "--readme", readme}))
			switch readmeMode {
			case "notes-only":
				require.NoError(t, os.WriteFile(readme, []byte(report.FieldNotesStartMarker+"\nnotes\n"+report.FieldNotesEndMarker), 0o600))
			case "missing":
				require.NoError(t, os.Remove(readme))
			case "custom":
				require.NoError(t, os.WriteFile(readme, []byte("# Custom maintainer README\n"), 0o600))
			}
			for _, name := range []string{"AGENTS.md", "CONTRIBUTING.md", "reports/manual.md"} {
				require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte("maintainer instructions\n"), 0o600))
			}
			runGit(t, repo, "add", ".")
			runGit(t, repo, "commit", "-m", "test: notes and docs")
			// Unfiltered publication must preserve both artifacts and docs.
			require.NoError(t, r.runPublish([]string{"--repo", repo, "--remote", remote, "--no-media", "--push"}))
			require.FileExists(t, filepath.Join(repo, report.FieldNotesJSONPath))
			require.NoError(t, os.WriteFile(filepath.Join(repo, "AGENTS.md"), []byte("unrelated staged change\n"), 0o600))
			runGit(t, repo, "add", "AGENTS.md")
			// Exercise cleanup independently of README presence and repeat after
			// --no-commit so exact deletions must be recovered from Git's index.
			args := []string{"--repo", repo, "--remote", remote, "--no-media", "--include-channels", "c1"}
			require.NoError(t, r.runPublish(append(args, "--no-commit")))
			require.NoFileExists(t, filepath.Join(repo, report.FieldNotesMarkdownPath))
			require.NoFileExists(t, filepath.Join(repo, report.FieldNotesJSONPath))
			require.NoError(t, r.runPublish(append(args, "--push")))
			for _, name := range []string{"AGENTS.md", "CONTRIBUTING.md", "reports/manual.md"} {
				require.FileExists(t, filepath.Join(repo, name))
			}
			if readmeMode == "custom" {
				require.FileExists(t, readme)
			} else {
				require.NoFileExists(t, readme)
			}
		})
	}
}
