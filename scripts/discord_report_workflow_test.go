//go:build ignore

package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// Explicit-file test, matching the maintenance workflow tests without changing
// module requirements: go test scripts/discord_report_workflow_test.go
func TestDiscordReportWorkflow(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	body, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", ".github", "workflows", "discord-backup-report.yml"))
	require.NoError(t, err)
	var workflow struct {
		On struct {
			Schedule []struct {
				Cron string `yaml:"cron"`
			} `yaml:"schedule"`
		} `yaml:"on"`
		Concurrency struct {
			Group  string `yaml:"group"`
			Cancel bool   `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
		Jobs map[string]struct {
			Steps []struct {
				Name string `yaml:"name"`
				Run  string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	require.NoError(t, yaml.Unmarshal(body, &workflow))
	require.Equal(t, "discord-backup-repo", workflow.Concurrency.Group)
	require.False(t, workflow.Concurrency.Cancel)
	require.Len(t, workflow.On.Schedule, 1)
	require.Equal(t, "17 7 * * *", workflow.On.Schedule[0].Cron)
	require.Len(t, workflow.Jobs, 1)
	var script string
	for _, step := range workflow.Jobs["report"].Steps {
		if step.Name == "Generate daily Discord report" {
			script = step.Run
		}
	}
	require.NotEmpty(t, script)
	require.Contains(t, script, `report --published --field-notes --readme "$BACKUP_REPO/README.md"`)
	require.Equal(t, 1, strings.Count(script, " report --published"))
	require.Contains(t, script, "report_paths=(README.md reports/latest-field-notes.md reports/latest-field-notes.json)")
	require.Contains(t, script, `add -- "${report_paths[@]}"`)
	require.Contains(t, script, `diff --cached --quiet -- "${report_paths[@]}"`)
	require.Contains(t, script, `commit --only -m "docs: update discord activity report and field notes" -- "${report_paths[@]}"`)
	require.NotContains(t, script, "add -A")
	require.NotContains(t, script, "OPENAI")
	require.NotContains(t, script, "cloud publish")

	start := strings.Index(script, "report_paths=(")
	end := strings.Index(script, `git -C "$BACKUP_REPO" push`)
	require.Greater(t, end, start)
	staging := script[start:end]
	if runtime.GOOS == "windows" {
		t.Skip("workflow staging executes Bash on Linux")
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", dir}, args...)...)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		return string(out)
	}
	git("init")
	git("config", "user.name", "Fixture")
	git("config", "user.email", "fixture@example.invalid")
	git("config", "commit.gpgsign", "false")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("unchanged activity\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "AGENTS.md"), []byte("committed instructions\n"), 0o600))
	git("add", ".")
	git("commit", "-m", "test: original")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "AGENTS.md"), []byte("unrelated staged instructions\n"), 0o600))
	git("add", "AGENTS.md")
	require.NoError(t, os.Mkdir(filepath.Join(dir, "reports"), 0o700))
	for _, name := range []string{"latest-field-notes.md", "latest-field-notes.json"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "reports", name), []byte("new untracked artifact\n"), 0o600))
	}
	run := func() {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "bash", "-euc", staging)
		cmd.Env = append(os.Environ(), "BACKUP_REPO="+dir)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
	}
	before := git("rev-parse", "HEAD")
	run()
	require.NotEqual(t, before, git("rev-parse", "HEAD"), "untracked artifacts require a commit")
	require.Equal(t, "committed instructions\n", git("show", "HEAD:AGENTS.md"))
	require.Contains(t, git("diff", "--cached", "--name-only"), "AGENTS.md")
	require.Contains(t, git("ls-tree", "-r", "--name-only", "HEAD"), "reports/latest-field-notes.json")
	before = git("rev-parse", "HEAD")
	run()
	require.Equal(t, before, git("rev-parse", "HEAD"), "unchanged outputs are a no-op")
}
