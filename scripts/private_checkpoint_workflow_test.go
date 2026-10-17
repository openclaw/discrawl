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

func TestPrivateCheckpointWorkflowBoundaries(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Join(filepath.Dir(file), "..")
	for _, name := range []string{"publish-discord-backup", "discord-backup-report", "migrate-discord-checkpoint"} {
		body, err := os.ReadFile(filepath.Join(root, ".github/workflows", name+".yml"))
		require.NoError(t, err)
		text := string(body)
		require.NotContains(t, text, "actions/cache/save")
		require.NotContains(t, text, "restore-keys:")
		require.Contains(t, text, "group: discord-backup-repo")
		require.Contains(t, text, "cancel-in-progress: false")
		require.Contains(t, text, `DISCRAWL_NO_AUTO_UPDATE: "1"`)
		var workflow struct {
			Jobs map[string]struct {
				Steps []struct {
					Run string `yaml:"run"`
				} `yaml:"steps"`
			} `yaml:"jobs"`
		}
		require.NoError(t, yaml.Unmarshal(body, &workflow))
		for _, job := range workflow.Jobs {
			for _, step := range job.Steps {
				if step.Run != "" {
					cmd := exec.CommandContext(t.Context(), "bash", "-n")
					cmd.Stdin = strings.NewReader(step.Run)
					out, err := cmd.CombinedOutput()
					require.NoError(t, err, "%s: %s", name, out)
				}
			}
		}
		switch name {
		case "publish-discord-backup":
			checkpoint := strings.Index(text, "name: Verify private ingestion checkpoint")
			guard := strings.Index(text, "name: Require successful native ingestion")
			publish := strings.Index(text, "name: Publish Discord archive")
			require.Greater(t, checkpoint, 0)
			require.Greater(t, guard, checkpoint)
			require.Greater(t, publish, guard)
			require.Contains(t, text[checkpoint:guard], "always() && !cancelled()")
			require.Contains(t, text[checkpoint:guard], "--writer-receipt")
			require.Contains(t, text[publish:], "success() && steps.checkpoint.outcome == 'success'")
			require.Contains(t, text, "private_discord_checkpoint.py acquire --timeout 1200")
			require.Contains(t, text, "private_discord_checkpoint.py policy")
			require.NotContains(t, text, "schedule:")
			require.NotContains(t, text, "cloud publish")
			require.NotContains(t, text, "publish-cloud.mjs")
			require.NotContains(t, text, "DISCRAWL_CLOUD_")
			require.NotContains(t, text, "reconcile_cloud")
			require.NotContains(t, text, "actions/cache/restore")
		case "discord-backup-report":
			require.NotContains(t, text, " subscribe ")
			require.NotContains(t, text, `--config "$CONFIG" update`)
			require.NotContains(t, text, "private_discord_checkpoint.py save")
			require.Contains(t, text, "Report skipped: current checkpoint has not been published")
			require.Less(t, strings.Index(text, "report-check"), strings.Index(text, "report --published"))
		case "migrate-discord-checkpoint":
			require.NotContains(t, text, "schedule:")
			require.Contains(t, text, "fail-on-cache-miss: true")
			require.Equal(t, 5, strings.Count(text, "verify_discord_checkpoint_migration.py"))
			require.Contains(t, text, "if: inputs.mode == 'migrate'")
			require.Contains(t, text, "if: inputs.mode == 'diagnose'")
			diagnostic := text[strings.Index(text, "  diagnose:"):strings.Index(text, "  finalize:")]
			require.Contains(t, diagnostic, "timeout-minutes: 20")
			require.Contains(t, diagnostic, "CHECKPOINT_MODE: diagnose")
			require.NotContains(t, diagnostic, "actions/cache")
			require.NotContains(t, diagnostic, "private_discord_checkpoint.py")
			finalize := text[strings.Index(text, "  finalize:"):]
			require.Contains(t, finalize, "if: inputs.mode == 'finalize'")
			require.Contains(t, finalize, "CHECKPOINT_MODE: finalize")
			require.Contains(t, finalize, "timeout-minutes: 45")
			require.Contains(t, finalize, "PROBE_RUN:")
			require.NotContains(t, finalize, "actions/cache")
			require.NotContains(t, finalize, "private_discord_checkpoint.py")
			recovery := text[strings.Index(text, "  recover-publisher:"):]
			require.Contains(t, recovery, "if: inputs.mode == 'recover-publisher'")
			require.Contains(t, recovery, "timeout-minutes: 34")
			require.Contains(t, recovery, "discrawl-recovery-native")
			require.NotContains(t, recovery, "DISCORD_BOT_TOKEN")
			require.NotContains(t, recovery, "actions/cache")
			require.NotContains(t, recovery, "--push")
			require.Contains(t, text, "independent-restore.sqlite")
			require.NotContains(t, text, " sync ")
			require.NotContains(t, text, "cloud publish")
		}
	}
	_, err := os.Stat(filepath.Join(root, ".github/workflows/repair-discord-cache.yml"))
	require.True(t, os.IsNotExist(err))
}
