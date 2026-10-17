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
			On struct {
				Schedule []struct {
					Cron string `yaml:"cron"`
				} `yaml:"schedule"`
				Dispatch struct {
					Inputs map[string]struct {
						Required bool `yaml:"required"`
					} `yaml:"inputs"`
				} `yaml:"workflow_dispatch"`
			} `yaml:"on"`
			Env  map[string]string `yaml:"env"`
			Jobs map[string]struct {
				Env   map[string]string `yaml:"env"`
				Steps []struct {
					Name           string            `yaml:"name"`
					Run            string            `yaml:"run"`
					Env            map[string]string `yaml:"env"`
					TimeoutMinutes int               `yaml:"timeout-minutes"`
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
			require.Len(t, workflow.On.Schedule, 2)
			require.Equal(t, "43 0,2,4,8,10,12,14,16,18,20,22 * * *", workflow.On.Schedule[0].Cron)
			require.Equal(t, "17 6 * * *", workflow.On.Schedule[1].Cron)
			phaseMinutes := 0
			for _, step := range workflow.Jobs["publish"].Steps {
				require.Positive(t, step.TimeoutMinutes)
				phaseMinutes += step.TimeoutMinutes
			}
			// The shortest gap is 04:43 to 06:17; leave room for every phase cap.
			require.Less(t, phaseMinutes, 94)
			require.Len(t, workflow.On.Dispatch.Inputs, 1)
			require.Contains(t, workflow.On.Dispatch.Inputs, "since")
			require.False(t, workflow.On.Dispatch.Inputs["since"].Required)
			require.Equal(t, "246adcd42b9a18d0f574daeda38a9a599fecd747", workflow.Env["POLICY_COMMIT"])
			require.Equal(t, "1906b49215932b465986e5aa35bc1eeec3a2f5efbd6f63fcd930832cd30fc81f", workflow.Env["POLICY_SHA256"])
			for _, key := range []string{"POLICY_COMMIT", "POLICY_SHA256"} {
				require.Equal(t, 1, strings.Count(text, key+":"))
				require.NotContains(t, workflow.Jobs["publish"].Env, key)
			}
			for _, step := range workflow.Jobs["publish"].Steps {
				require.NotContains(t, step.Env, "POLICY_COMMIT")
				require.NotContains(t, step.Env, "POLICY_SHA256")
				if step.Name == "Verify pinned private acquisition policy" {
					require.Equal(t, "${{ inputs.since }}", step.Env["HISTORY_SINCE"])
					require.Contains(t, step.Run, `--since "$HISTORY_SINCE"`)
				}
				if step.Name == "Sync native Discord state" {
					require.Equal(t, "${{ github.event_name == 'schedule' && github.event.schedule == '17 6 * * *' }}", step.Env["WITH_MEMBERS"])
					require.Equal(t, "${{ steps.policy.outputs.since }}", step.Env["HISTORY_SINCE"])
					for _, members := range []string{"true", "false", ""} {
						dir := t.TempDir()
						calls := filepath.Join(dir, "calls")
						require.NoError(t, os.WriteFile(filepath.Join(dir, "python3"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"$CALLS\"\n"), 0o700))
						cmd := exec.CommandContext(t.Context(), "bash", "-e", "-c", step.Run)
						cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "CALLS="+calls,
							"RUNNER_TEMP="+dir, "DISCORD_BOT_TOKEN=fixture", "DISCRAWL_GUILD_ID=100", "WITH_MEMBERS="+members,
							"HISTORY_SINCE=2026-01-02T00:00:00Z", "POLICY_SHA256="+workflow.Env["POLICY_SHA256"])
						out, err := cmd.CombinedOutput()
						require.NoError(t, err, "%s", out)
						args, err := os.ReadFile(calls)
						require.NoError(t, err)
						require.Equal(t, members == "true", strings.Contains(string(args), "--with-members\n"))
						require.Contains(t, string(args), "acquire\n--timeout\n1200\n")
					}
				}
			}
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
