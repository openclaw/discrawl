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
						Required bool     `yaml:"required"`
						Type     string   `yaml:"type"`
						Default  string   `yaml:"default"`
						Options  []string `yaml:"options"`
					} `yaml:"inputs"`
				} `yaml:"workflow_dispatch"`
			} `yaml:"on"`
			Env  map[string]string `yaml:"env"`
			Jobs map[string]struct {
				Env            map[string]string `yaml:"env"`
				TimeoutMinutes int               `yaml:"timeout-minutes"`
				Steps          []struct {
					ID             string            `yaml:"id"`
					If             string            `yaml:"if"`
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
			checkpoint := strings.Index(text, "name: Upload private ingestion checkpoint")
			verify := strings.Index(text, "name: Verify private ingestion checkpoint")
			promote := strings.Index(text, "name: Promote verified private checkpoint")
			guard := strings.Index(text, "name: Require successful native ingestion")
			publish := strings.Index(text, "name: Publish Discord archive")
			reconcile := strings.Index(text, "name: Reconcile retained checkpoint publication")
			restore := strings.Index(text, "name: Restore verified private checkpoint")
			require.Greater(t, reconcile, 0)
			require.Less(t, reconcile, restore)
			require.Contains(t, text, "CHECKPOINT_MODE: reconcile")
			require.Contains(t, text, "CHECKPOINT_MODE=publication-message")
			require.Greater(t, checkpoint, 0)
			require.Greater(t, verify, checkpoint)
			require.Greater(t, promote, verify)
			require.Greater(t, guard, promote)
			require.Contains(t, text[checkpoint:verify], "save --timeout 540")
			require.Contains(t, text[verify:promote], "verify-save --timeout 900")
			require.Contains(t, text[promote:guard], "promote-save --timeout 180")
			require.Greater(t, publish, guard)
			require.Contains(t, text[checkpoint:guard], "--writer-receipt")
			require.Contains(t, text, "private_discord_checkpoint.py acquire --timeout 1200")
			require.Contains(t, text, "private_discord_checkpoint.py policy")
			require.Len(t, workflow.On.Schedule, 2)
			require.Equal(t, "43 0,2,4,8,10,12,14,16,18,20,22 * * *", workflow.On.Schedule[0].Cron)
			require.Equal(t, "17 6 * * *", workflow.On.Schedule[1].Cron)
			commonMinutes, captureMinutes, recoveryMinutes := 0, 0, 0
			capture := false
			conditions := map[string]string{
				"Restore verified private checkpoint":         "${{ success() && steps.reconcile.outputs.recovered == 'false' }}",
				"Sync native Discord state":                   "${{ success() && steps.restore.outcome == 'success' }}",
				"Upload private ingestion checkpoint":         "${{ always() && !cancelled() && steps.restore.outcome == 'success' && steps.sync.outcome != 'skipped' }}",
				"Verify private ingestion checkpoint":         "${{ always() && !cancelled() && steps.checkpoint-upload.outcome == 'success' }}",
				"Promote verified private checkpoint":         "${{ always() && !cancelled() && steps.checkpoint-verify.outcome == 'success' }}",
				"Require successful native ingestion":         "${{ always() && !cancelled() && steps.sync.outcome != 'skipped' && steps.sync.outcome != 'success' }}",
				"Publish Discord archive":                     "${{ success() && steps.checkpoint.outcome == 'success' }}",
				"Setup Node for cloud publication":            "${{ success() && vars.DISCRAWL_CLOUD_PUBLISH_ENABLED == '1' && steps.git-publish.outputs.published == 'true' }}",
				"Publish the same current state to D1 and R2": "${{ success() && vars.DISCRAWL_CLOUD_PUBLISH_ENABLED == '1' && steps.git-publish.outputs.published == 'true' }}",
			}
			for _, step := range workflow.Jobs["publish"].Steps {
				require.Positive(t, step.TimeoutMinutes)
				if step.Name == "Reconcile retained checkpoint publication" {
					require.Equal(t, "reconcile", step.ID)
					recoveryMinutes = step.TimeoutMinutes
					capture = true
				} else if capture {
					// Recovered=true (or missing on failure) skips restore, and every
					// descendant requires its predecessor; no fresh work follows recovery.
					require.Contains(t, conditions, step.Name)
					require.Equal(t, conditions[step.Name], step.If)
					captureMinutes += step.TimeoutMinutes
				} else {
					commonMinutes += step.TimeoutMinutes
				}
			}
			require.Equal(t, 170, workflow.Jobs["publish"].TimeoutMinutes)
			require.Equal(t, 34, recoveryMinutes)
			// A no-op spends at most the 120s metadata budget; a recovery run
			// spends its existing 34min step and performs no fresh acquisition.
			require.Less(t, commonMinutes+captureMinutes+2, workflow.Jobs["publish"].TimeoutMinutes)
			require.Less(t, commonMinutes+recoveryMinutes, workflow.Jobs["publish"].TimeoutMinutes)
			require.Len(t, workflow.On.Dispatch.Inputs, 2)
			require.Contains(t, workflow.On.Dispatch.Inputs, "since")
			require.False(t, workflow.On.Dispatch.Inputs["since"].Required)
			require.Equal(t, "choice", workflow.On.Dispatch.Inputs["mode"].Type)
			require.Equal(t, "current", workflow.On.Dispatch.Inputs["mode"].Default)
			require.Equal(t, []string{"current", "historical"}, workflow.On.Dispatch.Inputs["mode"].Options)
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
					require.Equal(t, "${{ github.event_name == 'workflow_dispatch' && inputs.mode || 'current' }}", step.Env["HISTORY_MODE"])
					require.Contains(t, step.Run, `--acquisition-mode "$HISTORY_MODE"`)
				}
				if step.Name == "Sync native Discord state" {
					require.Contains(t, step.Run, "run --timeout 60 --phase init --")
					require.Contains(t, step.Run, `"$RUNNER_TEMP/discrawl-native" --public-errors --config "$CONFIG" init`)
					require.Equal(t, "${{ github.event_name == 'schedule' && github.event.schedule == '17 6 * * *' }}", step.Env["WITH_MEMBERS"])
					require.Equal(t, "${{ steps.policy.outputs.since }}", step.Env["HISTORY_SINCE"])
					require.Equal(t, "${{ github.event_name == 'workflow_dispatch' && inputs.mode || 'current' }}", step.Env["HISTORY_MODE"])
					for _, members := range []string{"true", "false", ""} {
						dir := t.TempDir()
						calls := filepath.Join(dir, "calls")
						require.NoError(t, os.WriteFile(filepath.Join(dir, "python3"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"$CALLS\"\n"), 0o700))
						cmd := exec.CommandContext(t.Context(), "bash", "--noprofile", "--norc", "-e", "-c", step.Run)
						cmd.Dir = dir
						cmd.Env = []string{
							"HOME=" + dir, "TMPDIR=" + dir, "PATH=" + dir + ":/usr/bin:/bin", "CALLS=" + calls,
							"RUNNER_TEMP=" + dir, "DISCORD_BOT_TOKEN=fixture", "DISCRAWL_GUILD_ID=100", "WITH_MEMBERS=" + members,
							"HISTORY_SINCE=2026-01-02T00:00:00Z", "HISTORY_MODE=historical", "POLICY_SHA256=" + workflow.Env["POLICY_SHA256"],
						}
						out, err := cmd.CombinedOutput()
						require.NoError(t, err, "%s", out)
						args, err := os.ReadFile(calls)
						require.NoError(t, err)
						require.Equal(t, members == "true", strings.Contains(string(args), "--with-members\n"))
						require.Contains(t, string(args), "acquire\n--timeout\n1200\n")
						require.Contains(t, string(args), "--acquisition-mode\nhistorical\n")
					}
				}
			}
			require.NotContains(t, text, "reconcile_cloud")
			require.NotContains(t, text, "actions/cache/restore")
		case "discord-backup-report":
			require.NotContains(t, text, " subscribe ")
			require.NotContains(t, text, `--config "$CONFIG" update`)
			require.NotContains(t, text, "private_discord_checkpoint.py save")
			require.Contains(t, text, "Report skipped: current checkpoint has not been published")
			check, report := strings.Index(text, "report-check"), strings.Index(text, "report --published")
			require.Positive(t, check)
			require.Greater(t, report, check)
		case "migrate-discord-checkpoint":
			require.NotContains(t, text, "schedule:")
			require.Contains(t, text, "fail-on-cache-miss: true")
			// Each privileged mode remains separately selected; helper invocation counts
			// are not a behavioral boundary.
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
			require.Contains(t, recovery, "discrawl-native")
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

// Exercise the actual workflow shell with no host credentials or network tools.
func TestCloudPublicationWorkflow(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	body, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", ".github/workflows/publish-discord-backup.yml"))
	require.NoError(t, err)
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				ID  string            `yaml:"id"`
				Run string            `yaml:"run"`
				Env map[string]string `yaml:"env"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	require.NoError(t, yaml.Unmarshal(body, &workflow))
	var script string
	for _, step := range workflow.Jobs["publish"].Steps {
		if step.ID != "cloud-publish" {
			continue
		}
		script = step.Run
		require.Equal(t, "${{ secrets.DISCORD_BACKUP_TOKEN }}", step.Env["DISCRAWL_CLOUD_AUTH_GITHUB_TOKEN"])
		require.Equal(t, "${{ github.workspace }}/.discrawl-ci/discrawl.db", step.Env["DISCRAWL_CLOUD_SOURCE_DB"])
	}
	require.NotEmpty(t, script, "successful native/Git collection must reach the existing cloud publisher")
	for _, failure := range []string{"", "revision", "git", "export", "cloud"} {
		t.Run("failure="+failure, func(t *testing.T) {
			dir := t.TempDir()
			calls, snapshot := filepath.Join(dir, "calls"), filepath.Join(dir, "cloud.db")
			write := func(name, text string) {
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0o700))
			}
			write("git", `#!/bin/bash
if [[ "$1" == rev-parse ]]; then
 if [[ "$FAILURE" == revision ]]; then echo wrong; else echo reviewed-source; fi
else
 [[ "$FAILURE" != git ]] || exit 1
 echo published-source
fi
`)
			write("discrawl-native", `#!/bin/bash
printf 'export:%s\n' "$*" >> "$CALLS"
[[ "$*" == *'cloud publish --export-only '* ]] || exit 20
printf 'frozen native export' > "$DISCRAWL_CLOUD_SNAPSHOT"
[[ "$FAILURE" != export ]]
`)
			write("node", `#!/bin/bash
[[ "$1" == scripts/publish-cloud.mjs ]] || exit 21
[[ "$DISCRAWL_CLOUD_SOURCE_COMMIT" == published-source ]] || exit 22
[[ "$DISCRAWL_CLOUD_SOURCE_DB" == "$DB" ]] || exit 23
[[ "$DISCRAWL_CLOUD_AUTH_GITHUB_TOKEN" == fixture-token ]] || exit 24
[[ -f "$DISCRAWL_CLOUD_SNAPSHOT" ]] || exit 25
printf 'cloud:%s\n' "$DISCRAWL_CLOUD_SOURCE_COMMIT" >> "$CALLS"
[[ "$FAILURE" != cloud ]]
`)
			cmd := exec.CommandContext(t.Context(), "bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script)
			cmd.Dir = dir
			cmd.Env = []string{
				"HOME=" + dir, "TMPDIR=" + dir, "PATH=" + dir + ":/usr/bin:/bin", "CALLS=" + calls, "FAILURE=" + failure,
				"RUNNER_TEMP=" + dir, "CONFIG=" + filepath.Join(dir, "config.toml"), "GITHUB_SHA=reviewed-source", "BACKUP_REPO=" + filepath.Join(dir, "archive"),
				"DB=" + filepath.Join(dir, "native.db"), "DISCRAWL_CLOUD_SOURCE_DB=" + filepath.Join(dir, "native.db"),
				"DISCRAWL_CLOUD_AUTH_GITHUB_TOKEN=fixture-token", "DISCRAWL_CLOUD_SNAPSHOT=" + snapshot,
			}
			out, err := cmd.CombinedOutput()
			if failure == "" {
				require.NoError(t, err, "%s", out)
			} else {
				require.Error(t, err, "%s", out)
			}
			_, err = os.Stat(snapshot)
			require.True(t, os.IsNotExist(err), "export is removed on success and failure")
			log, readErr := os.ReadFile(calls)
			if failure == "revision" || failure == "git" {
				require.True(t, os.IsNotExist(readErr), "changed source must not export or upload")
			} else {
				require.NoError(t, readErr)
				require.Equal(t, failure != "export", strings.Contains(string(log), "cloud:published-source"))
			}
			require.NotContains(t, string(out), "fixture-token")
		})
	}
}
