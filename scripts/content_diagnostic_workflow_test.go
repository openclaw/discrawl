//go:build ignore

package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestContentDiagnosticWorkflowIsAnIsolatedManualRead(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	body, err := os.ReadFile(filepath.Join(filepath.Dir(file), "../.github/workflows/diagnose-discord-content.yml"))
	require.NoError(t, err)
	var workflow struct {
		On          map[string]any    `yaml:"on"`
		Permissions map[string]string `yaml:"permissions"`
		Concurrency struct {
			Group  string `yaml:"group"`
			Cancel bool   `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
		Jobs map[string]struct {
			Timeout int               `yaml:"timeout-minutes"`
			Env     map[string]string `yaml:"env"`
			Steps   []struct {
				Uses string            `yaml:"uses"`
				With map[string]any    `yaml:"with"`
				Run  string            `yaml:"run"`
				Env  map[string]string `yaml:"env"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	require.NoError(t, yaml.Unmarshal(body, &workflow))
	// These are security policy boundaries, not evidence from source-text counts.
	require.Equal(t, []string{"workflow_dispatch"}, keys(workflow.On))
	require.Equal(t, map[string]string{"contents": "read"}, workflow.Permissions)
	require.Equal(t, "discord-backup-repo", workflow.Concurrency.Group)
	require.False(t, workflow.Concurrency.Cancel)
	require.Len(t, workflow.Jobs, 1)
	job := workflow.Jobs["diagnose"]
	require.Equal(t, 2, job.Timeout)
	require.Empty(t, job.Env)
	require.Len(t, job.Steps, 2)
	require.Equal(t, "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1", job.Steps[0].Uses)
	require.Equal(t, map[string]any{"persist-credentials": false}, job.Steps[0].With)
	require.Empty(t, job.Steps[0].Env)
	step := job.Steps[1]
	require.Empty(t, step.Uses)
	require.Equal(t, map[string]string{"DISCORD_BOT_TOKEN": "${{ secrets.DISCORD_BOT_TOKEN }}"}, step.Env)
	for _, status := range []string{"0", "1"} {
		t.Run("helper-exit-"+status, func(t *testing.T) {
			dir := t.TempDir()
			// Execute the real workflow shell with only synthetic commands/secrets.
			// No acquisition, network or GitHub command is available on this PATH.
			require.NoError(t, os.WriteFile(filepath.Join(dir, "git"), []byte("#!/bin/sh\nprintf '%040d\\n' 0\n"), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "python3"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$CALLS\"\necho synthetic-private-error >&2\nprintf '{\"error\":\"permission\"}\\n'\nexit \"$HELPER_STATUS\"\n"), 0o700))
			cmd := exec.CommandContext(t.Context(), "bash", "--noprofile", "--norc", "-e", "-c", step.Run)
			cmd.Dir = dir
			cmd.Env = []string{"PATH=" + dir, "CALLS=" + filepath.Join(dir, "calls"), "HELPER_STATUS=" + status, "DISCORD_BOT_TOKEN=synthetic-token"}
			out, err := cmd.CombinedOutput()
			if status == "0" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, string(out), "Read-only diagnostic stopped")
			}
			require.NotContains(t, string(out), "synthetic-private-error")
			require.NotContains(t, string(out), "synthetic-token")
			calls, err := os.ReadFile(filepath.Join(dir, "calls"))
			require.NoError(t, err)
			require.Equal(t, "-B\nscripts/diagnose_discord_content.py\n", string(calls))
		})
	}
}

func keys(values map[string]any) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	return result
}
