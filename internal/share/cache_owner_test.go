package share

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExternalSnapshotOwnerRefusesEveryGitMutation(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	testGitRun(t, ctx, repo, "init", "--initial-branch=main")
	testGitRun(t, ctx, repo, "remote", "add", "origin", "https://example.invalid/retained.git")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "keep"), []byte("retained"), 0o600))
	testGitRun(t, ctx, repo, "add", ".")
	testGitRun(t, ctx, repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "fixture")
	testGitRun(t, ctx, repo, "config", "crawlkit.snapshot-cache-owner", "external-reader")
	configBefore, err := os.ReadFile(filepath.Join(repo, ".git", "config"))
	require.NoError(t, err)
	opts := Options{RepoPath: repo, Remote: "https://example.invalid/replacement.git", Branch: "main", Tag: "snapshot"}
	for name, mutate := range map[string]func() error{
		"ensure":            func() error { return EnsureRepo(ctx, opts) },
		"pull":              func() error { return Pull(ctx, opts) },
		"export":            func() error { _, err := Export(ctx, nil, opts); return err },
		"commit":            func() error { _, err := Commit(ctx, opts, "must not commit"); return err },
		"push":              func() error { return Push(ctx, opts) },
		"tag preflight":     func() error { return ValidateTag(ctx, opts) },
		"tag creation":      func() error { _, err := CreateImmutableTag(ctx, opts); return err },
		"historical import": func() error { _, err := ImportAt(ctx, nil, opts, "HEAD"); return err },
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, mutate(), "external snapshot owner")
			configAfter, err := os.ReadFile(filepath.Join(repo, ".git", "config"))
			require.NoError(t, err)
			require.Equal(t, configBefore, configAfter, "refusal must precede origin changes")
			body, err := os.ReadFile(filepath.Join(repo, "keep"))
			require.NoError(t, err)
			require.Equal(t, "retained", string(body))
		})
	}
}

func TestInvalidSnapshotOwnerConfigurationDoesNotBecomeUnowned(t *testing.T) {
	repo := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(repo, ".git"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(repo, ".git", "config"), []byte("[invalid"), 0o600))
	require.ErrorContains(t, requireWritableRepo(t.Context(), Options{RepoPath: repo}), "cannot verify")
}
