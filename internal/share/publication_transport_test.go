package share

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/snapshot"
	"github.com/stretchr/testify/require"
)

func TestPublicationCommitUsesBoundedLiteralTransport(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("argv-budget wrapper requires a POSIX shell")
	}
	ctx := context.Background()
	repo := t.TempDir()
	opts := Options{RepoPath: repo, Branch: "main"}
	require.NoError(t, EnsureRepo(ctx, opts))
	configureGitUser(t, repo)

	unrelated := "media/name1match.bin"
	write := func(name, body string) {
		t.Helper()
		filename := filepath.Join(repo, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(filename), 0o700))
		require.NoError(t, os.WriteFile(filename, []byte(body), 0o600))
	}
	write(unrelated, "staged private note\n")
	testGitRun(t, ctx, repo, "add", "--", unrelated)
	write(unrelated, "unstaged private note\n")
	indexBefore := testGitOutput(t, ctx, repo, "diff", "--cached", "--binary", "--", unrelated)

	names := []string{"media/name[1]*?.bin", "media/name\t\"quoted\".bin", "media/-dash file.bin", "media/ space .bin"}
	for i := range 512 {
		names = append(names, "media/"+strconv.Itoa(i)+"-"+strings.Repeat("x", 80))
	}
	manifest := Manifest{Version: 1, Media: &MediaManifest{}}
	for _, name := range names {
		write(name, "fixture\n")
		manifest.Media.Files = append(manifest.Media.Files, snapshot.FileManifest{Path: name})
	}
	writeShareManifest(t, repo, manifest)

	realGit, err := exec.LookPath("git")
	require.NoError(t, err)
	shim := t.TempDir()
	// This small artificial budget detects growing argv without an OS stress test.
	const wrapper = `#!/bin/sh
case "$DISCRAWL_TEST_GIT_MODE" in
failure)
  printf 'synthetic private stdout\n'
  printf 'synthetic private stderr\n' >&2
  exit 42;;
unterminated)
  printf 'synthetic private unterminated path'
  exit 0;;
oversized)
  printf '%070000d' 0
  exit 0;;
wait)
  exec sleep 30;;
esac
size=0
for arg do size=$((size + ${#arg} + 1)); done
if [ "$size" -gt 2048 ]; then
  printf 'synthetic Git argv budget exceeded: %s\n' "$size" >&2
  exit 93
fi
exec "$DISCRAWL_TEST_REAL_GIT" "$@"
`
	require.NoError(t, os.WriteFile(filepath.Join(shim, "git"), []byte(wrapper), 0o700)) // #nosec G306 -- executable test-owned wrapper delegates only to the resolved Git binary.
	t.Setenv("DISCRAWL_TEST_REAL_GIT", realGit)
	t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))

	assertUnrelated := func() {
		t.Helper()
		require.Equal(t, indexBefore, testGitOutput(t, ctx, repo, "diff", "--cached", "--binary", "--", unrelated))
		body, err := os.ReadFile(filepath.Join(repo, unrelated)) // #nosec G304 -- fixed sentinel written by this test under its temporary repository.
		require.NoError(t, err)
		require.Equal(t, "unstaged private note\n", string(body))
	}
	changed, err := Commit(ctx, opts, "test: initial owned paths")
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "1", strings.TrimSpace(testGitOutput(t, ctx, repo, "rev-list", "--count", "HEAD")))
	tree := strings.Split(testGitOutput(t, ctx, repo, "ls-tree", "-r", "-z", "--name-only", "HEAD"), "\x00")
	require.Contains(t, tree, ManifestName)
	require.NotContains(t, tree, unrelated)
	for _, name := range names {
		require.Contains(t, tree, name)
	}
	assertUnrelated()
	head := testGitOutput(t, ctx, repo, "rev-parse", "HEAD")
	changed, err = Commit(ctx, opts, "test: unchanged owned paths")
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, head, testGitOutput(t, ctx, repo, "rev-parse", "HEAD"))
	assertUnrelated()

	for _, name := range names[:2] {
		require.NoError(t, os.Remove(filepath.Join(repo, name)))
	}
	testGitRun(t, ctx, repo, "--literal-pathspecs", "add", "-u", "--", names[0])
	manifest.Media.Files = manifest.Media.Files[2:]
	writeShareManifest(t, repo, manifest)
	changed, err = Commit(ctx, opts, "test: current and staged prior deletions")
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "2", strings.TrimSpace(testGitOutput(t, ctx, repo, "rev-list", "--count", "HEAD")))
	tree = strings.Split(testGitOutput(t, ctx, repo, "ls-tree", "-r", "-z", "--name-only", "HEAD"), "\x00")
	require.NotContains(t, tree, names[0])
	require.NotContains(t, tree, names[1])
	require.NotContains(t, tree, unrelated)
	assertUnrelated()

	t.Run("mutation errors omit output", func(t *testing.T) {
		t.Setenv("DISCRAWL_TEST_GIT_MODE", "failure")
		err := runPublicationGit(ctx, repo, []string{ManifestName}, "add", "--pathspec-from-file=-", "--pathspec-file-nul")
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		require.Equal(t, 42, exit.ExitCode())
		require.NotContains(t, err.Error(), "synthetic private")
	})
	for _, mode := range []string{"unterminated", "oversized", "failure"} {
		t.Run("staged output "+mode, func(t *testing.T) {
			t.Setenv("DISCRAWL_TEST_GIT_MODE", mode)
			changed, err := publicationHasChanges(ctx, repo, map[string]bool{ManifestName: true})
			require.Error(t, err)
			require.False(t, changed)
			require.NotContains(t, err.Error(), "synthetic private")
		})
	}
	for _, inspect := range []bool{false, true} {
		t.Run("cancellation "+strconv.FormatBool(inspect), func(t *testing.T) {
			t.Setenv("DISCRAWL_TEST_GIT_MODE", "wait")
			ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			var err error
			if inspect {
				_, err = publicationHasChanges(ctx, repo, map[string]bool{ManifestName: true})
			} else {
				err = runPublicationGit(ctx, repo, nil, "status")
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
}

func TestPublicationEmptySelectionNeverCommitsUnrelatedIndex(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	opts := Options{RepoPath: repo, Branch: "main"}
	require.NoError(t, EnsureRepo(ctx, opts))
	configureGitUser(t, repo)
	require.NoError(t, os.WriteFile(filepath.Join(repo, "private.txt"), []byte("private\n"), 0o600))
	testGitRun(t, ctx, repo, "add", "--", "private.txt")
	index := testGitOutput(t, ctx, repo, "ls-files", "--stage", "-z")
	changed, err := Commit(ctx, opts, "must not commit")
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, index, testGitOutput(t, ctx, repo, "ls-files", "--stage", "-z"))
	_, err = publicationGit(ctx, repo, nil, "rev-parse", "--verify", "HEAD")
	require.Error(t, err)
}
