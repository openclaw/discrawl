package share

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublicationReadOnlyRootPreservesPreviousPack(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("requires Unix directory permissions and a non-root user")
	}
	src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{RepoPath: repo}
	manifest, err := Export(t.Context(), src, opts)
	require.NoError(t, err)
	paths, err := publicationPaths(manifest)
	require.NoError(t, err)
	before := map[string][]byte{}
	for name := range paths {
		body, err := os.ReadFile(filepath.Join(repo, name))
		require.NoError(t, err)
		before[name] = body
	}
	_, err = src.DB().ExecContext(t.Context(), `update messages set content = 'edited fixture' where id = 'm1'`)
	require.NoError(t, err)
	stage := filepath.Join(t.TempDir(), "stage")
	next, err := Export(t.Context(), src, Options{RepoPath: stage})
	require.NoError(t, err)
	require.NoError(t, os.Chmod(repo, 0o555))
	t.Cleanup(func() { require.NoError(t, os.Chmod(repo, 0o755)) })
	// EnsureRepo enforces root permissions before Export. Exercise the actual
	// installer boundary where a filesystem failure can occur after that check.
	_, err = installPublication(t.Context(), repo, stage, paths, next, nil)
	require.Error(t, err)
	for name, want := range before {
		body, err := os.ReadFile(filepath.Join(repo, name))
		require.NoError(t, err, name)
		require.Equal(t, want, body, name)
	}
}

func TestPublicationManifestRenameFailureRollsBack(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("requires Unix directory permissions and a non-root user")
	}
	repo, stage, previous, manifest := publicationInstallFixture(t)
	before := publicationBytes(t, repo)
	t.Cleanup(func() { require.NoError(t, os.Chmod(repo, 0o755)) })
	installed, err := installPublication(t.Context(), repo, stage, previous, manifest, func(phase, _ string) error {
		if phase == "manifest" {
			return os.Chmod(repo, 0o555)
		}
		return nil
	})
	require.False(t, installed)
	require.ErrorContains(t, err, "install publication manifest")
	require.Equal(t, before, publicationBytes(t, repo))
}
