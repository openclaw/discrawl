package share

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublicationInstallFailures(t *testing.T) {
	tests := []struct {
		name, phase, file string
		cancel            bool
		installed         bool
	}{
		{"prepare manifest", "prepare", ManifestName, false, false},
		{"prepare later file", "prepare", "tables/messages/000002.jsonl.gz", false, false},
		{"first replacement", "install", "tables/messages/000001.jsonl.gz", false, false},
		{"later replacement", "install", "tables/messages/000002.jsonl.gz", false, false},
		{"manifest", "manifest", ManifestName, false, false},
		{"cancel mid-install", "install", "tables/messages/000002.jsonl.gz", true, false},
		{"cancel before manifest", "manifest", ManifestName, true, false},
		{"cleanup", "cleanup", "embeddings/fixture/model/v1/000001.jsonl.gz", false, true},
		{"cancel cleanup", "cleanup", "media/old.gz", true, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo, stage, previous, manifest := publicationInstallFixture(t)
			before := publicationBytes(t, repo)
			index := testGitOutput(t, t.Context(), repo, "diff", "--cached", "--binary")
			head := testGitOutput(t, t.Context(), repo, "rev-parse", "HEAD")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fault := errors.New("fixture failure")
			installed, err := installPublication(ctx, repo, stage, previous, manifest, func(phase, name string) error {
				if phase == test.phase && name == test.file {
					if test.cancel {
						cancel()
						return nil
					}
					return fault
				}
				return nil
			})
			require.Equal(t, test.installed, installed)
			if test.cancel {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, fault)
			}
			require.Equal(t, index, testGitOutput(t, t.Context(), repo, "diff", "--cached", "--binary"))
			require.Equal(t, head, testGitOutput(t, t.Context(), repo, "rev-parse", "HEAD"))
			after := publicationBytes(t, repo)
			if !installed {
				require.Equal(t, before, after)
				info, err := os.Stat(filepath.Join(repo, "tables/messages/000001.jsonl.gz"))
				require.NoError(t, err)
				require.Equal(t, fs.FileMode(0o640), info.Mode().Perm())
				require.Empty(t, publicationPrivateDirs(t, repo))
				return
			}
			require.ErrorContains(t, err, "manifest installed")
			current, err := publicationPaths(manifest)
			require.NoError(t, err)
			stageBytes := publicationBytes(t, stage)
			for name := range current {
				require.Equal(t, stageBytes[name], after[name], name)
			}
			require.NotEmpty(t, publicationPrivateDirs(t, repo))
			require.Equal(t, before["private.txt"], after["private.txt"])
		})
	}
}

func TestPublicationRollbackFailureRetainsPreimages(t *testing.T) {
	repo, stage, previous, manifest := publicationInstallFixture(t)
	original := publicationBytes(t, repo)
	index := testGitOutput(t, t.Context(), repo, "diff", "--cached", "--binary")
	fault, rollbackFault := errors.New("manifest failure"), errors.New("restore failure")
	installed, err := installPublication(t.Context(), repo, stage, previous, manifest, func(phase, name string) error {
		if phase == "manifest" {
			return fault
		}
		if phase == "rollback" && name == "tables/messages/000001.jsonl.gz" {
			return rollbackFault
		}
		return nil
	})
	require.False(t, installed)
	require.ErrorIs(t, err, fault)
	require.ErrorIs(t, err, rollbackFault)
	require.ErrorContains(t, err, "may be partially installed")
	require.ErrorContains(t, err, "before publishing again")
	require.Equal(t, index, testGitOutput(t, t.Context(), repo, "diff", "--cached", "--binary"))
	require.NoFileExists(t, filepath.Join(repo, "tables/messages/000003.jsonl.gz"))
	require.Equal(t, original[ManifestName], publicationBytes(t, repo)[ManifestName])
	var found bool
	for _, dir := range publicationPrivateDirs(t, repo) {
		if filepath.Dir(dir) == filepath.Join(repo, "tables/messages") && strings.Contains(filepath.Base(dir), "000001.jsonl.gz") {
			body, readErr := os.ReadFile(filepath.Join(dir, "previous"))
			require.NoError(t, readErr)
			require.Equal(t, original["tables/messages/000001.jsonl.gz"], string(body))
			require.Contains(t, err.Error(), dir)
			found = true
		}
	}
	require.True(t, found, "original working-tree bytes retained, not HEAD bytes")
}

func TestFirstPublicationRollback(t *testing.T) {
	repo, stage, _, manifest := publicationInstallFixture(t)
	fresh := t.TempDir()
	fault := errors.New("manifest failure")
	installed, err := installPublication(t.Context(), fresh, stage, nil, manifest, func(phase, _ string) error {
		if phase == "manifest" {
			return fault
		}
		return nil
	})
	require.False(t, installed)
	require.ErrorIs(t, err, fault)
	require.Empty(t, publicationBytes(t, fresh))
	require.Empty(t, publicationPrivateDirs(t, fresh))
	// A successful first installation needs no previous manifest or Git commit.
	installed, err = installPublication(t.Context(), fresh, stage, nil, manifest, nil)
	require.NoError(t, err)
	require.True(t, installed)
	require.Equal(t, publicationBytes(t, stage), publicationBytes(t, fresh))
	require.NotEmpty(t, publicationBytes(t, repo))
}

func TestExportReturnsInstalledManifestOnCleanupError(t *testing.T) {
	src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{RepoPath: repo}
	first, err := Export(t.Context(), src, opts)
	require.NoError(t, err)
	first.Media = &MediaManifest{Files: nil}
	// This is a previously declared media file, not an unrelated sibling.
	writeShareManifest(t, repo, first)
	name := "tables/messages/999999.jsonl.gz"
	first.Tables[0].Name = "messages"
	first.Tables[0].File = name
	first.Tables[0].Files = []string{name}
	first.Tables[0].FileManifests = nil
	require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte("obsolete"), 0o600))
	writeShareManifest(t, repo, first)
	fault := errors.New("cleanup failure")
	result, err := exportPublication(t.Context(), src, opts, func(phase, _ string) error {
		if phase == "cleanup" {
			return fault
		}
		return nil
	})
	require.ErrorIs(t, err, fault)
	require.False(t, result.GeneratedAt.IsZero())
	actual, readErr := ReadManifest(repo)
	require.NoError(t, readErr)
	require.Equal(t, actual, result)
}

func publicationInstallFixture(t *testing.T) (string, string, map[string]bool, Manifest) {
	t.Helper()
	repo, stage := t.TempDir(), t.TempDir()
	opts := Options{RepoPath: repo}
	require.NoError(t, EnsureRepo(t.Context(), opts))
	configureGitUser(t, repo)
	previous := map[string]bool{
		ManifestName: true, "tables/messages/000001.jsonl.gz": true,
		"tables/messages/000002.jsonl.gz":             true,
		"embeddings/fixture/model/v1/000001.jsonl.gz": true, "media/old.gz": true,
	}
	for name := range previous {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(repo, name)), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte("HEAD "+name), 0o640))
	}
	require.NoError(t, os.WriteFile(filepath.Join(repo, "private.txt"), []byte("HEAD private"), 0o600))
	testGitRun(t, t.Context(), repo, "add", ".")
	testGitRun(t, t.Context(), repo, "-c", "commit.gpgsign=false", "commit", "-m", "fixture")
	for name := range previous {
		require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte("working "+name), 0o640))
	}
	require.NoError(t, os.WriteFile(filepath.Join(repo, "private.txt"), []byte("index private"), 0o600))
	testGitRun(t, t.Context(), repo, "add", "private.txt")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "private.txt"), []byte("working private"), 0o600))
	manifest := Manifest{Version: 1, Tables: []TableManifest{{
		Name: "messages", Files: []string{
			"tables/messages/000001.jsonl.gz", "tables/messages/000002.jsonl.gz", "tables/messages/000003.jsonl.gz",
		},
	}}}
	current, err := publicationPaths(manifest)
	require.NoError(t, err)
	for name := range current {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(stage, name)), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(stage, name), []byte("new "+name), 0o600))
	}
	writeShareManifest(t, stage, manifest)
	return repo, stage, previous, manifest
}

func publicationBytes(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || strings.HasPrefix(entry.Name(), ".discrawl-install-") {
				return filepath.SkipDir
			}
			return nil
		}
		body, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, name)
		files[filepath.ToSlash(rel)] = string(body)
		return err
	}))
	return files
}

func publicationPrivateDirs(t *testing.T, root string) []string {
	t.Helper()
	var dirs []string
	require.NoError(t, filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && strings.HasPrefix(entry.Name(), ".discrawl-install-") {
			dirs = append(dirs, name)
			return filepath.SkipDir
		}
		return nil
	}))
	return dirs
}
