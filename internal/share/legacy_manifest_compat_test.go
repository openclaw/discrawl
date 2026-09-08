package share

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestEnrichManifestFromGitPreservesSourceMetadata(t *testing.T) {
	ctx := t.Context()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	t.Cleanup(func() { _ = src.Close() })
	opts := Options{RepoPath: t.TempDir(), Branch: "main"}
	manifest, err := Export(ctx, src, opts)
	require.NoError(t, err)
	configureGitUser(t, opts.RepoPath)
	_, err = Commit(ctx, opts, "fixture snapshot")
	require.NoError(t, err)
	originalBytes := mustReadFile(t, filepath.Join(opts.RepoPath, ManifestName))

	for i := range manifest.Tables {
		if manifest.Tables[i].Name == "messages" {
			manifest.Tables[i].FileManifests = nil
		}
	}
	for _, declaredGitHash := range []bool{false, true} {
		t.Run(map[bool]string{false: "genuine hash", true: "declared git prefix"}[declaredGitHash], func(t *testing.T) {
			if declaredGitHash {
				for i := range manifest.Tables {
					if manifest.Tables[i].Name == "guilds" {
						manifest.Tables[i].FileManifests[0].SHA256 = "git:declared-in-manifest"
					}
				}
			}
			before, err := json.Marshal(manifest)
			require.NoError(t, err)
			current := snapshotManifest(manifest)
			enriched := enrichManifestFromGit(ctx, opts.RepoPath, "HEAD", manifest)
			after, err := json.Marshal(manifest)
			require.NoError(t, err)
			require.Equal(t, before, after, "enrichment must not mutate the caller's table slice")
			require.Empty(t, tableEntry(t, manifest, "messages").FileManifests)
			for _, table := range current.Tables {
				if table.Name == "messages" {
					require.Empty(t, table.FileManifests, "raw Current must remain legacy")
				}
			}
			require.True(t, strings.HasPrefix(tableEntry(t, enriched, "messages").FileManifests[0].SHA256, "git:"))
			require.Equal(t, tableEntry(t, manifest, "guilds"), tableEntry(t, enriched, "guilds"), "declared modern metadata must remain authoritative, even with a git prefix")
			require.Equal(t, originalBytes, mustReadFile(t, filepath.Join(opts.RepoPath, ManifestName)))
		})
	}
}

func TestMergeIfChangedMultiShardLegacyAfterHistoricalRestore(t *testing.T) {
	ctx := t.Context()
	oldMax := maxShardBytes
	maxShardBytes = 150
	t.Cleanup(func() { maxShardBytes = oldMax })
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	t.Cleanup(func() { _ = src.Close() })
	upsertSnapshotFilterMessage(t, ctx, src, "m2", "c1", "u1", "second legacy message")
	opts := Options{RepoPath: t.TempDir(), Branch: "main"}
	publish := func(message string) Manifest {
		t.Helper()
		manifest, err := Export(ctx, src, opts)
		require.NoError(t, err)
		require.Greater(t, len(tableEntry(t, manifest, "messages").Files), 1)
		manifest = stripFileManifests(manifest)
		writeShareManifest(t, opts.RepoPath, manifest)
		configureGitUser(t, opts.RepoPath)
		committed, err := Commit(ctx, opts, message)
		require.NoError(t, err)
		require.True(t, committed)
		return manifest
	}
	initial := publish("initial legacy snapshot")
	initialRef := strings.TrimSpace(testGitOutput(t, ctx, opts.RepoPath, "rev-parse", "HEAD"))
	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close() })
	_, changed, err := MergeIfChanged(ctx, dst, opts)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, 2, legacyMessageCount(t, dst))

	upsertSnapshotFilterMessage(t, ctx, src, "m3", "c1", "u1", "third legacy message")
	current := publish("updated legacy snapshot")
	currentRef := strings.TrimSpace(testGitOutput(t, ctx, opts.RepoPath, "rev-parse", "HEAD"))
	currentBytes := mustReadFile(t, filepath.Join(opts.RepoPath, ManifestName))
	_, changed, err = MergeIfChanged(ctx, dst, opts)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, 3, legacyMessageCount(t, dst))

	historical, err := ImportAt(ctx, dst, opts, initialRef)
	require.NoError(t, err)
	require.Equal(t, 2, legacyMessageCount(t, dst))
	expected := enrichManifestFromGit(ctx, opts.RepoPath, initialRef, initial)
	require.Equal(t, expected, historical, "checkpoint fingerprints must use the resolved historical commit")
	previous, ok := PreviousMergedManifest(ctx, dst, opts)
	require.True(t, ok)
	imported, ok := PreviousImportedManifest(ctx, dst, opts)
	require.True(t, ok)
	wantState, err := json.Marshal(historical)
	require.NoError(t, err)
	for _, checkpoint := range []Manifest{previous, imported} {
		gotState, err := json.Marshal(checkpoint)
		require.NoError(t, err)
		require.JSONEq(t, string(wantState), string(gotState))
	}
	require.Equal(t, currentRef, strings.TrimSpace(testGitOutput(t, ctx, opts.RepoPath, "rev-parse", "HEAD")))
	require.Equal(t, currentBytes, mustReadFile(t, filepath.Join(opts.RepoPath, ManifestName)))

	latest, changed, err := MergeIfChanged(ctx, dst, opts)
	require.NoError(t, err)
	require.True(t, changed, "historical checkpoint must permit the next legacy merge")
	require.Equal(t, current.GeneratedAt, latest.GeneratedAt)
	require.Equal(t, 3, legacyMessageCount(t, dst))
	_, changed, err = MergeIfChanged(ctx, dst, opts)
	require.NoError(t, err)
	require.False(t, changed)
}

func legacyMessageCount(t *testing.T, s *store.Store) int {
	t.Helper()
	var count int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), "select count(*) from messages").Scan(&count))
	return count
}
