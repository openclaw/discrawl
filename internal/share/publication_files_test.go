package share

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPublicationOwnsExactFilesAndPreservesStagedWork(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "archive.db"))
	defer func() { _ = src.Close() }()
	blob, err := store.EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `insert into message_embeddings
		(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at)
		values ('m1','openai','fixture',?,2,?,?)`,
		store.EmbeddingInputVersion, blob, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{
		RepoPath: repo, Branch: "main", IncludeEmbeddings: true,
		EmbeddingProvider: "openai", EmbeddingModel: "fixture", EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
	first, err := Export(ctx, src, opts)
	require.NoError(t, err)
	configureGitUser(t, repo)
	committed, err := Commit(ctx, opts, "test: initial")
	require.NoError(t, err)
	require.True(t, committed)
	priorEmbedding := first.Embeddings[0].Files[0]

	unrelated := []string{"private.txt", "tables/notes.txt", "media/notes.txt", "embeddings/notes.txt", "reports/report1.md"}
	for _, name := range unrelated {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(repo, name)), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte("staged private note\n"), 0o600))
		testGitRun(t, ctx, repo, "add", "--", name)
		require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte("unstaged private note\n"), 0o600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(repo, "untracked.txt"), []byte("untracked\n"), 0o600))
	indexBefore := testGitOutput(t, ctx, repo, "diff", "--cached", "--binary", "--", unrelated[0], unrelated[1], unrelated[2], unrelated[3], unrelated[4])
	// A generated report's brackets and wildcard are literal, not Git patterns.
	opts.ReadmePath = "reports/report[1]*.md"
	require.NoError(t, os.WriteFile(filepath.Join(repo, opts.ReadmePath), []byte("generated\n"), 0o600))
	opts.IncludeEmbeddings = false
	_, err = Export(ctx, src, opts)
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(repo, priorEmbedding))
	// An already-staged managed deletion must remain part of our commit.
	testGitRun(t, ctx, repo, "add", "-u", "--", priorEmbedding)
	committed, err = Commit(ctx, opts, "test: no vectors")
	require.NoError(t, err)
	require.True(t, committed)
	tree := testGitOutput(t, ctx, repo, "ls-tree", "-r", "--name-only", "HEAD")
	require.NotContains(t, tree, priorEmbedding)
	require.Contains(t, tree, opts.ReadmePath)
	for _, name := range unrelated {
		require.NotContains(t, strings.Split(strings.TrimSpace(tree), "\n"), name)
		body, err := os.ReadFile(filepath.Join(repo, name))
		require.NoError(t, err)
		require.Equal(t, "unstaged private note\n", string(body))
	}
	require.Equal(t, indexBefore, testGitOutput(t, ctx, repo, "diff", "--cached", "--binary", "--", unrelated[0], unrelated[1], unrelated[2], unrelated[3], unrelated[4]))
	require.FileExists(t, filepath.Join(repo, "untracked.txt"))
	require.NoError(t, os.Remove(filepath.Join(repo, opts.ReadmePath)))
	committed, err = Commit(ctx, opts, "test: remove generated report")
	require.NoError(t, err)
	require.True(t, committed)
	require.NotContains(t, testGitOutput(t, ctx, repo, "ls-tree", "-r", "--name-only", "HEAD"), opts.ReadmePath)
}

func TestPublicationRejectsUnownedPreviousPaths(t *testing.T) {
	for _, name := range []string{"../private.txt", "tables/messages/private.txt", "tables/messages/../private.txt", "media/../../private.txt", "media/.git/config"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			src := seedStore(t, filepath.Join(t.TempDir(), "archive.db"))
			defer func() { _ = src.Close() }()
			repo := filepath.Join(t.TempDir(), "share")
			opts := Options{RepoPath: repo, Branch: "main"}
			manifest, err := Export(ctx, src, opts)
			require.NoError(t, err)
			manifest.Tables[0].Files = []string{name}
			body, err := json.Marshal(manifest)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(repo, ManifestName), body, 0o600))
			_, err = Export(ctx, src, opts)
			require.ErrorContains(t, err, "unowned publication path")
			after, err := os.ReadFile(filepath.Join(repo, ManifestName))
			require.NoError(t, err)
			require.Equal(t, body, after)
		})
	}
}

func TestPublicationRejectsSymlinkedDestination(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "archive.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{RepoPath: repo, Branch: "main"}
	require.NoError(t, EnsureRepo(ctx, opts))
	outside := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(repo, "tables")))
	_, err := Export(ctx, src, opts)
	require.ErrorContains(t, err, "symlinked")
	files, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Empty(t, files)
}
