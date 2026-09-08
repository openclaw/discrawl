package share

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestExportEmbeddingPolicyTransitions(t *testing.T) {
	ctx := t.Context()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	t.Cleanup(func() { _ = src.Close() })
	seedDirectMessageData(t, ctx, src)
	require.NoError(t, src.UpsertChannel(ctx, store.ChannelRecord{
		ID: "c2", GuildID: "g1", Name: "other", Kind: "text", RawJSON: `{}`,
	}))
	upsertSnapshotFilterMessage(t, ctx, src, "m2", "c2", "u2", "synthetic second message")
	blob, err := store.EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	for _, model := range []string{"model-a", "model-b"} {
		for _, id := range []string{"m1", "m2", "dm1"} {
			_, err := src.DB().ExecContext(ctx, `
				insert into message_embeddings values (?, 'openai', ?, ?, 2, ?, '2026-09-01T00:00:00Z')
			`, id, model, store.EmbeddingInputVersion, blob)
			require.NoError(t, err)
		}
	}
	_, before, err := src.ReadOnlyQuery(ctx, "select * from message_embeddings order by message_id, model")
	require.NoError(t, err)

	repo := t.TempDir()
	unrelated := filepath.Join(repo, "notes.txt")
	require.NoError(t, os.WriteFile(unrelated, []byte("keep unrelated output"), 0o600))
	opts := Options{
		RepoPath: repo, Branch: "main", IncludeEmbeddings: true,
		EmbeddingProvider: "openai", EmbeddingModel: "model-a",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
	previous, err := Export(ctx, src, opts)
	require.NoError(t, err)
	configureGitUser(t, repo)
	_, err = Commit(ctx, opts, "test: initial embedding policy")
	require.NoError(t, err)
	siblings := []string{
		"notes.txt",
		"embeddings/openai/model-a/" + store.EmbeddingInputVersion + "/undeclared.txt",
		"media/undeclared.txt",
	}
	for _, name := range siblings {
		path := filepath.Join(repo, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
		require.NoError(t, os.WriteFile(path, []byte("staged unrelated output"), 0o600))
		testGitRun(t, ctx, repo, "add", "--", name)
		require.NoError(t, os.WriteFile(path, []byte("keep unrelated output"), 0o600))
	}
	indexBefore := testGitOutput(t, ctx, repo, "diff", "--cached", "--binary")
	check := func(wantModel string, wantRows int) Manifest {
		t.Helper()
		manifest, err := Export(ctx, src, opts)
		require.NoError(t, err)
		disk := mustReadManifest(t, repo)
		require.Equal(t, manifest.Embeddings, disk.Embeddings)
		require.Equal(t, []byte("keep unrelated output"), mustReadFile(t, unrelated))
		_, after, err := src.ReadOnlyQuery(ctx, "select * from message_embeddings order by message_id, model")
		require.NoError(t, err)
		require.Equal(t, before, after, "export must not change any source vector, including DMs")
		currentFiles := map[string]bool{}
		for _, entry := range manifest.Embeddings {
			for _, name := range entry.Files {
				currentFiles[name] = true
			}
		}
		var removed []string
		for _, entry := range previous.Embeddings {
			for _, name := range entry.Files {
				if !currentFiles[name] {
					require.NoFileExists(t, filepath.Join(repo, name))
					// An already-staged managed deletion must still be published.
					testGitRun(t, ctx, repo, "add", "-u", "--", name)
					removed = append(removed, name)
				}
			}
		}
		_, err = Commit(ctx, opts, "test: embedding policy transition")
		require.NoError(t, err)
		tree := strings.Split(strings.TrimSpace(testGitOutput(t, ctx, repo, "ls-tree", "-r", "--name-only", "HEAD")), "\n")
		for _, name := range removed {
			require.NotContains(t, tree, name)
		}
		for name := range currentFiles {
			require.Contains(t, tree, name)
		}
		for _, name := range siblings {
			require.NotContains(t, tree, name)
			require.Equal(t, []byte("keep unrelated output"), mustReadFile(t, filepath.Join(repo, name)))
		}
		require.Equal(t, indexBefore, testGitOutput(t, ctx, repo, "diff", "--cached", "--binary"))
		previous = manifest
		if !opts.IncludeEmbeddings {
			require.Empty(t, manifest.Embeddings)
			return manifest
		}
		require.Len(t, manifest.Embeddings, 1)
		require.Equal(t, wantModel, manifest.Embeddings[0].Model)
		require.Equal(t, wantRows, manifest.Embeddings[0].Rows)
		text := snapshotFilesText(t, repo, manifest.Embeddings[0].Files)
		require.NotContains(t, text, `"message_id":"dm1"`)
		return manifest
	}
	check("model-a", 2)
	opts.IncludeEmbeddings = false
	check("", 0)
	opts.IncludeEmbeddings = true
	check("model-a", 2)
	opts.EmbeddingModel = "model-b"
	check("model-b", 2)
	opts.Filter.IncludeChannelIDs = []string{"c1"}
	narrowed := check("model-b", 1)
	text := snapshotFilesText(t, repo, narrowed.Embeddings[0].Files)
	require.Contains(t, text, `"message_id":"m1"`)
	require.NotContains(t, text, `"message_id":"m2"`)
	opts.Filter.IncludeChannelIDs = []string{"missing-channel"}
	empty := check("model-b", 0)
	require.Empty(t, snapshotFilesText(t, repo, empty.Embeddings[0].Files))
	opts.IncludeEmbeddings = false
	check("", 0)
}
