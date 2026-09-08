package share

import (
	"os"
	"path/filepath"
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
		if !opts.IncludeEmbeddings {
			require.Empty(t, manifest.Embeddings)
			require.NoDirExists(t, filepath.Join(repo, "embeddings"))
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
	require.NoDirExists(t, filepath.Join(repo, "embeddings", "openai", "model-a"))
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
