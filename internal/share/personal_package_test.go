package share

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/discordpackage"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPersonalPackageEvidenceStaysOutOfSharedSnapshot(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	t.Cleanup(func() { require.NoError(t, src.Close()) })
	input := t.TempDir()
	channel := filepath.Join(input, "messages", "900719925474099303")
	require.NoError(t, os.MkdirAll(channel, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(channel, "channel.json"), []byte(`{"id":"900719925474099303","type":1,"recipients":["900719925474099302"]}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(channel, "messages.json"), []byte(`[{"ID":"900719925474099304","Timestamp":"2026-01-01T00:00:00Z","Contents":"PRIVATE_PACKAGE_EVIDENCE","Attachments":"https://example.invalid/PRIVATE_PACKAGE_ATTACHMENT"}]`), 0o600))
	_, err := discordpackage.Import(ctx, src, discordpackage.Options{Path: input, AccountID: "900719925474099301"})
	require.NoError(t, err)
	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{
		RepoPath: repo, Branch: "main", IncludeEmbeddings: true,
		EmbeddingProvider: "openai", EmbeddingModel: "text-embedding-3-small",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	})
	require.NoError(t, err)
	for _, table := range SnapshotTables {
		body := snapshotTableText(t, repo, tableEntry(t, manifest, table))
		require.NotContains(t, body, "PRIVATE_PACKAGE")
		require.NotContains(t, body, "90071992547409930")
		require.NotContains(t, body, "discord_data_package")
	}
	require.Equal(t, 1, tableEntry(t, manifest, "messages").Rows)
}
