package share

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/crawlkit/snapshot"
	"github.com/stretchr/testify/require"
)

func TestPublicationLayoutPreservesIntegrity(t *testing.T) {
	src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
	defer func() { _ = src.Close() }()
	stage := t.TempDir()
	base, err := snapshot.Export(t.Context(), snapshot.ExportOptions{
		DB: src.DB(), RootDir: stage, Tables: []string{"messages"},
	})
	require.NoError(t, err)
	manifest := Manifest{
		Version: base.Version, GeneratedAt: base.GeneratedAt, Tables: base.Tables,
		Files: map[string]string{"fixture-alias": "unchanged"},
	}
	original, err := json.Marshal(manifest)
	require.NoError(t, err)
	var before Manifest
	require.NoError(t, json.Unmarshal(original, &before))
	old := manifest.Tables[0].Files[0]
	manifest.Tables[0].File = old
	require.Contains(t, old, "tables/.generations/")
	raw, err := os.ReadFile(filepath.Join(stage, old))
	require.NoError(t, err)
	manifest.Files[old] = "shard-alias"
	require.NoError(t, normalizePublicationTables(stage, &manifest))
	table := manifest.Tables[0]
	target := "tables/messages/" + filepath.Base(old)
	require.Equal(t, target, table.Files[0])
	require.Equal(t, target, table.File)
	require.Equal(t, target, table.FileManifests[0].Path)
	require.Equal(t, before.Tables[0].Columns, table.Columns)
	require.Equal(t, before.Tables[0].Rows, table.Rows)
	require.Equal(t, before.Tables[0].FileManifests[0].Rows, table.FileManifests[0].Rows)
	require.Equal(t, int64(len(raw)), table.FileManifests[0].Size)
	sum := sha256.Sum256(raw)
	require.Equal(t, hex.EncodeToString(sum[:]), table.FileManifests[0].SHA256)
	require.Equal(t, map[string]string{"fixture-alias": "unchanged", target: "shard-alias"}, manifest.Files)
	after, err := os.ReadFile(filepath.Join(stage, target))
	require.NoError(t, err)
	require.Equal(t, raw, after)
	require.NoError(t, normalizePublicationTables(stage, &manifest), "legacy layout is unchanged")
}

func TestPublicationLayoutRejectsCollisionsBeforeMoves(t *testing.T) {
	for _, kind := range []string{"generation", "legacy", "unlisted", "alias", "table", "unsupported", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			stage := t.TempDir()
			old := "tables/.generations/" + strings.Repeat("a", 32) + "/messages/000001.jsonl.gz"
			target := "tables/messages/000001.jsonl.gz"
			require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(stage, old)), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(stage, old), []byte("unchanged"), 0o600))
			manifest := Manifest{Tables: []TableManifest{{Name: "messages", Files: []string{old}}}}
			switch kind {
			case "generation":
				manifest.Tables[0].File = strings.Replace(old, strings.Repeat("a", 32), strings.Repeat("b", 32), 1)
			case "legacy":
				manifest.Tables[0].File = target
			case "unlisted", "symlink":
				require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(stage, target)), 0o700))
				if kind == "symlink" {
					require.NoError(t, os.Symlink(filepath.Join(stage, old), filepath.Join(stage, target)))
				} else {
					require.NoError(t, os.WriteFile(filepath.Join(stage, target), []byte("unlisted"), 0o600))
				}
			case "alias":
				manifest.Files = map[string]string{target: "unrelated"}
			case "table":
				manifest.Tables = append(manifest.Tables, manifest.Tables[0])
			case "unsupported":
				manifest.Tables[0].File = "tables/.generations/not-a-generation/messages/000001.jsonl.gz"
			}
			require.Error(t, normalizePublicationTables(stage, &manifest))
			body, err := os.ReadFile(filepath.Join(stage, old))
			require.NoError(t, err)
			require.Equal(t, "unchanged", string(body))
			require.Equal(t, old, manifest.Tables[0].Files[0])
		})
	}
}

func TestStablePublicationRejectsCorruptedBytes(t *testing.T) {
	src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(t.Context(), src, Options{RepoPath: repo, Producer: fixtureProducer()})
	require.NoError(t, err)
	name := tableEntry(t, manifest, "messages").Files[0]
	require.NotContains(t, name, ".generations")
	target := filepath.Join(repo, name)
	body, err := os.ReadFile(target)
	require.NoError(t, err)
	body[len(body)-1] ^= 1
	require.NoError(t, os.WriteFile(target, body, 0o600))
	dst := seedStore(t, filepath.Join(t.TempDir(), "dest.db"))
	defer func() { _ = dst.Close() }()
	_, err = dst.DB().ExecContext(t.Context(), `update messages set content = 'local sentinel' where id = 'm1'`)
	require.NoError(t, err)
	_, err = Import(t.Context(), dst, Options{RepoPath: repo})
	require.Error(t, err)
	var content string
	require.NoError(t, dst.DB().QueryRowContext(t.Context(), `select content from messages where id = 'm1'`).Scan(&content))
	require.Equal(t, "local sentinel", content)
	// A valid receipt does not weaken payload verification.
	_, _, err = workingPublicationBinding(repo, fixtureProducer())
	require.NoError(t, err)
}
