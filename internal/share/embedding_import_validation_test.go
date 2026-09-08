package share

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"path/filepath"
	"slices"
	"testing"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingImportsPreserveLocalDirectMessageVectors(t *testing.T) {
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	t.Cleanup(func() { _ = src.Close() })
	seedDeletedEmbeddingMessage(t, src)
	opts := embeddingValidationOptions(t.TempDir())
	manifest, err := Export(t.Context(), src, opts)
	require.NoError(t, err)
	rows := []map[string]any{
		embeddingValidationRow(t, "m1", []float32{3, 4}),
		embeddingValidationRow(t, "dm1", []float32{3, 4}),
		embeddingValidationRow(t, "deleted-message", []float32{3, 4}),
		embeddingValidationRow(t, "missing-message", []float32{3, 4}),
	}
	writeEmbeddingValidationRows(t, opts, &manifest, rows)

	for _, mode := range []string{"embeddings", "full", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			dst := seedStore(t, filepath.Join(t.TempDir(), "dst.db"))
			t.Cleanup(func() { _ = dst.Close() })
			seedDirectMessageData(t, t.Context(), dst)
			seedDeletedEmbeddingMessage(t, dst)
			for _, id := range []string{"m1", "dm1", "deleted-message"} {
				seedValidationVector(t, dst, id)
			}
			dmBefore := embeddingRowsForMessage(t, dst, "dm1")
			deletedBefore := embeddingRowsForMessage(t, dst, "deleted-message")
			require.NoError(t, runValidationImport(t, mode, dst, opts, manifest))
			require.Equal(t, dmBefore, embeddingRowsForMessage(t, dst, "dm1"), "preexisting local DM vectors must survive full table replacement")
			require.Equal(t, deletedBefore, embeddingRowsForMessage(t, dst, "deleted-message"))
			require.Empty(t, embeddingRowsForMessage(t, dst, "missing-message"))
			var blob []byte
			require.NoError(t, dst.DB().QueryRowContext(t.Context(), "select embedding_blob from message_embeddings where message_id = 'm1'").Scan(&blob))
			vector, err := store.DecodeEmbeddingVector(blob)
			require.NoError(t, err)
			require.Equal(t, []float32{3, 4}, vector, "canonical nonnumeric message keys remain supported")
			var dmCount int
			require.NoError(t, dst.DB().QueryRowContext(t.Context(), "select count(*) from messages where id = 'dm1' and guild_id = ?", store.DirectMessageGuildID).Scan(&dmCount))
			require.Equal(t, 1, dmCount)
		})
	}
}

func TestMalformedEmbeddingRowsRollBackEntireDestination(t *testing.T) {
	cases := []struct {
		name   string
		field  string
		value  any
		errMsg string
	}{
		{"provider", "provider", "other", "identity does not match"},
		{"model", "model", "other", "identity does not match"},
		{"input version", "input_version", "other", "identity does not match"},
		{"provider case", "provider", "OPENAI", "identity does not match"},
		{"blank key", "message_id", " \t", "message_id must not be blank"},
		{"zero dimensions", "dimensions", 0, "must be positive"},
		{"negative dimensions", "dimensions", -2, "must be positive"},
		{"fractional dimensions", "dimensions", 1.5, "decode dimensions"},
		{"overflow dimensions", "dimensions", json.Number("18446744073709551616"), "decode dimensions"},
		{"large dimensions", "dimensions", json.Number("4611686018427387906"), "decode"},
		{"missing dimensions", "dimensions", nil, "decode dimensions"},
		{"base64", "embedding_blob", "not-base64", "decode embedding blob"},
		{"unaligned", "embedding_blob", base64.StdEncoding.EncodeToString([]byte{1, 2, 3}), "float32 length"},
		{"empty vector", "embedding_blob", "", "float32 length"},
		{"length mismatch", "dimensions", 1, "float32 length"},
		{"NaN", "embedding_blob", embeddingValidationRow(t, "unused", []float32{float32(math.NaN()), 0})["embedding_blob"], "non-finite"},
		{"positive infinity", "embedding_blob", embeddingValidationRow(t, "unused", []float32{float32(math.Inf(1)), 0})["embedding_blob"], "non-finite"},
		{"negative infinity", "embedding_blob", embeddingValidationRow(t, "unused", []float32{float32(math.Inf(-1)), 0})["embedding_blob"], "non-finite"},
	}
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	t.Cleanup(func() { _ = src.Close() })
	_, err := src.DB().ExecContext(t.Context(), "update messages set content = 'replacement content' where id = 'm1'")
	require.NoError(t, err)
	opts := embeddingValidationOptions(t.TempDir())
	manifest, err := Export(t.Context(), src, opts)
	require.NoError(t, err)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A valid update precedes each malformed row, including rows whose
			// target is missing. Validation cannot be bypassed by a target skip.
			valid := embeddingValidationRow(t, "m1", []float32{3, 4})
			invalid := embeddingValidationRow(t, "missing-message", []float32{3, 4})
			invalid[tc.field] = tc.value
			writeEmbeddingValidationRows(t, opts, &manifest, []map[string]any{valid, invalid})
			for _, mode := range []string{"embeddings", "full", "replacement"} {
				t.Run(mode, func(t *testing.T) {
					dst := seedStore(t, filepath.Join(t.TempDir(), "dst.db"))
					t.Cleanup(func() { _ = dst.Close() })
					seedDirectMessageData(t, t.Context(), dst)
					seedValidationVector(t, dst, "m1")
					seedValidationVector(t, dst, "dm1")
					before := validationDatabaseContents(t, dst)
					require.ErrorContains(t, runValidationImport(t, mode, dst, opts, manifest), tc.errMsg)
					require.Equal(t, before, validationDatabaseContents(t, dst), "schema, canonical rows, indexes, local vectors and sync state must roll back together")
				})
			}
		})
	}
}

func embeddingValidationOptions(repo string) Options {
	return Options{
		RepoPath: repo, Branch: "main", IncludeEmbeddings: true,
		EmbeddingProvider: "openai", EmbeddingModel: "model",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
}

func embeddingValidationRow(t *testing.T, id string, vector []float32) map[string]any {
	t.Helper()
	blob, err := store.EncodeEmbeddingVector(vector)
	require.NoError(t, err)
	return map[string]any{
		"message_id": id, "provider": "openai", "model": "model",
		"input_version": store.EmbeddingInputVersion, "dimensions": len(vector),
		"embedding_blob": base64.StdEncoding.EncodeToString(blob), "embedded_at": "2026-09-01T00:00:00Z",
	}
}

func writeEmbeddingValidationRows(t *testing.T, opts Options, manifest *Manifest, rows []map[string]any) {
	t.Helper()
	entry := &manifest.Embeddings[0]
	entry.Rows = len(rows)
	var lines []string
	for _, row := range rows {
		body, err := json.Marshal(row)
		require.NoError(t, err)
		lines = append(lines, string(body))
	}
	writeGzipJSONLines(t, filepath.Join(opts.RepoPath, filepath.FromSlash(entry.Files[0])), lines)
	writeShareManifest(t, opts.RepoPath, *manifest)
}

func seedValidationVector(t *testing.T, s *store.Store, id string) {
	t.Helper()
	blob, err := store.EncodeEmbeddingVector([]float32{1, 2})
	require.NoError(t, err)
	_, err = s.DB().ExecContext(t.Context(), `
		insert into message_embeddings values (?, 'openai', 'model', ?, 2, ?, '2026-08-01T00:00:00Z')
	`, id, store.EmbeddingInputVersion, blob)
	require.NoError(t, err)
}

func seedDeletedEmbeddingMessage(t *testing.T, s *store.Store) {
	t.Helper()
	upsertSnapshotFilterMessage(t, t.Context(), s, "deleted-message", "c1", "u1", "synthetic deleted message")
	_, err := s.DB().ExecContext(t.Context(), "update messages set deleted_at = '2026-09-01T00:00:00Z' where id = 'deleted-message'")
	require.NoError(t, err)
}

func embeddingRowsForMessage(t *testing.T, s *store.Store, id string) [][]string {
	t.Helper()
	_, rows, err := s.ReadOnlyQuery(t.Context(), "select * from message_embeddings where message_id = '"+id+"' order by provider, model, input_version")
	require.NoError(t, err)
	return rows
}

func runValidationImport(t *testing.T, mode string, dst *store.Store, opts Options, manifest Manifest) error {
	t.Helper()
	switch mode {
	case "embeddings":
		return ImportEmbeddings(t.Context(), dst, opts, manifest)
	case "full":
		_, err := Import(t.Context(), dst, opts)
		return err
	case "replacement":
		_, _, err := Replace(t.Context(), dst, opts)
		return err
	default:
		t.Fatalf("unknown import mode %q", mode)
		return nil
	}
}

func validationDatabaseContents(t *testing.T, s *store.Store) map[string][][]string {
	t.Helper()
	_, schema, err := s.ReadOnlyQuery(t.Context(), "select type, name, tbl_name, sql from sqlite_schema order by type, name")
	require.NoError(t, err)
	out := map[string][][]string{"schema": schema}
	for _, entry := range schema {
		if entry[0] != "table" {
			continue
		}
		_, rows, err := s.ReadOnlyQuery(t.Context(), "select * from "+quoteIdent(entry[1]))
		require.NoError(t, err)
		slices.SortFunc(rows, slices.Compare)
		out[entry[1]] = rows
	}
	return out
}
