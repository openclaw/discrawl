package share

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/openclaw/discrawl/internal/store"
)

// Embedding snapshots are gzip-compressed JSONL. Bound decompression so a
// malformed snapshot cannot force unbounded JSON decoder reads.
var maxSharedEmbeddingDecompressedBytes int64 = 1 << 30

func ImportEmbeddings(ctx context.Context, s *store.Store, opts Options, manifest Manifest) error {
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := importEmbeddings(ctx, tx, opts, manifest.Embeddings); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func exportEmbeddings(ctx context.Context, db *sql.DB, opts Options) (EmbeddingManifest, error) {
	provider := strings.ToLower(strings.TrimSpace(opts.EmbeddingProvider))
	model := strings.TrimSpace(opts.EmbeddingModel)
	inputVersion := strings.TrimSpace(opts.EmbeddingInputVersion)
	if inputVersion == "" {
		inputVersion = store.EmbeddingInputVersion
	}
	if provider == "" || model == "" {
		return EmbeddingManifest{}, errors.New("embedding provider and model are required")
	}
	relDir := filepath.ToSlash(filepath.Join("embeddings", safePathSegment(provider), safePathSegment(model), safePathSegment(inputVersion)))
	if err := os.RemoveAll(filepath.Join(opts.RepoPath, "embeddings")); err != nil {
		return EmbeddingManifest{}, fmt.Errorf("reset embeddings dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(opts.RepoPath, filepath.FromSlash(relDir)), 0o755); err != nil {
		return EmbeddingManifest{}, fmt.Errorf("mkdir %s: %w", relDir, err)
	}
	filter, err := newSnapshotFilter(ctx, db, opts.Filter)
	if err != nil {
		return EmbeddingManifest{}, err
	}
	rows, err := db.QueryContext(ctx, `
		select e.message_id, e.provider, e.model, e.input_version, e.dimensions, e.embedding_blob, e.embedded_at
		from message_embeddings e
		join messages m on m.id = e.message_id
		where e.provider = ? and e.model = ? and e.input_version = ? and m.guild_id <> ?
		order by e.message_id
	`, provider, model, inputVersion, directMessageGuildID)
	if err != nil {
		return EmbeddingManifest{}, fmt.Errorf("query message_embeddings: %w", err)
	}
	defer func() { _ = rows.Close() }()
	writer := tableShardWriter{rootDir: opts.RepoPath, relDir: relDir, label: "message_embeddings"}
	if err := writer.open(); err != nil {
		return EmbeddingManifest{}, err
	}
	defer func() { _ = writer.close() }()
	columns := []string{"message_id", "provider", "model", "input_version", "dimensions", "embedding_blob", "embedded_at"}
	count := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return EmbeddingManifest{}, err
		}
		var (
			messageID  string
			rowProv    string
			rowModel   string
			rowInput   string
			dimensions int
			blob       []byte
			embeddedAt string
		)
		if err := rows.Scan(&messageID, &rowProv, &rowModel, &rowInput, &dimensions, &blob, &embeddedAt); err != nil {
			return EmbeddingManifest{}, fmt.Errorf("scan message_embeddings: %w", err)
		}
		if filter.active && !filter.allowedMessageIDs[messageID] {
			continue
		}
		body, err := json.Marshal(map[string]any{
			"message_id":     messageID,
			"provider":       rowProv,
			"model":          rowModel,
			"input_version":  rowInput,
			"dimensions":     dimensions,
			"embedding_blob": base64.StdEncoding.EncodeToString(blob),
			"embedded_at":    embeddedAt,
		})
		if err != nil {
			return EmbeddingManifest{}, fmt.Errorf("marshal message_embeddings row: %w", err)
		}
		if err := writer.rotateIfNeeded(); err != nil {
			return EmbeddingManifest{}, err
		}
		if _, err := writer.Write(body); err != nil {
			return EmbeddingManifest{}, fmt.Errorf("write message_embeddings row: %w", err)
		}
		if _, err := writer.Write([]byte{'\n'}); err != nil {
			return EmbeddingManifest{}, fmt.Errorf("write message_embeddings newline: %w", err)
		}
		count++
		if err := writer.finishRow(); err != nil {
			return EmbeddingManifest{}, err
		}
	}
	if err := rows.Err(); err != nil {
		return EmbeddingManifest{}, fmt.Errorf("iterate message_embeddings: %w", err)
	}
	if err := writer.close(); err != nil {
		return EmbeddingManifest{}, err
	}
	return EmbeddingManifest{
		Provider:     provider,
		Model:        model,
		InputVersion: inputVersion,
		Files:        writer.files,
		Columns:      columns,
		Rows:         count,
	}, nil
}

func importEmbeddings(ctx context.Context, tx *sql.Tx, opts Options, manifests []EmbeddingManifest) error {
	if len(manifests) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) select ?, ?, ?, ?, ?, ?, ?
		where exists (
			select 1 from messages
			where id = ? and guild_id <> ? and deleted_at is null
		)
		on conflict(message_id, provider, model, input_version) do update set
			dimensions = excluded.dimensions,
			embedding_blob = excluded.embedding_blob,
			embedded_at = excluded.embedded_at
	`)
	if err != nil {
		return fmt.Errorf("prepare import message_embeddings: %w", err)
	}
	defer func() { _ = stmt.Close() }()
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !embeddingManifestMatches(opts, manifest) {
			continue
		}
		files := manifest.Files
		if len(files) == 0 {
			return fmt.Errorf("embedding manifest %s/%s/%s has no files", manifest.Provider, manifest.Model, manifest.InputVersion)
		}
		for _, rel := range files {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := importEmbeddingFile(ctx, stmt, opts.RepoPath, rel, manifest); err != nil {
				return err
			}
		}
	}
	return nil
}

func importEmbeddingFile(ctx context.Context, stmt *sql.Stmt, repoPath, rel string, manifest EmbeddingManifest) error {
	path, err := embeddingRepoPath(repoPath, rel)
	if err != nil {
		return err
	}
	if _, err := regularFileInRoot(filepath.Join(repoPath, "embeddings"), path, rel, "embedding"); err != nil {
		return err
	}
	file, err := os.Open(path) // #nosec G304 -- path is confined by embeddingRepoPath and regularFileInRoot.
	if err != nil {
		return fmt.Errorf("open %s: %w", rel, err)
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("read gzip %s: %w", rel, err)
	}
	defer func() { _ = gz.Close() }()
	dec := json.NewDecoder(&limitedReader{
		r:     gz,
		limit: maxSharedEmbeddingDecompressedBytes,
		label: "embedding",
	})
	dec.UseNumber()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var row struct {
			MessageID     string      `json:"message_id"`
			Provider      string      `json:"provider"`
			Model         string      `json:"model"`
			InputVersion  string      `json:"input_version"`
			Dimensions    json.Number `json:"dimensions"`
			EmbeddingBlob string      `json:"embedding_blob"`
			EmbeddedAt    string      `json:"embedded_at"`
		}
		err := dec.Decode(&row)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("decode %s: %w", rel, err)
		}
		dimensions, err := strconv.Atoi(row.Dimensions.String())
		if err != nil {
			return fmt.Errorf("decode dimensions in %s: %w", rel, err)
		}
		if dimensions <= 0 {
			return fmt.Errorf("decode dimensions in %s: must be positive", rel)
		}
		blob, err := base64.StdEncoding.DecodeString(row.EmbeddingBlob)
		if err != nil {
			return fmt.Errorf("decode embedding blob in %s: %w", rel, err)
		}
		if len(blob)%4 != 0 || len(blob)/4 != dimensions {
			return fmt.Errorf("decode embedding blob in %s: float32 length does not match dimensions", rel)
		}
		values, err := store.DecodeEmbeddingVector(blob)
		if err != nil {
			return fmt.Errorf("decode embedding blob in %s: %w", rel, err)
		}
		for _, value := range values {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return fmt.Errorf("decode embedding blob in %s: non-finite value", rel)
			}
		}
		if strings.TrimSpace(row.MessageID) == "" {
			return fmt.Errorf("decode embedding row in %s: message_id must not be blank", rel)
		}
		if row.Provider != manifest.Provider || row.Model != manifest.Model || row.InputVersion != manifest.InputVersion {
			return fmt.Errorf("decode embedding row in %s: identity does not match manifest", rel)
		}
		// Older bundles can reference missing, deleted, or local-only messages.
		// Validate every decoded row, but only update canonical non-DM targets.
		if _, err := stmt.ExecContext(ctx, row.MessageID, row.Provider, row.Model, row.InputVersion, dimensions, blob, row.EmbeddedAt,
			row.MessageID, store.DirectMessageGuildID); err != nil {
			return fmt.Errorf("insert message_embeddings: %w", err)
		}
	}
	return nil
}

func embeddingRepoPath(repoPath, rel string) (string, error) {
	raw := strings.TrimSpace(rel)
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(raw)))
	switch {
	case raw == "", clean == ".", filepath.IsAbs(filepath.FromSlash(raw)), strings.HasPrefix(clean, "../"), clean == "..":
		return "", fmt.Errorf("invalid embedding manifest path %q", rel)
	case !strings.HasPrefix(clean, "embeddings/"):
		return "", fmt.Errorf("invalid embedding manifest path %q: must be under embeddings/", rel)
	case !strings.HasSuffix(clean, ".jsonl.gz"):
		return "", fmt.Errorf("invalid embedding manifest path %q: must end in .jsonl.gz", rel)
	}
	return filepath.Join(repoPath, filepath.FromSlash(clean)), nil
}

func embeddingManifestMatches(opts Options, manifest EmbeddingManifest) bool {
	if strings.TrimSpace(opts.EmbeddingProvider) != "" && manifest.Provider != strings.ToLower(strings.TrimSpace(opts.EmbeddingProvider)) {
		return false
	}
	if strings.TrimSpace(opts.EmbeddingModel) != "" && manifest.Model != strings.TrimSpace(opts.EmbeddingModel) {
		return false
	}
	inputVersion := strings.TrimSpace(opts.EmbeddingInputVersion)
	if inputVersion == "" {
		inputVersion = store.EmbeddingInputVersion
	}
	return manifest.InputVersion == inputVersion
}
