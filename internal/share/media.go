package share

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/openclaw/crawlkit/snapshot"
	"github.com/openclaw/discrawl/internal/media"
)

// The share manifest stores compressed media size, not raw size. Keep gzip
// restore/hash paths bounded so a malformed snapshot cannot expand forever.
var maxSharedMediaDecompressedBytes int64 = 1 << 30

func exportMedia(ctx context.Context, db *sql.DB, opts Options, filter *snapshotFilter) (*MediaManifest, error) {
	if strings.TrimSpace(opts.CacheDir) == "" {
		return nil, nil
	}
	if err := resetCompressedMediaExport(opts.RepoPath); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `
		select attachment_id, message_id, guild_id, channel_id, coalesce(media_path, ''), coalesce(content_sha256, '')
		from message_attachments
		where guild_id <> ? and coalesce(media_path, '') <> ''
		order by media_path, attachment_id
	`, directMessageGuildID)
	if err != nil {
		return nil, fmt.Errorf("query media attachments: %w", err)
	}
	defer func() { _ = rows.Close() }()
	manifest := &MediaManifest{}
	seen := map[string]struct{}{}
	for rows.Next() {
		var attachmentID, messageID, guildID, channelID, mediaPath, expectedHash string
		if err := rows.Scan(&attachmentID, &messageID, &guildID, &channelID, &mediaPath, &expectedHash); err != nil {
			return nil, err
		}
		if filter != nil {
			ok := filter.allow("message_attachments", map[string]any{
				"attachment_id": attachmentID,
				"message_id":    messageID,
				"guild_id":      guildID,
				"channel_id":    channelID,
			})
			if !ok {
				continue
			}
		}
		if _, ok := seen[mediaPath]; ok {
			manifest.Attachments++
			continue
		}
		source, err := media.LocalPath(opts.CacheDir, mediaPath)
		if err != nil {
			return nil, err
		}
		info, err := regularMediaFile(filepath.Join(opts.CacheDir, "media"), source, mediaPath)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if errors.Is(err, errUnsafeMediaPath) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("stat media %s: %w", mediaPath, err)
		}
		manifest.Attachments++
		seen[mediaPath] = struct{}{}
		rel := compressedMediaManifestPath(mediaPath)
		target, err := compressedMediaRepoPath(opts.RepoPath, mediaPath)
		if err != nil {
			return nil, err
		}
		if err := copyGzipFile(target, source); err != nil {
			return nil, fmt.Errorf("compress media %s: %w", mediaPath, err)
		}
		hash, err := fileSHA256(target)
		if err != nil {
			return nil, err
		}
		sourceHash, err := fileSHA256(source)
		if err != nil {
			return nil, err
		}
		if expectedHash != "" && sourceHash != expectedHash {
			return nil, fmt.Errorf("media hash mismatch for %s: got %s want %s", mediaPath, sourceHash, expectedHash)
		}
		compressedInfo, err := os.Stat(target)
		if err != nil {
			return nil, fmt.Errorf("stat compressed media %s: %w", rel, err)
		}
		manifest.Files = append(manifest.Files, snapshot.FileManifest{Path: rel, Size: compressedInfo.Size(), SHA256: hash})
		manifest.Bytes += info.Size()
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(manifest.Files) == 0 && manifest.Attachments == 0 {
		return nil, nil
	}
	return manifest, nil
}

func resetCompressedMediaExport(repoPath string) error {
	// repoPath is the private export staging tree, not the user's share checkout.
	if err := os.RemoveAll(filepath.Join(repoPath, "media")); err != nil {
		return fmt.Errorf("reset media dir: %w", err)
	}
	return nil
}

func validateMediaRoots(opts Options) error {
	if strings.TrimSpace(opts.CacheDir) == "" {
		return nil
	}
	repoMedia, err := resolvePathForOverlap(filepath.Join(opts.RepoPath, "media"))
	if err != nil {
		return fmt.Errorf("resolve repo media dir: %w", err)
	}
	cacheMedia, err := resolvePathForOverlap(filepath.Join(opts.CacheDir, "media"))
	if err != nil {
		return fmt.Errorf("resolve cache media dir: %w", err)
	}
	if pathsOverlap(repoMedia, cacheMedia) {
		return fmt.Errorf("share media dir %s overlaps cache media dir %s", repoMedia, cacheMedia)
	}
	return nil
}

func resolvePathForOverlap(path string) (string, error) {
	current, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	current = filepath.Clean(current)
	missing := []string{}
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for _, part := range slices.Backward(missing) {
				resolved = filepath.Join(resolved, part)
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return filepath.Clean(path), nil
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func pathsOverlap(a, b string) bool {
	if a == b {
		return true
	}
	return pathContains(a, b) || pathContains(b, a)
}

func pathContains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel != "." && rel != "" && !filepath.IsAbs(rel) && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func importMedia(ctx context.Context, opts Options, manifest *MediaManifest) (int, error) {
	if manifest == nil || strings.TrimSpace(opts.CacheDir) == "" {
		return 0, nil
	}
	copied := 0
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return copied, err
		}
		mediaPath, compressed, ok := mediaPathFromManifest(file.Path)
		if !ok || strings.TrimSpace(mediaPath) == "" {
			return copied, fmt.Errorf("invalid media manifest path %q", file.Path)
		}
		source, err := mediaSourcePath(opts.RepoPath, mediaPath, compressed)
		if err != nil {
			return copied, err
		}
		if _, err := regularMediaFile(filepath.Join(opts.RepoPath, "media"), source, file.Path); err != nil {
			return copied, err
		}
		info, err := os.Lstat(source)
		if err != nil {
			return copied, fmt.Errorf("stat media %s: %w", file.Path, err)
		}
		if !info.Mode().IsRegular() {
			return copied, fmt.Errorf("media %s is not a regular file", file.Path)
		}
		hash, err := fileSHA256(source)
		if err != nil {
			return copied, fmt.Errorf("hash media %s: %w", file.Path, err)
		}
		if file.SHA256 != "" && hash != file.SHA256 {
			return copied, fmt.Errorf("media hash mismatch for %s: got %s want %s", file.Path, hash, file.SHA256)
		}
		target, err := media.LocalPath(opts.CacheDir, mediaPath)
		if err != nil {
			return copied, err
		}
		targetHash := hash
		if compressed {
			targetHash, err = gzipFileSHA256(source)
			if err != nil {
				return copied, fmt.Errorf("hash compressed media %s: %w", file.Path, err)
			}
		}
		if sameFileHash(target, targetHash) {
			continue
		}
		if compressed {
			err = restoreGzipFile(target, source)
		} else {
			err = copyFile(target, source)
		}
		if err != nil {
			return copied, fmt.Errorf("restore media %s: %w", file.Path, err)
		}
		copied++
	}
	return copied, nil
}

func compressedMediaManifestPath(mediaPath string) string {
	return filepath.ToSlash(filepath.Join("media", mediaPath+".gz"))
}

func mediaPathFromManifest(path string) (string, bool, bool) {
	mediaPath, ok := strings.CutPrefix(filepath.ToSlash(path), "media/")
	if !ok {
		return "", false, false
	}
	if rawPath, ok := strings.CutSuffix(mediaPath, ".gz"); ok {
		return rawPath, true, true
	}
	return mediaPath, false, true
}

func compressedMediaRepoPath(repoPath, mediaPath string) (string, error) {
	return media.RepoPath(repoPath, mediaPath+".gz")
}

func mediaSourcePath(repoPath, mediaPath string, compressed bool) (string, error) {
	if compressed {
		return compressedMediaRepoPath(repoPath, mediaPath)
	}
	return media.RepoPath(repoPath, mediaPath)
}

func copyGzipFile(target, source string) error {
	src, err := os.Open(source) // #nosec G304 -- source is constrained by media path helpers.
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	return writeAtomicFile(target, func(tmp *os.File) error {
		gz, err := gzip.NewWriterLevel(tmp, gzip.BestCompression)
		if err != nil {
			return err
		}
		if _, err := io.Copy(gz, src); err != nil {
			_ = gz.Close()
			return err
		}
		return gz.Close()
	})
}

func restoreGzipFile(target, source string) error {
	src, err := os.Open(source) // #nosec G304 -- source is constrained by media path helpers.
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	gz, err := gzip.NewReader(src)
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()
	return writeAtomicFile(target, func(tmp *os.File) error {
		return copyWithLimit(tmp, gz, maxSharedMediaDecompressedBytes)
	})
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path) // #nosec G304 -- callers pass confined repo/cache paths.
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func gzipFileSHA256(path string) (string, error) {
	file, err := os.Open(path) // #nosec G304 -- callers pass confined repo/cache paths.
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return "", err
	}
	defer func() { _ = gz.Close() }()
	hasher := sha256.New()
	if err := copyWithLimit(hasher, gz, maxSharedMediaDecompressedBytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

type attachmentMediaRecord struct {
	TextContent   string
	MediaPath     string
	ContentSHA256 string
	ContentSize   int64
	FetchedAt     string
	FetchStatus   string
	FetchError    string
}

func attachmentMediaByID(ctx context.Context, tx *sql.Tx) (map[string]attachmentMediaRecord, error) {
	rows, err := tx.QueryContext(ctx, `
		select attachment_id, coalesce(media_path, ''), coalesce(content_sha256, ''),
		       content_size, coalesce(fetched_at, ''), coalesce(fetch_status, ''), coalesce(fetch_error, '')
		from message_attachments
		where coalesce(media_path, '') <> ''
	`)
	if err != nil {
		return nil, fmt.Errorf("query existing attachment media: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]attachmentMediaRecord{}
	for rows.Next() {
		var id string
		var record attachmentMediaRecord
		if err := rows.Scan(&id, &record.MediaPath, &record.ContentSHA256, &record.ContentSize, &record.FetchedAt, &record.FetchStatus, &record.FetchError); err != nil {
			return nil, err
		}
		out[id] = record
	}
	return out, rows.Err()
}

func preserveImportedAttachmentMedia(ctx context.Context, tx *sql.Tx, existing map[string]attachmentMediaRecord) error {
	if len(existing) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
		update message_attachments
		set media_path = ?, content_sha256 = ?, content_size = ?, fetched_at = ?,
		    fetch_status = ?, fetch_error = ?
		where attachment_id = ? and coalesce(media_path, '') = ''
	`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for attachmentID, record := range existing {
		if _, err := stmt.ExecContext(ctx, record.MediaPath, nullIfEmpty(record.ContentSHA256), record.ContentSize, nullIfEmpty(record.FetchedAt), record.FetchStatus, record.FetchError, attachmentID); err != nil {
			return fmt.Errorf("preserve attachment media %s: %w", attachmentID, err)
		}
	}
	return nil
}

func preserveIncrementalAttachmentState(ctx context.Context, tx *sql.Tx, row map[string]any) error {
	attachmentID := stringValue(row["attachment_id"])
	if attachmentID == "" {
		return nil
	}
	var record attachmentMediaRecord
	err := tx.QueryRowContext(ctx, `
		select coalesce(text_content, ''), coalesce(media_path, ''), coalesce(content_sha256, ''), content_size,
		       coalesce(fetched_at, ''), coalesce(fetch_status, ''), coalesce(fetch_error, '')
		from message_attachments
		where attachment_id = ?
	`, attachmentID).Scan(&record.TextContent, &record.MediaPath, &record.ContentSHA256, &record.ContentSize, &record.FetchedAt, &record.FetchStatus, &record.FetchError)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("query existing attachment media %s: %w", attachmentID, err)
	}
	if stringValue(row["text_content"]) == "" {
		row["text_content"] = record.TextContent
	}
	if stringValue(row["media_path"]) == "" {
		row["media_path"] = record.MediaPath
		row["content_sha256"] = record.ContentSHA256
		row["content_size"] = record.ContentSize
		row["fetched_at"] = record.FetchedAt
		row["fetch_status"] = record.FetchStatus
		row["fetch_error"] = record.FetchError
	}
	return nil
}
