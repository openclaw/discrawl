package share

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/mirror"
	"github.com/openclaw/crawlkit/snapshot"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/media"
	"github.com/openclaw/discrawl/internal/store"
)

func TestExportImportRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Tables)
	require.FileExists(t, filepath.Join(repo, ManifestName))
	require.NotEmpty(t, tableEntry(t, manifest, "messages").Files)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	var progress []ImportProgress
	imported, changed, err := ImportIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) { progress = append(progress, p) },
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, manifest.GeneratedAt, imported.GeneratedAt)
	require.Contains(t, progressPhases(progress), "start")
	require.Contains(t, progressPhases(progress), "table_start")
	require.Contains(t, progressPhases(progress), "file_done")
	require.Contains(t, progressPhases(progress), "rebuild_fts")
	require.Contains(t, progressPhases(progress), "done")

	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "launch", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m1", results[0].MessageID)

	mentions, err := dst.ListMentions(ctx, store.MentionListOptions{Target: "Ops", Limit: 10})
	require.NoError(t, err)
	require.Len(t, mentions, 1)

	lastImport, err := dst.GetSyncState(ctx, LastImportSyncScope)
	require.NoError(t, err)
	require.NotEmpty(t, lastImport)
	lastManifest, err := dst.GetSyncState(ctx, LastImportManifestSyncScope)
	require.NoError(t, err)
	require.Equal(t, manifest.GeneratedAt.Format(time.RFC3339Nano), lastManifest)
	require.False(t, NeedsImport(ctx, dst, 15*time.Minute))

	imported, changed, err = ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, manifest.GeneratedAt, imported.GeneratedAt)
	_, err = ImportAt(ctx, dst, Options{RepoPath: repo, Branch: "main"}, "")
	require.NoError(t, err)
}

func TestExportImportRestoresMediaFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	body := []byte("cached-media")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", hash[:2], hash+"-file.png"))
	require.NoError(t, addCachedAttachment(ctx, src, mediaPath, hash, int64(len(body))))
	srcCache := filepath.Join(dir, "src-cache")
	srcFile, err := media.LocalPath(srcCache, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(srcFile), 0o755))
	require.NoError(t, os.WriteFile(srcFile, body, 0o600))

	repo := filepath.Join(dir, "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, CacheDir: srcCache, Branch: "main", IncludeMedia: true})
	require.NoError(t, err)
	require.NotNil(t, manifest.Media)
	require.Equal(t, 1, manifest.Media.Attachments)
	require.Len(t, manifest.Media.Files, 1)
	require.Equal(t, compressedMediaManifestPath(mediaPath), manifest.Media.Files[0].Path)
	require.Equal(t, int64(len(body)), manifest.Media.Bytes)
	require.FileExists(t, filepath.Join(repo, filepath.FromSlash(manifest.Media.Files[0].Path)))
	_, err = os.Stat(filepath.Join(repo, "media", filepath.FromSlash(mediaPath)))
	require.True(t, os.IsNotExist(err))
	compressed, err := os.Open(filepath.Join(repo, filepath.FromSlash(manifest.Media.Files[0].Path)))
	require.NoError(t, err)
	gz, err := gzip.NewReader(compressed)
	require.NoError(t, err)
	restored, err := io.ReadAll(gz)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	require.NoError(t, compressed.Close())
	require.Equal(t, body, restored)

	dst, err := store.Open(ctx, filepath.Join(dir, "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	dstCache := filepath.Join(dir, "dst-cache")
	imported, changed, err := ImportIfChanged(ctx, dst, Options{RepoPath: repo, CacheDir: dstCache, Branch: "main", IncludeMedia: true})
	require.NoError(t, err)
	require.True(t, changed)
	require.NotNil(t, imported.Media)
	dstFile, err := media.LocalPath(dstCache, mediaPath)
	require.NoError(t, err)
	got, err := os.ReadFile(dstFile)
	require.NoError(t, err)
	require.Equal(t, body, got)
	rows, err := dst.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, mediaPath, rows[0].MediaPath)

	require.NoError(t, os.Remove(dstFile))
	imported, changed, err = ImportIfChanged(ctx, dst, Options{RepoPath: repo, CacheDir: dstCache, Branch: "main", IncludeMedia: true})
	require.NoError(t, err)
	require.True(t, changed)
	require.NotNil(t, imported.Media)
	got, err = os.ReadFile(dstFile)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestExportMigratesLegacyRawMediaFilesToGzip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	body := []byte("cached-media")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", hash[:2], hash+"-file.png"))
	require.NoError(t, addCachedAttachment(ctx, src, mediaPath, hash, int64(len(body))))
	srcCache := filepath.Join(dir, "src-cache")
	srcFile, err := media.LocalPath(srcCache, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(srcFile), 0o755))
	require.NoError(t, os.WriteFile(srcFile, body, 0o600))

	repo := filepath.Join(dir, "share")
	legacyRaw, err := media.RepoPath(repo, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(legacyRaw), 0o755))
	require.NoError(t, os.WriteFile(legacyRaw, []byte("legacy raw media"), 0o600))

	manifest, err := Export(ctx, src, Options{RepoPath: repo, CacheDir: srcCache, Branch: "main", IncludeMedia: true})
	require.NoError(t, err)
	require.NotNil(t, manifest.Media)
	require.Len(t, manifest.Media.Files, 1)
	require.Equal(t, compressedMediaManifestPath(mediaPath), manifest.Media.Files[0].Path)
	_, err = os.Stat(legacyRaw)
	require.True(t, os.IsNotExist(err))
	require.FileExists(t, filepath.Join(repo, filepath.FromSlash(compressedMediaManifestPath(mediaPath))))
}

func TestExportRejectsMismatchedMediaHash(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	body := []byte("cached-media")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", hash[:2], hash+"-file.png"))
	require.NoError(t, addCachedAttachment(ctx, src, mediaPath, strings.Repeat("0", 64), int64(len(body))))
	srcCache := filepath.Join(dir, "src-cache")
	srcFile, err := media.LocalPath(srcCache, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(srcFile), 0o755))
	require.NoError(t, os.WriteFile(srcFile, body, 0o600))

	_, err = Export(ctx, src, Options{RepoPath: filepath.Join(dir, "share"), CacheDir: srcCache, Branch: "main", IncludeMedia: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "media hash mismatch")
}

func TestExportRejectsOverlappingMediaRoots(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	cacheDir := filepath.Join(dir, "cache")
	mediaPath := "attachments/aa/file.png"
	cacheFile, err := media.LocalPath(cacheDir, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(cacheFile), 0o755))
	require.NoError(t, os.WriteFile(cacheFile, []byte("cached"), 0o600))

	_, err = Export(ctx, src, Options{RepoPath: cacheDir, CacheDir: cacheDir, Branch: "main", IncludeMedia: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "overlaps cache media dir")
	require.FileExists(t, cacheFile)

	_, err = Export(ctx, src, Options{RepoPath: cacheDir, CacheDir: cacheDir, Branch: "main", IncludeMedia: false})
	require.Error(t, err)
	require.Contains(t, err.Error(), "overlaps cache media dir")
	require.FileExists(t, cacheFile)
}

func TestExportRejectsSymlinkedOverlappingMediaRoots(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	cacheDir := filepath.Join(dir, "cache")
	cacheFile, err := media.LocalPath(cacheDir, "attachments/aa/file.png")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(cacheFile), 0o755))
	require.NoError(t, os.WriteFile(cacheFile, []byte("cached"), 0o600))
	repoPath := filepath.Join(dir, "repo-link")
	if err := os.Symlink(cacheDir, repoPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	_, err = Export(ctx, src, Options{RepoPath: repoPath, CacheDir: cacheDir, Branch: "main", IncludeMedia: false})
	require.Error(t, err)
	require.Contains(t, err.Error(), "overlaps cache media dir")
	require.FileExists(t, cacheFile)
}

func TestImportPreservesLocalAttachmentMediaMetadata(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	require.NoError(t, addUncachedAttachment(ctx, src))
	repo := filepath.Join(dir, "share")
	_, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	dst := seedStore(t, filepath.Join(dir, "dst.db"))
	defer func() { _ = dst.Close() }()
	body := []byte("local-cache")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", hash[:2], hash+"-file.png"))
	require.NoError(t, addCachedAttachment(ctx, dst, mediaPath, hash, int64(len(body))))

	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	rows, err := dst.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, mediaPath, rows[0].MediaPath)
	require.Equal(t, hash, rows[0].ContentSHA256)
	require.Equal(t, int64(len(body)), rows[0].ContentSize)
	require.Equal(t, "fetched", rows[0].FetchStatus)
}

func TestIncrementalImportPreservesLocalAttachmentMediaMetadata(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dst := seedStore(t, filepath.Join(dir, "dst.db"))
	defer func() { _ = dst.Close() }()
	body := []byte("local-cache")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", hash[:2], hash+"-file.png"))
	require.NoError(t, addCachedAttachment(ctx, dst, mediaPath, hash, int64(len(body))))

	tx, err := dst.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	row := map[string]any{
		"attachment_id":  "a1",
		"message_id":     "m1",
		"guild_id":       "g1",
		"channel_id":     "c1",
		"author_id":      "u1",
		"filename":       "file.png",
		"content_type":   "image/png",
		"size":           int64(len(body)),
		"url":            "https://cdn.example/file.png",
		"proxy_url":      nil,
		"text_content":   "",
		"media_path":     "",
		"content_sha256": "",
		"content_size":   int64(0),
		"fetched_at":     nil,
		"fetch_status":   "",
		"fetch_error":    "",
		"updated_at":     time.Now().UTC().Format(time.RFC3339Nano),
	}
	require.NoError(t, importIncrementalSnapshotRow(ctx, tx, "message_attachments", row))
	require.NoError(t, tx.Commit())

	rows, err := dst.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, mediaPath, rows[0].MediaPath)
	require.Equal(t, hash, rows[0].ContentSHA256)
	require.Equal(t, int64(len(body)), rows[0].ContentSize)
	require.Equal(t, "fetched", rows[0].FetchStatus)
}

func TestImportMediaRejectsSymlinkedFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	repo := filepath.Join(dir, "share")
	cacheDir := filepath.Join(dir, "cache")
	mediaPath := "attachments/aa/file.png"
	source, err := media.RepoPath(repo, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(source), 0o755))
	target := filepath.Join(dir, "outside.png")
	require.NoError(t, os.WriteFile(target, []byte("outside"), 0o600))
	if err := os.Symlink(target, source); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	_, err = importMedia(ctx, Options{RepoPath: repo, CacheDir: cacheDir}, &MediaManifest{
		Files: []snapshot.FileManifest{{Path: "media/" + mediaPath}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a regular file")
	_, err = os.Stat(filepath.Join(cacheDir, "media", filepath.FromSlash(mediaPath)))
	require.True(t, os.IsNotExist(err))
}

func TestImportMediaRejectsSymlinkedDirectories(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	repo := filepath.Join(dir, "share")
	cacheDir := filepath.Join(dir, "cache")
	mediaPath := "attachments/aa/file.png"
	outside := filepath.Join(dir, "outside")
	require.NoError(t, os.MkdirAll(outside, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(outside, "file.png"), []byte("outside"), 0o600))
	linkParent := filepath.Join(repo, "media", "attachments", "aa")
	require.NoError(t, os.MkdirAll(filepath.Dir(linkParent), 0o755))
	if err := os.Symlink(outside, linkParent); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	_, err := importMedia(ctx, Options{RepoPath: repo, CacheDir: cacheDir}, &MediaManifest{
		Files: []snapshot.FileManifest{{Path: "media/" + mediaPath}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "symlinked path component")
	_, err = os.Stat(filepath.Join(cacheDir, "media", filepath.FromSlash(mediaPath)))
	require.True(t, os.IsNotExist(err))
}

func TestImportMediaSupportsLegacyRawManifestFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	repo := filepath.Join(dir, "share")
	cacheDir := filepath.Join(dir, "cache")
	mediaPath := "attachments/aa/file.png"
	body := []byte("legacy raw body")
	source, err := media.RepoPath(repo, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(source), 0o755))
	require.NoError(t, os.WriteFile(source, body, 0o600))
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])

	copied, err := importMedia(ctx, Options{RepoPath: repo, CacheDir: cacheDir}, &MediaManifest{
		Files: []snapshot.FileManifest{{Path: "media/" + mediaPath, SHA256: hash}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, copied)
	target, err := media.LocalPath(cacheDir, mediaPath)
	require.NoError(t, err)
	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestImportMediaSupportsCompressedManifestFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	repo := filepath.Join(dir, "share")
	cacheDir := filepath.Join(dir, "cache")
	mediaPath := "attachments/aa/file.png"
	body := []byte("compressed body")
	sourceRaw := filepath.Join(dir, "source.png")
	require.NoError(t, os.WriteFile(sourceRaw, body, 0o600))
	source, err := compressedMediaRepoPath(repo, mediaPath)
	require.NoError(t, err)
	require.NoError(t, copyGzipFile(source, sourceRaw))
	compressedHash, err := fileSHA256(source)
	require.NoError(t, err)

	copied, err := importMedia(ctx, Options{RepoPath: repo, CacheDir: cacheDir}, &MediaManifest{
		Files: []snapshot.FileManifest{{Path: compressedMediaManifestPath(mediaPath), SHA256: compressedHash}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, copied)
	target, err := media.LocalPath(cacheDir, mediaPath)
	require.NoError(t, err)
	require.Equal(t, body, mustReadFile(t, target))

	copied, err = importMedia(ctx, Options{RepoPath: repo, CacheDir: cacheDir}, &MediaManifest{
		Files: []snapshot.FileManifest{{Path: compressedMediaManifestPath(mediaPath), SHA256: compressedHash}},
	})
	require.NoError(t, err)
	require.Zero(t, copied)
}

func TestImportMediaRejectsInvalidAndMismatchedFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	repo := filepath.Join(dir, "share")
	cacheDir := filepath.Join(dir, "cache")

	copied, err := importMedia(ctx, Options{RepoPath: repo, CacheDir: cacheDir}, nil)
	require.NoError(t, err)
	require.Zero(t, copied)

	_, err = importMedia(ctx, Options{RepoPath: repo, CacheDir: cacheDir}, &MediaManifest{
		Files: []snapshot.FileManifest{{Path: "tables/not-media.jsonl.gz"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid media manifest path")

	mediaPath := "attachments/aa/file.png"
	source, err := media.RepoPath(repo, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(source), 0o755))
	require.NoError(t, os.WriteFile(source, []byte("body"), 0o600))
	_, err = importMedia(ctx, Options{RepoPath: repo, CacheDir: cacheDir}, &MediaManifest{
		Files: []snapshot.FileManifest{{Path: "media/" + mediaPath, SHA256: strings.Repeat("0", 64)}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "media hash mismatch")
}

func TestImportIncrementalRestoresMediaWhenTablesUnchanged(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	body := []byte("cached-media")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", hash[:2], hash+"-file.png"))
	require.NoError(t, addCachedAttachment(ctx, src, mediaPath, hash, int64(len(body))))
	srcCache := filepath.Join(dir, "src-cache")
	srcFile, err := media.LocalPath(srcCache, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(srcFile), 0o755))
	require.NoError(t, os.WriteFile(srcFile, body, 0o600))

	repo := filepath.Join(dir, "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, CacheDir: srcCache, Branch: "main", IncludeMedia: true})
	require.NoError(t, err)
	dst, err := store.Open(ctx, filepath.Join(dir, "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = Import(ctx, dst, Options{RepoPath: repo, CacheDir: filepath.Join(dir, "dst-cache"), Branch: "main", IncludeMedia: false})
	require.NoError(t, err)

	dstCache := filepath.Join(dir, "dst-cache")
	imported, changed, err := ImportIncremental(ctx, dst, Options{RepoPath: repo, CacheDir: dstCache, Branch: "main", IncludeMedia: true}, manifest, manifest)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, manifest.GeneratedAt, imported.GeneratedAt)
	dstFile, err := media.LocalPath(dstCache, mediaPath)
	require.NoError(t, err)
	got, err := os.ReadFile(dstFile)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestShareIncrementalPlanHandlesSupportedModesAndRejectsOthers(t *testing.T) {
	_, supported := shareIncrementalPlan(snapshot.ImportPlan{Full: true, Reason: "schema changed"})
	require.False(t, supported)

	_, supported = shareIncrementalPlan(snapshot.ImportPlan{Tables: []snapshot.TableImportPlan{{
		Table: snapshot.TableManifest{Name: "custom"},
		Mode:  snapshot.TableImportFiles,
	}}})
	require.False(t, supported)

	plan, supported := shareIncrementalPlan(snapshot.ImportPlan{Tables: []snapshot.TableImportPlan{{
		Table: snapshot.TableManifest{Name: "messages"},
		Mode:  snapshot.TableImportReplace,
	}}})
	require.True(t, supported)
	require.Equal(t, snapshot.TableImportReplace, plan.Tables[0].Mode)

	plan, supported = shareIncrementalPlan(snapshot.ImportPlan{Tables: []snapshot.TableImportPlan{
		{Table: snapshot.TableManifest{Name: "messages"}, Mode: snapshot.TableImportFiles},
		{Table: snapshot.TableManifest{Name: "guilds"}, Mode: snapshot.TableImportFiles},
		{Table: snapshot.TableManifest{Name: "channels"}, Mode: snapshot.TableImportReplace},
		{Table: snapshot.TableManifest{Name: "sync_state"}, Mode: snapshot.TableImportSkip},
	}})
	require.True(t, supported)
	require.Len(t, plan.Tables, 4)
	require.Equal(t, snapshot.TableImportReplace, plan.Tables[1].Mode)
}

func TestMessageFTSHelpers(t *testing.T) {
	id, ok := messageFTSRowID("42")
	require.True(t, ok)
	require.Equal(t, int64(42), id)
	id, ok = messageFTSRowID("18446744073709551615")
	require.True(t, ok)
	require.NotZero(t, id)
	_, ok = messageFTSRowID("")
	require.False(t, ok)
	require.Nil(t, nullIfEmpty(""))
	require.Equal(t, "value", nullIfEmpty("value"))
}

func TestImportPlanSearchRebuildsKeepsEarlierChannelRebuild(t *testing.T) {
	rebuildMessages, rebuildMembers := importPlanSearchRebuilds(snapshot.ImportPlan{
		Tables: []snapshot.TableImportPlan{
			{Table: snapshot.TableManifest{Name: "channels"}, Mode: snapshot.TableImportReplace},
			{Table: snapshot.TableManifest{Name: "messages"}, Mode: snapshot.TableImportFiles},
		},
	})
	require.True(t, rebuildMessages)
	require.False(t, rebuildMembers)
}

func TestPreviousImportedManifestFallsBackToGitHistory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(dir, "share")
	require.NoError(t, exec.CommandContext(ctx, "git", "init", repo).Run())
	configureGitUser(t, repo)
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NoError(t, exec.CommandContext(ctx, "git", "-C", repo, "add", ".").Run())
	require.NoError(t, exec.CommandContext(ctx, "git", "-C", repo, "commit", "-m", "snapshot").Run())

	dst, err := store.Open(ctx, filepath.Join(dir, "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	require.NoError(t, dst.SetSyncState(ctx, LastImportManifestSyncScope, manifest.GeneratedAt.Format(time.RFC3339Nano)))

	previous, ok := PreviousImportedManifest(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.True(t, ok)
	require.Equal(t, manifest.GeneratedAt, previous.GeneratedAt)
	require.NotEmpty(t, tableEntry(t, previous, "messages").FileManifests)
}

func TestMediaPathValidationHelpers(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "media")
	require.NoError(t, os.MkdirAll(root, 0o755))
	file := filepath.Join(root, "attachments", "aa", "file.png")
	require.NoError(t, os.MkdirAll(filepath.Dir(file), 0o755))
	require.NoError(t, os.WriteFile(file, []byte("body"), 0o600))

	info, err := regularMediaFile(root, file, "file.png")
	require.NoError(t, err)
	require.Equal(t, int64(4), info.Size())
	_, err = regularMediaFile(root, root, "root")
	require.Error(t, err)
	require.ErrorIs(t, err, errUnsafeMediaPath)
	require.True(t, pathsOverlap(root, filepath.Join(root, "attachments")))
	require.False(t, pathsOverlap(root, filepath.Join(dir, "other")))
	require.NoError(t, resetCompressedMediaExport(dir))
	_, err = os.Stat(root)
	require.True(t, os.IsNotExist(err))

	raw, compressed, ok := mediaPathFromManifest("media/attachments/aa/file.png.gz")
	require.True(t, ok)
	require.True(t, compressed)
	require.Equal(t, "attachments/aa/file.png", raw)
	raw, compressed, ok = mediaPathFromManifest("media/attachments/aa/file.png")
	require.True(t, ok)
	require.False(t, compressed)
	require.Equal(t, "attachments/aa/file.png", raw)
	_, _, ok = mediaPathFromManifest("tables/messages.jsonl.gz")
	require.False(t, ok)
}

func TestMediaCopyHashHelpers(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.bin")
	target := filepath.Join(dir, "nested", "target.bin")
	body := []byte("copy-body")
	require.NoError(t, os.WriteFile(source, body, 0o600))

	require.NoError(t, copyFile(target, source))
	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, body, got)
	hash, err := fileSHA256(source)
	require.NoError(t, err)
	require.True(t, sameFileHash(target, hash))
	require.False(t, sameFileHash(filepath.Join(dir, "missing.bin"), hash))
	require.Error(t, copyFile(filepath.Join(dir, "other.bin"), filepath.Join(dir, "missing.bin")))

	gzipTarget := filepath.Join(dir, "nested", "target.bin.gz")
	require.NoError(t, copyGzipFile(gzipTarget, source))
	require.NotEqual(t, body, mustReadFile(t, gzipTarget))
	gzipHash, err := gzipFileSHA256(gzipTarget)
	require.NoError(t, err)
	require.Equal(t, hash, gzipHash)
	restored := filepath.Join(dir, "restored.bin")
	require.NoError(t, restoreGzipFile(restored, gzipTarget))
	require.Equal(t, body, mustReadFile(t, restored))
	_, err = gzipFileSHA256(source)
	require.Error(t, err)
	_, err = gzipFileSHA256(filepath.Join(dir, "missing.gz"))
	require.Error(t, err)
	require.Error(t, copyGzipFile(filepath.Join(dir, "missing.gz"), filepath.Join(dir, "missing-source.bin")))
	require.Error(t, restoreGzipFile(filepath.Join(dir, "bad.bin"), source))
	require.Error(t, restoreGzipFile(filepath.Join(dir, "bad.bin"), filepath.Join(dir, "missing.gz")))

	oldLimit := maxSharedMediaDecompressedBytes
	maxSharedMediaDecompressedBytes = 4
	t.Cleanup(func() { maxSharedMediaDecompressedBytes = oldLimit })
	_, err = gzipFileSHA256(gzipTarget)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decompressed size")
	err = restoreGzipFile(filepath.Join(dir, "too-large.bin"), gzipTarget)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decompressed size")
}

func TestPublicPermissionHelpers(t *testing.T) {
	rawGuild := `{"roles":[{"id":"g1","permissions":"1024"}]}`
	permissions, ok := everyoneGuildPermissions(rawGuild, "g1")
	require.True(t, ok)
	require.Equal(t, permissionViewChannel, permissions)
	_, ok = everyoneGuildPermissions(`{"roles":[{"id":"g1","permissions":{}}]}`, "g1")
	require.False(t, ok)
	_, ok = everyoneGuildPermissions(`not-json`, "g1")
	require.False(t, ok)
	_, ok = everyoneGuildPermissions(`{"roles":[]}`, "g1")
	require.False(t, ok)

	require.Equal(t, int64(0), applyEveryoneOverwrite(permissionViewChannel, `{"permission_overwrites":[{"id":"g1","type":"role","deny":"1024"}]}`, "g1"))
	require.Equal(t, permissionViewChannel, applyEveryoneOverwrite(0, `{"permission_overwrites":[{"id":"g1","type":0,"allow":1024}]}`, "g1"))
	require.Equal(t, permissionViewChannel, applyEveryoneOverwrite(permissionViewChannel, `not-json`, "g1"))

	parsed, ok := parsePermissionBits(json.Number("1024"))
	require.True(t, ok)
	require.Equal(t, permissionViewChannel, parsed)
	parsed, ok = parsePermissionBits(float64(1024))
	require.True(t, ok)
	require.Equal(t, permissionViewChannel, parsed)
	parsed, ok = parsePermissionBits(nil)
	require.True(t, ok)
	require.Zero(t, parsed)
	_, ok = parsePermissionBits(json.Number("bad"))
	require.False(t, ok)
	_, ok = parsePermissionBits(struct{}{})
	require.False(t, ok)

	require.True(t, isRoleOverwrite(json.Number("0")))
	require.True(t, isRoleOverwrite(float64(0)))
	require.True(t, isRoleOverwrite("role"))
	require.False(t, isRoleOverwrite("member"))
	require.False(t, isRoleOverwrite(json.Number("bad")))
	require.False(t, isRoleOverwrite(struct{}{}))
}

func TestPublicSnapshotFilterHonorsCategoryAndThreadPermissions(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{
		ID:      "g1",
		Name:    "Guild",
		RawJSON: `{"roles":[{"id":"g1","permissions":"1024"}]}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "cat-deny",
		GuildID: "g1",
		Kind:    "category",
		Name:    "Private",
		RawJSON: `{"permission_overwrites":[{"id":"g1","type":"role","deny":"1024"}]}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:       "child-denied",
		GuildID:  "g1",
		ParentID: "cat-deny",
		Kind:     "text",
		Name:     "denied",
		RawJSON:  `{}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "public",
		GuildID: "g1",
		Kind:    "text",
		Name:    "public",
		RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:             "thread",
		GuildID:        "g1",
		ThreadParentID: "public",
		Kind:           "thread_public",
		Name:           "thread",
		RawJSON:        `{}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:              "private-thread",
		GuildID:         "g1",
		ThreadParentID:  "public",
		Kind:            "thread_private",
		Name:            "private-thread",
		IsPrivateThread: true,
		RawJSON:         `{}`,
	}))

	filter, err := newSnapshotFilter(ctx, s.DB(), FilterOptions{PublicOnly: true})
	require.NoError(t, err)
	require.False(t, filter.publicChannel("child-denied"))
	require.True(t, filter.publicChannel("public"))
	require.True(t, filter.publicChannel("thread"))
	require.False(t, filter.publicChannel("private-thread"))
	require.False(t, filter.publicChannel("missing"))
}

func TestExportSkipsMissingMediaFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	body := []byte("missing-media")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", hash[:2], hash+"-missing.png"))
	require.NoError(t, addCachedAttachment(ctx, src, mediaPath, hash, int64(len(body))))

	repo := filepath.Join(dir, "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, CacheDir: filepath.Join(dir, "cache"), Branch: "main", IncludeMedia: true})
	require.NoError(t, err)
	require.Nil(t, manifest.Media)
}

func TestExportMediaRejectsInvalidPathAndHashMismatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	require.NoError(t, addCachedAttachment(ctx, src, "../bad.png", strings.Repeat("a", 64), 4))
	_, err := Export(ctx, src, Options{RepoPath: filepath.Join(dir, "share-bad-path"), CacheDir: filepath.Join(dir, "cache"), Branch: "main", IncludeMedia: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid media path")

	src2 := seedStore(t, filepath.Join(dir, "src2.db"))
	defer func() { _ = src2.Close() }()
	body := []byte("actual")
	actual := sha256.Sum256(body)
	actualHash := hex.EncodeToString(actual[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", actualHash[:2], actualHash+"-file.png"))
	require.NoError(t, addCachedAttachment(ctx, src2, mediaPath, strings.Repeat("b", 64), int64(len(body))))
	cacheDir := filepath.Join(dir, "cache2")
	cacheFile, err := media.LocalPath(cacheDir, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(cacheFile), 0o755))
	require.NoError(t, os.WriteFile(cacheFile, body, 0o600))

	_, err = Export(ctx, src2, Options{RepoPath: filepath.Join(dir, "share-bad-hash"), CacheDir: cacheDir, Branch: "main", IncludeMedia: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "media hash mismatch")
}

func TestExportSkipsSymlinkedMediaFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	body := []byte("outside-media")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", hash[:2], hash+"-file.png"))
	require.NoError(t, addCachedAttachment(ctx, src, mediaPath, hash, int64(len(body))))
	cacheDir := filepath.Join(dir, "cache")
	cacheFile, err := media.LocalPath(cacheDir, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(cacheFile), 0o755))
	target := filepath.Join(dir, "outside.png")
	require.NoError(t, os.WriteFile(target, body, 0o600))
	if err := os.Symlink(target, cacheFile); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	repo := filepath.Join(dir, "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, CacheDir: cacheDir, Branch: "main", IncludeMedia: true})
	require.NoError(t, err)
	require.Nil(t, manifest.Media)
	_, err = os.Lstat(filepath.Join(repo, "media", filepath.FromSlash(mediaPath)))
	require.True(t, os.IsNotExist(err))
}

func TestExportSkipsSymlinkedMediaDirectories(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	body := []byte("outside-media")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", hash[:2], hash+"-file.png"))
	require.NoError(t, addCachedAttachment(ctx, src, mediaPath, hash, int64(len(body))))
	cacheDir := filepath.Join(dir, "cache")
	outside := filepath.Join(dir, "outside")
	require.NoError(t, os.MkdirAll(outside, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(outside, hash+"-file.png"), body, 0o600))
	linkParent := filepath.Join(cacheDir, "media", "attachments", hash[:2])
	require.NoError(t, os.MkdirAll(filepath.Dir(linkParent), 0o755))
	if err := os.Symlink(outside, linkParent); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	repo := filepath.Join(dir, "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, CacheDir: cacheDir, Branch: "main", IncludeMedia: true})
	require.NoError(t, err)
	require.Nil(t, manifest.Media)
	_, err = os.Lstat(filepath.Join(repo, "media", filepath.FromSlash(mediaPath)))
	require.True(t, os.IsNotExist(err))
}

func TestImportIfChangedUsesIncrementalTailImport(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotEmpty(t, tableEntry(t, manifest, "messages").FileManifests)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, changed, err := ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "delta landed fast",
			NormalizedContent: "delta landed fast",
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
	}}))
	updated, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotEqual(t, manifest.GeneratedAt, updated.GeneratedAt)

	var progress []ImportProgress
	imported, changed, err := ImportIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) { progress = append(progress, p) },
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, updated.GeneratedAt, imported.GeneratedAt)
	require.Contains(t, progressPhases(progress), "table_start")
	require.Contains(t, progressPhases(progress), "rebuild_fts")

	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "delta landed", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m2", results[0].MessageID)
	state, err := dst.GetSyncState(ctx, LastImportManifestJSONScope)
	require.NoError(t, err)
	require.Contains(t, state, `"file_manifests"`)
}

func TestImportIfChangedUsesMixedIncrementalPlanForMetadataChanges(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, changed, err := ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, src.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "launch", RawJSON: `{}`}))
	require.NoError(t, src.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Launch Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"bio":"delta member"}`,
	}))
	require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "launch",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "mixed delta landed",
			NormalizedContent: "mixed delta landed",
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"m2"}`,
		Options:     store.WriteOptions{AppendEvent: true},
		Attachments: []store.AttachmentRecord{{
			AttachmentID: "a2",
			MessageID:    "m2",
			GuildID:      "g1",
			ChannelID:    "c1",
			AuthorID:     "u1",
			Filename:     "delta.txt",
			TextContent:  "attached delta",
		}},
		Mentions: []store.MentionEventRecord{{
			MessageID:  "m2",
			GuildID:    "g1",
			ChannelID:  "c1",
			AuthorID:   "u1",
			TargetType: "role",
			TargetID:   "r2",
			TargetName: "Launch",
			EventAt:    now,
		}},
	}}))
	updated, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotEqual(t, manifest.GeneratedAt, updated.GeneratedAt)

	previous, ok := PreviousImportedManifest(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.True(t, ok)
	planned, supported := shareIncrementalPlan(snapshot.PlanIncrementalImport(snapshotManifest(previous), snapshotManifest(updated)))
	require.True(t, supported, "%+v", planned)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "channels").Mode)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "members").Mode)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "messages").Mode)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "message_events").Mode)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "message_attachments").Mode)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "mention_events").Mode)

	var progress []ImportProgress
	imported, changed, err := ImportIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) { progress = append(progress, p) },
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, updated.GeneratedAt, imported.GeneratedAt)
	require.Contains(t, progressPhases(progress), "rebuild_fts")
	require.Contains(t, progressPhases(progress), "rebuild_member_fts")
	require.Equal(t, importPlanRowCount(planned), progressTotalRows(t, progress, "start"))
	require.Positive(t, progressTotalRows(t, progress, "start"))

	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "checklist", Channel: "launch", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m1", results[0].MessageID)

	results, err = dst.SearchMessages(ctx, store.SearchOptions{Query: "mixed delta", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m2", results[0].MessageID)
	_, rows, err := dst.ReadOnlyQuery(ctx, "select name from channels where id = 'c1'")
	require.NoError(t, err)
	require.Equal(t, "launch", rows[0][0])
	_, rows, err = dst.ReadOnlyQuery(ctx, "select count(*) from mention_events")
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])
	_, rows, err = dst.ReadOnlyQuery(ctx, "select count(*) from message_events")
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])
	_, rows, err = dst.ReadOnlyQuery(ctx, "select count(*) from member_fts where member_fts match 'delta'")
	require.NoError(t, err)
	require.Equal(t, "1", rows[0][0])
}

func TestImportIfChangedInfersLegacyManifestFilesFromGit(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	require.NoError(t, exec.CommandContext(ctx, "git", "init", repo).Run())
	configureGitUser(t, repo)
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	writeShareManifest(t, repo, stripFileManifests(manifest))
	require.NoError(t, exec.CommandContext(ctx, "git", "-C", repo, "add", ".").Run())
	require.NoError(t, exec.CommandContext(ctx, "git", "-C", repo, "commit", "-m", "initial snapshot").Run())

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, changed, err := ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "legacy git delta",
			NormalizedContent: "legacy git delta",
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
	}}))
	updated, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	writeShareManifest(t, repo, stripFileManifests(updated))
	require.NoError(t, exec.CommandContext(ctx, "git", "-C", repo, "add", ".").Run())
	require.NoError(t, exec.CommandContext(ctx, "git", "-C", repo, "commit", "-m", "tail snapshot").Run())

	previous, ok := PreviousImportedManifest(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.True(t, ok)
	planned, supported := shareIncrementalPlan(snapshot.PlanIncrementalImport(snapshotManifest(previous), snapshotManifest(enrichManifestFromGit(ctx, repo, "HEAD", stripFileManifests(updated)))))
	require.True(t, supported, "%+v", planned)
	require.True(t, planned.Changed(), "%+v", planned)

	var progress []ImportProgress
	_, changed, err = ImportIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) { progress = append(progress, p) },
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, progressPhases(progress), "rebuild_fts")
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "legacy git delta", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestApplyImportPragmasBoundImportMemory(t *testing.T) {
	ctx := context.Background()
	s := seedStore(t, filepath.Join(t.TempDir(), "dst.db"))
	defer func() { _ = s.Close() }()

	restore, err := applyImportPragmas(ctx, s.DB())
	require.NoError(t, err)
	defer func() { require.NoError(t, restore(ctx)) }()

	var tempStore int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma temp_store`).Scan(&tempStore))
	require.Equal(t, 1, tempStore, "snapshot imports should use file-backed temporary storage")

	var cacheSize int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma cache_size`).Scan(&cacheSize))
	require.GreaterOrEqual(t, cacheSize, -65536)

	var journalMode string
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma journal_mode`).Scan(&journalMode))
	require.NotEqual(t, "off", strings.ToLower(journalMode))

	var synchronous int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma synchronous`).Scan(&synchronous))
	require.NotZero(t, synchronous)
}

func TestImportRepairsBlankMessageGuildIDs(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	_, err := src.DB().ExecContext(ctx, `update messages set guild_id = '' where id = 'm1'`)
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `update message_events set guild_id = '' where message_id = 'm1'`)
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `update mention_events set guild_id = '' where message_id = 'm1'`)
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	_, err = Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.Contains(t, snapshotTableText(t, repo, tableEntry(t, mustReadManifest(t, repo), "messages")), `"guild_id":""`)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	var guildID string
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select guild_id from messages where id = 'm1'`).Scan(&guildID))
	require.Equal(t, "g1", guildID)
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select guild_id from message_events where message_id = 'm1'`).Scan(&guildID))
	require.Equal(t, "g1", guildID)
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select guild_id from mention_events where message_id = 'm1'`).Scan(&guildID))
	require.Equal(t, "g1", guildID)
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "launch", GuildIDs: []string{"g1"}, Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "g1", results[0].GuildID)
}

func TestSnapshotExcludesLocalEmbeddingState(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	_, err := src.DB().ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, provider, model, input_version, updated_at)
		values ('m1', 'done', 0, 'ollama', 'nomic-embed-text', ?, ?)
	`, store.EmbeddingInputVersion, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values ('m1', 'ollama', 'nomic-embed-text', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, []byte{0, 0, 0, 0, 0, 0, 0, 0}, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotContains(t, tableNames(manifest), "embedding_jobs")
	require.NotContains(t, tableNames(manifest), "message_embeddings")
	require.Empty(t, manifest.Embeddings)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = dst.DB().ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, provider, model, input_version, updated_at)
		values ('m1', 'pending', 0, 'ollama', 'nomic-embed-text', ?, ?)
	`, store.EmbeddingInputVersion, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	var state string
	require.NoError(t, dst.DB().QueryRowContext(ctx, `
		select state from embedding_jobs where message_id = 'm1'
	`).Scan(&state))
	require.Equal(t, "pending", state)
}

func TestSnapshotExcludesAndPreservesDirectMessages(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	seedDirectMessageData(t, ctx, src)

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.Equal(t, 1, tableEntry(t, manifest, "guilds").Rows)
	require.Equal(t, 1, tableEntry(t, manifest, "channels").Rows)
	require.Equal(t, 1, tableEntry(t, manifest, "messages").Rows)
	require.NotContains(t, snapshotTableText(t, repo, tableEntry(t, manifest, "guilds")), directMessageGuildID)
	require.NotContains(t, snapshotTableText(t, repo, tableEntry(t, manifest, "channels")), directMessageGuildID)
	require.NotContains(t, snapshotTableText(t, repo, tableEntry(t, manifest, "messages")), "private dm content")
	require.NotContains(t, snapshotTableText(t, repo, tableEntry(t, manifest, "sync_state")), "wiretap:last_import")
	manifest = appendSnapshotRow(t, repo, manifest, "messages", map[string]any{
		"id":                 "hostile-dm",
		"guild_id":           directMessageGuildID,
		"channel_id":         "dm-c2",
		"author_id":          "u9",
		"message_type":       0,
		"created_at":         "2026-04-24T16:00:00Z",
		"content":            "hostile imported dm",
		"normalized_content": "hostile imported dm",
		"pinned":             0,
		"has_attachments":    0,
		"raw_json":           `{}`,
		"updated_at":         "2026-04-24T16:00:00Z",
	})
	manifest = appendSnapshotRow(t, repo, manifest, "sync_state", map[string]any{
		"scope":      "wiretap:hostile",
		"cursor":     "private",
		"updated_at": "2026-04-24T16:00:00Z",
	})
	writeShareManifest(t, repo, manifest)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	seedDirectMessageData(t, ctx, dst)

	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	dmResults, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "private dm content", Limit: 10})
	require.NoError(t, err)
	require.Len(t, dmResults, 1)
	require.Equal(t, directMessageGuildID, dmResults[0].GuildID)
	guildResults, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "launch checklist", Limit: 10})
	require.NoError(t, err)
	require.Len(t, guildResults, 1)
	wiretapState, err := dst.GetSyncState(ctx, "wiretap:last_import")
	require.NoError(t, err)
	require.Equal(t, "2026-04-24T15:33:17Z", wiretapState)
	hostileResults, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "hostile imported dm", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, hostileResults)
	_, rows, err := dst.ReadOnlyQuery(ctx, "select count(*) from sync_state where scope = 'wiretap:hostile'")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestSnapshotFilterKeepsOnlyIncludedPublicChannels(t *testing.T) {
	ctx := context.Background()
	src, err := store.Open(ctx, filepath.Join(t.TempDir(), "src.db"))
	require.NoError(t, err)
	defer func() { _ = src.Close() }()

	require.NoError(t, src.UpsertGuild(ctx, store.GuildRecord{
		ID:      "g1",
		Name:    "Guild",
		RawJSON: `{"roles":[{"id":"g1","permissions":"1024"}]}`,
	}))
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "public", RawJSON: `{}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "private", RawJSON: `{"permission_overwrites":[{"id":"g1","type":0,"allow":"0","deny":"1024"}]}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "cat1", GuildID: "g1", Kind: "category", Name: "private-category", RawJSON: `{"permission_overwrites":[{"id":"g1","type":0,"allow":"0","deny":"1024"}]}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "c3", GuildID: "g1", ParentID: "cat1", Kind: "text", Name: "inherits-private", RawJSON: `{}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "f1", GuildID: "g1", Kind: "forum", Name: "public-forum", RawJSON: `{}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "t1", GuildID: "g1", ParentID: "f1", ThreadParentID: "f1", Kind: "thread_public", Name: "public-thread", RawJSON: `{}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "tp1", GuildID: "g1", ParentID: "f1", ThreadParentID: "f1", Kind: "thread_private", Name: "private-thread", IsPrivateThread: true, RawJSON: `{}`})

	for _, userID := range []string{"u1", "u2", "u3", "u4"} {
		require.NoError(t, src.UpsertMember(ctx, store.MemberRecord{
			GuildID:     "g1",
			UserID:      userID,
			Username:    userID,
			RoleIDsJSON: `[]`,
			RawJSON:     `{}`,
		}))
	}
	upsertSnapshotFilterMessage(t, ctx, src, "m1", "c1", "u1", "public content")
	upsertSnapshotFilterMessage(t, ctx, src, "m2", "c2", "u2", "private content")
	upsertSnapshotFilterMessage(t, ctx, src, "m3", "c3", "u3", "category private content")
	upsertSnapshotFilterMessage(t, ctx, src, "mt1", "t1", "u4", "thread content")
	upsertSnapshotFilterMessage(t, ctx, src, "mtp1", "tp1", "u2", "private thread content")
	require.NoError(t, src.SetSyncState(ctx, "channel:c1:latest_message_id", "m1"))
	require.NoError(t, src.SetSyncState(ctx, "channel:c2:latest_message_id", "m2"))
	require.NoError(t, src.SetSyncState(ctx, "guild:g1:members:last_success", time.Now().UTC().Format(time.RFC3339Nano)))
	require.NoError(t, src.SetSyncState(ctx, "guild:g1:custom", "ok"))
	require.NoError(t, src.SetSyncState(ctx, LastImportManifestJSONScope, `{"tables":[{"name":"messages","rows":999}],"leak":"private content"}`))

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{
		RepoPath: repo,
		Branch:   "main",
		Filter: FilterOptions{
			PublicOnly:        true,
			IncludeChannelIDs: []string{"c1", "c2", "c3", "f1"},
		},
	})
	require.NoError(t, err)

	channelText := snapshotTableText(t, repo, tableEntry(t, manifest, "channels"))
	require.Contains(t, channelText, `"id":"c1"`)
	require.Contains(t, channelText, `"id":"f1"`)
	require.Contains(t, channelText, `"id":"t1"`)
	require.NotContains(t, channelText, `"id":"c2"`)
	require.NotContains(t, channelText, `"id":"c3"`)
	require.NotContains(t, channelText, `"id":"tp1"`)

	messageText := snapshotTableText(t, repo, tableEntry(t, manifest, "messages"))
	require.Contains(t, messageText, "public content")
	require.Contains(t, messageText, "thread content")
	require.NotContains(t, messageText, "private content")
	require.NotContains(t, messageText, "category private content")
	require.NotContains(t, messageText, "private thread content")

	memberText := snapshotTableText(t, repo, tableEntry(t, manifest, "members"))
	require.Contains(t, memberText, `"user_id":"u1"`)
	require.Contains(t, memberText, `"user_id":"u4"`)
	require.NotContains(t, memberText, `"user_id":"u2"`)
	require.NotContains(t, memberText, `"user_id":"u3"`)

	syncText := snapshotTableText(t, repo, tableEntry(t, manifest, "sync_state"))
	require.Contains(t, syncText, "channel:c1:latest_message_id")
	require.Contains(t, syncText, "guild:g1:custom")
	require.NotContains(t, syncText, "channel:c2:latest_message_id")
	require.NotContains(t, syncText, "guild:g1:members:last_success")
	require.NotContains(t, syncText, "share:last_import_manifest_json")
	require.NotContains(t, syncText, "private content")
	require.NotContains(t, syncText, `"rows":999`)

	_, rows, err := src.ReadOnlyQuery(ctx, "select count(*) from messages where content like '%private%'")
	require.NoError(t, err)
	require.Equal(t, "3", rows[0][0])
}

func TestExportImportEmbeddingsOptIn(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	vector := []float32{1, 0.5}
	blob, err := store.EncodeEmbeddingVector(vector)
	require.NoError(t, err)
	embeddedAt := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values ('m1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, blob, embeddedAt)
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{
		RepoPath:              repo,
		Branch:                "main",
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "text-embedding-3-small",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
	manifest, err := Export(ctx, src, opts)
	require.NoError(t, err)
	require.Len(t, manifest.Embeddings, 1)
	require.Equal(t, 1, manifest.Embeddings[0].Rows)
	require.NotEmpty(t, manifest.Embeddings[0].Files)
	require.FileExists(t, filepath.Join(repo, filepath.FromSlash(manifest.Embeddings[0].Files[0])))

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = Import(ctx, dst, opts)
	require.NoError(t, err)

	var gotBlob []byte
	var gotDimensions int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `
		select dimensions, embedding_blob
		from message_embeddings
		where message_id = 'm1'
		  and provider = 'openai'
		  and model = 'text-embedding-3-small'
		  and input_version = ?
	`, store.EmbeddingInputVersion).Scan(&gotDimensions, &gotBlob))
	require.Equal(t, 2, gotDimensions)
	gotVector, err := store.DecodeEmbeddingVector(gotBlob)
	require.NoError(t, err)
	require.Equal(t, vector, gotVector)
}

func TestExportEmbeddingsExcludesDirectMessages(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	seedDirectMessageData(t, ctx, src)

	blob, err := store.EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values
			('m1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?),
			('dm1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, blob, time.Now().UTC().Format(time.RFC3339Nano), store.EmbeddingInputVersion, blob, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{
		RepoPath:              repo,
		Branch:                "main",
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "text-embedding-3-small",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	})
	require.NoError(t, err)
	require.Len(t, manifest.Embeddings, 1)
	require.Equal(t, 1, manifest.Embeddings[0].Rows)
	text := snapshotFilesText(t, repo, manifest.Embeddings[0].Files)
	require.Contains(t, text, `"message_id":"m1"`)
	require.NotContains(t, text, "dm1")
}

func TestArchiveExportDropsEmbeddingBundleUnlessOptedIn(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	blob, err := store.EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values ('m1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, blob, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	embeddingOpts := Options{
		RepoPath:              repo,
		Branch:                "main",
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "text-embedding-3-small",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
	manifest, err := Export(ctx, src, embeddingOpts)
	require.NoError(t, err)
	require.Len(t, manifest.Embeddings, 1)
	embeddingFile := filepath.Join(repo, filepath.FromSlash(manifest.Embeddings[0].Files[0]))
	require.FileExists(t, embeddingFile)

	archiveManifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.Empty(t, archiveManifest.Embeddings)
}

func TestImportEmbeddingsFiltersByConfiguredIdentity(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	blob, err := store.EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values ('m1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, blob, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	exportOpts := Options{
		RepoPath:              repo,
		Branch:                "main",
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "text-embedding-3-small",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
	manifest, err := Export(ctx, src, exportOpts)
	require.NoError(t, err)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	require.NoError(t, ImportEmbeddings(ctx, dst, Options{
		RepoPath:              repo,
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "ollama",
		EmbeddingModel:        "nomic-embed-text",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}, manifest))

	_, rows, err := dst.ReadOnlyQuery(ctx, "select count(*) from message_embeddings")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestImportIfChangedSkipsSameManifest(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	importedManifest, imported, err := ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, imported)
	require.Equal(t, manifest.GeneratedAt, importedManifest.GeneratedAt)

	require.NoError(t, dst.UpsertMessage(ctx, store.MessageRecord{
		ID:                "local-only",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "live delta preserved",
		NormalizedContent: "live delta preserved",
		RawJSON:           `{}`,
	}))

	_, imported, err = ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.False(t, imported)
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "live delta", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "local-only", results[0].MessageID)
}

func TestExportShardsLargeTables(t *testing.T) {
	ctx := context.Background()
	prevMaxShardBytes := maxShardBytes
	maxShardBytes = 150
	t.Cleanup(func() { maxShardBytes = prevMaxShardBytes })

	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	now := time.Now().UTC()
	for i := range 25 {
		id := "extra-" + strconv.Itoa(i)
		require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
			Record: store.MessageRecord{
				ID:                id,
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "general",
				AuthorID:          "u1",
				AuthorName:        "Peter",
				MessageType:       0,
				CreatedAt:         now.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
				Content:           strings.Repeat("unique launch shard payload "+id+" ", 8),
				NormalizedContent: strings.Repeat("unique launch shard payload "+id+" ", 8),
				RawJSON:           `{}`,
			},
			EventType:   "upsert",
			PayloadJSON: `{"id":"` + id + `"}`,
			Options:     store.WriteOptions{AppendEvent: true},
		}}))
	}

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	messages := tableEntry(t, manifest, "messages")
	require.Greater(t, len(messages.Files), 1)
	require.Empty(t, messages.File)
	for _, rel := range messages.Files {
		info, err := os.Stat(filepath.Join(repo, filepath.FromSlash(rel)))
		require.NoError(t, err)
		require.Less(t, info.Size(), int64(100*1024*1024))
	}

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "shard payload", Limit: 50})
	require.NoError(t, err)
	require.Len(t, results, 25)
}

func TestGitCommitDetectsNoChanges(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{RepoPath: repo, Branch: "main"}
	_, err := Export(ctx, src, opts)
	require.NoError(t, err)
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", repo, "config", "user.name", "discrawl test").Run())
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", repo, "config", "user.email", "discrawl@example.com").Run())

	committed, err := Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)

	committed, err = Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.False(t, committed)
}

func TestPullAndPushWithBareRemote(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	dir := t.TempDir()
	remote := filepath.Join(dir, "remote.git")
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", dir, "init", "--bare", remote).Run())

	publisher := filepath.Join(dir, "publisher")
	opts := Options{RepoPath: publisher, Remote: remote, Branch: "main"}
	_, err := Export(ctx, src, opts)
	require.NoError(t, err)
	configureGitUser(t, publisher)
	committed, err := Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	opts.Tag = "snapshot/test"
	tag, err := CreateImmutableTag(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, "snapshot/test", tag)
	require.NoError(t, Push(ctx, opts))

	subscriber := filepath.Join(dir, "subscriber")
	subOpts := Options{RepoPath: subscriber, Remote: remote, Branch: "main"}
	require.NoError(t, Pull(ctx, subOpts))
	require.FileExists(t, filepath.Join(subscriber, ManifestName))
}

func TestValidateTagBeforeRemoteSync(t *testing.T) {
	ctx := context.Background()
	repo := filepath.Join(t.TempDir(), "share")
	require.NoError(t, EnsureRepo(ctx, Options{RepoPath: repo, Branch: "main"}))
	err := ValidateTag(ctx, Options{
		RepoPath: repo,
		Remote:   "https://example.invalid/archive.git",
		Branch:   "main",
		Tag:      "bad tag",
	})
	require.ErrorContains(t, err, "invalid snapshot tag")
	require.NoError(t, ValidateTag(ctx, Options{}))
	require.Error(t, ValidateTag(ctx, Options{Remote: "remote", Tag: "snapshot/valid"}))
	require.Error(t, ValidateTag(ctx, Options{Tag: "snapshot/valid"}))
	localRepo := filepath.Join(t.TempDir(), "local-share")
	require.NoError(t, ValidateTag(ctx, Options{RepoPath: localRepo, Branch: "main", Tag: "snapshot/valid"}))
	require.Equal(t, []string{"tables/messages/000001.jsonl.gz"}, tableSnapshotFiles(TableManifest{Files: []string{"tables/messages/000001.jsonl.gz"}}))
	require.Equal(t, []string{"tables/messages.jsonl.gz"}, tableSnapshotFiles(TableManifest{File: "tables/messages.jsonl.gz"}))
	require.Nil(t, tableSnapshotFiles(TableManifest{}))
	err = materializeRefFile(ctx, mirror.Options{}, "HEAD", "../escape", t.TempDir())
	require.ErrorContains(t, err, "invalid historical share path")
	require.NoError(t, os.MkdirAll(filepath.Join(localRepo, "tables"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(localRepo, "tables", "sample.txt"), []byte("sample\n"), 0o600))
	committed, err := mirror.Commit(ctx, mirror.Options{RepoPath: localRepo}, "sample")
	require.NoError(t, err)
	require.True(t, committed)
	materialized := t.TempDir()
	require.NoError(t, materializeRefFile(ctx, mirror.Options{RepoPath: localRepo}, "HEAD", "tables/sample.txt", materialized))
	require.Equal(t, []byte("sample\n"), mustReadFile(t, filepath.Join(materialized, "tables", "sample.txt")))
	require.NoError(t, os.WriteFile(filepath.Join(localRepo, ManifestName), []byte(`{`), 0o600))
	committed, err = mirror.Commit(ctx, mirror.Options{RepoPath: localRepo}, "malformed manifest")
	require.NoError(t, err)
	require.True(t, committed)
	_, err = ImportAt(ctx, nil, Options{RepoPath: localRepo}, "HEAD")
	require.ErrorContains(t, err, "parse share manifest")
}

func TestHistoricalRefErrorPaths(t *testing.T) {
	ctx := context.Background()
	repo := filepath.Join(t.TempDir(), "share")
	require.NoError(t, EnsureRepo(ctx, Options{RepoPath: repo, Branch: "main"}))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "sample.txt"), []byte("sample\n"), 0o600))
	committed, err := mirror.Commit(ctx, mirror.Options{RepoPath: repo}, "sample")
	require.NoError(t, err)
	require.True(t, committed)
	_, err = ImportAt(ctx, nil, Options{RepoPath: repo}, "HEAD")
	require.Error(t, err)
	_, err = ImportAt(ctx, nil, Options{}, "HEAD")
	require.Error(t, err)

	writeManifest := func(manifest Manifest, message string) {
		t.Helper()
		body, marshalErr := json.Marshal(manifest)
		require.NoError(t, marshalErr)
		require.NoError(t, os.WriteFile(filepath.Join(repo, ManifestName), body, 0o600))
		changed, commitErr := mirror.Commit(ctx, mirror.Options{RepoPath: repo}, message)
		require.NoError(t, commitErr)
		require.True(t, changed)
	}
	writeManifest(Manifest{Version: 1, GeneratedAt: time.Now().UTC(), Tables: []TableManifest{{Name: "messages", Files: []string{"tables/missing.jsonl.gz"}}}}, "missing table")
	_, err = ImportAt(ctx, nil, Options{RepoPath: repo}, "HEAD")
	require.Error(t, err)

	writeManifest(Manifest{Version: 1, GeneratedAt: time.Now().UTC(), Embeddings: []EmbeddingManifest{{Files: []string{"embeddings/missing.jsonl.gz"}}}}, "skipped embeddings")
	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, _ = ImportAt(ctx, dst, Options{RepoPath: repo}, "HEAD")
	_, err = ImportAt(ctx, dst, Options{RepoPath: repo, IncludeEmbeddings: true}, "HEAD")
	require.Error(t, err)

	writeManifest(Manifest{Version: 1, GeneratedAt: time.Now().UTC(), Media: &MediaManifest{Files: []snapshot.FileManifest{{Path: "media/missing.gz"}}}}, "missing media")
	_, err = ImportAt(ctx, dst, Options{RepoPath: repo, IncludeMedia: true}, "HEAD")
	require.Error(t, err)

	blockedParent := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(blockedParent, "tables"), []byte("blocked"), 0o600))
	err = materializeRefFile(ctx, mirror.Options{RepoPath: repo}, "HEAD~3", "sample.txt", filepath.Join(blockedParent, "tables", "child"))
	require.Error(t, err)
	writeTarget := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(writeTarget, "sample.txt"), 0o755))
	err = materializeRefFile(ctx, mirror.Options{RepoPath: repo}, "HEAD~3", "sample.txt", writeTarget)
	require.Error(t, err)
}

func TestImportAtRestoresTaggedSnapshotWithoutMovingCheckout(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := seedStore(t, filepath.Join(dir, "src.db"))
	defer func() { _ = src.Close() }()
	mediaBody := []byte("historical media")
	mediaSum := sha256.Sum256(mediaBody)
	mediaHash := hex.EncodeToString(mediaSum[:])
	mediaPath := filepath.ToSlash(filepath.Join("attachments", mediaHash[:2], mediaHash+"-history.txt"))
	require.NoError(t, addCachedAttachment(ctx, src, mediaPath, mediaHash, int64(len(mediaBody))))
	srcCache := filepath.Join(dir, "src-cache")
	srcMedia, err := media.LocalPath(srcCache, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(srcMedia), 0o755))
	require.NoError(t, os.WriteFile(srcMedia, mediaBody, 0o600))
	embeddingBlob, err := store.EncodeEmbeddingVector([]float32{1, 0.5})
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values ('m1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, embeddingBlob, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	opts := Options{
		RepoPath:              filepath.Join(dir, "share"),
		CacheDir:              srcCache,
		Branch:                "main",
		Tag:                   "snapshot-old",
		IncludeMedia:          true,
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "text-embedding-3-small",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
	_, err = Export(ctx, src, opts)
	require.NoError(t, err)
	committed, err := Commit(ctx, opts, "old snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	tag, err := CreateImmutableTag(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, "snapshot-old", tag)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			CreatedAt:         now,
			Content:           "new snapshot",
			NormalizedContent: "new snapshot",
			RawJSON:           `{}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"m1"}`,
	}}))
	opts.Tag = ""
	_, err = Export(ctx, src, opts)
	require.NoError(t, err)
	committed, err = Commit(ctx, opts, "new snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	headBefore := strings.TrimSpace(testGitOutput(t, ctx, opts.RepoPath, "rev-parse", "HEAD"))

	dst, err := store.Open(ctx, filepath.Join(dir, "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	restoreOpts := opts
	restoreOpts.CacheDir = filepath.Join(dir, "dst-cache")
	manifest, err := ImportAt(ctx, dst, restoreOpts, "snapshot-old")
	require.NoError(t, err)
	require.False(t, manifest.GeneratedAt.IsZero())
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "launch", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "launch checklist ready", results[0].Content)
	restoredMedia, err := media.LocalPath(restoreOpts.CacheDir, mediaPath)
	require.NoError(t, err)
	require.Equal(t, mediaBody, mustReadFile(t, restoredMedia))
	var embeddingCount int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from message_embeddings where message_id = 'm1'`).Scan(&embeddingCount))
	require.Equal(t, 1, embeddingCount)
	require.Equal(t, headBefore, strings.TrimSpace(testGitOutput(t, ctx, opts.RepoPath, "rev-parse", "HEAD")))
}

func TestPushRebasesRemoteReadmeUpdates(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	dir := t.TempDir()
	remote := filepath.Join(dir, "remote.git")
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", dir, "init", "--bare", remote).Run())

	publisher := filepath.Join(dir, "publisher")
	opts := Options{RepoPath: publisher, Remote: remote, Branch: "main"}
	_, err := Export(ctx, src, opts)
	require.NoError(t, err)
	configureGitUser(t, publisher)
	require.NoError(t, os.WriteFile(filepath.Join(publisher, "README.md"), []byte("report: first\n\nfield notes: old\n"), 0o600))
	committed, err := Commit(ctx, opts, "test: initial snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, Push(ctx, opts))

	reporter := filepath.Join(dir, "reporter")
	testGitRun(t, ctx, dir, "clone", "--branch", "main", remote, reporter)
	configureGitUser(t, reporter)
	require.NoError(t, os.WriteFile(filepath.Join(reporter, "README.md"), []byte("report: first\n\nfield notes: fresh\n"), 0o600))
	testGitRun(t, ctx, reporter, "commit", "-am", "docs: update field notes")
	testGitRun(t, ctx, reporter, "push", "-u", "origin", "main")

	require.NoError(t, os.WriteFile(filepath.Join(publisher, "README.md"), []byte("report: second\n\nfield notes: old\n"), 0o600))
	committed, err = Commit(ctx, opts, "test: update report")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, Push(ctx, opts))

	subscriber := filepath.Join(dir, "subscriber")
	require.NoError(t, Pull(ctx, Options{RepoPath: subscriber, Remote: remote, Branch: "main"}))
	body, err := os.ReadFile(filepath.Join(subscriber, "README.md"))
	require.NoError(t, err)
	require.Contains(t, string(body), "report: second")
	require.Contains(t, string(body), "field notes: fresh")
}

func TestImportValueConvertsJSONNumbers(t *testing.T) {
	t.Parallel()

	require.Equal(t, int64(42), importValue(json.Number("42")))
	require.InDelta(t, 3.5, importValue(json.Number("3.5")), 0)
	require.Equal(t, "not-a-number", importValue(json.Number("not-a-number")))
	require.Equal(t, "plain", importValue("plain"))
}

func TestManifestStateAndReadEdges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	_, err = ReadManifest(t.TempDir())
	require.ErrorIs(t, err, ErrNoManifest)

	repo := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(repo, ManifestName), []byte(`{`), 0o600))
	_, err = ReadManifest(repo)
	require.ErrorContains(t, err, "parse share manifest")
	require.NoError(t, os.WriteFile(filepath.Join(repo, ManifestName), []byte(`{"version":99}`), 0o600))
	_, err = ReadManifest(repo)
	require.ErrorContains(t, err, "unsupported share manifest version 99")

	now := time.Now().UTC().Truncate(time.Nanosecond)
	manifest := Manifest{Version: 1, GeneratedAt: now}
	require.False(t, ManifestAlreadyImported(ctx, s, Manifest{}))
	require.False(t, ManifestAlreadyImported(ctx, s, manifest))
	require.NoError(t, s.SetSyncState(ctx, LastImportManifestSyncScope, "not-time"))
	require.False(t, ManifestAlreadyImported(ctx, s, manifest))
	require.NoError(t, MarkImported(ctx, s, Manifest{}))
	require.False(t, ManifestAlreadyImported(ctx, s, manifest))
	require.NoError(t, MarkImported(ctx, s, manifest))
	require.True(t, ManifestAlreadyImported(ctx, s, manifest))

	require.False(t, NeedsImport(ctx, s, 15*time.Minute))
	require.NoError(t, s.SetSyncState(ctx, LastImportSyncScope, "bad-time"))
	require.True(t, NeedsImport(ctx, s, 15*time.Minute))
	require.NoError(t, s.SetSyncState(ctx, LastImportSyncScope, time.Now().UTC().Add(-20*time.Minute).Format(time.RFC3339Nano)))
	require.True(t, NeedsImport(ctx, s, 15*time.Minute))
	require.NoError(t, s.SetSyncState(ctx, LastImportSyncScope, time.Now().UTC().Format(time.RFC3339Nano)))
	require.False(t, NeedsImport(ctx, s, 0))
}

func TestRepoCommandEdges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	require.ErrorContains(t, EnsureRepo(ctx, Options{}), "repo path is empty")
	require.NoError(t, Pull(ctx, Options{}))
	require.Error(t, Pull(ctx, Options{Remote: "remote"}))
	require.NoError(t, Pull(ctx, Options{RepoPath: filepath.Join(t.TempDir(), "local-share")}))

	repo := filepath.Join(t.TempDir(), "repo")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, ".git"), 0o755))
	require.NoError(t, EnsureRepo(ctx, Options{RepoPath: repo}))

	err := Push(ctx, Options{RepoPath: repo, Branch: "main"})
	require.ErrorContains(t, err, "git push -u origin main")
	err = Push(ctx, Options{RepoPath: repo})
	require.ErrorContains(t, err, "git push -u origin main")
}

func TestShareSmallHelpersAndValidation(t *testing.T) {
	t.Parallel()

	require.Equal(t, "_", safePathSegment(" "))
	require.Equal(t, "_", safePathSegment("."))
	require.Equal(t, "_", safePathSegment(".."))
	require.Equal(t, "OpenAI_compatible-v1.2", safePathSegment("OpenAI compatible-v1.2"))
	require.Equal(t, `"weird""table"`, quoteIdent(`weird"table`))
	require.Equal(t, `insert into "messages"("id","weird""column") values(?,?)`, insertSQL("messages", []string{"id", `weird"column`}))
	require.Equal(t, "blob", exportValue([]byte("blob")))
	require.Equal(t, "plain", exportValue("plain"))
	require.Equal(t, int64(42), importValue(json.Number("42")))
	require.InDelta(t, 3.5, importValue(json.Number("3.5")), 0)
	require.Equal(t, "nope", importValue(json.Number("nope")))
	require.Equal(t, "plain", importValue("plain"))
	require.Equal(t, "plain", stringValue("plain"))
	require.Equal(t, "42", stringValue(json.Number("42")))
	require.Empty(t, stringValue(42))

	query, args := snapshotExportQuery("messages")
	require.Equal(t, "select * from messages where guild_id != ?", query)
	require.Equal(t, []any{directMessageGuildID}, args)
	query, args = snapshotExportQuery("guilds")
	require.Equal(t, "select * from guilds where id != ?", query)
	require.Equal(t, []any{directMessageGuildID}, args)
	query, args = snapshotExportQuery("sync_state")
	require.Equal(t, "select * from sync_state where scope not like 'wiretap:%'", query)
	require.Nil(t, args)
	query, args = snapshotExportQuery("custom")
	require.Equal(t, "select * from custom", query)
	require.Nil(t, args)

	query, args = snapshotDeleteQuery("channels")
	require.Equal(t, "delete from channels where guild_id != ?", query)
	require.Equal(t, []any{directMessageGuildID}, args)
	query, args = snapshotDeleteQuery("guilds")
	require.Equal(t, "delete from guilds where id != ?", query)
	require.Equal(t, []any{directMessageGuildID}, args)
	query, args = snapshotDeleteQuery("message_events")
	require.Equal(t, "delete from message_events where guild_id != ?", query)
	require.Equal(t, []any{directMessageGuildID}, args)
	query, args = snapshotDeleteQuery("sync_state")
	require.Equal(t, "delete from sync_state where scope not like 'wiretap:%'", query)
	require.Nil(t, args)
	query, args = snapshotDeleteQuery("custom")
	require.Equal(t, "delete from custom", query)
	require.Nil(t, args)

	require.True(t, isDirectMessageSnapshotRow("guilds", map[string]any{"id": directMessageGuildID}))
	require.True(t, isDirectMessageSnapshotRow("channels", map[string]any{"guild_id": directMessageGuildID}))
	require.True(t, isDirectMessageSnapshotRow("sync_state", map[string]any{"scope": "wiretap:last_import"}))
	require.False(t, isDirectMessageSnapshotRow("sync_state", map[string]any{"scope": "share:last_import"}))
	require.False(t, isDirectMessageSnapshotRow("custom", map[string]any{"guild_id": directMessageGuildID}))
	require.True(t, isLocalOnlyGuildID(directMessageGuildID))
	require.False(t, isLocalOnlyGuildID("g1"))

	require.Equal(t, []string{"message_id", "guild_id"}, importColumns(TableManifest{Name: "message_events", Columns: []string{"event_id", "message_id", "guild_id"}}))
	require.Equal(t, []string{"event_id", "message_id"}, importColumns(TableManifest{Name: "messages", Columns: []string{"event_id", "message_id"}}))
	require.Equal(t, 7, manifestRowCount(Manifest{
		Tables:     []TableManifest{{Rows: 2}, {Rows: 3}},
		Embeddings: []EmbeddingManifest{{Rows: 2}},
	}))
	var seen []ImportProgress
	Options{Progress: func(progress ImportProgress) { seen = append(seen, progress) }}.reportProgress(ImportProgress{Phase: "phase"})
	require.Equal(t, []ImportProgress{{Phase: "phase"}}, seen)
	Options{}.reportProgress(ImportProgress{Phase: "ignored"})
	require.Equal(t, mirror.Options{RepoPath: "repo", Remote: "origin", Branch: "main", DirMode: 0o750}, mirrorOptions(Options{RepoPath: "repo", Remote: "origin", Branch: "main"}))

	var buf bytes.Buffer
	cw := &countingWriter{w: &buf}
	n, err := cw.Write([]byte("abc"))
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, int64(3), cw.n)

	require.True(t, embeddingManifestMatches(Options{EmbeddingProvider: " OPENAI ", EmbeddingModel: "model"}, EmbeddingManifest{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
	}))
	require.False(t, embeddingManifestMatches(Options{EmbeddingProvider: "ollama"}, EmbeddingManifest{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
	}))

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorContains(t, importTable(ctx, tx, Options{RepoPath: t.TempDir()}, TableManifest{Name: "messages", Columns: []string{"id"}}), "has no files")
	require.NoError(t, tx.Rollback())

	require.ErrorContains(t, ImportEmbeddings(ctx, s, Options{
		RepoPath:              t.TempDir(),
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "model",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}, Manifest{Embeddings: []EmbeddingManifest{{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
	}}}), "has no files")
}

func TestTableShardWriterRotates(t *testing.T) {
	oldMax := maxShardBytes
	maxShardBytes = 1
	t.Cleanup(func() { maxShardBytes = oldMax })

	writer := tableShardWriter{rootDir: t.TempDir(), relDir: "tables/messages", label: "messages"}
	require.NoError(t, os.MkdirAll(filepath.Join(writer.rootDir, filepath.FromSlash(writer.relDir)), 0o755))
	require.NoError(t, writer.open())
	_, err := writer.Write([]byte(`{"id":"m1"}` + "\n"))
	require.NoError(t, err)
	require.NoError(t, writer.finishRow())
	require.NoError(t, writer.rotateIfNeeded())
	_, err = writer.Write([]byte(`{"id":"m2"}` + "\n"))
	require.NoError(t, err)
	require.NoError(t, writer.finishRow())
	require.NoError(t, writer.close())
	require.Len(t, writer.files, 2)
	for _, rel := range writer.files {
		require.FileExists(t, filepath.Join(writer.rootDir, filepath.FromSlash(rel)))
	}
}

func TestLegacyManifestFileImportAndEmbeddingDecodeErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	repo := t.TempDir()
	tableRel := filepath.ToSlash(filepath.Join("tables", "guilds", "legacy.jsonl.gz"))
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "tables", "guilds"), 0o755))
	writeGzipJSONLines(t, filepath.Join(repo, filepath.FromSlash(tableRel)), []string{
		`{"id":"g1","name":"Guild","icon":null,"raw_json":"{}","updated_at":"2026-04-22T12:00:00Z"}`,
	})
	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, importTable(ctx, tx, Options{RepoPath: repo}, TableManifest{
		Name:    "guilds",
		File:    tableRel,
		Columns: []string{"id", "name", "icon", "raw_json", "updated_at"},
	}))
	require.NoError(t, tx.Commit())

	_, rows, err := s.ReadOnlyQuery(ctx, "select id, name from guilds")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"g1", "Guild"}}, rows)

	embeddingRel := filepath.ToSlash(filepath.Join("embeddings", "bad.jsonl.gz"))
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "embeddings"), 0o755))
	writeGzipJSONLines(t, filepath.Join(repo, filepath.FromSlash(embeddingRel)), []string{
		`{"message_id":"m1","provider":"openai","model":"model","input_version":"` + store.EmbeddingInputVersion + `","dimensions":3.5,"embedding_blob":"AAAA","embedded_at":"2026-04-22T12:00:00Z"}`,
	})
	tx, err = s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorContains(t, importEmbeddings(ctx, tx, Options{
		RepoPath:              repo,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "model",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}, []EmbeddingManifest{{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
		Files:        []string{embeddingRel},
	}}), "decode dimensions")
	require.NoError(t, tx.Rollback())

	writeGzipJSONLines(t, filepath.Join(repo, filepath.FromSlash(embeddingRel)), []string{
		`{"message_id":"m1","provider":"openai","model":"model","input_version":"` + store.EmbeddingInputVersion + `","dimensions":2,"embedding_blob":"not-base64","embedded_at":"2026-04-22T12:00:00Z"}`,
	})
	tx, err = s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorContains(t, importEmbeddings(ctx, tx, Options{
		RepoPath:              repo,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "model",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}, []EmbeddingManifest{{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
		Files:        []string{embeddingRel},
	}}), "decode embedding blob")
	require.NoError(t, tx.Rollback())
}

func TestImportEmbeddingsRejectsUnsafeManifestFiles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	repo := t.TempDir()
	for _, rel := range []string{
		"../escape.jsonl.gz",
		filepath.ToSlash(filepath.Join("tables", "messages", "000001.jsonl.gz")),
		filepath.ToSlash(filepath.Join("embeddings", "openai", "model", "000001.jsonl")),
	} {
		tx, err := s.DB().BeginTx(ctx, nil)
		require.NoError(t, err)
		require.ErrorContains(t, importEmbeddings(ctx, tx, Options{
			RepoPath:              repo,
			EmbeddingProvider:     "openai",
			EmbeddingModel:        "model",
			EmbeddingInputVersion: store.EmbeddingInputVersion,
		}, []EmbeddingManifest{{
			Provider:     "openai",
			Model:        "model",
			InputVersion: store.EmbeddingInputVersion,
			Files:        []string{rel},
		}}), "invalid embedding manifest path")
		require.NoError(t, tx.Rollback())
	}
}

func TestImportEmbeddingsBoundsDecompressedInput(t *testing.T) {
	oldLimit := maxSharedEmbeddingDecompressedBytes
	maxSharedEmbeddingDecompressedBytes = 32
	t.Cleanup(func() { maxSharedEmbeddingDecompressedBytes = oldLimit })

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	repo := t.TempDir()
	embeddingRel := filepath.ToSlash(filepath.Join("embeddings", "openai", "model", "000001.jsonl.gz"))
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "embeddings", "openai", "model"), 0o755))
	writeGzipJSONLines(t, filepath.Join(repo, filepath.FromSlash(embeddingRel)), []string{
		`{"message_id":"m1","provider":"openai","model":"model","input_version":"` + store.EmbeddingInputVersion + `","dimensions":2,"embedding_blob":"AAAA","embedded_at":"2026-04-22T12:00:00Z"}`,
	})

	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorContains(t, importEmbeddings(ctx, tx, Options{
		RepoPath:              repo,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "model",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}, []EmbeddingManifest{{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
		Files:        []string{embeddingRel},
	}}), "embedding decompressed size exceeds")
	require.NoError(t, tx.Rollback())
}

func writeGzipJSONLines(t *testing.T, path string, lines []string) {
	t.Helper()
	file, err := os.Create(path)
	require.NoError(t, err)
	gz := gzip.NewWriter(file)
	for _, line := range lines {
		_, err = gz.Write([]byte(line + "\n"))
		require.NoError(t, err)
	}
	require.NoError(t, gz.Close())
	require.NoError(t, file.Close())
}

func appendSnapshotRow(t *testing.T, repo string, manifest Manifest, tableName string, row map[string]any) Manifest {
	t.Helper()
	for i := range manifest.Tables {
		if manifest.Tables[i].Name != tableName {
			continue
		}
		rel := filepath.ToSlash(filepath.Join("tables", tableName, "hostile-"+strconv.Itoa(len(manifest.Tables[i].Files))+".jsonl.gz"))
		full := filepath.Join(repo, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		body, err := json.Marshal(row)
		require.NoError(t, err)
		writeGzipJSONLines(t, full, []string{string(body)})
		manifest.Tables[i].Files = append(manifest.Tables[i].Files, rel)
		manifest.Tables[i].Rows++
		return manifest
	}
	t.Fatalf("table %s not found", tableName)
	return manifest
}

func writeShareManifest(t *testing.T, repo string, manifest Manifest) {
	t.Helper()
	body, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(repo, ManifestName), append(body, '\n'), 0o600))
}

func stripFileManifests(manifest Manifest) Manifest {
	for i := range manifest.Tables {
		manifest.Tables[i].FileManifests = nil
	}
	return manifest
}

func snapshotTableText(t *testing.T, repo string, table TableManifest) string {
	t.Helper()
	return snapshotFilesText(t, repo, table.Files)
}

func snapshotFilesText(t *testing.T, repo string, files []string) string {
	t.Helper()
	var out strings.Builder
	for _, rel := range files {
		file, err := os.Open(filepath.Join(repo, filepath.FromSlash(rel)))
		require.NoError(t, err)
		gz, err := gzip.NewReader(file)
		require.NoError(t, err)
		_, err = io.Copy(&out, gz)
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, file.Close())
	}
	return out.String()
}

func seedStore(t *testing.T, path string) *store.Store {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, path)
	require.NoError(t, err)
	now := time.Now().UTC()
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"bio":"Runs launch ops"}`,
	}))
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now.Format(time.RFC3339Nano),
			Content:           "launch checklist ready",
			NormalizedContent: "launch checklist ready",
			RawJSON:           `{}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"m1"}`,
		Options:     store.WriteOptions{AppendEvent: true},
		Mentions: []store.MentionEventRecord{{
			MessageID:  "m1",
			GuildID:    "g1",
			ChannelID:  "c1",
			AuthorID:   "u1",
			TargetType: "role",
			TargetID:   "r1",
			TargetName: "Ops",
			EventAt:    now.Format(time.RFC3339Nano),
		}},
	}}))
	return s
}

func addCachedAttachment(ctx context.Context, s *store.Store, mediaPath, hash string, size int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "launch checklist ready",
			NormalizedContent: "launch checklist ready file.png",
			HasAttachments:    true,
			RawJSON:           `{}`,
		},
		Attachments: []store.AttachmentRecord{{
			AttachmentID:  "a1",
			MessageID:     "m1",
			GuildID:       "g1",
			ChannelID:     "c1",
			AuthorID:      "u1",
			Filename:      "file.png",
			ContentType:   "image/png",
			Size:          size,
			URL:           "https://cdn.example/file.png",
			MediaPath:     mediaPath,
			ContentSHA256: hash,
			ContentSize:   size,
			FetchedAt:     now,
			FetchStatus:   "fetched",
		}},
	}}); err != nil {
		return err
	}
	return nil
}

func addUncachedAttachment(ctx context.Context, s *store.Store) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "launch checklist ready",
			NormalizedContent: "launch checklist ready file.png",
			HasAttachments:    true,
			RawJSON:           `{}`,
		},
		Attachments: []store.AttachmentRecord{{
			AttachmentID: "a1",
			MessageID:    "m1",
			GuildID:      "g1",
			ChannelID:    "c1",
			AuthorID:     "u1",
			Filename:     "file.png",
			ContentType:  "image/png",
			Size:         11,
			URL:          "https://cdn.example/file.png",
		}},
	}})
}

func upsertSnapshotFilterChannel(t *testing.T, ctx context.Context, s *store.Store, channel store.ChannelRecord) {
	t.Helper()
	require.NoError(t, s.UpsertChannel(ctx, channel))
}

func upsertSnapshotFilterMessage(t *testing.T, ctx context.Context, s *store.Store, id, channelID, authorID, content string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                id,
			GuildID:           "g1",
			ChannelID:         channelID,
			ChannelName:       channelID,
			AuthorID:          authorID,
			AuthorName:        authorID,
			MessageType:       0,
			CreatedAt:         now,
			Content:           content,
			NormalizedContent: content,
			RawJSON:           `{}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"` + id + `"}`,
		Options:     store.WriteOptions{AppendEvent: true},
	}}))
}

func seedDirectMessageData(t *testing.T, ctx context.Context, s *store.Store) {
	t.Helper()
	now := time.Date(2026, 4, 24, 15, 33, 17, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: directMessageGuildID, Name: "Discord Direct Messages", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "dm-c1", GuildID: directMessageGuildID, Kind: "dm", Name: "Alice", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "dm1",
			GuildID:           directMessageGuildID,
			ChannelID:         "dm-c1",
			ChannelName:       "Alice",
			AuthorID:          "u2",
			AuthorName:        "Alice",
			MessageType:       0,
			CreatedAt:         now.Format(time.RFC3339Nano),
			Content:           "private dm content",
			NormalizedContent: "private dm content",
			RawJSON:           `{}`,
		},
		EventType:   "wiretap",
		PayloadJSON: `{"id":"dm1"}`,
		Options:     store.WriteOptions{AppendEvent: true},
		Attachments: []store.AttachmentRecord{{
			AttachmentID: "att-dm1",
			MessageID:    "dm1",
			GuildID:      directMessageGuildID,
			ChannelID:    "dm-c1",
			AuthorID:     "u2",
			Filename:     "private.txt",
		}},
		Mentions: []store.MentionEventRecord{{
			MessageID:  "dm1",
			GuildID:    directMessageGuildID,
			ChannelID:  "dm-c1",
			AuthorID:   "u2",
			TargetType: "user",
			TargetID:   "u3",
			TargetName: "Bob",
			EventAt:    now.Format(time.RFC3339Nano),
		}},
	}}))
	require.NoError(t, s.SetSyncState(ctx, "wiretap:last_import", now.Format(time.RFC3339)))
}

func testGitRun(t *testing.T, ctx context.Context, dir string, args ...string) {
	t.Helper()
	_ = testGitOutput(t, ctx, dir, args...)
}

func testGitOutput(t *testing.T, ctx context.Context, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	body, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", body)
	return string(body)
}

func configureGitUser(t *testing.T, repo string) {
	t.Helper()
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", repo, "config", "user.name", "discrawl test").Run())
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", repo, "config", "user.email", "discrawl@example.com").Run())
}

func mustReadManifest(t *testing.T, repo string) Manifest {
	t.Helper()
	manifest, err := ReadManifest(repo)
	require.NoError(t, err)
	return manifest
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	return body
}

func tableEntry(t *testing.T, manifest Manifest, name string) TableManifest {
	t.Helper()
	for _, table := range manifest.Tables {
		if table.Name == name {
			return table
		}
	}
	t.Fatalf("table %s not found", name)
	return TableManifest{}
}

func importPlanTable(t *testing.T, plan snapshot.ImportPlan, name string) snapshot.TableImportPlan {
	t.Helper()
	for _, table := range plan.Tables {
		if table.Table.Name == name {
			return table
		}
	}
	t.Fatalf("plan table %s not found", name)
	return snapshot.TableImportPlan{}
}

func tableNames(manifest Manifest) []string {
	names := make([]string, 0, len(manifest.Tables))
	for _, table := range manifest.Tables {
		names = append(names, table.Name)
	}
	return names
}

func progressPhases(progress []ImportProgress) []string {
	phases := make([]string, 0, len(progress))
	for _, item := range progress {
		phases = append(phases, item.Phase)
	}
	return phases
}

func progressTotalRows(t *testing.T, progress []ImportProgress, phase string) int {
	t.Helper()
	for _, item := range progress {
		if item.Phase == phase {
			return item.TotalRows
		}
	}
	t.Fatalf("progress phase %s not found", phase)
	return 0
}
