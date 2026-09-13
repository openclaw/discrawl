package share

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/mirror"
	"github.com/openclaw/crawlkit/snapshot"
	"github.com/openclaw/discrawl/internal/store"
)

type Manifest struct {
	Version     int                 `json:"version"`
	GeneratedAt time.Time           `json:"generated_at"`
	Tables      []TableManifest     `json:"tables"`
	Media       *MediaManifest      `json:"media,omitempty"`
	Embeddings  []EmbeddingManifest `json:"embeddings,omitempty"`
	Files       map[string]string   `json:"files,omitempty"`
}

type TableManifest = snapshot.TableManifest

type EmbeddingManifest struct {
	Provider     string   `json:"provider"`
	Model        string   `json:"model"`
	InputVersion string   `json:"input_version"`
	Files        []string `json:"files"`
	Columns      []string `json:"columns"`
	Rows         int      `json:"rows"`
}

type MediaManifest struct {
	Attachments int                     `json:"attachments"`
	Files       []snapshot.FileManifest `json:"files"`
	Bytes       int64                   `json:"bytes"`
}

func ManifestAlreadyImported(ctx context.Context, s *store.Store, manifest Manifest) bool {
	if manifest.GeneratedAt.IsZero() {
		return false
	}
	last, err := s.GetSyncState(ctx, LastImportManifestSyncScope)
	if err != nil || strings.TrimSpace(last) == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339Nano, last)
	if err != nil {
		return false
	}
	return t.Equal(manifest.GeneratedAt)
}

func MarkImported(ctx context.Context, s *store.Store, manifest Manifest) error {
	if err := s.SetSyncState(ctx, LastImportSyncScope, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if manifest.GeneratedAt.IsZero() {
		return MarkMerged(ctx, s, manifest)
	}
	if err := s.SetSyncState(ctx, LastImportManifestSyncScope, manifest.GeneratedAt.Format(time.RFC3339Nano)); err != nil {
		return err
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal imported manifest state: %w", err)
	}
	if err := s.SetSyncState(ctx, LastImportManifestJSONScope, string(body)); err != nil {
		return err
	}
	return MarkMerged(ctx, s, manifest)
}

func PreviousImportedManifest(ctx context.Context, s *store.Store, opts Options) (Manifest, bool) {
	body, err := s.GetSyncState(ctx, LastImportManifestJSONScope)
	if err == nil && strings.TrimSpace(body) != "" {
		var manifest Manifest
		if json.Unmarshal([]byte(body), &manifest) == nil && !manifest.GeneratedAt.IsZero() {
			return manifest, true
		}
	}
	last, err := s.GetSyncState(ctx, LastImportManifestSyncScope)
	if err != nil || strings.TrimSpace(last) == "" {
		return Manifest{}, false
	}
	generatedAt, err := time.Parse(time.RFC3339Nano, last)
	if err != nil {
		return Manifest{}, false
	}
	manifest, err := manifestFromGitHistory(ctx, opts.RepoPath, generatedAt)
	if err != nil {
		return Manifest{}, false
	}
	return manifest, true
}

func manifestFromGitHistory(ctx context.Context, repoPath string, generatedAt time.Time) (Manifest, error) {
	opts := mirror.Options{RepoPath: repoPath}
	commits, err := mirror.CommitsChanging(ctx, opts, ManifestName, 500)
	if err != nil {
		return Manifest{}, err
	}
	for _, hash := range commits {
		body, _, err := mirror.ReadFileAt(ctx, opts, hash, ManifestName)
		if err != nil {
			continue
		}
		var manifest Manifest
		if err := json.Unmarshal(body, &manifest); err != nil {
			continue
		}
		if manifest.GeneratedAt.Equal(generatedAt) {
			return enrichManifestFromGit(ctx, repoPath, hash, manifest), nil
		}
	}
	return Manifest{}, fmt.Errorf("imported manifest %s not found in git history", generatedAt.Format(time.RFC3339Nano))
}

func enrichManifestFromGit(ctx context.Context, repoPath, rev string, manifest Manifest) Manifest {
	if strings.TrimSpace(repoPath) == "" || manifestHasFileManifests(manifest) {
		return manifest
	}
	files, err := gitTreeFiles(ctx, repoPath, rev)
	if err != nil {
		return manifest
	}
	// Enrichment must not change the authoritative on-disk manifest's metadata.
	manifest.Tables = slices.Clone(manifest.Tables)
	for i := range manifest.Tables {
		table := &manifest.Tables[i]
		if len(table.FileManifests) > 0 {
			continue
		}
		paths := table.Files
		if len(paths) == 0 && strings.TrimSpace(table.File) != "" {
			paths = []string{table.File}
		}
		table.FileManifests = make([]snapshot.FileManifest, 0, len(paths))
		for _, path := range paths {
			info, ok := files[path]
			if !ok {
				table.FileManifests = nil
				break
			}
			rows := 0
			if len(paths) == 1 {
				rows = table.Rows
			}
			table.FileManifests = append(table.FileManifests, snapshot.FileManifest{
				Path:   path,
				Rows:   rows,
				Size:   info.Size,
				SHA256: "git:" + info.Object,
			})
		}
	}
	return manifest
}

func manifestHasFileManifests(manifest Manifest) bool {
	for _, table := range manifest.Tables {
		if (len(table.Files) > 0 || strings.TrimSpace(table.File) != "") && len(table.FileManifests) == 0 {
			return false
		}
	}
	return true
}

func gitTreeFiles(ctx context.Context, repoPath, rev string) (map[string]mirror.TreeFile, error) {
	if strings.TrimSpace(rev) == "" {
		rev = "HEAD"
	}
	entries, err := mirror.ListTreeFiles(ctx, mirror.Options{RepoPath: repoPath}, rev, "tables")
	if err != nil {
		return nil, err
	}
	files := make(map[string]mirror.TreeFile, len(entries))
	for _, entry := range entries {
		files[entry.Path] = entry
	}
	return files, nil
}

func snapshotManifest(manifest Manifest) snapshot.Manifest {
	return snapshot.Manifest{
		Version:     manifest.Version,
		GeneratedAt: manifest.GeneratedAt,
		Tables:      manifest.Tables,
		Files:       manifest.Files,
	}
}

func ReadManifest(repoPath string) (Manifest, error) {
	data, err := os.ReadFile(filepath.Join(repoPath, ManifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, ErrNoManifest
		}
		return Manifest{}, fmt.Errorf("read share manifest: %w", err)
	}
	return parseManifest(data)
}

func parseManifest(data []byte) (Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("parse share manifest: %w", err)
	}
	if manifest.Version != 1 {
		return Manifest{}, fmt.Errorf("unsupported share manifest version %d", manifest.Version)
	}
	return manifest, nil
}

func NeedsImport(ctx context.Context, s *store.Store, staleAfter time.Duration) bool {
	if staleAfter <= 0 {
		staleAfter = 15 * time.Minute
	}
	last, err := s.GetSyncState(ctx, LastCheckSyncScope)
	if err != nil || strings.TrimSpace(last) == "" {
		last, err = s.GetSyncState(ctx, LastImportSyncScope)
	}
	if err != nil || strings.TrimSpace(last) == "" {
		return true
	}
	t, err := time.Parse(time.RFC3339Nano, last)
	if err != nil {
		return true
	}
	return time.Since(t) >= staleAfter
}
