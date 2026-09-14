package share

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/openclaw/crawlkit/mirror"
	"github.com/openclaw/crawlkit/snapshot"
	"github.com/openclaw/discrawl/internal/store"
)

type ImportProgress struct {
	Phase     string
	Table     string
	File      string
	FileIndex int
	FileCount int
	Rows      int
	TotalRows int
}

func Import(ctx context.Context, s *store.Store, opts Options) (Manifest, error) {
	manifest, err := ReadManifest(opts.RepoPath)
	if err != nil {
		return Manifest{}, err
	}
	manifest = enrichManifestFromGit(ctx, opts.RepoPath, "HEAD", manifest)
	return importSnapshot(ctx, s, opts, manifest)
}

// Git fingerprints belong to checkpoint metadata. snapshot.Import reads its
// integrity metadata from the original manifest under opts.RepoPath.
func importSnapshot(ctx context.Context, s *store.Store, opts Options, manifest Manifest) (Manifest, error) {
	opts.reportProgress(ImportProgress{Phase: "start", TotalRows: manifestRowCount(manifest)})
	restorePragmas, err := applyImportPragmas(ctx, s.DB())
	if err != nil {
		return Manifest{}, err
	}
	pragmasRestored := false
	existingMedia := map[string]attachmentMediaRecord{}
	defer func() {
		if !pragmasRestored {
			_ = restorePragmas(ctx)
		}
	}()
	if _, err := snapshot.Import(ctx, snapshot.ImportOptions{
		DB:           s.DB(),
		RootDir:      opts.RepoPath,
		DeleteTables: SnapshotTables,
		Progress: func(progress snapshot.ImportProgress) {
			opts.reportProgress(ImportProgress{
				Phase:     progress.Phase,
				Table:     progress.Table,
				File:      progress.File,
				FileIndex: progress.FileIndex,
				FileCount: progress.FileCount,
				Rows:      progress.Rows,
				TotalRows: progress.TotalRows,
			})
		},
		Filter: func(table string, row map[string]any) (bool, error) {
			if isDirectMessageSnapshotRow(table, row) {
				return false, nil
			}
			if err := validateSnapshotRow(table, row); err != nil {
				return false, err
			}
			return true, nil
		},
		BeforeImport: func(ctx context.Context, tx *sql.Tx) error {
			var err error
			existingMedia, err = attachmentMediaByID(ctx, tx)
			if err != nil {
				return err
			}
			for _, table := range []string{"message_fts", "member_fts"} {
				if _, err := tx.ExecContext(ctx, "drop table if exists "+table); err != nil {
					return fmt.Errorf("drop %s: %w", table, err)
				}
			}
			return nil
		},
		DeleteTable: func(ctx context.Context, tx *sql.Tx, table string) error {
			query, args := snapshotDeleteQuery(table)
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf("clear %s: %w", table, err)
			}
			return nil
		},
		AfterImport: func(ctx context.Context, tx *sql.Tx) error {
			if err := repairImportedGuildIDs(ctx, tx); err != nil {
				return err
			}
			if err := preserveImportedAttachmentMedia(ctx, tx, existingMedia); err != nil {
				return err
			}
			if opts.IncludeEmbeddings {
				return importEmbeddings(ctx, tx, opts, manifest.Embeddings)
			}
			return nil
		},
	}); err != nil {
		return Manifest{}, err
	}
	opts.reportProgress(ImportProgress{Phase: "rebuild_fts"})
	if err := s.RebuildSearchIndexes(ctx); err != nil {
		return Manifest{}, err
	}
	if opts.IncludeMedia {
		if _, err := importMedia(ctx, opts, manifest.Media); err != nil {
			return Manifest{}, err
		}
	}
	if err := MarkImported(ctx, s, manifest); err != nil {
		return Manifest{}, err
	}
	if err := restorePragmas(ctx); err != nil {
		return Manifest{}, err
	}
	pragmasRestored = true
	opts.reportProgress(ImportProgress{Phase: "done", TotalRows: manifestRowCount(manifest)})
	return manifest, nil
}

func applyImportPragmas(ctx context.Context, db *sql.DB) (func(context.Context) error, error) {
	// Snapshot imports touch most of the archive. Keep SQLite's crash recovery
	// enabled; journal_mode=off can leave the live DB malformed if the process
	// or host dies mid-import. Keep temporary storage file-backed and bound the
	// page cache so large imports and FTS rebuilds do not exhaust small hosts.
	for _, stmt := range []string{
		`pragma temp_store = file`,
		`pragma cache_size = -32768`,
		`pragma journal_mode = wal`,
		`pragma synchronous = normal`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("apply import pragma %q: %w", stmt, err)
		}
	}
	return func(ctx context.Context) error {
		for _, stmt := range []string{
			`pragma journal_mode = wal`,
			`pragma synchronous = normal`,
		} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("restore import pragma %q: %w", stmt, err)
			}
		}
		return nil
	}, nil
}

// Replace makes the local public archive exactly match the snapshot. It always
// reconciles the database, even when the manifest itself has not changed.
func Replace(ctx context.Context, s *store.Store, opts Options) (Manifest, bool, error) {
	imported, err := Import(ctx, s, opts)
	if err != nil {
		return Manifest{}, false, err
	}
	return imported, true, nil
}

func importMergePlan(
	ctx context.Context,
	s *store.Store,
	opts Options,
	previous Manifest,
	manifest Manifest,
	current snapshot.Manifest,
	plan snapshot.ImportPlan,
) (Manifest, bool, error) {
	if !plan.Changed() {
		if opts.IncludeEmbeddings {
			if err := ImportEmbeddings(ctx, s, opts, manifest); err != nil {
				return Manifest{}, false, err
			}
		}
		copied := 0
		if opts.IncludeMedia {
			var err error
			copied, err = importMedia(ctx, opts, manifest.Media)
			if err != nil {
				return Manifest{}, false, err
			}
		}
		if err := MarkMerged(ctx, s, manifest); err != nil {
			return Manifest{}, false, err
		}
		return manifest, copied > 0, nil
	}
	opts.reportProgress(ImportProgress{Phase: "start", TotalRows: importPlanRowCount(plan)})
	restorePragmas, err := applyImportPragmas(ctx, s.DB())
	if err != nil {
		return Manifest{}, false, err
	}
	pragmasRestored := false
	existingMedia := map[string]attachmentMediaRecord{}
	defer func() {
		if !pragmasRestored {
			_ = restorePragmas(ctx)
		}
	}()
	if _, _, err := snapshot.ImportIncremental(ctx, snapshot.IncrementalImportOptions{
		DB:       s.DB(),
		RootDir:  opts.RepoPath,
		Previous: snapshotManifest(previous),
		Current:  current,
		Plan:     plan,
		Progress: func(progress snapshot.ImportProgress) {
			opts.reportProgress(ImportProgress{
				Phase:     progress.Phase,
				Table:     progress.Table,
				File:      progress.File,
				FileIndex: progress.FileIndex,
				FileCount: progress.FileCount,
				Rows:      progress.Rows,
				TotalRows: progress.TotalRows,
			})
		},
		Filter: func(table string, row map[string]any) (bool, error) {
			if isDirectMessageSnapshotRow(table, row) {
				return false, nil
			}
			if err := validateSnapshotRow(table, row); err != nil {
				return false, err
			}
			return true, nil
		},
		BeforeImport: func(ctx context.Context, tx *sql.Tx) error {
			var err error
			existingMedia, err = attachmentMediaByID(ctx, tx)
			return err
		},
		DeleteTable: func(ctx context.Context, tx *sql.Tx, table string) error {
			query, args := snapshotDeleteQuery(table)
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf("clear %s: %w", table, err)
			}
			return nil
		},
		ImportRow: importMergeSnapshotRow,
		AfterImport: func(ctx context.Context, tx *sql.Tx) error {
			if err := repairImportedGuildIDs(ctx, tx); err != nil {
				return err
			}
			if err := preserveImportedAttachmentMedia(ctx, tx, existingMedia); err != nil {
				return err
			}
			if opts.IncludeEmbeddings {
				return importEmbeddings(ctx, tx, opts, manifest.Embeddings)
			}
			return nil
		},
	}); err != nil {
		return Manifest{}, false, err
	}
	rebuildMessageFTS, rebuildMemberFTS := mergePlanSearchRebuilds(plan)
	if rebuildMessageFTS {
		opts.reportProgress(ImportProgress{Phase: "rebuild_fts"})
		if err := s.RebuildMessageSearchIndex(ctx); err != nil {
			return Manifest{}, false, err
		}
	}
	if rebuildMemberFTS {
		opts.reportProgress(ImportProgress{Phase: "rebuild_member_fts"})
		if err := s.RebuildMemberSearchIndex(ctx); err != nil {
			return Manifest{}, false, err
		}
	}
	if opts.IncludeMedia {
		if _, err := importMedia(ctx, opts, manifest.Media); err != nil {
			return Manifest{}, false, err
		}
	}
	if err := MarkMerged(ctx, s, manifest); err != nil {
		return Manifest{}, false, err
	}
	if err := restorePragmas(ctx); err != nil {
		return Manifest{}, false, err
	}
	pragmasRestored = true
	opts.reportProgress(ImportProgress{Phase: "done", TotalRows: importPlanRowCount(plan)})
	return manifest, true, nil
}

func (opts Options) reportProgress(progress ImportProgress) {
	if opts.Progress != nil {
		opts.Progress(progress)
	}
}

func manifestRowCount(manifest Manifest) int {
	total := 0
	for _, table := range manifest.Tables {
		total += table.Rows
	}
	for _, embeddings := range manifest.Embeddings {
		total += embeddings.Rows
	}
	if manifest.Media != nil {
		total += manifest.Media.Attachments
	}
	return total
}

func importPlanRowCount(plan snapshot.ImportPlan) int {
	if plan.Full {
		return 0
	}
	total := 0
	for _, tablePlan := range plan.Tables {
		switch tablePlan.Mode {
		case snapshot.TableImportReplace:
			total += tablePlan.Table.Rows
		case snapshot.TableImportFiles:
			for _, file := range tablePlan.Files {
				total += file.Rows
			}
		}
	}
	return total
}

// ImportAt restores a snapshot from a Git ref without changing the share checkout.
func ImportAt(ctx context.Context, s *store.Store, opts Options, ref string) (Manifest, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return Import(ctx, s, opts)
	}
	if err := mirror.Fetch(ctx, mirrorOptions(opts)); err != nil {
		return Manifest{}, err
	}
	manifestBody, commit, err := mirror.ReadFileAt(ctx, mirrorOptions(opts), ref, ManifestName)
	if err != nil {
		return Manifest{}, err
	}
	manifest, err := parseManifest(manifestBody)
	if err != nil {
		return Manifest{}, err
	}
	manifest = enrichManifestFromGit(ctx, opts.RepoPath, commit, manifest)
	tempDir, err := os.MkdirTemp("", "discrawl-share-ref-*")
	if err != nil {
		return Manifest{}, fmt.Errorf("create historical share directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()
	if err := os.WriteFile(filepath.Join(tempDir, ManifestName), manifestBody, 0o600); err != nil {
		return Manifest{}, fmt.Errorf("write historical manifest: %w", err)
	}
	for _, table := range manifest.Tables {
		for _, file := range tableSnapshotFiles(table) {
			if err := materializeRefFile(ctx, mirrorOptions(opts), commit, file, tempDir); err != nil {
				return Manifest{}, err
			}
		}
	}
	for _, embeddings := range manifest.Embeddings {
		if !opts.IncludeEmbeddings {
			break
		}
		for _, file := range embeddings.Files {
			if err := materializeRefFile(ctx, mirrorOptions(opts), commit, file, tempDir); err != nil {
				return Manifest{}, err
			}
		}
	}
	if opts.IncludeMedia && manifest.Media != nil {
		for _, file := range manifest.Media.Files {
			if err := materializeRefFile(ctx, mirrorOptions(opts), commit, file.Path, tempDir); err != nil {
				return Manifest{}, err
			}
		}
	}
	historicalOpts := opts
	historicalOpts.RepoPath = tempDir
	historicalOpts.Remote = ""
	historicalOpts.Tag = ""
	return importSnapshot(ctx, s, historicalOpts, manifest)
}

func tableSnapshotFiles(table TableManifest) []string {
	if len(table.Files) > 0 {
		return table.Files
	}
	if strings.TrimSpace(table.File) != "" {
		return []string{table.File}
	}
	return nil
}

func materializeRefFile(ctx context.Context, opts mirror.Options, ref, filePath, targetRoot string) error {
	clean := path.Clean(filepath.ToSlash(strings.TrimSpace(filePath)))
	native := filepath.FromSlash(clean)
	if clean == "." || clean == ".." || path.IsAbs(clean) || filepath.IsAbs(native) || filepath.VolumeName(native) != "" || strings.HasPrefix(clean, "../") || strings.ContainsRune(clean, '\x00') {
		return fmt.Errorf("invalid historical share path %q", filePath)
	}
	body, _, err := mirror.ReadFileAt(ctx, opts, ref, clean)
	if err != nil {
		return err
	}
	target := filepath.Join(targetRoot, native)
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return fmt.Errorf("create historical share directory: %w", err)
	}
	if err := os.WriteFile(target, body, 0o600); err != nil {
		return fmt.Errorf("write historical share file %s: %w", clean, err)
	}
	return nil
}
