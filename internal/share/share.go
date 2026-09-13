package share

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/openclaw/crawlkit/mirror"
	"github.com/openclaw/crawlkit/snapshot"
	"github.com/openclaw/discrawl/internal/store"
)

const (
	ManifestName                = "manifest.json"
	LastImportSyncScope         = "share:last_import_at"
	LastImportManifestSyncScope = "share:last_import_manifest_generated_at"
	LastImportManifestJSONScope = "share:last_import_manifest_json"
	directMessageGuildID        = "@me"
)

var (
	ErrNoManifest      = snapshot.ErrNoManifest
	errUnsafeMediaPath = errors.New("unsafe media path")
)

var SnapshotTables = []string{
	"guilds",
	"channels",
	"members",
	"messages",
	"message_events",
	"message_attachments",
	"mention_events",
	"sync_state",
}

type Options struct {
	RepoPath              string
	CacheDir              string
	Remote                string
	Branch                string
	Tag                   string
	Filter                FilterOptions
	IncludeMedia          bool
	IncludeEmbeddings     bool
	EmbeddingProvider     string
	EmbeddingModel        string
	EmbeddingInputVersion string
	Producer              *PublicationProducer
	// ReadmePath is a repo-relative report explicitly generated or removed by this publication.
	ReadmePath string
	Progress   func(ImportProgress)
}

func Export(ctx context.Context, s *store.Store, opts Options) (Manifest, error) {
	return exportPublication(ctx, s, opts, nil)
}

func exportPublication(ctx context.Context, s *store.Store, opts Options, before publicationStep) (Manifest, error) {
	if err := opts.Producer.validate(); err != nil {
		return Manifest{}, err
	}
	if err := validateMediaRoots(opts); err != nil {
		return Manifest{}, err
	}
	if strings.TrimSpace(opts.Remote) != "" {
		if err := mirror.EnsureRemote(ctx, mirrorOptions(opts)); err != nil {
			return Manifest{}, err
		}
	}
	if err := mirror.SyncForWrite(ctx, mirrorOptions(opts)); err != nil {
		return Manifest{}, err
	}
	previous, err := previousPublicationPaths(ctx, opts.RepoPath, false)
	if err != nil {
		return Manifest{}, err
	}
	if err := validateProducerDestination(opts.RepoPath, previous, opts.Producer); err != nil {
		return Manifest{}, err
	}
	stage, err := os.MkdirTemp("", "discrawl-export-*")
	if err != nil {
		return Manifest{}, err
	}
	defer func() { _ = os.RemoveAll(stage) }()
	destination := opts.RepoPath
	opts.RepoPath = stage
	filter, err := newSnapshotFilter(ctx, s.DB(), opts.Filter)
	if err != nil {
		return Manifest{}, err
	}
	base, err := snapshot.Export(ctx, snapshot.ExportOptions{
		DB:            s.DB(),
		RootDir:       opts.RepoPath,
		Tables:        SnapshotTables,
		MaxShardBytes: maxShardBytes,
		Filter: func(table string, row map[string]any) (bool, error) {
			if !filter.allow(table, row) {
				return false, nil
			}
			if table == "message_attachments" {
				if err := validateAttachmentSnapshotText(row); err != nil {
					return false, err
				}
			}
			if filter.active && table == "guilds" {
				if err := projectPublishedGuild(row); err != nil {
					return false, err
				}
			}
			return true, nil
		},
	})
	if err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{
		Version:     base.Version,
		GeneratedAt: base.GeneratedAt,
		Tables:      base.Tables,
		Files:       base.Files,
	}
	if err := normalizePublicationTables(stage, &manifest); err != nil {
		return Manifest{}, err
	}
	if opts.IncludeEmbeddings {
		entry, err := exportEmbeddings(ctx, s.DB(), opts)
		if err != nil {
			return Manifest{}, err
		}
		manifest.Embeddings = []EmbeddingManifest{entry}
	}
	if opts.IncludeMedia {
		entry, err := exportMedia(ctx, s.DB(), opts, filter)
		if err != nil {
			return Manifest{}, err
		}
		if entry != nil {
			manifest.Media = entry
		}
	}
	if opts.Producer != nil {
		if manifest.Files == nil {
			manifest.Files = map[string]string{}
		}
		manifest.Files["producer"] = producerName
	}
	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return Manifest{}, err
	}
	body = append(body, '\n')
	if err := os.WriteFile(filepath.Join(opts.RepoPath, ManifestName), body, 0o600); err != nil {
		return Manifest{}, fmt.Errorf("write manifest: %w", err)
	}
	if opts.Producer != nil {
		receipt, err := encodePublicationReceipt(body, *opts.Producer)
		if err != nil {
			return Manifest{}, err
		}
		if err := os.WriteFile(filepath.Join(stage, producerName), receipt, 0o600); err != nil {
			return Manifest{}, err
		}
	}
	installed, err := installPublication(ctx, destination, stage, previous, manifest, before)
	if installed {
		return manifest, err
	}
	if err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateAttachmentSnapshotText(row map[string]any) error {
	// Only fixed public schema names may enter this contents-free diagnostic.
	// The callback cannot distinguish original TEXT from scanned BLOB values.
	for _, column := range [...]string{
		"attachment_id", "message_id", "guild_id", "channel_id", "author_id",
		"filename", "content_type", "size", "url", "proxy_url", "text_content",
		"media_path", "content_sha256", "content_size", "fetched_at",
		"fetch_status", "fetch_error", "updated_at",
	} {
		if text, ok := row[column].(string); ok && !utf8.ValidString(text) {
			return fmt.Errorf("message_attachments.%s contains invalid UTF-8", column)
		}
	}
	return nil
}
