package share

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/mirror"
	"github.com/openclaw/crawlkit/snapshot"
	"github.com/openclaw/discrawl/internal/media"
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

const shardFlushRows = 1024

var maxShardBytes int64 = 40 * 1024 * 1024

// The share manifest stores compressed media size, not raw size. Keep gzip
// restore/hash paths bounded so a malformed snapshot cannot expand forever.
var maxSharedMediaDecompressedBytes int64 = 1 << 30

// Embedding snapshots are gzip-compressed JSONL. Bound decompression so a
// malformed snapshot cannot force unbounded JSON decoder reads.
var maxSharedEmbeddingDecompressedBytes int64 = 1 << 30

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
	Progress              func(ImportProgress)
}

type FilterOptions struct {
	PublicOnly        bool
	IncludeChannelIDs []string
	ExcludeChannelIDs []string
}

func (o FilterOptions) Active() bool {
	return o.PublicOnly || len(stringSet(o.IncludeChannelIDs)) > 0 || len(stringSet(o.ExcludeChannelIDs)) > 0
}

type PublishScopeCount struct {
	Candidate int `json:"candidate"`
	Allowed   int `json:"allowed"`
	Excluded  int `json:"excluded"`
}

type PublishScopeGuild struct {
	GuildID           string `json:"guild_id"`
	GuildName         string `json:"guild_name,omitempty"`
	SourceHint        string `json:"source_hint"`
	MetadataReady     bool   `json:"metadata_ready"`
	CandidateChannels int    `json:"candidate_channels"`
	AllowedChannels   int    `json:"allowed_channels"`
	CandidateMessages int    `json:"candidate_messages"`
	AllowedMessages   int    `json:"allowed_messages"`
}

type PublishScopePreflight struct {
	Ready             bool                `json:"ready"`
	PublicOnly        bool                `json:"public_only"`
	IncludeChannelIDs []string            `json:"include_channel_ids,omitempty"`
	ExcludeChannelIDs []string            `json:"exclude_channel_ids,omitempty"`
	Guilds            []PublishScopeGuild `json:"guilds"`
	Channels          PublishScopeCount   `json:"channels"`
	Messages          PublishScopeCount   `json:"messages"`
	Empty             bool                `json:"empty"`
	EmptyReason       string              `json:"empty_reason,omitempty"`
	Warnings          []string            `json:"warnings,omitempty"`
	RepairCommand     string              `json:"repair_command,omitempty"`
}

// PreflightPublishScope evaluates the same filters used by Export without
// touching the snapshot repository or any configured remote.
func PreflightPublishScope(ctx context.Context, s *store.Store, opts FilterOptions) (PublishScopePreflight, error) {
	candidateOpts := FilterOptions{
		IncludeChannelIDs: opts.IncludeChannelIDs,
		ExcludeChannelIDs: opts.ExcludeChannelIDs,
	}
	candidateFilter, err := newSnapshotFilter(ctx, s.DB(), candidateOpts)
	if err != nil {
		return PublishScopePreflight{}, err
	}
	selectedFilter, err := newSnapshotFilter(ctx, s.DB(), opts)
	if err != nil {
		return PublishScopePreflight{}, err
	}

	report := PublishScopePreflight{
		Ready:             true,
		PublicOnly:        opts.PublicOnly,
		IncludeChannelIDs: slices.Sorted(maps.Keys(stringSet(opts.IncludeChannelIDs))),
		ExcludeChannelIDs: slices.Sorted(maps.Keys(stringSet(opts.ExcludeChannelIDs))),
		Guilds:            []PublishScopeGuild{},
	}
	guilds, err := loadPublishScopeGuilds(ctx, s.DB())
	if err != nil {
		return PublishScopePreflight{}, err
	}
	channelGuilds, err := countPublishScopeChannels(ctx, s.DB(), candidateFilter, selectedFilter, guilds, &report)
	if err != nil {
		return PublishScopePreflight{}, err
	}
	if err := countPublishScopeMessages(ctx, s.DB(), candidateFilter, selectedFilter, guilds, channelGuilds, &report); err != nil {
		return PublishScopePreflight{}, err
	}

	report.Channels.Excluded = report.Channels.Candidate - report.Channels.Allowed
	report.Messages.Excluded = report.Messages.Candidate - report.Messages.Allowed
	for _, guild := range guilds {
		if opts.PublicOnly && (guild.CandidateChannels > 0 || guild.CandidateMessages > 0) && !guild.MetadataReady {
			report.Ready = false
			report.Warnings = append(report.Warnings,
				fmt.Sprintf("guild %s lacks usable @everyone role metadata; public-only selection fails closed", guild.GuildID))
		}
		report.Guilds = append(report.Guilds, *guild)
	}
	sort.Slice(report.Guilds, func(i, j int) bool { return report.Guilds[i].GuildID < report.Guilds[j].GuildID })
	sort.Strings(report.Warnings)
	if !report.Ready {
		report.RepairCommand = "discrawl sync --source discord"
	}
	report.Empty = report.Messages.Allowed == 0
	if report.Empty {
		switch {
		case report.Messages.Candidate == 0:
			report.EmptyReason = "no_matching_messages"
		case !report.Ready:
			report.EmptyReason = "metadata_incomplete"
		default:
			report.EmptyReason = "filters_match_no_publishable_messages"
		}
	}
	return report, nil
}

func loadPublishScopeGuilds(ctx context.Context, db *sql.DB) (map[string]*PublishScopeGuild, error) {
	rows, err := db.QueryContext(ctx, `select id, name, raw_json from guilds where id != ? order by id`, directMessageGuildID)
	if err != nil {
		return nil, fmt.Errorf("query guild publish preflight: %w", err)
	}
	defer func() { _ = rows.Close() }()
	guilds := map[string]*PublishScopeGuild{}
	for rows.Next() {
		var id, name, raw string
		if err := rows.Scan(&id, &name, &raw); err != nil {
			return nil, fmt.Errorf("scan guild publish preflight: %w", err)
		}
		_, metadataReady := everyoneGuildPermissions(raw, id)
		guilds[id] = &PublishScopeGuild{
			GuildID:       id,
			GuildName:     name,
			SourceHint:    publishSourceHint(raw, metadataReady),
			MetadataReady: metadataReady,
		}
	}
	return guilds, rows.Err()
}

func countPublishScopeChannels(
	ctx context.Context,
	db *sql.DB,
	candidateFilter, selectedFilter *snapshotFilter,
	guilds map[string]*PublishScopeGuild,
	report *PublishScopePreflight,
) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, `select id, guild_id from channels where guild_id != ?`, directMessageGuildID)
	if err != nil {
		return nil, fmt.Errorf("query channel publish preflight: %w", err)
	}
	defer func() { _ = rows.Close() }()
	channelGuilds := map[string]string{}
	for rows.Next() {
		var channelID, guildID string
		if err := rows.Scan(&channelID, &guildID); err != nil {
			return nil, fmt.Errorf("scan channel publish preflight: %w", err)
		}
		channelGuilds[channelID] = guildID
		if !candidateFilter.allowChannelID(channelID) {
			continue
		}
		report.Channels.Candidate++
		guild := ensurePublishScopeGuild(guilds, guildID)
		guild.CandidateChannels++
		if selectedFilter.allowChannelID(channelID) {
			report.Channels.Allowed++
			guild.AllowedChannels++
		}
	}
	return channelGuilds, rows.Err()
}

func countPublishScopeMessages(
	ctx context.Context,
	db *sql.DB,
	candidateFilter, selectedFilter *snapshotFilter,
	guilds map[string]*PublishScopeGuild,
	channelGuilds map[string]string,
	report *PublishScopePreflight,
) error {
	rows, err := db.QueryContext(ctx, `select id, guild_id, channel_id from messages where guild_id != ?`, directMessageGuildID)
	if err != nil {
		return fmt.Errorf("query message publish preflight: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var messageID, messageGuildID, channelID string
		if err := rows.Scan(&messageID, &messageGuildID, &channelID); err != nil {
			return fmt.Errorf("scan message publish preflight: %w", err)
		}
		if !candidateFilter.allowChannelID(channelID) {
			continue
		}
		guildID := messageGuildID
		if channelGuildID, ok := channelGuilds[channelID]; ok {
			guildID = channelGuildID
		}
		report.Messages.Candidate++
		guild := ensurePublishScopeGuild(guilds, guildID)
		guild.CandidateMessages++
		if selectedFilter.allowedMessageIDs[messageID] || selectedFilter.allowChannelID(channelID) {
			report.Messages.Allowed++
			guild.AllowedMessages++
		}
	}
	return rows.Err()
}

func ensurePublishScopeGuild(guilds map[string]*PublishScopeGuild, guildID string) *PublishScopeGuild {
	if guild := guilds[guildID]; guild != nil {
		return guild
	}
	guild := &PublishScopeGuild{GuildID: guildID, SourceHint: "unknown"}
	guilds[guildID] = guild
	return guild
}

func publishSourceHint(raw string, metadataReady bool) string {
	var payload struct {
		Source string `json:"source"`
	}
	if decodeJSONUseNumber(raw, &payload) == nil && strings.EqualFold(strings.TrimSpace(payload.Source), "discord_desktop") {
		return "discord_desktop"
	}
	if metadataReady {
		return "discord_metadata"
	}
	return "unknown"
}

type ImportProgress struct {
	Phase     string
	Table     string
	File      string
	FileIndex int
	FileCount int
	Rows      int
	TotalRows int
}

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

func EnsureRepo(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.RepoPath) == "" {
		return errors.New("share repo path is empty")
	}
	return mirror.EnsureRepo(ctx, mirrorOptions(opts))
}

func Pull(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.Remote) == "" && strings.TrimSpace(opts.RepoPath) == "" {
		return nil
	}
	if strings.TrimSpace(opts.Remote) == "" {
		return EnsureRepo(ctx, opts)
	}
	if err := mirror.EnsureRemote(ctx, mirrorOptions(opts)); err != nil {
		return err
	}
	pullOpts := mirrorOptions(opts)
	pullOpts.Remote = ""
	return mirror.PullCurrent(ctx, pullOpts)
}

func Commit(ctx context.Context, opts Options, message string) (bool, error) {
	return mirror.Commit(ctx, mirrorOptions(opts), message)
}

func Push(ctx context.Context, opts Options) error {
	var err error
	if strings.TrimSpace(opts.Tag) == "" {
		err = mirror.Push(ctx, mirrorOptions(opts))
	} else {
		err = mirror.PushSnapshot(ctx, mirrorOptions(opts), opts.Tag)
	}
	if err != nil {
		branch := opts.Branch
		if strings.TrimSpace(branch) == "" {
			branch = "main"
		}
		return fmt.Errorf("git push -u origin %s: %w", branch, err)
	}
	return nil
}

func ValidateTag(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.Tag) == "" {
		return nil
	}
	if strings.TrimSpace(opts.Remote) != "" {
		if err := mirror.EnsureRemote(ctx, mirrorOptions(opts)); err != nil {
			return err
		}
	} else if err := mirror.EnsureRepo(ctx, mirrorOptions(opts)); err != nil {
		return err
	}
	if err := mirror.ValidateTag(ctx, mirrorOptions(opts), opts.Tag); err != nil {
		return err
	}
	if err := mirror.SyncForWrite(ctx, mirrorOptions(opts)); err != nil {
		return err
	}
	return nil
}

func CreateImmutableTag(ctx context.Context, opts Options) (string, error) {
	return mirror.CreateImmutableTag(ctx, mirrorOptions(opts), opts.Tag)
}

func Export(ctx context.Context, s *store.Store, opts Options) (Manifest, error) {
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
			return filter.allow(table, row), nil
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
	} else {
		if err := os.RemoveAll(filepath.Join(opts.RepoPath, "media")); err != nil {
			return Manifest{}, fmt.Errorf("reset media dir: %w", err)
		}
	}
	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return Manifest{}, err
	}
	body = append(body, '\n')
	if err := os.WriteFile(filepath.Join(opts.RepoPath, ManifestName), body, 0o600); err != nil {
		return Manifest{}, fmt.Errorf("write manifest: %w", err)
	}
	return manifest, nil
}

func Import(ctx context.Context, s *store.Store, opts Options) (Manifest, error) {
	manifest, err := ReadManifest(opts.RepoPath)
	if err != nil {
		return Manifest{}, err
	}
	manifest = enrichManifestFromGit(ctx, opts.RepoPath, "HEAD", manifest)
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
		Current:  snapshotManifest(manifest),
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

func mirrorOptions(opts Options) mirror.Options {
	return mirror.Options{RepoPath: opts.RepoPath, Remote: opts.Remote, Branch: opts.Branch, DirMode: 0o750}
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
	manifestBody, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return Manifest{}, fmt.Errorf("marshal historical manifest: %w", err)
	}
	manifestBody = append(manifestBody, '\n')
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
	return Import(ctx, s, historicalOpts)
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
	// A publish rewrites media from the local cache. Clearing the tree here is
	// the forward migration from legacy raw media files to gzip-only snapshots.
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

func regularMediaFile(root, path, label string) (os.FileInfo, error) {
	return regularFileInRoot(root, path, label, "media")
}

func regularFileInRoot(root, path, label, kind string) (os.FileInfo, error) {
	root = filepath.Clean(root)
	path = filepath.Clean(path)
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == "" || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return nil, fmt.Errorf("%w: %s %s escapes %s root", errUnsafeMediaPath, kind, label, kind)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("%w: %s root for %s is not a directory", errUnsafeMediaPath, kind, label)
	}
	current := root
	parts := strings.Split(rel, string(filepath.Separator))
	for i, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, fmt.Errorf("%w: invalid %s path %q", errUnsafeMediaPath, kind, label)
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if i == len(parts)-1 {
				return nil, fmt.Errorf("%w: %s %s is not a regular file", errUnsafeMediaPath, kind, label)
			}
			return nil, fmt.Errorf("%w: %s %s contains symlinked path component", errUnsafeMediaPath, kind, label)
		}
		if i < len(parts)-1 {
			if !info.IsDir() {
				return nil, fmt.Errorf("%w: %s %s parent is not a directory", errUnsafeMediaPath, kind, label)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: %s %s is not a regular file", errUnsafeMediaPath, kind, label)
		}
		return info, nil
	}
	return nil, fmt.Errorf("%w: invalid %s path %q", errUnsafeMediaPath, kind, label)
}

func copyFile(target, source string) error {
	src, err := os.Open(source) // #nosec G304 -- source is constrained by media path helpers.
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	return writeAtomicFile(target, func(tmp *os.File) error {
		_, err := io.Copy(tmp, src)
		return err
	})
}

func writeAtomicFile(target string, write func(*os.File) error) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".copy-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if err := write(tmp); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, target); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
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

func copyWithLimit(dst io.Writer, src io.Reader, limit int64) error {
	if limit <= 0 {
		return errors.New("media decompression limit must be positive")
	}
	n, err := io.Copy(dst, io.LimitReader(src, limit+1))
	if err != nil {
		return err
	}
	if n > limit {
		return fmt.Errorf("media decompressed size exceeds %d bytes", limit)
	}
	return nil
}

type limitedReader struct {
	r     io.Reader
	limit int64
	n     int64
	label string
}

func (r *limitedReader) Read(p []byte) (int, error) {
	if r.limit <= 0 {
		return 0, fmt.Errorf("%s decompression limit must be positive", r.label)
	}
	remaining := r.limit + 1 - r.n
	if remaining <= 0 {
		return 0, fmt.Errorf("%s decompressed size exceeds %d bytes", r.label, r.limit)
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.r.Read(p)
	r.n += int64(n)
	if r.n > r.limit {
		return n, fmt.Errorf("%s decompressed size exceeds %d bytes", r.label, r.limit)
	}
	return n, err
}

func sameFileHash(path, hash string) bool {
	current, err := fileSHA256(path)
	return err == nil && current == hash
}

func importTable(ctx context.Context, tx *sql.Tx, opts Options, table TableManifest) error {
	files := table.Files
	if len(files) == 0 && strings.TrimSpace(table.File) != "" {
		files = []string{table.File}
	}
	if len(files) == 0 {
		return fmt.Errorf("manifest table %s has no files", table.Name)
	}
	columns := importColumns(table)
	stmt, err := tx.PrepareContext(ctx, insertSQL(table.Name, columns))
	if err != nil {
		return fmt.Errorf("prepare import %s: %w", table.Name, err)
	}
	defer func() { _ = stmt.Close() }()
	for i, rel := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		opts.reportProgress(ImportProgress{Phase: "file_start", Table: table.Name, File: rel, FileIndex: i + 1, FileCount: len(files), TotalRows: table.Rows})
		rows, err := importTableFile(ctx, stmt, opts.RepoPath, table, columns, rel)
		if err != nil {
			return err
		}
		opts.reportProgress(ImportProgress{Phase: "file_done", Table: table.Name, File: rel, FileIndex: i + 1, FileCount: len(files), Rows: rows, TotalRows: table.Rows})
	}
	return nil
}

func importTableFile(ctx context.Context, stmt *sql.Stmt, repoPath string, table TableManifest, columns []string, rel string) (int, error) {
	path := filepath.Join(repoPath, filepath.FromSlash(rel))
	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", rel, err)
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return 0, fmt.Errorf("read gzip %s: %w", rel, err)
	}
	defer func() { _ = gz.Close() }()
	dec := json.NewDecoder(gz)
	dec.UseNumber()
	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		row := map[string]any{}
		err := dec.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, fmt.Errorf("decode %s: %w", rel, err)
		}
		if isDirectMessageSnapshotRow(table.Name, row) {
			continue
		}
		if err := validateSnapshotRow(table.Name, row); err != nil {
			return count, fmt.Errorf("validate %s: %w", rel, err)
		}
		values := make([]any, len(columns))
		for i, column := range columns {
			values[i] = importValue(row[column])
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return count, fmt.Errorf("insert %s: %w", table.Name, err)
		}
		count++
	}
	return count, nil
}

func validateSnapshotRow(table string, row map[string]any) error {
	if table != "messages" && table != "guilds" && table != "members" {
		return nil
	}
	if table == "guilds" || table == "members" {
		for _, column := range []string{"deleted_at", "deletion_source", "deletion_reason"} {
			if _, ok := row[column]; !ok {
				row[column] = nil
			}
		}
		if err := validateSnapshotRevision(table, row); err != nil {
			return err
		}
	}
	raw, ok := row["deleted_at"]
	if !ok || raw == nil {
		if table == "guilds" || table == "members" {
			return validateLiveSnapshotTombstoneMetadata(table, row)
		}
		return nil
	}
	value, ok := raw.(string)
	if !ok {
		return errors.New("messages.deleted_at must be a string or null")
	}
	value = strings.TrimSpace(value)
	if value == "" {
		row["deleted_at"] = nil
		if table == "guilds" || table == "members" {
			return validateLiveSnapshotTombstoneMetadata(table, row)
		}
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return fmt.Errorf("%s.deleted_at must be RFC3339: %w", table, err)
	}
	row["deleted_at"] = parsed.UTC().Format(time.RFC3339Nano)
	if table == "guilds" || table == "members" {
		for _, column := range []string{"deletion_source", "deletion_reason"} {
			value, ok := row[column].(string)
			if !ok || strings.TrimSpace(value) == "" {
				return fmt.Errorf("%s.%s must be a non-empty string for a tombstone", table, column)
			}
			row[column] = strings.TrimSpace(value)
		}
	}
	return nil
}

func validateSnapshotRevision(table string, row map[string]any) error {
	raw := row["updated_at"]
	value, ok := raw.(string)
	if !ok || strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s.updated_at must be an RFC3339 string", table)
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("%s.updated_at must be RFC3339: %w", table, err)
	}
	row["updated_at"] = parsed.UTC().Format(time.RFC3339Nano)
	return nil
}

func validateLiveSnapshotTombstoneMetadata(table string, row map[string]any) error {
	for _, column := range []string{"deletion_source", "deletion_reason"} {
		raw := row[column]
		if raw == nil {
			continue
		}
		value, ok := raw.(string)
		if !ok || strings.TrimSpace(value) != "" {
			return fmt.Errorf("%s.%s must be null for a live row", table, column)
		}
		row[column] = nil
	}
	return nil
}

func repairImportedGuildIDs(ctx context.Context, tx *sql.Tx) error {
	repairs := []struct {
		table string
		query string
	}{
		{"messages", `
			update messages
			set guild_id = (
				select c.guild_id
				from channels c
				where c.id = messages.channel_id
			)
			where coalesce(guild_id, '') = ''
			  and exists (
				select 1
				from channels c
				where c.id = messages.channel_id
				  and coalesce(c.guild_id, '') != ''
			  )`},
		{"message_attachments", `
			update message_attachments
			set guild_id = coalesce(
				nullif((select m.guild_id from messages m where m.id = message_attachments.message_id), ''),
				(select c.guild_id from channels c where c.id = message_attachments.channel_id)
			)
			where coalesce(guild_id, '') = ''
			  and coalesce(
				nullif((select m.guild_id from messages m where m.id = message_attachments.message_id), ''),
				(select c.guild_id from channels c where c.id = message_attachments.channel_id)
			  ) is not null`},
		{"message_events", `
			update message_events
			set guild_id = coalesce(
				nullif((select m.guild_id from messages m where m.id = message_events.message_id), ''),
				(select c.guild_id from channels c where c.id = message_events.channel_id)
			)
			where coalesce(guild_id, '') = ''
			  and coalesce(
				nullif((select m.guild_id from messages m where m.id = message_events.message_id), ''),
				(select c.guild_id from channels c where c.id = message_events.channel_id)
			  ) is not null`},
		{"mention_events", `
			update mention_events
			set guild_id = coalesce(
				nullif((select m.guild_id from messages m where m.id = mention_events.message_id), ''),
				(select c.guild_id from channels c where c.id = mention_events.channel_id)
			)
			where coalesce(guild_id, '') = ''
			  and coalesce(
				nullif((select m.guild_id from messages m where m.id = mention_events.message_id), ''),
				(select c.guild_id from channels c where c.id = mention_events.channel_id)
			  ) is not null`},
	}
	for _, repair := range repairs {
		if _, err := tx.ExecContext(ctx, repair.query); err != nil {
			return fmt.Errorf("repair imported %s guild ids: %w", repair.table, err)
		}
	}
	return nil
}

func importColumns(table TableManifest) []string {
	if table.Name != "message_events" && table.Name != "mention_events" {
		return table.Columns
	}
	columns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		if column != "event_id" {
			columns = append(columns, column)
		}
	}
	return columns
}

type snapshotFilter struct {
	active            bool
	publicOnly        bool
	includeChannels   map[string]struct{}
	excludeChannels   map[string]struct{}
	allowedChannels   map[string]struct{}
	allowedGuilds     map[string]struct{}
	allowedMembers    map[string]map[string]struct{}
	allowedMessageIDs map[string]bool
	channels          map[string]snapshotChannel
	guilds            map[string]string
	publicMemo        map[string]bool
	publicSeen        map[string]bool
}

type snapshotChannel struct {
	ID              string
	GuildID         string
	ParentID        string
	ThreadParentID  string
	Kind            string
	IsPrivateThread bool
	RawJSON         string
}

func newSnapshotFilter(ctx context.Context, db *sql.DB, opts FilterOptions) (*snapshotFilter, error) {
	f := &snapshotFilter{
		publicOnly:        opts.PublicOnly,
		includeChannels:   stringSet(opts.IncludeChannelIDs),
		excludeChannels:   stringSet(opts.ExcludeChannelIDs),
		allowedChannels:   map[string]struct{}{},
		allowedGuilds:     map[string]struct{}{},
		allowedMembers:    map[string]map[string]struct{}{},
		allowedMessageIDs: map[string]bool{},
		channels:          map[string]snapshotChannel{},
		guilds:            map[string]string{},
		publicMemo:        map[string]bool{},
		publicSeen:        map[string]bool{},
	}
	f.active = opts.Active()
	if !f.active {
		return f, nil
	}
	if err := f.loadGuilds(ctx, db); err != nil {
		return nil, err
	}
	if err := f.loadChannels(ctx, db); err != nil {
		return nil, err
	}
	for id, ch := range f.channels {
		if f.channelAllowed(ch) {
			f.allowedChannels[id] = struct{}{}
			if ch.GuildID != "" {
				f.allowedGuilds[ch.GuildID] = struct{}{}
			}
		}
	}
	if err := f.loadAllowedMessages(ctx, db); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *snapshotFilter) loadGuilds(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `select id, raw_json from guilds`)
	if err != nil {
		return fmt.Errorf("query guilds for share filter: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return fmt.Errorf("scan guild share filter: %w", err)
		}
		f.guilds[id] = raw
	}
	return rows.Err()
}

func (f *snapshotFilter) loadChannels(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `
		select id, guild_id, coalesce(parent_id, ''), kind, is_private_thread,
		       coalesce(thread_parent_id, ''), raw_json
		from channels
	`)
	if err != nil {
		return fmt.Errorf("query channels for share filter: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var ch snapshotChannel
		if err := rows.Scan(&ch.ID, &ch.GuildID, &ch.ParentID, &ch.Kind, &ch.IsPrivateThread, &ch.ThreadParentID, &ch.RawJSON); err != nil {
			return fmt.Errorf("scan channel share filter: %w", err)
		}
		f.channels[ch.ID] = ch
	}
	return rows.Err()
}

func (f *snapshotFilter) loadAllowedMessages(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `select id, guild_id, channel_id, author_id from messages where guild_id != ?`, directMessageGuildID)
	if err != nil {
		return fmt.Errorf("query messages for share filter: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var messageID, guildID, channelID, authorID string
		if err := rows.Scan(&messageID, &guildID, &channelID, &authorID); err != nil {
			return fmt.Errorf("scan message share filter: %w", err)
		}
		if !f.allowChannelID(channelID) {
			continue
		}
		f.allowedMessageIDs[messageID] = true
		if guildID != "" && authorID != "" {
			if f.allowedMembers[guildID] == nil {
				f.allowedMembers[guildID] = map[string]struct{}{}
			}
			f.allowedMembers[guildID][authorID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows, err = db.QueryContext(ctx, `
		select guild_id, channel_id, target_id
		from mention_events
		where target_type = 'user' and guild_id != ?
	`, directMessageGuildID)
	if err != nil {
		return fmt.Errorf("query mentions for share filter: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var guildID, channelID, userID string
		if err := rows.Scan(&guildID, &channelID, &userID); err != nil {
			return fmt.Errorf("scan mention share filter: %w", err)
		}
		if !f.allowChannelID(channelID) {
			continue
		}
		if guildID != "" && userID != "" && f.allowedMembers[guildID] != nil {
			f.allowedMembers[guildID][userID] = struct{}{}
		}
	}
	return rows.Err()
}

func (f *snapshotFilter) allow(table string, row map[string]any) bool {
	if isDirectMessageSnapshotRow(table, row) {
		return false
	}
	if !f.active {
		return true
	}
	switch table {
	case "guilds":
		_, ok := f.allowedGuilds[stringValue(row["id"])]
		return ok
	case "channels":
		return f.allowChannelID(stringValue(row["id"]))
	case "members":
		guildID := stringValue(row["guild_id"])
		userID := stringValue(row["user_id"])
		_, ok := f.allowedMembers[guildID][userID]
		return ok
	case "messages", "message_events":
		return f.allowedMessageIDs[stringValue(row["message_id"])] || f.allowChannelID(stringValue(row["channel_id"]))
	case "message_attachments", "mention_events":
		return f.allowChannelID(stringValue(row["channel_id"]))
	case "sync_state":
		return f.allowSyncState(stringValue(row["scope"]))
	default:
		return true
	}
}

func (f *snapshotFilter) allowChannelID(channelID string) bool {
	if !f.active {
		return true
	}
	_, ok := f.allowedChannels[channelID]
	return ok
}

func (f *snapshotFilter) allowSyncState(scope string) bool {
	if rest, ok := strings.CutPrefix(scope, "channel:"); ok {
		channelID, _, _ := strings.Cut(rest, ":")
		return f.allowChannelID(channelID)
	}
	if strings.HasPrefix(scope, "wiretap:") {
		return false
	}
	if strings.HasPrefix(scope, "share:") {
		return false
	}
	if rest, ok := strings.CutPrefix(scope, "guild:"); ok {
		if strings.HasSuffix(scope, ":members:last_success") {
			return false
		}
		guildID, _, _ := strings.Cut(rest, ":")
		_, ok := f.allowedGuilds[guildID]
		return ok
	}
	return false
}

func (f *snapshotFilter) channelAllowed(ch snapshotChannel) bool {
	if ch.ID == "" {
		return false
	}
	if _, blocked := f.excludeChannels[ch.ID]; blocked {
		return false
	}
	parentID := channelParentID(ch)
	if parentID != "" {
		if _, blocked := f.excludeChannels[parentID]; blocked {
			return false
		}
	}
	if len(f.includeChannels) > 0 {
		if _, ok := f.includeChannels[ch.ID]; !ok {
			_, parentOK := f.includeChannels[parentID]
			if !strings.HasPrefix(ch.Kind, "thread_") || !parentOK {
				return false
			}
		}
	}
	if f.publicOnly && !f.publicChannel(ch.ID) {
		return false
	}
	return true
}

func (f *snapshotFilter) publicChannel(channelID string) bool {
	if cached, ok := f.publicMemo[channelID]; ok {
		return cached
	}
	if f.publicSeen[channelID] {
		return false
	}
	f.publicSeen[channelID] = true
	defer delete(f.publicSeen, channelID)
	ch, ok := f.channels[channelID]
	if !ok || ch.GuildID == "" || ch.IsPrivateThread || ch.Kind == "thread_private" {
		f.publicMemo[channelID] = false
		return false
	}
	if strings.HasPrefix(ch.Kind, "thread_") {
		parentID := channelParentID(ch)
		allowed := parentID != "" && f.publicChannel(parentID)
		f.publicMemo[channelID] = allowed
		return allowed
	}
	permissions, ok := everyoneGuildPermissions(f.guilds[ch.GuildID], ch.GuildID)
	if !ok {
		f.publicMemo[channelID] = false
		return false
	}
	if parent, ok := f.channels[ch.ParentID]; ok && parent.Kind == "category" {
		permissions = applyEveryoneOverwrite(permissions, parent.RawJSON, ch.GuildID)
	}
	permissions = applyEveryoneOverwrite(permissions, ch.RawJSON, ch.GuildID)
	allowed := permissions&permissionViewChannel != 0
	f.publicMemo[channelID] = allowed
	return allowed
}

const permissionViewChannel int64 = 1 << 10

func everyoneGuildPermissions(rawGuild string, guildID string) (int64, bool) {
	var payload struct {
		Roles []struct {
			ID          string `json:"id"`
			Permissions any    `json:"permissions"`
		} `json:"roles"`
	}
	if err := decodeJSONUseNumber(rawGuild, &payload); err != nil {
		return 0, false
	}
	for _, role := range payload.Roles {
		if role.ID != guildID {
			continue
		}
		permissions, ok := parsePermissionBits(role.Permissions)
		if !ok {
			return 0, false
		}
		return permissions, true
	}
	return 0, false
}

func applyEveryoneOverwrite(permissions int64, rawChannel string, guildID string) int64 {
	var payload struct {
		PermissionOverwrites []struct {
			ID    string `json:"id"`
			Type  any    `json:"type"`
			Allow any    `json:"allow"`
			Deny  any    `json:"deny"`
		} `json:"permission_overwrites"`
	}
	if err := decodeJSONUseNumber(rawChannel, &payload); err != nil {
		return permissions
	}
	for _, overwrite := range payload.PermissionOverwrites {
		if overwrite.ID != guildID || !isRoleOverwrite(overwrite.Type) {
			continue
		}
		allow, _ := parsePermissionBits(overwrite.Allow)
		deny, _ := parsePermissionBits(overwrite.Deny)
		permissions &^= deny
		permissions |= allow
	}
	return permissions
}

func decodeJSONUseNumber(raw string, value any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(value)
}

func parsePermissionBits(value any) (int64, bool) {
	switch v := value.(type) {
	case nil:
		return 0, true
	case float64:
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		return parsed, err == nil
	case json.Number:
		parsed, err := strconv.ParseInt(v.String(), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func isRoleOverwrite(value any) bool {
	switch v := value.(type) {
	case float64:
		return int(v) == 0
	case json.Number:
		parsed, err := strconv.Atoi(v.String())
		return err == nil && parsed == 0
	case string:
		return v == "0" || strings.EqualFold(v, "role")
	default:
		return false
	}
}

func channelParentID(ch snapshotChannel) string {
	if ch.ThreadParentID != "" {
		return ch.ThreadParentID
	}
	return ch.ParentID
}

func stringSet(in []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func snapshotExportQuery(table string) (string, []any) {
	switch table {
	case "guilds":
		return "select * from guilds where id != ?", []any{directMessageGuildID}
	case "channels", "members", "messages", "message_events", "message_attachments", "mention_events":
		return "select * from " + table + " where guild_id != ?", []any{directMessageGuildID}
	case "sync_state":
		return "select * from sync_state where scope not like 'wiretap:%'", nil
	default:
		return "select * from " + table, nil
	}
}

func snapshotDeleteQuery(table string) (string, []any) {
	switch table {
	case "guilds":
		return "delete from guilds where id != ?", []any{directMessageGuildID}
	case "message_events", "mention_events":
		return "delete from " + table + " where guild_id != ?", []any{directMessageGuildID}
	case "channels", "members", "messages", "message_attachments":
		return "delete from " + table + " where guild_id != ?", []any{directMessageGuildID}
	case "sync_state":
		return "delete from sync_state where scope not like 'wiretap:%'", nil
	default:
		return "delete from " + table, nil
	}
}

func isDirectMessageSnapshotRow(table string, row map[string]any) bool {
	switch table {
	case "guilds":
		return isLocalOnlyGuildID(stringValue(row["id"]))
	case "channels", "members", "messages", "message_events", "message_attachments", "mention_events":
		return isLocalOnlyGuildID(stringValue(row["guild_id"]))
	case "sync_state":
		scope := stringValue(row["scope"])
		return strings.HasPrefix(scope, "wiretap:")
	default:
		return false
	}
}

func isLocalOnlyGuildID(guildID string) bool {
	return guildID == directMessageGuildID
}

func importEmbeddings(ctx context.Context, tx *sql.Tx, opts Options, manifests []EmbeddingManifest) error {
	if len(manifests) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values(?, ?, ?, ?, ?, ?, ?)
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
			if err := importEmbeddingFile(ctx, stmt, opts.RepoPath, rel); err != nil {
				return err
			}
		}
	}
	return nil
}

func importEmbeddingFile(ctx context.Context, stmt *sql.Stmt, repoPath, rel string) error {
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
		blob, err := base64.StdEncoding.DecodeString(row.EmbeddingBlob)
		if err != nil {
			return fmt.Errorf("decode embedding blob in %s: %w", rel, err)
		}
		if _, err := stmt.ExecContext(ctx, row.MessageID, row.Provider, row.Model, row.InputVersion, dimensions, blob, row.EmbeddedAt); err != nil {
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

type tableShardWriter struct {
	rootDir     string
	relDir      string
	label       string
	nextShard   int
	rowsInShard int
	files       []string
	file        *os.File
	counter     *countingWriter
	gz          *gzip.Writer
}

func (w *tableShardWriter) open() error {
	rel := filepath.ToSlash(filepath.Join(w.relDir, fmt.Sprintf("%06d.jsonl.gz", w.nextShard)))
	path := filepath.Join(w.rootDir, filepath.FromSlash(rel))
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", rel, err)
	}
	w.nextShard++
	w.rowsInShard = 0
	w.files = append(w.files, rel)
	w.file = file
	w.counter = &countingWriter{w: file}
	w.gz = gzip.NewWriter(w.counter)
	return nil
}

func (w *tableShardWriter) Write(p []byte) (int, error) {
	return w.gz.Write(p)
}

func (w *tableShardWriter) rotateIfNeeded() error {
	if maxShardBytes <= 0 || w.rowsInShard == 0 || w.counter.n < maxShardBytes {
		return nil
	}
	if err := w.close(); err != nil {
		return err
	}
	return w.open()
}

func (w *tableShardWriter) finishRow() error {
	w.rowsInShard++
	if maxShardBytes > 1024*1024 && w.rowsInShard%shardFlushRows != 0 {
		return nil
	}
	if err := w.gz.Flush(); err != nil {
		return fmt.Errorf("flush %s shard: %w", w.label, err)
	}
	return nil
}

func (w *tableShardWriter) close() error {
	var closeErr error
	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			closeErr = err
		}
		w.gz = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		w.file = nil
	}
	if closeErr != nil {
		return fmt.Errorf("close %s shard: %w", w.label, closeErr)
	}
	return nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func exportValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

func importValue(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v.String(), 64); err == nil {
			return f
		}
		return v.String()
	default:
		return v
	}
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
		if _, err := stmt.ExecContext(ctx, record.MediaPath, nullableString(record.ContentSHA256), record.ContentSize, nullableString(record.FetchedAt), record.FetchStatus, record.FetchError, attachmentID); err != nil {
			return fmt.Errorf("preserve attachment media %s: %w", attachmentID, err)
		}
	}
	return nil
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func upsertMergeSnapshotRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any) (bool, error) {
	if table == "message_events" || table == "mention_events" {
		delete(row, "event_id")
	}
	if table == "message_attachments" {
		if err := preserveIncrementalAttachmentState(ctx, tx, row); err != nil {
			return false, err
		}
	}
	if table == "guilds" || table == "members" {
		apply, err := shouldMergeTombstoneEntityRow(ctx, tx, table, row)
		if err != nil || !apply {
			return false, err
		}
	}
	// Guild/member revisions were compared chronologically above. Reapplying the
	// generic lexical SQL guard would reject valid RFC3339 offset timestamps.
	protectNewer := slices.Contains([]string{"channels", "messages"}, table)
	return upsertSnapshotRow(ctx, tx, table, row, protectNewer)
}

func shouldMergeTombstoneEntityRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any) (bool, error) {
	var query string
	var args []any
	switch table {
	case "guilds":
		query = `select updated_at, deleted_at is not null from guilds where id = ?`
		args = []any{importValue(row["id"])}
	case "members":
		query = `select updated_at, deleted_at is not null from members where guild_id = ? and user_id = ?`
		args = []any{importValue(row["guild_id"]), importValue(row["user_id"])}
	default:
		return true, nil
	}
	var localUpdated string
	var localTombstone bool
	err := tx.QueryRowContext(ctx, query, args...).Scan(&localUpdated, &localTombstone)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read existing %s merge revision: %w", table, err)
	}
	comparison := compareSnapshotRevisions(stringValue(row["updated_at"]), localUpdated)
	if comparison != 0 {
		return comparison > 0, nil
	}
	incomingTombstone := row["deleted_at"] != nil && strings.TrimSpace(stringValue(row["deleted_at"])) != ""
	return !localTombstone || incomingTombstone, nil
}

func compareSnapshotRevisions(left, right string) int {
	leftTime, leftErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(left))
	rightTime, rightErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(right))
	if leftErr == nil && rightErr == nil {
		switch {
		case leftTime.Before(rightTime):
			return -1
		case leftTime.After(rightTime):
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(strings.TrimSpace(left), strings.TrimSpace(right))
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

func upsertSnapshotRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any, protectNewer bool) (bool, error) {
	cols := make([]string, 0, len(row))
	for col := range row {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	quoted := make([]string, 0, len(cols))
	updates := make([]string, 0, len(cols))
	placeholders := make([]string, 0, len(cols))
	args := make([]any, 0, len(cols))
	for _, col := range cols {
		quotedCol := quoteIdent(col)
		quoted = append(quoted, quotedCol)
		updates = append(updates, quotedCol+" = excluded."+quotedCol)
		placeholders = append(placeholders, "?")
		args = append(args, importValue(row[col]))
	}
	verb := "insert or replace into "
	suffix := ""
	if protectNewer {
		verb = "insert into "
		suffix = " on conflict do update set " + strings.Join(updates, ",") +
			" where coalesce(julianday(excluded.\"updated_at\"), 0) >= coalesce(julianday(" + quoteIdent(table) + ".\"updated_at\"), 0)"
	}
	stmt := verb + quoteIdent(table) + "(" + strings.Join(quoted, ",") + ") values(" + strings.Join(placeholders, ",") + ")" + suffix
	result, err := tx.ExecContext(ctx, stmt, args...)
	if err != nil {
		return false, fmt.Errorf("insert %s: %w", table, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check inserted %s row: %w", table, err)
	}
	return changed > 0, nil
}

func upsertMessageFTSRow(ctx context.Context, tx *sql.Tx, messageID string) error {
	rowID, ok := messageFTSRowID(messageID)
	if !ok {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `delete from message_fts where rowid = ?`, rowID); err != nil {
		return fmt.Errorf("delete message_fts %s: %w", messageID, err)
	}
	var (
		guildID     string
		channelID   string
		authorID    string
		authorName  string
		channelName string
		content     string
	)
	if err := tx.QueryRowContext(ctx, `
		select
			m.guild_id,
			m.channel_id,
			coalesce(m.author_id, ''),
			coalesce(
				json_extract(m.raw_json, '$.member.nick'),
				json_extract(m.raw_json, '$.author.global_name'),
				json_extract(m.raw_json, '$.author.username'),
				''
			),
			coalesce(c.name, ''),
			m.normalized_content
		from messages m
		left join channels c on c.id = m.channel_id
		where m.id = ?
	`, messageID).Scan(&guildID, &channelID, &authorID, &authorName, &channelName, &content); err != nil {
		return fmt.Errorf("query message_fts %s: %w", messageID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		insert into message_fts(rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content)
		values(?, ?, ?, ?, ?, ?, ?, ?)
	`, rowID, messageID, guildID, channelID, nullIfEmpty(authorID), authorName, channelName, content); err != nil {
		return fmt.Errorf("insert message_fts %s: %w", messageID, err)
	}
	return nil
}

func messageFTSRowID(messageID string) (int64, bool) {
	if messageID == "" {
		return 0, false
	}
	rowID, err := strconv.ParseInt(messageID, 10, 64)
	if err == nil && rowID > 0 {
		return rowID, true
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(messageID))
	rowID = int64(hash.Sum64() & ((uint64(1) << 63) - 1))
	if rowID == 0 {
		rowID = 1
	}
	return rowID, true
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func insertSQL(table string, columns []string) string {
	quoted := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quoteIdent(column)
		placeholders[i] = "?"
	}
	return "insert into " + quoteIdent(table) + "(" + strings.Join(quoted, ",") + ") values(" + strings.Join(placeholders, ",") + ")"
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func safePathSegment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "_"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	clean := b.String()
	if clean == "." || clean == ".." {
		return "_"
	}
	return clean
}
