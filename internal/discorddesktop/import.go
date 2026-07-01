package discorddesktop

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/openclaw/discrawl/internal/store"
)

const (
	DirectMessageGuildID   = "@me"
	DirectMessageGuildName = "Discord Direct Messages"
	defaultMaxFileBytes    = 64 << 20
	maxObjectBytes         = 4 << 20
	cacheSniffBytes        = 1 << 20
	checkpointEveryFiles   = 256
)

var (
	channelRouteRE     = regexp.MustCompile(`/channels/(@me|[0-9]{12,24})/([0-9]{12,24})`)
	apiMessagesRouteRE = regexp.MustCompile(`/api/v[0-9]+/channels/[0-9]{12,24}/messages`)
)

type Options struct {
	Path         string
	MaxFileBytes int64
	DryRun       bool
	FullCache    bool
	Now          func() time.Time
}

type Stats struct {
	Path                  string    `json:"path"`
	FilesVisited          int       `json:"files_visited"`
	FilesScanned          int       `json:"files_scanned"`
	FilesSkipped          int       `json:"files_skipped"`
	FilesUnchanged        int       `json:"files_unchanged"`
	CacheFilesFastSkipped int       `json:"cache_files_fast_skipped"`
	BytesScanned          int64     `json:"bytes_scanned"`
	JSONObjects           int       `json:"json_objects"`
	Guilds                int       `json:"guilds"`
	Channels              int       `json:"channels"`
	Messages              int       `json:"messages"`
	DMMessages            int       `json:"dm_messages"`
	DMChannels            int       `json:"dm_channels"`
	GuildMessages         int       `json:"guild_messages"`
	SkippedMessages       int       `json:"skipped_messages"`
	SkippedChannels       int       `json:"skipped_channels"`
	Checkpoints           int       `json:"checkpoints"`
	DryRun                bool      `json:"dry_run,omitempty"`
	FullCache             bool      `json:"full_cache,omitempty"`
	StartedAt             time.Time `json:"started_at"`
	FinishedAt            time.Time `json:"finished_at"`
}

type snapshot struct {
	guilds     map[string]store.GuildRecord
	channels   map[string]store.ChannelRecord
	messages   map[string]store.MessageMutation
	routes     map[string]string
	userLabels map[string]userLabel
}

type fileFingerprint struct {
	Size      int64  `json:"size"`
	ModUnixNS int64  `json:"mod_unix_ns"`
	Status    string `json:"status,omitempty"`
}

type scanState struct {
	previous map[string]fileFingerprint
	current  map[string]fileFingerprint
	channels map[string]store.ChannelRecord
}

type fileSource int

const (
	fileSourceContext fileSource = iota
	fileSourceCacheData
)

type fileCandidate struct {
	absPath     string
	relPath     string
	relKey      string
	source      fileSource
	info        fs.FileInfo
	fingerprint fileFingerprint
}

type scanTotals struct {
	guilds          map[string]struct{}
	channels        map[string]struct{}
	messages        map[string]struct{}
	dmMessages      map[string]struct{}
	guildMessages   map[string]struct{}
	dmChannels      map[string]struct{}
	skippedMessages map[string]struct{}
	skippedChannels map[string]struct{}
}

type unresolvedMessages map[string]string

const wiretapFileIndexScope = "wiretap:file_index:v1"

const (
	fileStatusImported = "imported"
	fileStatusSkipped  = "skipped"
)

func DefaultPath() string {
	home, _ := os.UserHomeDir()
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "discord")
	case "windows":
		if appData := strings.TrimSpace(os.Getenv("APPDATA")); appData != "" {
			return filepath.Join(appData, "discord")
		}
		return filepath.Join(home, "AppData", "Roaming", "discord")
	default:
		if configHome := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); configHome != "" {
			return filepath.Join(configHome, "discord")
		}
		return filepath.Join(home, ".config", "discord")
	}
}

func Import(ctx context.Context, st *store.Store, opts Options) (Stats, error) {
	if st == nil && !opts.DryRun {
		return Stats{}, errors.New("store is required")
	}
	state, err := loadScanState(ctx, st, opts)
	if err != nil {
		return Stats{}, err
	}
	if opts.FullCache {
		stats, snap, err := scanFullCache(ctx, opts, state)
		if err != nil {
			return stats, err
		}
		stats.DryRun = opts.DryRun
		if opts.DryRun {
			return stats, nil
		}
		if err := writeSnapshot(ctx, st, snap, len(state.previous) == 0); err != nil {
			return stats, err
		}
		if err := saveFileIndex(ctx, st, opts, state.current); err != nil {
			return stats, err
		}
		stats.Checkpoints = 1
		if err := saveCoverageStats(ctx, st, stats); err != nil {
			return stats, err
		}
		return stats, nil
	}
	stats, err := scanAndImport(ctx, st, opts, state)
	if err != nil {
		return stats, err
	}
	stats.DryRun = opts.DryRun
	if !opts.DryRun {
		if err := saveCoverageStats(ctx, st, stats); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func saveCoverageStats(ctx context.Context, st *store.Store, stats Stats) error {
	return st.SetWiretapImportStats(ctx, store.WiretapImportStats{
		FilesScanned:    stats.FilesScanned,
		Messages:        stats.Messages,
		Channels:        stats.Channels,
		SkippedMessages: stats.SkippedMessages,
		SkippedChannels: stats.SkippedChannels,
		StartedAt:       stats.StartedAt,
		FinishedAt:      stats.FinishedAt,
	})
}

func loadScanState(ctx context.Context, st *store.Store, opts Options) (scanState, error) {
	state := scanState{
		previous: map[string]fileFingerprint{},
		current:  map[string]fileFingerprint{},
		channels: map[string]store.ChannelRecord{},
	}
	if st == nil || opts.DryRun {
		return state, nil
	}
	raw, err := st.GetSyncState(ctx, fileIndexScope(opts))
	if err != nil {
		return state, err
	}
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &state.previous); err != nil {
			state.previous = map[string]fileFingerprint{}
		}
	}
	channels, err := st.Channels(ctx, "")
	if err != nil {
		return state, err
	}
	for _, channel := range channels {
		state.channels[channel.ID] = store.ChannelRecord{
			ID:      channel.ID,
			GuildID: channel.GuildID,
			Kind:    channel.Kind,
			Name:    channel.Name,
		}
	}
	return state, nil
}

func fileIndexScope(Options) string {
	return wiretapFileIndexScope
}

func saveFileIndex(ctx context.Context, st *store.Store, opts Options, index map[string]fileFingerprint) error {
	body, err := json.Marshal(index)
	if err != nil {
		return err
	}
	return st.SetSyncState(ctx, fileIndexScope(opts), string(body))
}

func sameFileFingerprint(a, b fileFingerprint) bool {
	return a.Size == b.Size && a.ModUnixNS == b.ModUnixNS
}

func isImportedFingerprint(fingerprint fileFingerprint) bool {
	return fingerprint.Status == "" || fingerprint.Status == fileStatusImported
}

func importedFingerprint(fingerprint fileFingerprint) fileFingerprint {
	fingerprint.Status = fileStatusImported
	return fingerprint
}

func skippedFingerprint(fingerprint fileFingerprint) fileFingerprint {
	fingerprint.Status = fileStatusSkipped
	return fingerprint
}

func snapshotHasChanges(snap snapshot) bool {
	return len(snap.guilds) > 0 || len(snap.channels) > 0 || len(snap.messages) > 0
}

func scanAndImport(ctx context.Context, st *store.Store, opts Options, state scanState) (Stats, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	root := strings.TrimSpace(opts.Path)
	if root == "" {
		root = DefaultPath()
	}
	stats := Stats{Path: root, FullCache: opts.FullCache, StartedAt: now().UTC()}
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		stats.FinishedAt = now().UTC()
		return stats, ignoreCacheFileError(err)
	}
	defer func() { _ = rootFS.Close() }()
	contextFiles, cacheFiles, err := discoverCandidates(ctx, root, rootFS, opts, state, &stats)
	if err != nil {
		stats.FinishedAt = now().UTC()
		return stats, err
	}
	fullScan := len(state.previous) == 0
	if fullScan && !opts.DryRun {
		if err := st.DeleteGuildData(ctx, "@unknown"); err != nil {
			stats.FinishedAt = now().UTC()
			return stats, err
		}
	}
	run := newImportRun(ctx, st, opts, state, rootFS, &stats)
	if err := run.scanContext(contextFiles); err != nil {
		stats.FinishedAt = now().UTC()
		return stats, err
	}
	if err := collectCacheRouteHints(ctx, rootFS, cacheFiles, run.base); err != nil {
		stats.FinishedAt = now().UTC()
		return stats, err
	}
	if err := run.scanCacheBatches(cacheFiles); err != nil {
		stats.FinishedAt = now().UTC()
		return stats, err
	}
	if err := run.retryPending(); err != nil {
		stats.FinishedAt = now().UTC()
		return stats, err
	}
	if !opts.DryRun {
		if len(contextFiles) == 0 && len(cacheFiles) == 0 {
			if err := st.SetSyncState(ctx, "wiretap:last_import", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				stats.FinishedAt = now().UTC()
				return stats, err
			}
			if err := saveFileIndex(ctx, st, opts, state.current); err != nil {
				stats.FinishedAt = now().UTC()
				return stats, err
			}
			stats.Checkpoints++
		}
		if err := st.DeleteOrphanChannels(ctx, DirectMessageGuildID); err != nil {
			stats.FinishedAt = now().UTC()
			return stats, err
		}
	}
	stats.FinishedAt = now().UTC()
	return stats, nil
}

func scanFullCache(ctx context.Context, opts Options, state scanState) (Stats, snapshot, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	root := strings.TrimSpace(opts.Path)
	if root == "" {
		root = DefaultPath()
	}
	maxBytes := opts.MaxFileBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxFileBytes
	}
	stats := Stats{Path: root, FullCache: true, StartedAt: now().UTC()}
	snap := newSnapshot()
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		stats.FinishedAt = now().UTC()
		return stats, snap, ignoreCacheFileError(err)
	}
	defer func() { _ = rootFS.Close() }()
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return ignoreCacheFileError(err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if entry.IsDir() {
			if shouldSkipDir(entry.Name()) && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		stats.FilesVisited++
		info, err := entry.Info()
		if err != nil {
			stats.FilesSkipped++
			return ignoreCacheFileError(err)
		}
		if !isCandidateFile(path) || info.Size() <= 0 || info.Size() > maxBytes {
			stats.FilesSkipped++
			return nil
		}
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			stats.FilesSkipped++
			return ignoreCacheFileError(err)
		}
		relKey := filepath.ToSlash(relPath)
		fingerprint := fileFingerprint{
			Size:      info.Size(),
			ModUnixNS: info.ModTime().UnixNano(),
		}
		state.current[relKey] = importedFingerprint(fingerprint)
		if previous, ok := state.previous[relKey]; ok && sameFileFingerprint(previous, fingerprint) && isImportedFingerprint(previous) {
			stats.FilesUnchanged++
			return nil
		}
		data, err := rootFS.ReadFile(relPath)
		if err != nil {
			stats.FilesSkipped++
			return ignoreCacheFileError(err)
		}
		stats.FilesScanned++
		stats.BytesScanned += int64(len(data))
		collectChannelRoutes(snap, bytes.ToValidUTF8(data, nil))
		objects := extractJSONValues(bytes.ToValidUTF8(data, nil))
		for _, payload := range extractGzipPayloads(data, maxBytes) {
			if err := ctx.Err(); err != nil {
				return err
			}
			collectChannelRoutes(snap, bytes.ToValidUTF8(payload, nil))
			objects = append(objects, extractJSONValues(bytes.ToValidUTF8(payload, nil))...)
		}
		stats.JSONObjects += len(objects)
		for _, raw := range objects {
			if err := ctx.Err(); err != nil {
				return err
			}
			var value any
			if err := json.Unmarshal(raw, &value); err != nil {
				continue
			}
			collectValue(snap, state.channels, value, info.ModTime().UTC())
		}
		return nil
	}); err != nil {
		return stats, snap, err
	}
	totals := newScanTotals()
	finalizeSnapshot(snap, state.channels, totals, &stats, true)
	stats.FinishedAt = now().UTC()
	return stats, snap, nil
}

func discoverCandidates(ctx context.Context, root string, rootFS *os.Root, opts Options, state scanState, stats *Stats) ([]fileCandidate, []fileCandidate, error) {
	var contextFiles []fileCandidate
	var cacheFiles []fileCandidate
	maxBytes := opts.MaxFileBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxFileBytes
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return ignoreCacheFileError(err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if entry.IsDir() {
			if shouldSkipDir(entry.Name()) && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		stats.FilesVisited++
		info, err := entry.Info()
		if err != nil {
			stats.FilesSkipped++
			return ignoreCacheFileError(err)
		}
		if !isCandidateFile(path) || info.Size() <= 0 || info.Size() > maxBytes {
			stats.FilesSkipped++
			return nil
		}
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			stats.FilesSkipped++
			return ignoreCacheFileError(err)
		}
		relKey := filepath.ToSlash(relPath)
		fingerprint := fileFingerprint{
			Size:      info.Size(),
			ModUnixNS: info.ModTime().UnixNano(),
		}
		candidate := fileCandidate{
			absPath:     path,
			relPath:     relPath,
			relKey:      relKey,
			source:      sourceForPath(root, path, relPath),
			info:        info,
			fingerprint: fingerprint,
		}
		if candidate.source == fileSourceCacheData {
			if previous, ok := state.previous[relKey]; ok && sameFileFingerprint(previous, fingerprint) {
				if isImportedFingerprint(previous) {
					state.current[relKey] = previous
					stats.FilesUnchanged++
					return nil
				}
			}
			if !opts.FullCache {
				ok, err := cacheFileHasRouteHint(rootFS, relPath)
				if err != nil {
					stats.FilesSkipped++
					return ignoreCacheFileError(err)
				}
				if !ok {
					state.current[relKey] = skippedFingerprint(fingerprint)
					stats.FilesSkipped++
					stats.CacheFilesFastSkipped++
					return nil
				}
			}
			cacheFiles = append(cacheFiles, candidate)
			return nil
		}
		if previous, ok := state.previous[relKey]; ok && sameFileFingerprint(previous, fingerprint) {
			state.current[relKey] = previous
			stats.FilesUnchanged++
			return nil
		}
		contextFiles = append(contextFiles, candidate)
		return nil
	})
	return contextFiles, cacheFiles, err
}

func scanCandidates(ctx context.Context, rootFS *os.Root, opts Options, candidates []fileCandidate, snap snapshot, channelLookup map[string]store.ChannelRecord, stats *Stats) error {
	maxBytes := opts.MaxFileBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxFileBytes
	}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := rootFS.ReadFile(candidate.relPath)
		if err != nil {
			stats.FilesSkipped++
			if err := ignoreCacheFileError(err); err != nil {
				return err
			}
			continue
		}
		stats.FilesScanned++
		stats.BytesScanned += int64(len(data))
		collectChannelRoutes(snap, bytes.ToValidUTF8(data, nil))
		objects := extractJSONValues(bytes.ToValidUTF8(data, nil))
		for _, payload := range extractGzipPayloads(data, maxBytes) {
			if err := ctx.Err(); err != nil {
				return err
			}
			collectChannelRoutes(snap, bytes.ToValidUTF8(payload, nil))
			objects = append(objects, extractJSONValues(bytes.ToValidUTF8(payload, nil))...)
		}
		stats.JSONObjects += len(objects)
		for _, raw := range objects {
			if err := ctx.Err(); err != nil {
				return err
			}
			var value any
			if err := json.Unmarshal(raw, &value); err != nil {
				continue
			}
			collectValue(snap, channelLookup, value, candidate.info.ModTime().UTC())
		}
	}
	return nil
}

func collectCacheRouteHints(ctx context.Context, rootFS *os.Root, candidates []fileCandidate, snap snapshot) error {
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := readFilePrefix(rootFS, candidate.relPath)
		if err != nil {
			if err := ignoreCacheFileError(err); err != nil {
				return err
			}
			continue
		}
		collectChannelRoutes(snap, bytes.ToValidUTF8(data, nil))
	}
	return nil
}

func newScanTotals() scanTotals {
	return scanTotals{
		guilds:          map[string]struct{}{},
		channels:        map[string]struct{}{},
		messages:        map[string]struct{}{},
		dmMessages:      map[string]struct{}{},
		guildMessages:   map[string]struct{}{},
		dmChannels:      map[string]struct{}{},
		skippedMessages: map[string]struct{}{},
		skippedChannels: map[string]struct{}{},
	}
}

func finalizeSnapshot(snap snapshot, channelLookup map[string]store.ChannelRecord, totals scanTotals, stats *Stats, recordSkipped bool) unresolvedMessages {
	reconcileMessages(snap, channelLookup)
	inferDirectMessageNames(snap)
	reconcileMessages(snap, channelLookup)
	unresolved := unresolvedMessages{}
	for id, msg := range snap.messages {
		guildID := msg.Record.GuildID
		if guildID == "" {
			unresolved[id] = msg.Record.ChannelID
			if recordSkipped {
				totals.skippedMessages[id] = struct{}{}
				totals.skippedChannels[msg.Record.ChannelID] = struct{}{}
			}
			delete(snap.messages, id)
			continue
		}
		if _, ok := snap.guilds[guildID]; !ok {
			snap.guilds[guildID] = syntheticGuild(guildID, guildName(guildID))
		}
		if _, ok := snap.channels[msg.Record.ChannelID]; !ok {
			if channel, ok := channelLookup[msg.Record.ChannelID]; ok && channel.GuildID != "" {
				snap.channels[msg.Record.ChannelID] = channel
			} else {
				snap.channels[msg.Record.ChannelID] = syntheticChannel(msg.Record.ChannelID, guildID, msg.Record.ChannelName)
			}
		}
		snap.messages[id] = msg
	}
	for _, msg := range snap.messages {
		totals.messages[msg.Record.ID] = struct{}{}
		switch msg.Record.GuildID {
		case DirectMessageGuildID:
			totals.dmMessages[msg.Record.ID] = struct{}{}
			totals.dmChannels[msg.Record.ChannelID] = struct{}{}
		default:
			totals.guildMessages[msg.Record.ID] = struct{}{}
		}
	}
	for id, channel := range snap.channels {
		channelLookup[id] = channel
		totals.channels[id] = struct{}{}
	}
	for id := range snap.guilds {
		totals.guilds[id] = struct{}{}
	}
	stats.DMChannels = len(totals.dmChannels)
	stats.SkippedChannels = len(totals.skippedChannels)
	stats.Guilds = len(totals.guilds)
	stats.Channels = len(totals.channels)
	stats.Messages = len(totals.messages)
	stats.DMMessages = len(totals.dmMessages)
	stats.GuildMessages = len(totals.guildMessages)
	stats.SkippedMessages = len(totals.skippedMessages)
	return unresolved
}

func mergeUnresolved(dst, src unresolvedMessages) {
	maps.Copy(dst, src)
}

func recordUnresolved(unresolved unresolvedMessages, totals scanTotals, stats *Stats) {
	for messageID, channelID := range unresolved {
		totals.skippedMessages[messageID] = struct{}{}
		totals.skippedChannels[channelID] = struct{}{}
	}
	stats.SkippedChannels = len(totals.skippedChannels)
	stats.SkippedMessages = len(totals.skippedMessages)
}

func commitSnapshot(ctx context.Context, st *store.Store, opts Options, state scanState, candidates []fileCandidate, snap snapshot, checkpoint bool, stats *Stats) error {
	if opts.DryRun {
		return nil
	}
	if !checkpoint {
		if snapshotHasChanges(snap) {
			return writeSnapshot(ctx, st, snapshotWithoutMessageEvents(snap), false)
		}
		return nil
	}
	if snapshotHasChanges(snap) {
		if err := writeSnapshot(ctx, st, snap, false); err != nil {
			return err
		}
	} else if err := st.SetSyncState(ctx, "wiretap:last_import", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	for _, candidate := range candidates {
		state.current[candidate.relKey] = importedFingerprint(candidate.fingerprint)
	}
	if err := saveFileIndex(ctx, st, opts, state.current); err != nil {
		return err
	}
	stats.Checkpoints++
	return nil
}

func checkpointScannedCandidates(ctx context.Context, st *store.Store, opts Options, state scanState, candidates []fileCandidate, stats *Stats) error {
	if opts.DryRun {
		return nil
	}
	if err := st.SetSyncState(ctx, "wiretap:last_import", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	for _, candidate := range candidates {
		state.current[candidate.relKey] = importedFingerprint(candidate.fingerprint)
	}
	if err := saveFileIndex(ctx, st, opts, state.current); err != nil {
		return err
	}
	stats.Checkpoints++
	return nil
}

func snapshotWithoutMessageEvents(snap snapshot) snapshot {
	out := snapshot{
		guilds:     snap.guilds,
		channels:   snap.channels,
		messages:   make(map[string]store.MessageMutation, len(snap.messages)),
		routes:     snap.routes,
		userLabels: snap.userLabels,
	}
	for id, message := range snap.messages {
		message.Options.AppendEvent = false
		out.messages[id] = message
	}
	return out
}

func newSnapshot() snapshot {
	return snapshot{
		guilds:     map[string]store.GuildRecord{},
		channels:   map[string]store.ChannelRecord{},
		messages:   map[string]store.MessageMutation{},
		routes:     map[string]string{},
		userLabels: map[string]userLabel{},
	}
}

func newSnapshotWithContext(base snapshot) snapshot {
	snap := newSnapshot()
	maps.Copy(snap.routes, base.routes)
	maps.Copy(snap.userLabels, base.userLabels)
	return snap
}

func mergeSnapshotContext(base snapshot, next snapshot) {
	for channelID, guildID := range next.routes {
		collectChannelRoute(base, channelID, guildID)
	}
	maps.Copy(base.userLabels, next.userLabels)
	maps.Copy(base.channels, next.channels)
}

func copyChannelLookup(in map[string]store.ChannelRecord) map[string]store.ChannelRecord {
	out := make(map[string]store.ChannelRecord, len(in))
	maps.Copy(out, in)
	return out
}

func sourceForPath(root, path, relPath string) fileSource {
	if isRouteFilteredCachePath(root, path, relPath) {
		return fileSourceCacheData
	}
	return fileSourceContext
}

func isRouteFilteredCachePath(root, path, relPath string) bool {
	cleanRoot := filepath.ToSlash(root)
	cleanPath := filepath.ToSlash(path)
	cleanRel := filepath.ToSlash(relPath)
	return filepath.Base(cleanRoot) == "Cache_Data" ||
		filepath.Base(cleanRoot) == "CacheStorage" ||
		strings.Contains(cleanPath, "/Cache/Cache_Data/") ||
		strings.Contains(cleanPath, "/Service Worker/CacheStorage/") ||
		strings.HasPrefix(cleanRel, "Cache_Data/") ||
		strings.HasPrefix(cleanRel, "Service Worker/CacheStorage/")
}

func cacheFileHasRouteHint(rootFS *os.Root, relPath string) (bool, error) {
	data, err := readFilePrefix(rootFS, relPath)
	if err != nil {
		return false, err
	}
	return channelRouteRE.Match(data) || apiMessagesRouteRE.Match(data), nil
}

func readFilePrefix(rootFS *os.Root, relPath string) ([]byte, error) {
	file, err := rootFS.Open(relPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, cacheSniffBytes))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func ignoreCacheFileError(error) error {
	return nil
}

func writeSnapshot(ctx context.Context, st *store.Store, snap snapshot, prune bool) error {
	if prune {
		if err := st.DeleteGuildData(ctx, "@unknown"); err != nil {
			return err
		}
	}
	guilds := mapValues(snap.guilds)
	sort.Slice(guilds, func(i, j int) bool { return guilds[i].ID < guilds[j].ID })
	for _, guild := range guilds {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := st.UpsertGuild(ctx, guild); err != nil {
			return recordImportFailure(ctx, st, store.FailureRef{
				Operation: "import_guild",
				Source:    "wiretap",
				GuildID:   guild.ID,
			}, err)
		}
		if err := resolveImportFailures(ctx, st, store.FailureRef{
			Operation: "import_guild",
			Source:    "wiretap",
			GuildID:   guild.ID,
		}); err != nil {
			return err
		}
	}
	channels := mapValues(snap.channels)
	sort.Slice(channels, func(i, j int) bool { return channels[i].ID < channels[j].ID })
	for _, channel := range channels {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := st.UpsertChannel(ctx, channel); err != nil {
			return recordImportFailure(ctx, st, store.FailureRef{
				Operation: "import_channel",
				Source:    "wiretap",
				GuildID:   channel.GuildID,
				ChannelID: channel.ID,
			}, err)
		}
		if err := resolveImportFailures(ctx, st, store.FailureRef{
			Operation: "import_channel",
			Source:    "wiretap",
			GuildID:   channel.GuildID,
			ChannelID: channel.ID,
		}); err != nil {
			return err
		}
	}
	messages := mapValues(snap.messages)
	sort.Slice(messages, func(i, j int) bool { return messages[i].Record.ID < messages[j].Record.ID })
	if err := st.UpsertMessages(ctx, messages); err != nil {
		return recordImportFailure(ctx, st, store.FailureRef{
			Operation: "import_messages",
			Source:    "wiretap",
		}, err)
	}
	messageIDs := make([]string, 0, len(messages))
	for _, message := range messages {
		messageIDs = append(messageIDs, message.Record.ID)
	}
	if err := resolveImportFailureIdentity(ctx, st, store.FailureRef{
		Operation: "import_messages",
		Source:    "wiretap",
	}); err != nil {
		return err
	}
	if err := resolveImportMessageFailures(ctx, st, messageIDs); err != nil {
		return err
	}
	if prune {
		if err := st.DeleteOrphanChannels(ctx, DirectMessageGuildID); err != nil {
			return err
		}
	}
	return st.SetSyncState(ctx, "wiretap:last_import", time.Now().UTC().Format(time.RFC3339Nano))
}

func recordImportFailure(ctx context.Context, st *store.Store, ref store.FailureRef, failure error) error {
	ledgerCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := st.RecordFailure(ledgerCtx, ref, failure); err != nil {
		return fmt.Errorf("%w (record failure ledger: %w)", failure, err)
	}
	return failure
}

func resolveImportFailures(ctx context.Context, st *store.Store, ref store.FailureRef) error {
	ledgerCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return st.ResolveFailures(ledgerCtx, ref)
}

func resolveImportFailureIdentity(ctx context.Context, st *store.Store, ref store.FailureRef) error {
	ledgerCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return st.ResolveFailureIdentity(ledgerCtx, ref)
}

func resolveImportMessageFailures(ctx context.Context, st *store.Store, messageIDs []string) error {
	ledgerCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return st.ResolveMessageFailures(ledgerCtx, store.FailureRef{
		Operation: "import_messages",
		Source:    "wiretap",
	}, messageIDs)
}

func collectValue(snap snapshot, channelLookup map[string]store.ChannelRecord, value any, fallbackTime time.Time) {
	switch typed := value.(type) {
	case map[string]any:
		collectUserLabel(snap, typed)
		collectSelectedDirectMessageRoutes(snap, typed)
		if channel, ok := parseChannel(typed); ok {
			snap.channels[channel.ID] = channel
			channelLookup[channel.ID] = channel
			if channel.GuildID == DirectMessageGuildID {
				if _, ok := snap.guilds[channel.GuildID]; !ok {
					snap.guilds[channel.GuildID] = syntheticGuild(channel.GuildID, guildName(channel.GuildID))
				}
			}
		}
		if message, ok := parseMessage(typed, fallbackTime, channelLookup); ok {
			snap.messages[message.Record.ID] = message
		}
		for _, child := range typed {
			collectValue(snap, channelLookup, child, fallbackTime)
		}
	case []any:
		for _, child := range typed {
			collectValue(snap, channelLookup, child, fallbackTime)
		}
	}
}

func collectChannelRoutes(snap snapshot, data []byte) {
	for _, match := range channelRouteRE.FindAllSubmatch(data, -1) {
		if len(match) != 3 {
			continue
		}
		guildID := string(match[1])
		channelID := string(match[2])
		if !looksSnowflake(channelID) {
			continue
		}
		collectChannelRoute(snap, channelID, guildID)
	}
}

func collectSelectedDirectMessageRoutes(snap snapshot, raw map[string]any) {
	for _, candidate := range selectedChannelRouteCandidates(raw) {
		if selected, _ := candidate["selectedChannelIds"].(map[string]any); selected != nil {
			if channelID := stringField(selected, "null"); looksSnowflake(channelID) {
				collectChannelRoute(snap, channelID, DirectMessageGuildID)
			}
		}
		if guildValue, hasGuild := candidate["selectedGuildId"]; hasGuild && guildValue == nil {
			if channelID := stringField(candidate, "selectedChannelId"); looksSnowflake(channelID) {
				collectChannelRoute(snap, channelID, DirectMessageGuildID)
			}
		}
	}
}

func selectedChannelRouteCandidates(raw map[string]any) []map[string]any {
	candidates := []map[string]any{raw}
	for _, key := range []string{"_state", "state"} {
		if child, _ := raw[key].(map[string]any); child != nil {
			candidates = append(candidates, child)
		}
	}
	return candidates
}

func collectChannelRoute(snap snapshot, channelID, guildID string) {
	if !looksSnowflake(channelID) || guildID == "" {
		return
	}
	if existing, ok := snap.routes[channelID]; ok && existing != guildID {
		snap.routes[channelID] = ""
		return
	}
	snap.routes[channelID] = guildID
}

func parseChannel(raw map[string]any) (store.ChannelRecord, bool) {
	id := stringField(raw, "id")
	if !looksSnowflake(id) {
		return store.ChannelRecord{}, false
	}
	if _, hasChannelID := raw["channel_id"]; hasChannelID {
		return store.ChannelRecord{}, false
	}
	typeValue, hasType := intField(raw, "type")
	name := strings.TrimSpace(stringField(raw, "name"))
	recipients, hasRecipients := raw["recipients"].([]any)
	guildID := stringField(raw, "guild_id")
	isDM := guildID == "" && (typeValue == 1 || typeValue == 3 || hasRecipients)
	if !hasType && !hasRecipients && name == "" {
		return store.ChannelRecord{}, false
	}
	if isDM {
		guildID = DirectMessageGuildID
	}
	if guildID == "" {
		return store.ChannelRecord{}, false
	}
	if name == "" {
		name = recipientLabel(recipients)
	}
	if name == "" {
		if isDM {
			name = "dm-" + shortID(id)
		} else {
			name = "channel-" + shortID(id)
		}
	}
	rawJSON := channelRawJSON(raw, id, guildID, name, kindForChannelType(typeValue, isDM))
	return store.ChannelRecord{
		ID:      id,
		GuildID: guildID,
		Kind:    kindForChannelType(typeValue, isDM),
		Name:    name,
		RawJSON: rawJSON,
	}, true
}

func parseMessage(raw map[string]any, fallbackTime time.Time, channels map[string]store.ChannelRecord) (store.MessageMutation, bool) {
	id := stringField(raw, "id")
	channelID := stringField(raw, "channel_id")
	if !looksSnowflake(id) || !looksSnowflake(channelID) {
		return store.MessageMutation{}, false
	}
	author, _ := raw["author"].(map[string]any)
	content, hasContent := raw["content"].(string)
	if !hasContent && len(author) == 0 {
		return store.MessageMutation{}, false
	}
	createdAt := parseDiscordTime(stringField(raw, "timestamp"))
	if createdAt.IsZero() {
		createdAt = snowflakeTime(id)
	}
	if createdAt.IsZero() {
		createdAt = fallbackTime
	}
	if createdAt.IsZero() {
		return store.MessageMutation{}, false
	}
	guildID := stringField(raw, "guild_id")
	if guildID == "" {
		if channel, ok := channels[channelID]; ok && channel.GuildID != "" {
			guildID = channel.GuildID
		}
	}
	channelName := "channel-" + shortID(channelID)
	if channel, ok := channels[channelID]; ok && channel.Name != "" {
		channelName = channel.Name
	}
	authorID := stringField(author, "id")
	authorName := firstNonEmpty(
		stringField(author, "global_name"),
		stringField(author, "display_name"),
		stringField(author, "username"),
	)
	msgType, _ := intField(raw, "type")
	editedAt := parseDiscordTime(stringField(raw, "edited_timestamp"))
	attachments := parseAttachments(raw, id, guildID, channelID, authorID)
	mentions := parseMentions(raw, id, guildID, channelID, authorID, createdAt)
	normalized := normalizeText(content, attachmentText(attachments), embedText(raw))
	return store.MessageMutation{
		Record: store.MessageRecord{
			ID:                id,
			GuildID:           guildID,
			ChannelID:         channelID,
			ChannelName:       channelName,
			AuthorID:          authorID,
			AuthorName:        authorName,
			MessageType:       msgType,
			CreatedAt:         createdAt.UTC().Format(time.RFC3339Nano),
			EditedAt:          formatOptionalTime(editedAt),
			Content:           content,
			NormalizedContent: normalized,
			ReplyToMessageID:  messageReferenceID(raw),
			Pinned:            boolField(raw, "pinned"),
			HasAttachments:    len(attachments) > 0,
			RawJSON:           messageRawJSON(raw, id, guildID, channelID, authorID),
		},
		EventType:   "wiretap",
		PayloadJSON: messageRawJSON(raw, id, guildID, channelID, authorID),
		Options: store.WriteOptions{
			AppendEvent:      true,
			EnqueueEmbedding: false,
		},
		Attachments: attachments,
		Mentions:    mentions,
	}, true
}

func reconcileMessages(snap snapshot, channelLookup map[string]store.ChannelRecord) {
	for id, msg := range snap.messages {
		channel, ok := channelLookup[msg.Record.ChannelID]
		if !ok {
			if guildID := snap.routes[msg.Record.ChannelID]; guildID != "" {
				msg.Record.GuildID = guildID
				if guildID == DirectMessageGuildID {
					channel = syntheticChannel(msg.Record.ChannelID, guildID, "")
					snap.channels[msg.Record.ChannelID] = channel
					channelLookup[msg.Record.ChannelID] = channel
					ok = true
				}
			}
		}
		if !ok {
			if msg.Record.GuildID != "" {
				for i := range msg.Attachments {
					msg.Attachments[i].GuildID = msg.Record.GuildID
				}
				for i := range msg.Mentions {
					msg.Mentions[i].GuildID = msg.Record.GuildID
				}
				msg.Record.RawJSON = withRawGuildID(msg.Record.RawJSON, msg.Record.GuildID)
				msg.PayloadJSON = withRawGuildID(msg.PayloadJSON, msg.Record.GuildID)
			}
			snap.messages[id] = msg
			continue
		}
		if channel.GuildID != "" {
			msg.Record.GuildID = channel.GuildID
			for i := range msg.Attachments {
				msg.Attachments[i].GuildID = channel.GuildID
			}
			for i := range msg.Mentions {
				msg.Mentions[i].GuildID = channel.GuildID
			}
		}
		if channel.Name != "" {
			msg.Record.ChannelName = channel.Name
		}
		msg.Record.RawJSON = withRawGuildID(msg.Record.RawJSON, msg.Record.GuildID)
		msg.PayloadJSON = withRawGuildID(msg.PayloadJSON, msg.Record.GuildID)
		snap.messages[id] = msg
	}
}

func withRawGuildID(rawJSON, guildID string) string {
	if rawJSON == "" || guildID == "" {
		return rawJSON
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(rawJSON), &raw); err != nil {
		return rawJSON
	}
	raw["guild_id"] = guildID
	body, err := json.Marshal(raw)
	if err != nil {
		return rawJSON
	}
	return string(body)
}

func extractGzipPayloads(data []byte, maxBytes int64) [][]byte {
	var out [][]byte
	for offset := range len(data) - 1 {
		if data[offset] != 0x1f || data[offset+1] != 0x8b {
			continue
		}
		reader, err := gzip.NewReader(bytes.NewReader(data[offset:]))
		if err != nil {
			continue
		}
		reader.Multistream(false)
		payload, readErr := io.ReadAll(io.LimitReader(reader, maxBytes+1))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || int64(len(payload)) > maxBytes {
			continue
		}
		out = append(out, payload)
	}
	return out
}

func extractJSONValues(data []byte) [][]byte {
	candidate := bytes.TrimSpace(data)
	if len(candidate) <= maxObjectBytes && len(candidate) > 0 && json.Valid(candidate) {
		switch candidate[0] {
		case '{', '[':
			return [][]byte{append([]byte(nil), candidate...)}
		}
	}
	return extractJSONObjects(data)
}

func extractJSONObjects(data []byte) [][]byte {
	var out [][]byte
	depth := 0
	start := -1
	inString := false
	escaped := false
	for i, b := range data {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch b {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			if depth > 0 {
				inString = true
			}
		case '{':
			if depth == 0 {
				start = i
			}
			depth++
		case '}':
			if depth == 0 {
				continue
			}
			depth--
			if depth == 0 && start >= 0 {
				if i-start+1 <= maxObjectBytes {
					candidate := bytes.TrimSpace(data[start : i+1])
					if json.Valid(candidate) {
						out = append(out, append([]byte(nil), candidate...))
					}
				}
				start = -1
			}
		}
	}
	return out
}

func shouldSkipDir(name string) bool {
	switch strings.ToLower(name) {
	case "blob_storage", "component_crx_cache", "crashpad", "dawngraphitecache",
		"dawnwebgpucache", "download_cache", "gpucache", "gpu-cache",
		"shadercache", "spellcheck", "videodecodestats", "widevinecdm":
		return true
	default:
		return false
	}
}

func isCandidateFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".ldb", ".log", ".json", ".txt":
		return true
	default:
		clean := filepath.ToSlash(path)
		return strings.Contains(clean, "/Cache/Cache_Data/") ||
			strings.Contains(clean, "/Service Worker/CacheStorage/") ||
			strings.Contains(clean, "/WebStorage/")
	}
}

func parseAttachments(raw map[string]any, messageID, guildID, channelID, authorID string) []store.AttachmentRecord {
	items, _ := raw["attachments"].([]any)
	out := make([]store.AttachmentRecord, 0, len(items))
	for i, item := range items {
		attachment, _ := item.(map[string]any)
		if len(attachment) == 0 {
			continue
		}
		id := stringField(attachment, "id")
		if id == "" {
			id = fmt.Sprintf("%s:%d", messageID, i)
		}
		out = append(out, store.AttachmentRecord{
			AttachmentID: id,
			MessageID:    messageID,
			GuildID:      guildID,
			ChannelID:    channelID,
			AuthorID:     authorID,
			Filename:     firstNonEmpty(stringField(attachment, "filename"), id),
			ContentType:  stringField(attachment, "content_type"),
			Size:         int64Field(attachment, "size"),
			URL:          stringField(attachment, "url"),
			ProxyURL:     stringField(attachment, "proxy_url"),
		})
	}
	return out
}

func parseMentions(raw map[string]any, messageID, guildID, channelID, authorID string, eventAt time.Time) []store.MentionEventRecord {
	items, _ := raw["mentions"].([]any)
	out := make([]store.MentionEventRecord, 0, len(items))
	for _, item := range items {
		mention, _ := item.(map[string]any)
		id := stringField(mention, "id")
		if id == "" {
			continue
		}
		out = append(out, store.MentionEventRecord{
			MessageID:  messageID,
			GuildID:    guildID,
			ChannelID:  channelID,
			AuthorID:   authorID,
			TargetType: "user",
			TargetID:   id,
			TargetName: firstNonEmpty(stringField(mention, "global_name"), stringField(mention, "username")),
			EventAt:    eventAt.UTC().Format(time.RFC3339Nano),
		})
	}
	return out
}

func attachmentText(attachments []store.AttachmentRecord) []string {
	out := make([]string, 0, len(attachments))
	for _, attachment := range attachments {
		out = append(out, attachment.Filename)
	}
	return out
}

func embedText(raw map[string]any) []string {
	items, _ := raw["embeds"].([]any)
	out := []string{}
	for _, item := range items {
		embed, _ := item.(map[string]any)
		for _, key := range []string{"title", "description"} {
			if value := strings.TrimSpace(stringField(embed, key)); value != "" {
				out = append(out, value)
			}
		}
	}
	return out
}

func normalizeText(parts ...any) string {
	flat := []string{}
	for _, part := range parts {
		switch typed := part.(type) {
		case string:
			if text := cleanText(typed); text != "" {
				flat = append(flat, text)
			}
		case []string:
			for _, item := range typed {
				if text := cleanText(item); text != "" {
					flat = append(flat, text)
				}
			}
		}
	}
	return strings.Join(flat, "\n")
}

func cleanText(raw string) string {
	raw = strings.ToValidUTF8(raw, "")
	var b strings.Builder
	spacePending := false
	for _, r := range raw {
		switch {
		case r == '\u200b' || r == '\u200c' || r == '\u200d' || r == '\ufeff':
			continue
		case unicode.IsControl(r):
			continue
		case unicode.IsSpace(r):
			spacePending = b.Len() > 0
		default:
			if spacePending {
				b.WriteByte(' ')
				spacePending = false
			}
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

func messageReferenceID(raw map[string]any) string {
	ref, _ := raw["message_reference"].(map[string]any)
	return stringField(ref, "message_id")
}

func syntheticGuild(id, name string) store.GuildRecord {
	raw := marshalJSONString(map[string]any{
		"id":     id,
		"name":   name,
		"source": "discord_desktop",
	}, "{}")
	return store.GuildRecord{ID: id, Name: name, RawJSON: raw}
}

func syntheticChannel(id, guildID, name string) store.ChannelRecord {
	if name == "" {
		name = "channel-" + shortID(id)
	}
	raw := marshalJSONString(map[string]any{
		"id":       id,
		"guild_id": guildID,
		"name":     name,
		"source":   "discord_desktop",
	}, "{}")
	kind := "text"
	if guildID == DirectMessageGuildID {
		kind = "dm"
		if strings.Contains(name, ", ") {
			kind = "group_dm"
		}
	}
	return store.ChannelRecord{ID: id, GuildID: guildID, Kind: kind, Name: name, RawJSON: raw}
}

func guildName(id string) string {
	switch id {
	case DirectMessageGuildID:
		return DirectMessageGuildName
	default:
		return "Discord Desktop Guild " + id
	}
}

func kindForChannelType(typeValue int, dm bool) string {
	if dm {
		if typeValue == 3 {
			return "group_dm"
		}
		return "dm"
	}
	switch typeValue {
	case 0:
		return "text"
	case 5:
		return "announcement"
	case 10:
		return "thread_announcement"
	case 11:
		return "thread_public"
	case 12:
		return "thread_private"
	case 15:
		return "forum"
	default:
		return "desktop"
	}
}

func channelRawJSON(raw map[string]any, id, guildID, name, kind string) string {
	return marshalJSONString(map[string]any{
		"id":       id,
		"guild_id": guildID,
		"name":     name,
		"kind":     kind,
		"source":   "discord_desktop",
		"type":     raw["type"],
	}, "{}")
}

func messageRawJSON(raw map[string]any, id, guildID, channelID, authorID string) string {
	payload := map[string]any{
		"id":                 id,
		"guild_id":           guildID,
		"channel_id":         channelID,
		"author_id":          authorID,
		"source":             "discord_desktop",
		"type":               raw["type"],
		"timestamp":          raw["timestamp"],
		"edited_timestamp":   raw["edited_timestamp"],
		"message_reference":  raw["message_reference"],
		"attachment_count":   lenArray(raw["attachments"]),
		"mention_count":      lenArray(raw["mentions"]),
		"desktop_cache_note": "raw desktop cache payload intentionally not stored",
	}
	if author := sanitizedRawAuthor(raw, authorID); len(author) > 0 {
		payload["author"] = author
	}
	return marshalJSONString(payload, "{}")
}

func recipientLabel(items []any) string {
	names := []string{}
	for _, item := range items {
		recipient, _ := item.(map[string]any)
		name := firstNonEmpty(
			stringField(recipient, "global_name"),
			stringField(recipient, "display_name"),
			stringField(recipient, "username"),
		)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func parseDiscordTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC()
	}
	return time.Time{}
}

func snowflakeTime(id string) time.Time {
	value, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return time.Time{}
	}
	ms := int64((value >> 22) + 1420070400000)
	return time.UnixMilli(ms).UTC()
}

func formatOptionalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func looksSnowflake(value string) bool {
	if len(value) < 12 || len(value) > 24 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func shortID(id string) string {
	if len(id) <= 6 {
		return id
	}
	return id[len(id)-6:]
}

func stringField(raw map[string]any, key string) string {
	value, ok := raw[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func intField(raw map[string]any, key string) (int, bool) {
	value, ok := raw[key]
	if !ok || value == nil {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return int(typed), true
	case int:
		return typed, true
	case json.Number:
		i, err := typed.Int64()
		return int(i), err == nil
	default:
		return 0, false
	}
}

func int64Field(raw map[string]any, key string) int64 {
	value, ok := raw[key]
	if !ok || value == nil {
		return 0
	}
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case int64:
		return typed
	case int:
		return int64(typed)
	case json.Number:
		i, _ := typed.Int64()
		return i
	default:
		return 0
	}
}

func boolField(raw map[string]any, key string) bool {
	value, _ := raw[key].(bool)
	return value
}

func lenArray(value any) int {
	items, _ := value.([]any)
	return len(items)
}

func firstNonEmpty(items ...string) string {
	for _, item := range items {
		if strings.TrimSpace(item) != "" {
			return strings.TrimSpace(item)
		}
	}
	return ""
}

func mapValues[M ~map[string]T, T any](m M) []T {
	out := make([]T, 0, len(m))
	for _, value := range m {
		out = append(out, value)
	}
	return out
}

func marshalJSONString(value any, fallback string) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fallback
	}
	return string(raw)
}
