package discorddesktop

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

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
			if err := saveFileIndex(ctx, st, state.current); err != nil {
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
	messageSources := map[string][]string{}
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
		if previous, ok := state.previous[relKey]; ok && sameFileFingerprint(previous, fingerprint) && isImportedFingerprint(previous) {
			state.current[relKey] = previous
			stats.FilesUnchanged++
			return nil
		}
		state.current[relKey] = skippedFingerprint(fingerprint)
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
		// Share channel context while retaining each message's source files.
		fileSnap := snap
		fileSnap.messages = make(map[string]store.MessageMutation)
		for _, raw := range objects {
			if err := ctx.Err(); err != nil {
				return err
			}
			var value any
			if err := json.Unmarshal(raw, &value); err != nil {
				continue
			}
			collectValue(fileSnap, state.channels, value, info.ModTime().UTC())
		}
		for id, message := range fileSnap.messages {
			snap.messages[id] = message
			messageSources[id] = append(messageSources[id], relKey)
		}
		state.current[relKey] = importedFingerprint(fingerprint)
		return nil
	}); err != nil {
		return stats, snap, err
	}
	totals := newScanTotals()
	for id := range finalizeSnapshot(snap, state.channels, totals, &stats, true) {
		for _, key := range messageSources[id] {
			state.current[key] = skippedFingerprint(state.current[key])
		}
	}
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
		if previous, ok := state.previous[relKey]; ok && sameFileFingerprint(previous, fingerprint) && isImportedFingerprint(previous) {
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
