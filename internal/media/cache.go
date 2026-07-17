package media

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/openclaw/discrawl/internal/store"
)

const (
	DefaultMaxBytes int64 = 100 << 20
	cacheSubdir           = "media"
)

type FetchOptions struct {
	CacheDir     string
	List         store.AttachmentListOptions
	MaxBytes     int64
	Force        bool
	HTTPClient   *http.Client
	Now          func() time.Time
	StatusUpdate bool
}

type FetchStats struct {
	Attachments int   `json:"attachments"`
	Fetched     int   `json:"fetched"`
	Reused      int   `json:"reused"`
	Skipped     int   `json:"skipped"`
	Failed      int   `json:"failed"`
	Bytes       int64 `json:"bytes"`
}

func Fetch(ctx context.Context, s *store.Store, opts FetchOptions) (FetchStats, error) {
	if strings.TrimSpace(opts.CacheDir) == "" {
		return FetchStats{}, errors.New("cache dir is required")
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = DefaultMaxBytes
	}
	opts.HTTPClient = attachmentHTTPClient(opts.HTTPClient)
	if opts.Now == nil {
		opts.Now = time.Now
	}
	list := opts.List
	limit := list.Limit
	missingOnly := list.MissingOnly
	// A stored media_path is only metadata; the cache file may have been deleted
	// or intentionally omitted from a Git snapshot import. Fetch all matching
	// rows and skip only after checking the filesystem.
	if missingOnly || !opts.Force {
		list.MissingOnly = false
		list.Limit = 0
	}
	attachments, err := s.ListAttachments(ctx, list)
	if err != nil {
		return FetchStats{}, err
	}
	stats := FetchStats{}
	for _, attachment := range attachments {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if attachment.MediaPath != "" && (missingOnly || !opts.Force) {
			if mediaFileReusable(opts.CacheDir, attachment) {
				if err := resolveAttachmentFailure(ctx, s, attachment); err != nil {
					return stats, err
				}
				stats.Reused++
				continue
			}
		}
		if limit > 0 && stats.Attachments >= limit {
			break
		}
		stats.Attachments++
		result, err := fetchOne(ctx, opts, attachment)
		switch {
		case err != nil:
			stats.Failed++
			if recordErr := s.RecordFailure(ctx, attachmentFailureRef(attachment), err); recordErr != nil {
				return stats, recordErr
			}
			if opts.StatusUpdate {
				if err := s.UpdateAttachmentFetchStatus(ctx, attachment.AttachmentID, opts.Now().UTC().Format(time.RFC3339Nano), "failed", clampError(err.Error())); err != nil {
					_ = s.RecordFailure(ctx, attachmentFailureRef(attachment), err)
					return stats, err
				}
			}
		case result.status == "skipped":
			stats.Skipped++
			if opts.StatusUpdate {
				if err := s.UpdateAttachmentFetchStatus(ctx, attachment.AttachmentID, opts.Now().UTC().Format(time.RFC3339Nano), result.reason, ""); err != nil {
					return stats, err
				}
			}
			if err := resolveAttachmentFailure(ctx, s, attachment); err != nil {
				return stats, err
			}
		default:
			stats.Fetched++
			stats.Bytes += result.size
			if err := s.UpdateAttachmentMedia(ctx, store.AttachmentMediaUpdate{
				AttachmentID:  attachment.AttachmentID,
				MediaPath:     result.mediaPath,
				ContentSHA256: result.sha256,
				ContentSize:   result.size,
				FetchedAt:     opts.Now().UTC().Format(time.RFC3339Nano),
				FetchStatus:   "fetched",
			}); err != nil {
				_ = s.RecordFailure(ctx, attachmentFailureRef(attachment), err)
				return stats, err
			}
			if err := resolveAttachmentFailure(ctx, s, attachment); err != nil {
				return stats, err
			}
		}
	}
	return stats, nil
}

func attachmentHTTPClient(client *http.Client) *http.Client {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	clone := *client
	previous := clone.CheckRedirect
	clone.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 || !isAllowedAttachmentURL(req.URL.String()) {
			return errors.New("attachment redirect denied")
		}
		if previous != nil {
			if err := previous(req, via); err != nil {
				return err
			}
			if !isAllowedAttachmentURL(req.URL.String()) {
				return errors.New("attachment redirect denied")
			}
		}
		return nil
	}
	return &clone
}

func attachmentFailureRef(attachment store.AttachmentRow) store.FailureRef {
	return store.FailureRef{
		Operation:   "fetch_attachment",
		Source:      "media",
		GuildID:     attachment.GuildID,
		ChannelID:   attachment.ChannelID,
		MessageID:   attachment.MessageID,
		RelatedKind: "attachment_id",
		RelatedID:   attachment.AttachmentID,
	}
}

func resolveAttachmentFailure(ctx context.Context, s *store.Store, attachment store.AttachmentRow) error {
	return s.ResolveFailures(ctx, attachmentFailureRef(attachment))
}

func mediaFileReusable(cacheDir string, attachment store.AttachmentRow) bool {
	path, err := LocalPath(cacheDir, attachment.MediaPath)
	if err != nil {
		return false
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	if attachment.ContentSHA256 == "" {
		return true
	}
	current, err := fileSHA256(path)
	return err == nil && current == attachment.ContentSHA256
}

type fetchResult struct {
	status    string
	reason    string
	mediaPath string
	sha256    string
	size      int64
}

func fetchOne(ctx context.Context, opts FetchOptions, attachment store.AttachmentRow) (fetchResult, error) {
	urls, invalidURL := candidateURLs(attachment)
	if len(urls) == 0 {
		if invalidURL {
			return fetchResult{status: "skipped", reason: "invalid_url"}, nil
		}
		return fetchResult{status: "skipped", reason: "no_url"}, nil
	}
	var lastErr error
	for _, url := range urls {
		result, err := fetchURL(ctx, opts, attachment, url)
		if err == nil || result.status == "skipped" {
			return result, err
		}
		lastErr = err
	}
	return fetchResult{}, lastErr
}

func candidateURLs(attachment store.AttachmentRow) ([]string, bool) {
	seen := map[string]struct{}{}
	out := []string{}
	invalidURL := false
	for _, raw := range []string{attachment.URL, attachment.ProxyURL} {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if _, ok := seen[raw]; ok {
			continue
		}
		seen[raw] = struct{}{}
		if !isAllowedAttachmentURL(raw) {
			invalidURL = true
			continue
		}
		out = append(out, raw)
	}
	return out, invalidURL
}

func fetchURL(ctx context.Context, opts FetchOptions, attachment store.AttachmentRow, url string) (fetchResult, error) {
	if !isAllowedAttachmentURL(url) {
		return fetchResult{status: "skipped", reason: "invalid_url"}, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fetchResult{}, errors.New("build attachment fetch request")
	}
	resp, err := opts.HTTPClient.Do(req)
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			return fetchResult{}, context.Canceled
		case errors.Is(err, context.DeadlineExceeded):
			return fetchResult{}, context.DeadlineExceeded
		default:
			return fetchResult{}, errors.New("attachment fetch request failed")
		}
	}
	defer func() { _ = resp.Body.Close() }()
	// Injected clients can supply their own redirect policy, so validate the
	// final response URL at the fetch boundary as well.
	if resp.Request == nil || resp.Request.URL == nil || !isAllowedAttachmentURL(resp.Request.URL.String()) {
		return fetchResult{}, errors.New("attachment response URL denied")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fetchResult{}, fmt.Errorf("attachment fetch returned HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength > opts.MaxBytes {
		return fetchResult{status: "skipped", reason: "too_large"}, nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, opts.MaxBytes+1))
	if err != nil {
		return fetchResult{}, err
	}
	if int64(len(body)) > opts.MaxBytes {
		return fetchResult{status: "skipped", reason: "too_large"}, nil
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := attachmentMediaPath(hash, attachment.Filename, attachment.ContentType)
	target, err := LocalPath(opts.CacheDir, mediaPath)
	if err != nil {
		return fetchResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fetchResult{}, err
	}
	needsWrite, err := mediaTargetNeedsWrite(target, hash)
	if err != nil {
		return fetchResult{}, err
	}
	if needsWrite || opts.Force {
		tmp, err := os.CreateTemp(filepath.Dir(target), ".download-*")
		if err != nil {
			return fetchResult{}, err
		}
		tmpPath := tmp.Name()
		if _, err := tmp.Write(body); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
			return fetchResult{}, err
		}
		if err := tmp.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return fetchResult{}, err
		}
		if info, err := os.Lstat(target); err == nil && !info.Mode().IsRegular() {
			_ = os.Remove(target)
		}
		if err := os.Rename(tmpPath, target); err != nil {
			_ = os.Remove(tmpPath)
			return fetchResult{}, err
		}
	}
	return fetchResult{mediaPath: mediaPath, sha256: hash, size: int64(len(body))}, nil
}

func isAllowedAttachmentURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return false
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	switch {
	case host == "cdn.discordapp.com", host == "media.discordapp.net":
		return true
	case strings.HasPrefix(host, "images-ext-") && strings.HasSuffix(host, ".discordapp.net"):
		shard := strings.TrimSuffix(strings.TrimPrefix(host, "images-ext-"), ".discordapp.net")
		return allDigits(shard)
	default:
		return false
	}
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func mediaTargetNeedsWrite(target, hash string) (bool, error) {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return true, nil
	}
	current, err := fileSHA256(target)
	if err != nil {
		return false, err
	}
	return current != hash, nil
}

func attachmentMediaPath(hash, filename, contentType string) string {
	name := safeFilename(filename)
	if name == "" {
		name = "attachment" + extensionForContentType(contentType)
	}
	name = truncateFilename(name, 190)
	return filepath.ToSlash(filepath.Join("attachments", hash[:2], hash+"-"+name))
}

func truncateFilename(name string, maxLen int) string {
	if len(name) <= maxLen {
		return name
	}
	ext := filepath.Ext(name)
	if len(ext) >= maxLen {
		return name[:maxLen]
	}
	baseLen := maxLen - len(ext)
	if baseLen <= 0 {
		return name[:maxLen]
	}
	base := strings.TrimRight(name[:baseLen], ".-")
	if base == "" {
		return strings.TrimLeft(ext, ".")
	}
	return base + ext
}

func extensionForContentType(contentType string) string {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil || mediaType == "" {
		return ""
	}
	exts, err := mime.ExtensionsByType(mediaType)
	if err != nil || len(exts) == 0 {
		return ""
	}
	return exts[0]
}

func safeFilename(raw string) string {
	raw = filepath.Base(strings.TrimSpace(raw))
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r == '.' || r == '-' || r == '_':
			b.WriteRune(r)
		case r <= unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r)):
			b.WriteRune(r)
		case unicode.IsSpace(r):
			b.WriteByte('-')
		}
	}
	return strings.Trim(strings.TrimSpace(b.String()), ".-")
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path) // #nosec G304 -- callers pass confined cache paths.
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

func LocalPath(cacheDir, mediaPath string) (string, error) {
	root := filepath.Clean(filepath.Join(cacheDir, cacheSubdir))
	clean := filepath.Clean(filepath.FromSlash(strings.TrimSpace(mediaPath)))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid media path %q", mediaPath)
	}
	full := filepath.Clean(filepath.Join(root, clean))
	if full != root && !strings.HasPrefix(full, root+string(filepath.Separator)) {
		return "", fmt.Errorf("media path escapes cache: %q", mediaPath)
	}
	return full, nil
}

func RepoPath(repoPath, mediaPath string) (string, error) {
	root := filepath.Clean(filepath.Join(repoPath, cacheSubdir))
	clean := filepath.Clean(filepath.FromSlash(strings.TrimSpace(mediaPath)))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid media path %q", mediaPath)
	}
	full := filepath.Clean(filepath.Join(root, clean))
	if full != root && !strings.HasPrefix(full, root+string(filepath.Separator)) {
		return "", fmt.Errorf("media path escapes repo: %q", mediaPath)
	}
	return full, nil
}

func clampError(message string) string {
	message = strings.TrimSpace(message)
	if len(message) <= 512 {
		return message
	}
	return message[:512]
}
