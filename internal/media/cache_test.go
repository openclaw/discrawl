package media

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestFetchCachesAttachmentMedia(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	body := []byte("image-bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/file.png" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, seedAttachment(ctx, s, server.URL+"/file.png"))

	cacheDir := t.TempDir()
	stats, err := Fetch(ctx, s, FetchOptions{
		CacheDir:     cacheDir,
		MaxBytes:     1024,
		StatusUpdate: true,
		Now:          func() time.Time { return time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC) },
	})
	require.NoError(t, err)
	require.Equal(t, FetchStats{Attachments: 1, Fetched: 1, Bytes: int64(len(body))}, stats)

	rows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "fetched", rows[0].FetchStatus)
	sum := sha256.Sum256(body)
	require.Equal(t, hex.EncodeToString(sum[:]), rows[0].ContentSHA256)
	path, err := LocalPath(cacheDir, rows[0].MediaPath)
	require.NoError(t, err)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, body, got)

	stats, err = Fetch(ctx, s, FetchOptions{CacheDir: cacheDir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Reused)
}

func TestFetchLimitAppliesAfterExistingCacheCheck(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, seedAttachmentWithIDs(ctx, s, "m1", "a1", "https://example.test/one.png"))
	require.NoError(t, seedAttachmentWithIDs(ctx, s, "m2", "a2", "https://example.test/two.png"))

	cacheDir := t.TempDir()
	_, err = Fetch(ctx, s, FetchOptions{
		CacheDir:   cacheDir,
		MaxBytes:   1024,
		HTTPClient: staticHTTPClient([]byte("one")),
		List:       store.AttachmentListOptions{MessageID: "m1"},
	})
	require.NoError(t, err)

	stats, err := Fetch(ctx, s, FetchOptions{
		CacheDir:   cacheDir,
		MaxBytes:   1024,
		HTTPClient: staticHTTPClient([]byte("two")),
		List:       store.AttachmentListOptions{Limit: 1, MissingOnly: true},
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Reused)
	require.Equal(t, 1, stats.Attachments)
	require.Equal(t, 1, stats.Fetched)
}

func TestFetchForceRewritesCachedMedia(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	body := []byte("canonical")
	require.NoError(t, seedAttachment(ctx, s, "https://example.test/file.png"))

	cacheDir := t.TempDir()
	_, err = Fetch(ctx, s, FetchOptions{
		CacheDir:   cacheDir,
		MaxBytes:   1024,
		HTTPClient: staticHTTPClient(body),
	})
	require.NoError(t, err)
	rows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	path, err := LocalPath(cacheDir, rows[0].MediaPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte("corrupt"), 0o600))

	stats, err := Fetch(ctx, s, FetchOptions{
		CacheDir:   cacheDir,
		MaxBytes:   1024,
		HTTPClient: staticHTTPClient(body),
		Force:      true,
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Fetched)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestFetchForceMissingSkipsReusableCachedMedia(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, seedAttachmentWithIDs(ctx, s, "m1", "a1", "https://example.test/one.png"))
	require.NoError(t, seedAttachmentWithIDs(ctx, s, "m2", "a2", "https://example.test/two.png"))

	cacheDir := t.TempDir()
	_, err = Fetch(ctx, s, FetchOptions{
		CacheDir:   cacheDir,
		MaxBytes:   1024,
		HTTPClient: staticHTTPClient([]byte("one")),
		List:       store.AttachmentListOptions{MessageID: "m1"},
	})
	require.NoError(t, err)

	stats, err := Fetch(ctx, s, FetchOptions{
		CacheDir:   cacheDir,
		MaxBytes:   1024,
		HTTPClient: staticHTTPClient([]byte("two")),
		Force:      true,
		List:       store.AttachmentListOptions{Limit: 1, MissingOnly: true},
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Reused)
	require.Equal(t, 1, stats.Attachments)
	require.Equal(t, 1, stats.Fetched)

	rows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	path, err := LocalPath(cacheDir, rows[0].MediaPath)
	require.NoError(t, err)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("one"), got)
}

func TestFetchRepairsCorruptCachedMedia(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	body := []byte("canonical")
	require.NoError(t, seedAttachment(ctx, s, "https://example.test/file.png"))

	cacheDir := t.TempDir()
	_, err = Fetch(ctx, s, FetchOptions{
		CacheDir:   cacheDir,
		MaxBytes:   1024,
		HTTPClient: staticHTTPClient(body),
	})
	require.NoError(t, err)
	rows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	path, err := LocalPath(cacheDir, rows[0].MediaPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte("corrupt"), 0o600))

	stats, err := Fetch(ctx, s, FetchOptions{
		CacheDir:   cacheDir,
		MaxBytes:   1024,
		HTTPClient: staticHTTPClient(body),
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Fetched)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestFetchCapsLongCacheFilename(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	longName := strings.Repeat("a", 320) + ".png"
	require.NoError(t, seedAttachmentRecord(ctx, s, "m1", "a1", longName, "https://example.test/file.png"))

	cacheDir := t.TempDir()
	_, err = Fetch(ctx, s, FetchOptions{
		CacheDir:   cacheDir,
		MaxBytes:   1024,
		HTTPClient: staticHTTPClient([]byte("image")),
	})
	require.NoError(t, err)
	rows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.LessOrEqual(t, len(filepath.Base(rows[0].MediaPath)), 255)
	require.Truef(t, strings.HasSuffix(filepath.Base(rows[0].MediaPath), ".png"), "media path %q", rows[0].MediaPath)
	path, err := LocalPath(cacheDir, rows[0].MediaPath)
	require.NoError(t, err)
	require.FileExists(t, path)
}

func TestFetchRecordsSkippedAndFailedStatuses(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, seedAttachmentWithIDs(ctx, s, "m1", "a1", ""))
	require.NoError(t, seedAttachmentWithIDs(ctx, s, "m2", "a2", "https://example.test/fail.png"))

	stats, err := Fetch(ctx, s, FetchOptions{
		CacheDir:     t.TempDir(),
		MaxBytes:     1024,
		StatusUpdate: true,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New(strings.Repeat("x", 600))
		})},
	})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Attachments)
	require.Equal(t, 1, stats.Skipped)
	require.Equal(t, 1, stats.Failed)

	rows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Equal(t, "no_url", rows[0].FetchStatus)
	rows, err = s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m2"})
	require.NoError(t, err)
	require.Equal(t, "failed", rows[0].FetchStatus)
	require.Len(t, rows[0].FetchError, 512)
}

func TestFetchSkipsOversizedResponses(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, seedAttachment(ctx, s, "https://example.test/large.bin"))

	stats, err := Fetch(ctx, s, FetchOptions{
		CacheDir:     t.TempDir(),
		MaxBytes:     4,
		StatusUpdate: true,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body := []byte("too-large")
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        make(http.Header),
				Body:          io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)),
				Request:       req,
			}, nil
		})},
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Skipped)
	rows, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "m1"})
	require.NoError(t, err)
	require.Equal(t, "too_large", rows[0].FetchStatus)
	require.Empty(t, rows[0].MediaPath)
}

func TestFetchUsesProxyFallbackAndBodyLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, seedAttachmentRecord(ctx, s, "m1", "a1", "file.bin", "https://example.test/primary"))
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
			Content:           "see attached",
			NormalizedContent: "see attached",
			HasAttachments:    true,
			RawJSON:           `{}`,
		},
		Attachments: []store.AttachmentRecord{{
			AttachmentID: "a1",
			MessageID:    "m1",
			GuildID:      "g1",
			ChannelID:    "c1",
			AuthorID:     "u1",
			Filename:     "file.bin",
			ContentType:  "application/octet-stream",
			Size:         8,
			URL:          "https://example.test/primary",
			ProxyURL:     "https://example.test/proxy",
		}},
	}}))

	calls := []string{}
	stats, err := Fetch(ctx, s, FetchOptions{
		CacheDir: t.TempDir(),
		MaxBytes: 4,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls = append(calls, req.URL.Path)
			if req.URL.Path == "/primary" {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("nope")),
					Request:    req,
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("12345")),
				Request:    req,
			}, nil
		})},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"/primary", "/proxy"}, calls)
	require.Equal(t, 1, stats.Skipped)
}

func TestMediaPathHelpers(t *testing.T) {
	t.Parallel()

	hash := strings.Repeat("a", 64)
	require.Equal(t, "attachments/aa/"+hash+"-attachment.png", attachmentMediaPath(hash, "", "image/png"))
	require.Equal(t, "report-final.png", safeFilename(" ../report final!.png "))
	require.Equal(t, "name.png", truncateFilename("name.png", 20))
	require.Equal(t, "aaa.png", truncateFilename("aaaaa.png", 7))
	require.Empty(t, extensionForContentType("not a content type"))
	require.Empty(t, clampError("   "))
	require.Equal(t, "short", clampError(" short "))

	repo := t.TempDir()
	path, err := RepoPath(repo, "attachments/aa/file.png")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(repo, "media", "attachments", "aa", "file.png"), path)
	_, err = RepoPath(repo, "../escape")
	require.Error(t, err)
	_, err = LocalPath(repo, "")
	require.Error(t, err)
}

func TestMediaTargetNeedsWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "file.bin")
	hash := sha256.Sum256([]byte("same"))
	hashString := hex.EncodeToString(hash[:])

	needsWrite, err := mediaTargetNeedsWrite(target, hashString)
	require.NoError(t, err)
	require.True(t, needsWrite)
	require.NoError(t, os.WriteFile(target, []byte("same"), 0o600))
	needsWrite, err = mediaTargetNeedsWrite(target, hashString)
	require.NoError(t, err)
	require.False(t, needsWrite)
	needsWrite, err = mediaTargetNeedsWrite(target, strings.Repeat("0", 64))
	require.NoError(t, err)
	require.True(t, needsWrite)

	require.NoError(t, os.Remove(target))
	require.NoError(t, os.Mkdir(target, 0o755))
	needsWrite, err = mediaTargetNeedsWrite(target, hashString)
	require.NoError(t, err)
	require.True(t, needsWrite)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func staticHTTPClient(body []byte) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        make(http.Header),
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: int64(len(body)),
			Request:       req,
		}, nil
	})}
}

func seedAttachment(ctx context.Context, s *store.Store, url string) error {
	return seedAttachmentWithIDs(ctx, s, "m1", "a1", url)
}

func seedAttachmentWithIDs(ctx context.Context, s *store.Store, messageID, attachmentID, url string) error {
	return seedAttachmentRecord(ctx, s, messageID, attachmentID, "file.png", url)
}

func seedAttachmentRecord(ctx context.Context, s *store.Store, messageID, attachmentID, filename, url string) error {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	if err := s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}); err != nil {
		return err
	}
	if err := s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}); err != nil {
		return err
	}
	return s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                messageID,
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "see attached",
			NormalizedContent: "see attached",
			HasAttachments:    true,
			RawJSON:           `{}`,
		},
		Attachments: []store.AttachmentRecord{{
			AttachmentID: attachmentID,
			MessageID:    messageID,
			GuildID:      "g1",
			ChannelID:    "c1",
			AuthorID:     "u1",
			Filename:     filename,
			ContentType:  "image/png",
			Size:         11,
			URL:          url,
		}},
	}})
}
