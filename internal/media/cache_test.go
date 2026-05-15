package media

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
		require.Equal(t, "/file.png", r.URL.Path)
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
