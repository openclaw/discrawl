package share

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestImportRealSnapshot(t *testing.T) {
	repo := strings.TrimSpace(os.Getenv("DISCRAWL_REAL_REPO"))
	if repo == "" {
		t.Skip("set DISCRAWL_REAL_REPO to run real snapshot import validation")
	}

	ctx := context.Background()
	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	_, changed, err := ImportIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) {
			if p.Phase == "start" || p.Phase == "rebuild_fts" || p.Phase == "done" {
				t.Logf("import progress phase=%s total_rows=%d", p.Phase, p.TotalRows)
			}
		},
	})
	require.NoError(t, err)
	require.True(t, changed)

	var messageCount int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from messages`).Scan(&messageCount))
	require.Positive(t, messageCount)
	var ftsCount int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from message_fts`).Scan(&ftsCount))
	require.Equal(t, messageCount, ftsCount)
}

func TestImportMemoryBounded(t *testing.T) {
	if os.Getenv("DISCRAWL_OOM_REGRESSION") != "1" {
		t.Skip("set DISCRAWL_OOM_REGRESSION=1 to run the memory-bounded import regression")
	}

	ctx := context.Background()
	repo := t.TempDir()
	messageRows := envInt(t, "DISCRAWL_OOM_ROWS", 80000)
	textBytes := envInt(t, "DISCRAWL_OOM_TEXT_BYTES", 2048)
	t.Logf("building synthetic snapshot rows=%d text_bytes=%d", messageRows, textBytes)
	writeSyntheticMemorySnapshot(t, repo, messageRows, textBytes)
	t.Log("synthetic snapshot built; starting import")

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	var progress []ImportProgress
	_, changed, err := ImportIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) { progress = append(progress, p) },
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, progressPhases(progress), "rebuild_fts")

	needle := "oomunique000042"
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: needle, Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0].Content, needle)
}

func writeSyntheticMemorySnapshot(t *testing.T, repo string, messageRows, textBytes int) {
	t.Helper()
	generatedAt := time.Now().UTC()
	updatedAt := generatedAt.Format(time.RFC3339Nano)
	guildFile := "tables/guilds/000000.jsonl.gz"
	channelFile := "tables/channels/000000.jsonl.gz"
	memberFile := "tables/members/000000.jsonl.gz"
	messageFile := "tables/messages/000000.jsonl.gz"

	writeJSONLGzip(t, repo, guildFile, func(enc *json.Encoder) int {
		require.NoError(t, enc.Encode(map[string]any{
			"id":         "g1",
			"name":       "Guild",
			"icon":       "",
			"raw_json":   `{}`,
			"updated_at": updatedAt,
		}))
		return 1
	})
	writeJSONLGzip(t, repo, channelFile, func(enc *json.Encoder) int {
		require.NoError(t, enc.Encode(map[string]any{
			"id":                "c1",
			"guild_id":          "g1",
			"parent_id":         "",
			"kind":              "text",
			"name":              "general",
			"topic":             "",
			"position":          0,
			"is_nsfw":           false,
			"is_archived":       false,
			"is_locked":         false,
			"is_private_thread": false,
			"thread_parent_id":  "",
			"archive_timestamp": "",
			"raw_json":          `{}`,
			"updated_at":        updatedAt,
		}))
		return 1
	})
	writeJSONLGzip(t, repo, memberFile, func(enc *json.Encoder) int {
		require.NoError(t, enc.Encode(map[string]any{
			"guild_id":      "g1",
			"user_id":       "u1",
			"username":      "peter",
			"global_name":   "",
			"display_name":  "Peter",
			"nick":          "",
			"discriminator": "",
			"avatar":        "",
			"bot":           false,
			"joined_at":     "",
			"role_ids_json": `[]`,
			"raw_json":      `{"bio":"memory regression profile"}`,
			"updated_at":    updatedAt,
		}))
		return 1
	})
	writeJSONLGzip(t, repo, messageFile, func(enc *json.Encoder) int {
		for i := range messageRows {
			messageID := strconv.FormatInt(1456744319972282449+int64(i), 10)
			unique := fmt.Sprintf("oomunique%06d", i)
			content := syntheticMemoryContent(unique, textBytes)
			require.NoError(t, enc.Encode(map[string]any{
				"id":                  messageID,
				"guild_id":            "g1",
				"channel_id":          "c1",
				"author_id":           "u1",
				"message_type":        0,
				"created_at":          updatedAt,
				"edited_at":           "",
				"deleted_at":          "",
				"content":             content,
				"normalized_content":  content,
				"reply_to_message_id": "",
				"pinned":              false,
				"has_attachments":     false,
				"raw_json":            `{"author":{"username":"Peter"}}`,
				"updated_at":          updatedAt,
			}))
		}
		return messageRows
	})

	manifest := Manifest{
		Version:     1,
		GeneratedAt: generatedAt,
		Tables: []TableManifest{
			{Name: "guilds", Files: []string{guildFile}, Columns: []string{"id", "name", "icon", "raw_json", "updated_at"}, Rows: 1},
			{Name: "channels", Files: []string{channelFile}, Columns: []string{"id", "guild_id", "parent_id", "kind", "name", "topic", "position", "is_nsfw", "is_archived", "is_locked", "is_private_thread", "thread_parent_id", "archive_timestamp", "raw_json", "updated_at"}, Rows: 1},
			{Name: "members", Files: []string{memberFile}, Columns: []string{"guild_id", "user_id", "username", "global_name", "display_name", "nick", "discriminator", "avatar", "bot", "joined_at", "role_ids_json", "raw_json", "updated_at"}, Rows: 1},
			{Name: "messages", Files: []string{messageFile}, Columns: []string{"id", "guild_id", "channel_id", "author_id", "message_type", "created_at", "edited_at", "deleted_at", "content", "normalized_content", "reply_to_message_id", "pinned", "has_attachments", "raw_json", "updated_at"}, Rows: messageRows},
		},
	}
	body, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(repo, ManifestName), append(body, '\n'), 0o600))
}

func writeJSONLGzip(t *testing.T, repo, rel string, writeRows func(*json.Encoder) int) {
	t.Helper()
	path := filepath.Join(repo, filepath.FromSlash(rel))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	file, err := os.Create(path)
	require.NoError(t, err)
	gz := gzip.NewWriter(file)
	enc := json.NewEncoder(gz)
	rows := writeRows(enc)
	require.Positive(t, rows)
	require.NoError(t, gz.Close())
	require.NoError(t, file.Close())
}

func syntheticMemoryContent(unique string, size int) string {
	if size <= len(unique) {
		return unique
	}
	var b strings.Builder
	b.Grow(size)
	b.WriteString(unique)
	for i := 0; b.Len() < size; i++ {
		_, _ = fmt.Fprintf(&b, " token_%s_%04d", unique, i)
	}
	return b.String()[:size]
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	require.NoError(t, err)
	require.Positive(t, value)
	return value
}
