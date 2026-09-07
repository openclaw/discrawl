package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestLexicalRebuildCLIUpgradesExistingArchive(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "archive.db")
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.LogDir = filepath.Join(dir, "logs")
	cfg.Search.Lexical.Languages = []string{"ar"}
	path := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(path, cfg))
	s, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{ID: "1", GuildID: "g", ChannelID: "c", Content: "والكتاب", NormalizedContent: "والكتاب", RawJSON: `{}`, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}))
	require.NoError(t, s.Close())
	var out bytes.Buffer
	err = Run(ctx, []string{"--config", path, "lexical", "install"}, &out, &bytes.Buffer{})
	require.ErrorContains(t, err, "usage: discrawl lexical rebuild")
	require.NoError(t, Run(ctx, []string{"--config", path, "--json", "lexical", "rebuild"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), `"ar"`)
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", path, "--json", "search", "كتاب"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), `"message_id": "1"`)
}

func TestLexicalHelpDoesNotCreateConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "config.toml")
	for _, args := range [][]string{{"lexical", "--help"}, {"lexical", "rebuild", "--help"}} {
		var out bytes.Buffer
		require.NoError(t, Run(context.Background(), append([]string{"--config", path}, args...), &out, &bytes.Buffer{}))
		require.Contains(t, out.String(), "Usage: discrawl lexical rebuild")
	}
	_, err := os.Stat(filepath.Dir(path))
	require.ErrorIs(t, err, os.ErrNotExist)
}
