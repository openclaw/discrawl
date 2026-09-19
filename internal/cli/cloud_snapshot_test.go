package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/config"
	"github.com/stretchr/testify/require"
)

func TestCloudExportOnlyUsesFilteredSnapshotWithoutRemote(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))
	source := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, addCLIDMAttachment(ctx, source))
	require.NoError(t, source.Close())
	output := filepath.Join(dir, "cloud.db")
	args := []string{"--config", cfgPath, "cloud", "publish", "--export-only", output}
	require.NoError(t, Run(ctx, args, &bytes.Buffer{}, &bytes.Buffer{}))
	db, err := sql.Open("sqlite", output)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	var count int
	require.NoError(t, db.QueryRow("select count(*) from messages where guild_id='@me'").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, db.QueryRow("select count(*) from messages").Scan(&count))
	require.Equal(t, 1, count)
	require.ErrorContains(t, Run(ctx, args, &bytes.Buffer{}, &bytes.Buffer{}), "must not already exist")
}

func TestCloudPublishUsesSameSnapshotWhenSourceChangesDuringUpload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))
	source := seedCLIStore(t, cfg.DBPath)
	defer func() { require.NoError(t, source.Close()) }()
	var original string
	require.NoError(t, source.DB().QueryRow("select content from messages where id='m100'").Scan(&original))
	t.Setenv("DISCRAWL_TEST_FROZEN_TOKEN", "fixture-token")
	changed := false
	var ingested string
	var compressed bytes.Buffer
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if req.Method == http.MethodPost {
			if !changed {
				_, err := source.DB().Exec("update messages set content='newer local content' where id='m100'")
				require.NoError(t, err)
				changed = true
			}
			var body crawlremote.IngestRequest
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			if body.Table == "messages" {
				ingested = body.Rows[0][5].(string)
			}
			require.NoError(t, json.NewEncoder(w).Encode(crawlremote.IngestResult{RowsAccepted: int64(len(body.Rows)), Complete: body.Final}))
			return
		}
		if req.Header.Get("X-Crawl-Sqlite-Upload") == "bundle-part" {
			_, err := io.Copy(&compressed, req.Body)
			require.NoError(t, err)
			require.NoError(t, json.NewEncoder(w).Encode(crawlremote.SQLiteUploadResult{Complete: false}))
			return
		}
		var manifest crawlremote.SQLiteBundleManifest
		require.NoError(t, json.NewDecoder(req.Body).Decode(&manifest))
		require.NoError(t, json.NewEncoder(w).Encode(crawlremote.SQLiteBundleUploadResult{Complete: true, Bundle: &crawlremote.SQLiteBundle{Manifest: &manifest}}))
	}))
	defer server.Close()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "cloud", "publish", "--remote", server.URL, "--archive", "discrawl/fixture", "--token-env", "DISCRAWL_TEST_FROZEN_TOKEN"}, &bytes.Buffer{}, &bytes.Buffer{}))
	require.Equal(t, original, ingested)
	reader, err := gzip.NewReader(&compressed)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	bytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	snapshot := filepath.Join(dir, "received.db")
	require.NoError(t, os.WriteFile(snapshot, bytes, 0o600))
	db, err := sql.Open("sqlite", snapshot)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	var downloaded string
	require.NoError(t, db.QueryRow("select content from messages where message_id='m100'").Scan(&downloaded))
	require.Equal(t, ingested, downloaded)
}
