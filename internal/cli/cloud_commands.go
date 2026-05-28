package cli

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/config"
)

const (
	discrawlCloudBatchSize             = 250
	discrawlCloudSQLiteBundleChunkSize = int64(64 * 1024 * 1024)
)

func (r *runtime) runCloud(args []string) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		return printCommandUsage(r.stdout, []string{"cloud"})
	}
	switch args[0] {
	case "publish":
		return r.runCloudPublish(args[1:])
	default:
		return usageErr(fmt.Errorf("unknown cloud subcommand %q", args[0]))
	}
}

func (r *runtime) runCloudPublish(args []string) error {
	fs := flag.NewFlagSet("cloud publish", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	remoteEndpoint := fs.String("remote", "", "")
	archive := fs.String("archive", "", "")
	tokenEnv := fs.String("token-env", "", "")
	sqliteOnly := fs.Bool("sqlite-only", false, "")
	jsonOut := fs.Bool("json", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if *jsonOut {
		r.json = true
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("cloud publish takes flags only"))
	}
	return r.withLocalStoreReadOnly(func() error {
		if r.store == nil {
			return dbErr(errors.New("cloud publish requires a local SQLite archive"))
		}
		endpoint := firstNonEmpty(*remoteEndpoint, r.cfg.Remote.Endpoint)
		archiveID := firstNonEmpty(*archive, r.cfg.Remote.Archive)
		if endpoint == "" {
			return usageErr(errors.New("cloud publish requires --remote or remote.endpoint"))
		}
		if archiveID == "" {
			return usageErr(errors.New("cloud publish requires --archive or remote.archive"))
		}
		remoteCfg := crawlremote.Config{
			Mode:     crawlremote.ModePublisher,
			Endpoint: endpoint,
			Archive:  archiveID,
			TokenEnv: firstNonEmpty(*tokenEnv, r.cfg.Remote.TokenEnv, config.DefaultRemoteTokenEnv),
		}
		client, err := crawlremote.NewClientFromConfig(remoteCfg, crawlremote.Options{
			UserAgent:  "discrawl/" + version,
			HTTPClient: &http.Client{Timeout: 10 * time.Minute},
		})
		if err != nil {
			return configErr(err)
		}
		manifest := crawlremote.IngestManifest{
			App:           "discrawl",
			Archive:       archiveID,
			SchemaName:    "discrawl-cloud-v1",
			SchemaVersion: 1,
			SchemaHash:    "discrawl-cloud-v1",
			Mode:          crawlremote.ModePublisher,
			Source:        "sqlite",
		}
		guildCount, channelCount, memberCount, messageCount, err := cloudPublishCounts(r.ctx, r.store.DB())
		if err != nil {
			return dbErr(err)
		}
		if !*sqliteOnly {
			guilds, err := publishRows(r.ctx, r.store.DB(), discrawlGuildExportSQL)
			if err != nil {
				return dbErr(err)
			}
			channels, err := publishRows(r.ctx, r.store.DB(), discrawlChannelExportSQL)
			if err != nil {
				return dbErr(err)
			}
			messages, err := publishRows(r.ctx, r.store.DB(), discrawlMessageExportSQL)
			if err != nil {
				return dbErr(err)
			}
			members, err := publishRows(r.ctx, r.store.DB(), discrawlMemberExportSQL)
			if err != nil {
				return dbErr(err)
			}
			guildCount, err = sendIngestRows(r.ctx, client, archiveID, manifest, "guilds", discrawlGuildColumns, guilds, false)
			if err != nil {
				return err
			}
			channelCount, err = sendIngestRows(r.ctx, client, archiveID, manifest, "channels", discrawlChannelColumns, channels, false)
			if err != nil {
				return err
			}
			memberCount, err = sendIngestRows(r.ctx, client, archiveID, manifest, "members", discrawlMemberColumns, members, false)
			if err != nil {
				return err
			}
			messageCount, err = sendIngestRows(r.ctx, client, archiveID, manifest, "messages", discrawlMessageColumns, messages, true)
			if err != nil {
				return err
			}
		}
		sqliteBundle, err := uploadSQLiteArchive(r.ctx, client, "discrawl", archiveID, r.store.DB(), r.cfg.DBPath, manifest, map[string]int64{
			"guilds":   guildCount,
			"channels": channelCount,
			"members":  memberCount,
			"messages": messageCount,
		})
		if err != nil {
			return err
		}
		return r.print(map[string]any{
			"remote":        strings.TrimRight(endpoint, "/"),
			"archive":       archiveID,
			"guilds":        guildCount,
			"channels":      channelCount,
			"members":       memberCount,
			"messages":      messageCount,
			"sqlite_only":   *sqliteOnly,
			"sqlite_bundle": sqliteBundle,
		})
	})
}

func cloudPublishCounts(ctx context.Context, db *sql.DB) (guilds int64, channels int64, members int64, messages int64, err error) {
	if guilds, err = countCloudRows(ctx, db, "select count(*) from guilds where id != '@me'"); err != nil {
		return 0, 0, 0, 0, err
	}
	if channels, err = countCloudRows(ctx, db, "select count(*) from channels where guild_id != '@me'"); err != nil {
		return 0, 0, 0, 0, err
	}
	if members, err = countCloudRows(ctx, db, "select count(*) from members where guild_id != '@me'"); err != nil {
		return 0, 0, 0, 0, err
	}
	if messages, err = countCloudRows(ctx, db, "select count(*) from messages where guild_id != '@me'"); err != nil {
		return 0, 0, 0, 0, err
	}
	return guilds, channels, members, messages, nil
}

func countCloudRows(ctx context.Context, db *sql.DB, query string) (int64, error) {
	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func publishRows(ctx context.Context, db *sql.DB, query string) ([][]any, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([][]any, 0)
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for i, value := range values {
			if bytes, ok := value.([]byte); ok {
				values[i] = string(bytes)
			}
		}
		out = append(out, values)
	}
	return out, rows.Err()
}

func sendIngestRows(ctx context.Context, client *crawlremote.Client, archive string, manifest crawlremote.IngestManifest, table string, columns []string, rows [][]any, final bool) (int64, error) {
	var total int64
	if len(rows) == 0 {
		result, err := client.Ingest(ctx, "discrawl", archive, crawlremote.IngestRequest{
			Manifest: manifest,
			Table:    table,
			Columns:  columns,
			Rows:     [][]any{},
			Final:    final,
		})
		return result.RowsAccepted, err
	}
	for start := 0; start < len(rows); start += discrawlCloudBatchSize {
		end := min(start+discrawlCloudBatchSize, len(rows))
		result, err := client.Ingest(ctx, "discrawl", archive, crawlremote.IngestRequest{
			Manifest: manifest,
			Table:    table,
			Columns:  columns,
			Rows:     rows[start:end],
			Cursor:   cursorFor(start),
			Final:    final && end == len(rows),
		})
		if err != nil {
			return total, err
		}
		total += result.RowsAccepted
	}
	return total, nil
}

func cursorFor(start int) string {
	if start == 0 {
		return ""
	}
	return strconv.Itoa(start)
}

func uploadSQLiteArchive(ctx context.Context, client *crawlremote.Client, app, archive string, db *sql.DB, dbPath string, manifest crawlremote.IngestManifest, counts map[string]int64) (*crawlremote.SQLiteBundle, error) {
	snapshotPath, cleanup, err := sqliteSnapshotPath(ctx, db)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	bundle, err := crawlremote.BuildGzipSQLiteBundle(ctx, crawlremote.SQLiteBundleBuildOptions{
		App:        app,
		Archive:    archive,
		SourcePath: snapshotPath,
		ChunkSize:  discrawlCloudSQLiteBundleChunkSize,
		Counts:     counts,
		Privacy: map[string]any{
			"excludes_guild_id":         "@me",
			"includes_private_messages": false,
			"includes_raw_json":         false,
		},
	})
	if err != nil {
		return nil, err
	}
	defer bundle.Cleanup()
	result, err := client.UploadSQLiteBundleFiles(ctx, app, archive, bundle.Manifest, bundle.Parts)
	if err != nil {
		return nil, err
	}
	return result.Bundle, nil
}

func sqliteSnapshotPath(ctx context.Context, db *sql.DB) (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "discrawl-cloud-sqlite-*")
	if err != nil {
		return "", func() {}, fmt.Errorf("create sqlite snapshot dir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }
	snapshotPath := filepath.Join(tmpDir, "archive.db")
	if err := writeCloudSQLiteExport(ctx, db, snapshotPath); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return snapshotPath, cleanup, nil
}

func writeCloudSQLiteExport(ctx context.Context, source *sql.DB, snapshotPath string) error {
	out, err := sql.Open("sqlite", snapshotPath)
	if err != nil {
		return fmt.Errorf("open sqlite cloud export: %w", err)
	}
	defer func() { _ = out.Close() }()
	if _, err := out.ExecContext(ctx, "pragma busy_timeout = 5000"); err != nil {
		return fmt.Errorf("configure sqlite cloud export: %w", err)
	}
	for _, stmt := range discrawlCloudSQLiteSchema {
		if _, err := out.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("write sqlite cloud export: %w", err)
		}
	}
	for _, export := range discrawlCloudSQLiteExports {
		if err := copyCloudSQLiteRows(ctx, source, out, export.table, export.columns, export.query); err != nil {
			return err
		}
	}
	for _, stmt := range discrawlCloudSQLiteIndexes {
		if _, err := out.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("index sqlite cloud export: %w", err)
		}
	}
	if _, err := out.ExecContext(ctx, "vacuum"); err != nil {
		return fmt.Errorf("vacuum sqlite cloud export: %w", err)
	}
	return nil
}

func copyCloudSQLiteRows(ctx context.Context, source, out *sql.DB, table string, columns []string, query string) error {
	rows, err := source.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query sqlite cloud export %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	tx, err := out.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite cloud export %s: %w", table, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("insert into %s(%s) values(%s)", table, strings.Join(columns, ","), sqlPlaceholders(len(columns))))
	if err != nil {
		return fmt.Errorf("prepare sqlite cloud export %s: %w", table, err)
	}
	defer func() { _ = stmt.Close() }()
	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("scan sqlite cloud export %s: %w", table, err)
		}
		for i, value := range values {
			if bytes, ok := value.([]byte); ok {
				values[i] = string(bytes)
			}
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("insert sqlite cloud export %s: %w", table, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite cloud export %s: %w", table, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite cloud export %s: %w", table, err)
	}
	committed = true
	return nil
}

func sqlPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func cloudFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open sqlite snapshot for hash: %w", err)
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash sqlite snapshot: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

var discrawlCloudSQLiteSchema = []string{
	`pragma journal_mode = off`,
	`pragma synchronous = off`,
	`create table guilds(guild_id text primary key, name text, updated_at text)`,
	`create table channels(channel_id text primary key, guild_id text not null, name text, type text, parent_id text, updated_at text)`,
	`create table members(guild_id text not null, user_id text not null, username text, display_name text, updated_at text, primary key(guild_id, user_id))`,
	`create table messages(message_id text primary key, channel_id text not null, guild_id text not null, author_id text, author_username text, content text, created_at text, edited_at text)`,
}

var discrawlCloudSQLiteExports = []struct {
	table   string
	columns []string
	query   string
}{
	{table: "guilds", columns: discrawlGuildColumns, query: discrawlGuildExportSQL},
	{table: "channels", columns: discrawlChannelColumns, query: discrawlChannelExportSQL},
	{table: "members", columns: discrawlMemberColumns, query: discrawlMemberExportSQL},
	{table: "messages", columns: discrawlMessageColumns, query: discrawlMessageExportSQL},
}

var discrawlCloudSQLiteIndexes = []string{
	`create index idx_messages_created on messages(created_at, message_id)`,
	`create index idx_messages_channel_created on messages(channel_id, created_at, message_id)`,
	`create index idx_messages_guild_created on messages(guild_id, created_at, message_id)`,
}

var discrawlGuildColumns = []string{"guild_id", "name", "updated_at"}

const discrawlGuildExportSQL = `
select id as guild_id, name, updated_at
from guilds
where id != '@me'
order by id`

var discrawlChannelColumns = []string{"channel_id", "guild_id", "name", "type", "parent_id", "updated_at"}

const discrawlChannelExportSQL = `
select id as channel_id, guild_id, name, kind as type, coalesce(parent_id, '') as parent_id, updated_at
from channels
where guild_id != '@me'
order by guild_id, id`

var discrawlMemberColumns = []string{"guild_id", "user_id", "username", "display_name", "updated_at"}

const discrawlMemberExportSQL = `
select guild_id, user_id, username, coalesce(nullif(display_name, ''), nullif(nick, ''), username) as display_name, updated_at
from members
where guild_id != '@me'
order by guild_id, user_id`

var discrawlMessageColumns = []string{"message_id", "channel_id", "guild_id", "author_id", "author_username", "content", "created_at", "edited_at"}

const discrawlMessageExportSQL = `
select m.id as message_id, m.channel_id, m.guild_id, coalesce(m.author_id, '') as author_id,
       coalesce(nullif(mem.display_name, ''), nullif(mem.nick, ''), nullif(mem.username, ''), '') as author_username,
       m.content, m.created_at, coalesce(m.edited_at, '') as edited_at
from messages m
left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
where m.guild_id != '@me'
order by m.created_at desc, m.id`
