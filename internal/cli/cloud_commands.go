package cli

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/config"
)

const discrawlCloudBatchSize = 250

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
		client, err := crawlremote.NewClientFromConfig(remoteCfg, crawlremote.Options{UserAgent: "discrawl/" + version})
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
		guildCount, err := sendIngestRows(r.ctx, client, archiveID, manifest, "guilds", discrawlGuildColumns, guilds, false)
		if err != nil {
			return err
		}
		channelCount, err := sendIngestRows(r.ctx, client, archiveID, manifest, "channels", discrawlChannelColumns, channels, false)
		if err != nil {
			return err
		}
		memberCount, err := sendIngestRows(r.ctx, client, archiveID, manifest, "members", discrawlMemberColumns, members, false)
		if err != nil {
			return err
		}
		messageCount, err := sendIngestRows(r.ctx, client, archiveID, manifest, "messages", discrawlMessageColumns, messages, true)
		if err != nil {
			return err
		}
		return r.print(map[string]any{
			"remote":   strings.TrimRight(endpoint, "/"),
			"archive":  archiveID,
			"guilds":   guildCount,
			"channels": channelCount,
			"members":  memberCount,
			"messages": messageCount,
		})
	})
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
