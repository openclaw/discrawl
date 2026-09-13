package share

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

func importTable(ctx context.Context, tx *sql.Tx, opts Options, table TableManifest) error {
	files := table.Files
	if len(files) == 0 && strings.TrimSpace(table.File) != "" {
		files = []string{table.File}
	}
	if len(files) == 0 {
		return fmt.Errorf("manifest table %s has no files", table.Name)
	}
	columns := importColumns(table)
	stmt, err := tx.PrepareContext(ctx, insertSQL(table.Name, columns))
	if err != nil {
		return fmt.Errorf("prepare import %s: %w", table.Name, err)
	}
	defer func() { _ = stmt.Close() }()
	for i, rel := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		opts.reportProgress(ImportProgress{Phase: "file_start", Table: table.Name, File: rel, FileIndex: i + 1, FileCount: len(files), TotalRows: table.Rows})
		rows, err := importTableFile(ctx, stmt, opts.RepoPath, table, columns, rel)
		if err != nil {
			return err
		}
		opts.reportProgress(ImportProgress{Phase: "file_done", Table: table.Name, File: rel, FileIndex: i + 1, FileCount: len(files), Rows: rows, TotalRows: table.Rows})
	}
	return nil
}

func importTableFile(ctx context.Context, stmt *sql.Stmt, repoPath string, table TableManifest, columns []string, rel string) (int, error) {
	path := filepath.Join(repoPath, filepath.FromSlash(rel))
	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", rel, err)
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return 0, fmt.Errorf("read gzip %s: %w", rel, err)
	}
	defer func() { _ = gz.Close() }()
	dec := json.NewDecoder(gz)
	dec.UseNumber()
	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		row := map[string]any{}
		err := dec.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, fmt.Errorf("decode %s: %w", rel, err)
		}
		if isDirectMessageSnapshotRow(table.Name, row) {
			continue
		}
		if err := validateSnapshotRow(table.Name, row); err != nil {
			return count, fmt.Errorf("validate %s: %w", rel, err)
		}
		values := make([]any, len(columns))
		for i, column := range columns {
			values[i] = importValue(row[column])
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return count, fmt.Errorf("insert %s: %w", table.Name, err)
		}
		count++
	}
	return count, nil
}

func validateSnapshotRow(table string, row map[string]any) error {
	if table != "messages" && table != "guilds" && table != "members" {
		return nil
	}
	if table == "guilds" || table == "members" {
		for _, column := range []string{"deleted_at", "deletion_source", "deletion_reason"} {
			if _, ok := row[column]; !ok {
				row[column] = nil
			}
		}
		if err := validateSnapshotRevision(table, row); err != nil {
			return err
		}
	}
	raw, ok := row["deleted_at"]
	if !ok || raw == nil {
		if table == "guilds" || table == "members" {
			return validateLiveSnapshotTombstoneMetadata(table, row)
		}
		return nil
	}
	value, ok := raw.(string)
	if !ok {
		return errors.New("messages.deleted_at must be a string or null")
	}
	value = strings.TrimSpace(value)
	if value == "" {
		row["deleted_at"] = nil
		if table == "guilds" || table == "members" {
			return validateLiveSnapshotTombstoneMetadata(table, row)
		}
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return fmt.Errorf("%s.deleted_at must be RFC3339: %w", table, err)
	}
	row["deleted_at"] = parsed.UTC().Format(time.RFC3339Nano)
	if table == "guilds" || table == "members" {
		for _, column := range []string{"deletion_source", "deletion_reason"} {
			value, ok := row[column].(string)
			if !ok || strings.TrimSpace(value) == "" {
				return fmt.Errorf("%s.%s must be a non-empty string for a tombstone", table, column)
			}
			row[column] = strings.TrimSpace(value)
		}
	}
	return nil
}

func validateSnapshotRevision(table string, row map[string]any) error {
	raw := row["updated_at"]
	value, ok := raw.(string)
	if !ok || strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s.updated_at must be an RFC3339 string", table)
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("%s.updated_at must be RFC3339: %w", table, err)
	}
	row["updated_at"] = parsed.UTC().Format(time.RFC3339Nano)
	return nil
}

func validateLiveSnapshotTombstoneMetadata(table string, row map[string]any) error {
	for _, column := range []string{"deletion_source", "deletion_reason"} {
		raw := row[column]
		if raw == nil {
			continue
		}
		value, ok := raw.(string)
		if !ok || strings.TrimSpace(value) != "" {
			return fmt.Errorf("%s.%s must be null for a live row", table, column)
		}
		row[column] = nil
	}
	return nil
}

func repairImportedGuildIDs(ctx context.Context, tx *sql.Tx) error {
	repairs := []struct {
		table string
		query string
	}{
		{"messages", `
			update messages
			set guild_id = (
				select c.guild_id
				from channels c
				where c.id = messages.channel_id
			)
			where coalesce(guild_id, '') = ''
			  and exists (
				select 1
				from channels c
				where c.id = messages.channel_id
				  and coalesce(c.guild_id, '') != ''
			  )`},
		{"message_attachments", `
			update message_attachments
			set guild_id = coalesce(
				nullif((select m.guild_id from messages m where m.id = message_attachments.message_id), ''),
				(select c.guild_id from channels c where c.id = message_attachments.channel_id)
			)
			where coalesce(guild_id, '') = ''
			  and coalesce(
				nullif((select m.guild_id from messages m where m.id = message_attachments.message_id), ''),
				(select c.guild_id from channels c where c.id = message_attachments.channel_id)
			  ) is not null`},
		{"message_events", `
			update message_events
			set guild_id = coalesce(
				nullif((select m.guild_id from messages m where m.id = message_events.message_id), ''),
				(select c.guild_id from channels c where c.id = message_events.channel_id)
			)
			where coalesce(guild_id, '') = ''
			  and coalesce(
				nullif((select m.guild_id from messages m where m.id = message_events.message_id), ''),
				(select c.guild_id from channels c where c.id = message_events.channel_id)
			  ) is not null`},
		{"mention_events", `
			update mention_events
			set guild_id = coalesce(
				nullif((select m.guild_id from messages m where m.id = mention_events.message_id), ''),
				(select c.guild_id from channels c where c.id = mention_events.channel_id)
			)
			where coalesce(guild_id, '') = ''
			  and coalesce(
				nullif((select m.guild_id from messages m where m.id = mention_events.message_id), ''),
				(select c.guild_id from channels c where c.id = mention_events.channel_id)
			  ) is not null`},
	}
	for _, repair := range repairs {
		if _, err := tx.ExecContext(ctx, repair.query); err != nil {
			return fmt.Errorf("repair imported %s guild ids: %w", repair.table, err)
		}
	}
	return nil
}

func importColumns(table TableManifest) []string {
	if table.Name != "message_events" && table.Name != "mention_events" {
		return table.Columns
	}
	columns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		if column != "event_id" {
			columns = append(columns, column)
		}
	}
	return columns
}

func upsertMergeSnapshotRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any) (bool, error) {
	if table == "message_events" || table == "mention_events" {
		delete(row, "event_id")
	}
	if table == "message_attachments" {
		if err := preserveIncrementalAttachmentState(ctx, tx, row); err != nil {
			return false, err
		}
	}
	if table == "guilds" || table == "members" {
		apply, err := shouldMergeTombstoneEntityRow(ctx, tx, table, row)
		if err != nil || !apply {
			return false, err
		}
	}
	// Guild/member revisions were compared chronologically above. Reapplying the
	// generic lexical SQL guard would reject valid RFC3339 offset timestamps.
	protectNewer := slices.Contains([]string{"channels", "messages"}, table)
	return upsertSnapshotRow(ctx, tx, table, row, protectNewer)
}

func shouldMergeTombstoneEntityRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any) (bool, error) {
	var query string
	var args []any
	switch table {
	case "guilds":
		query = `select updated_at, deleted_at is not null from guilds where id = ?`
		args = []any{importValue(row["id"])}
	case "members":
		query = `select updated_at, deleted_at is not null from members where guild_id = ? and user_id = ?`
		args = []any{importValue(row["guild_id"]), importValue(row["user_id"])}
	default:
		return true, nil
	}
	var localUpdated string
	var localTombstone bool
	err := tx.QueryRowContext(ctx, query, args...).Scan(&localUpdated, &localTombstone)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read existing %s merge revision: %w", table, err)
	}
	comparison := compareSnapshotRevisions(stringValue(row["updated_at"]), localUpdated)
	if comparison != 0 {
		return comparison > 0, nil
	}
	incomingTombstone := row["deleted_at"] != nil && strings.TrimSpace(stringValue(row["deleted_at"])) != ""
	return !localTombstone || incomingTombstone, nil
}

func compareSnapshotRevisions(left, right string) int {
	leftTime, leftErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(left))
	rightTime, rightErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(right))
	if leftErr == nil && rightErr == nil {
		switch {
		case leftTime.Before(rightTime):
			return -1
		case leftTime.After(rightTime):
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(strings.TrimSpace(left), strings.TrimSpace(right))
}

func upsertSnapshotRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any, protectNewer bool) (bool, error) {
	cols := make([]string, 0, len(row))
	for col := range row {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	quoted := make([]string, 0, len(cols))
	updates := make([]string, 0, len(cols))
	placeholders := make([]string, 0, len(cols))
	args := make([]any, 0, len(cols))
	for _, col := range cols {
		quotedCol := quoteIdent(col)
		quoted = append(quoted, quotedCol)
		updates = append(updates, quotedCol+" = excluded."+quotedCol)
		placeholders = append(placeholders, "?")
		args = append(args, importValue(row[col]))
	}
	verb := "insert or replace into "
	suffix := ""
	if protectNewer {
		verb = "insert into "
		suffix = " on conflict do update set " + strings.Join(updates, ",") +
			" where coalesce(julianday(excluded.\"updated_at\"), 0) >= coalesce(julianday(" + quoteIdent(table) + ".\"updated_at\"), 0)"
	}
	stmt := verb + quoteIdent(table) + "(" + strings.Join(quoted, ",") + ") values(" + strings.Join(placeholders, ",") + ")" + suffix
	result, err := tx.ExecContext(ctx, stmt, args...)
	if err != nil {
		return false, fmt.Errorf("insert %s: %w", table, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check inserted %s row: %w", table, err)
	}
	return changed > 0, nil
}

func upsertMessageFTSRow(ctx context.Context, tx *sql.Tx, messageID string) error {
	rowID, ok := messageFTSRowID(messageID)
	if !ok {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `delete from message_fts where rowid = ?`, rowID); err != nil {
		return fmt.Errorf("delete message_fts %s: %w", messageID, err)
	}
	var (
		guildID     string
		channelID   string
		authorID    string
		authorName  string
		channelName string
		content     string
	)
	if err := tx.QueryRowContext(ctx, `
		select
			m.guild_id,
			m.channel_id,
			coalesce(m.author_id, ''),
			coalesce(
				json_extract(m.raw_json, '$.member.nick'),
				json_extract(m.raw_json, '$.author.global_name'),
				json_extract(m.raw_json, '$.author.username'),
				''
			),
			coalesce(c.name, ''),
			m.normalized_content
		from messages m
		left join channels c on c.id = m.channel_id
		where m.id = ?
	`, messageID).Scan(&guildID, &channelID, &authorID, &authorName, &channelName, &content); err != nil {
		return fmt.Errorf("query message_fts %s: %w", messageID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		insert into message_fts(rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content)
		values(?, ?, ?, ?, ?, ?, ?, ?)
	`, rowID, messageID, guildID, channelID, nullIfEmpty(authorID), authorName, channelName, content); err != nil {
		return fmt.Errorf("insert message_fts %s: %w", messageID, err)
	}
	return nil
}

func messageFTSRowID(messageID string) (int64, bool) {
	if messageID == "" {
		return 0, false
	}
	rowID, err := strconv.ParseInt(messageID, 10, 64)
	if err == nil && rowID > 0 {
		return rowID, true
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(messageID))
	rowID = int64(hash.Sum64() & ((uint64(1) << 63) - 1))
	if rowID == 0 {
		rowID = 1
	}
	return rowID, true
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func insertSQL(table string, columns []string) string {
	quoted := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quoteIdent(column)
		placeholders[i] = "?"
	}
	return "insert into " + quoteIdent(table) + "(" + strings.Join(quoted, ",") + ") values(" + strings.Join(placeholders, ",") + ")"
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
