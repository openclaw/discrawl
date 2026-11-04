package share

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

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

func upsertMergeSnapshotRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any) (bool, error) {
	if table == "message_events" || table == "mention_events" {
		delete(row, "event_id")
	}
	if table == "message_attachments" {
		if err := preserveIncrementalAttachmentState(ctx, tx, row); err != nil {
			return false, err
		}
	}
	if slices.Contains([]string{"guilds", "members", "channels", "messages"}, table) {
		apply, err := shouldMergeObservedEntityRow(ctx, tx, table, row)
		if err != nil || !apply {
			return false, err
		}
	}
	// Guild/member revisions were compared chronologically above. Reapplying the
	// generic lexical SQL guard would reject valid RFC3339 offset timestamps.
	protectNewer := slices.Contains([]string{"channels", "messages"}, table)
	return upsertSnapshotRow(ctx, tx, table, row, protectNewer)
}

func shouldMergeObservedEntityRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any) (bool, error) {
	var query string
	var args []any
	switch table {
	case "guilds":
		query = `select updated_at, deleted_at is not null from guilds where id = ?`
		args = []any{importValue(row["id"])}
	case "members":
		query = `select updated_at, deleted_at is not null from members where guild_id = ? and user_id = ?`
		args = []any{importValue(row["guild_id"]), importValue(row["user_id"])}
	case "channels", "messages":
		query = "select updated_at, false from " + quoteIdent(table) + " where id=?"
		args = []any{importValue(row["id"])}
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
	// Content updated_at no longer advances for unchanged native observations.
	// Preserve the observation fence when deciding whether a remote revision
	// may replace local metadata, including publication permissions.
	rowExpr := "json_array(?)"
	if table == "members" {
		rowExpr = "json_array(?,?)"
	}
	var checked string
	err = tx.QueryRowContext(ctx, "select checked_at from entity_checks where table_name=? and row_key="+rowExpr+" and check_kind='observed'", append([]any{table}, args...)...).Scan(&checked)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err == nil && compareSnapshotRevisions(checked, localUpdated) > 0 {
		localUpdated = checked
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
	rowID, ok := store.MessageFTSRowID(messageID)
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

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
