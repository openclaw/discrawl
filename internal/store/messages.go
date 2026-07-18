package store

import (
	"context"
	"regexp"
	"strings"
	"time"
)

var (
	discordUserMentionRE    = regexp.MustCompile(`<@!?([A-Za-z0-9]+)>`)
	discordChannelMentionRE = regexp.MustCompile(`<#([A-Za-z0-9]+)>`)
)

type MessageListOptions struct {
	GuildIDs     []string
	Channel      string
	Author       string
	Since        time.Time
	Before       time.Time
	Limit        int
	Last         int
	IncludeEmpty bool
}

type MentionListOptions struct {
	GuildIDs   []string
	Channel    string
	Author     string
	Target     string
	TargetType string
	Since      time.Time
	Before     time.Time
	Limit      int
}

type MessageRow struct {
	MessageID       string    `json:"message_id"`
	GuildID         string    `json:"guild_id"`
	GuildName       string    `json:"guild_name,omitempty"`
	ChannelID       string    `json:"channel_id"`
	ChannelName     string    `json:"channel_name"`
	AuthorID        string    `json:"author_id"`
	AuthorName      string    `json:"author_name"`
	Content         string    `json:"content"`
	DisplayContent  string    `json:"display_content,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	ReplyToMessage  string    `json:"reply_to_message_id,omitempty"`
	Source          string    `json:"source,omitempty"`
	HasAttachments  bool      `json:"has_attachments"`
	AttachmentNames string    `json:"attachment_names,omitempty"`
	AttachmentText  string    `json:"attachment_text,omitempty"`
	Pinned          bool      `json:"pinned"`
}

func (s *Store) ListMessages(ctx context.Context, opts MessageListOptions) ([]MessageRow, error) {
	args := []any{}
	clauses := []string{"1=1"}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, "m.guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if channel := normalizeChannelFilter(opts.Channel); channel != "" {
		clauses = append(clauses, "(m.channel_id = ? or c.name = ? or c.name like ?)")
		args = append(args, channel, channel, "%"+channel+"%")
	}
	if author := strings.TrimSpace(opts.Author); author != "" {
		clauses = append(clauses, `(m.author_id = ? or coalesce(mem.username, '') = ? or coalesce(mem.display_name, '') = ? or coalesce(mem.username, '') like ? or coalesce(mem.display_name, '') like ? or json_extract(m.raw_json, '$.author.username') = ?)`)
		args = append(args, author, author, author, "%"+author+"%", "%"+author+"%", author)
	}
	if !opts.Since.IsZero() {
		clauses = append(clauses, "m.created_at >= ?")
		args = append(args, opts.Since.UTC().Format(timeLayout))
	}
	if !opts.Before.IsZero() {
		clauses = append(clauses, "m.created_at < ?")
		args = append(args, opts.Before.UTC().Format(timeLayout))
	}
	if !opts.IncludeEmpty {
		clauses = append(clauses, "trim(coalesce(m.normalized_content, '')) <> ''")
	}

	baseQuery := `
		select
			m.id,
			m.guild_id,
			coalesce(g.name, ''),
			m.channel_id,
			coalesce(c.name, ''),
			coalesce(m.author_id, ''),
			coalesce(
				nullif(mem.display_name, ''),
				nullif(mem.nick, ''),
				nullif(mem.global_name, ''),
				nullif(mem.username, ''),
				nullif(json_extract(m.raw_json, '$.author.global_name'), ''),
				nullif(json_extract(m.raw_json, '$.author.username'), ''),
				''
			),
			case
				when trim(coalesce(m.content, '')) <> '' then m.content
				else m.normalized_content
			end,
			m.created_at,
			coalesce(m.reply_to_message_id, ''),
			coalesce(json_extract(m.raw_json, '$.source'), ''),
			m.has_attachments,
			coalesce((select group_concat(a.filename, ', ') from message_attachments a where a.message_id = m.id), ''),
			coalesce((select group_concat(a.text_content, char(10)) from message_attachments a where a.message_id = m.id and trim(a.text_content) <> ''), ''),
			m.pinned
		from messages m
		left join guilds g on g.id = m.guild_id and g.deleted_at is null
		left join channels c on c.id = m.channel_id
		left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id and mem.deleted_at is null
		where ` + strings.Join(clauses, " and ") + `
	`

	query := baseQuery
	switch {
	case opts.Last > 0:
		query = `
			select * from (` + baseQuery + `
				order by m.created_at desc, m.id desc
				limit ?
			) recent
			order by created_at asc, id asc
		`
		args = append(args, opts.Last)
	case opts.Limit > 0:
		query += `
			order by m.created_at asc, m.id asc
			limit ?
		`
		args = append(args, opts.Limit)
	default:
		query += `
			order by m.created_at asc, m.id asc
		`
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []MessageRow
	for rows.Next() {
		var row MessageRow
		var created string
		var hasAttachments int
		var pinned int
		if err := rows.Scan(
			&row.MessageID,
			&row.GuildID,
			&row.GuildName,
			&row.ChannelID,
			&row.ChannelName,
			&row.AuthorID,
			&row.AuthorName,
			&row.Content,
			&created,
			&row.ReplyToMessage,
			&row.Source,
			&hasAttachments,
			&row.AttachmentNames,
			&row.AttachmentText,
			&pinned,
		); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		row.HasAttachments = hasAttachments == 1
		row.Pinned = pinned == 1
		row.DisplayContent = row.Content
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, s.resolveMessageDisplayMentions(ctx, out)
}

func (s *Store) ListMessagesWithThreadContext(ctx context.Context, opts MessageListOptions) ([]MessageRow, error) {
	rows, err := s.ListMessages(ctx, opts)
	if err != nil {
		return nil, err
	}
	return s.hydrateMessageThreadContext(ctx, rows, opts.Limit+opts.Last)
}

func (s *Store) hydrateMessageThreadContext(ctx context.Context, rows []MessageRow, limit int) ([]MessageRow, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	rootIDs := make([]any, 0, len(rows))
	seenRoots := map[string]struct{}{}
	visible := map[string]struct{}{}
	for _, row := range rows {
		if id := strings.TrimSpace(row.MessageID); id != "" {
			visible[id] = struct{}{}
		}
	}
	for _, row := range rows {
		rootID := strings.TrimSpace(row.ReplyToMessage)
		if rootID == "" {
			continue
		}
		if _, ok := visible[rootID]; ok {
			continue
		}
		if _, ok := seenRoots[rootID]; ok {
			continue
		}
		seenRoots[rootID] = struct{}{}
		rootIDs = append(rootIDs, rootID)
	}
	if len(rootIDs) == 0 {
		return rows, nil
	}
	query := `
		select
			m.id,
			m.guild_id,
			coalesce(g.name, ''),
			m.channel_id,
			coalesce(c.name, ''),
			coalesce(m.author_id, ''),
			coalesce(
				nullif(mem.display_name, ''),
				nullif(mem.nick, ''),
				nullif(mem.global_name, ''),
				nullif(mem.username, ''),
				nullif(json_extract(m.raw_json, '$.author.global_name'), ''),
				nullif(json_extract(m.raw_json, '$.author.username'), ''),
				''
			),
			case
				when trim(coalesce(m.content, '')) <> '' then m.content
				else m.normalized_content
			end,
			m.created_at,
			coalesce(m.reply_to_message_id, ''),
			coalesce(json_extract(m.raw_json, '$.source'), ''),
			m.has_attachments,
			coalesce((select group_concat(a.filename, ', ') from message_attachments a where a.message_id = m.id), ''),
			coalesce((select group_concat(a.text_content, char(10)) from message_attachments a where a.message_id = m.id and trim(a.text_content) <> ''), ''),
			m.pinned
		from messages m
		left join guilds g on g.id = m.guild_id and g.deleted_at is null
		left join channels c on c.id = m.channel_id
		left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id and mem.deleted_at is null
		where m.id in (` + placeholders(len(rootIDs)) + `)
		order by m.created_at asc, m.id asc`
	contextRows, err := s.db.QueryContext(ctx, query, rootIDs...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = contextRows.Close() }()
	extra, err := scanMessageRows(contextRows)
	if err != nil {
		return nil, err
	}
	if err := contextRows.Err(); err != nil {
		return nil, err
	}
	if err := s.resolveMessageDisplayMentions(ctx, extra); err != nil {
		return nil, err
	}
	return mergeMessageRows(rows, extra), nil
}

func scanMessageRows(rows rowScanner) ([]MessageRow, error) {
	var out []MessageRow
	for rows.Next() {
		var row MessageRow
		var created string
		var hasAttachments int
		var pinned int
		if err := rows.Scan(
			&row.MessageID,
			&row.GuildID,
			&row.GuildName,
			&row.ChannelID,
			&row.ChannelName,
			&row.AuthorID,
			&row.AuthorName,
			&row.Content,
			&created,
			&row.ReplyToMessage,
			&row.Source,
			&hasAttachments,
			&row.AttachmentNames,
			&row.AttachmentText,
			&pinned,
		); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		row.HasAttachments = hasAttachments == 1
		row.Pinned = pinned == 1
		row.DisplayContent = row.Content
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type rowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func mergeMessageRows(primary, extra []MessageRow) []MessageRow {
	out := make([]MessageRow, 0, len(primary)+len(extra))
	seen := map[string]struct{}{}
	appendRow := func(row MessageRow) {
		key := row.GuildID + "\x00" + row.ChannelID + "\x00" + row.MessageID
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, row)
	}
	for _, row := range primary {
		appendRow(row)
	}
	for _, row := range extra {
		appendRow(row)
	}
	return out
}

func normalizeChannelFilter(raw string) string {
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "#"))
}

func (s *Store) resolveMessageDisplayMentions(ctx context.Context, rows []MessageRow) error {
	if len(rows) == 0 {
		return nil
	}
	ids := make([]any, 0, len(rows))
	indexByID := make(map[string]int, len(rows))
	for index, row := range rows {
		id := strings.TrimSpace(row.MessageID)
		if id == "" {
			continue
		}
		ids = append(ids, id)
		indexByID[id] = index
	}
	if len(ids) == 0 {
		return nil
	}
	query := `select message_id, target_type, target_id, target_name from mention_events where message_id in (` + placeholders(len(ids)) + `)`
	mentionRows, err := s.db.QueryContext(ctx, query, ids...)
	if err != nil {
		return err
	}
	defer func() { _ = mentionRows.Close() }()
	for mentionRows.Next() {
		var messageID, targetType, targetID, targetName string
		if err := mentionRows.Scan(&messageID, &targetType, &targetID, &targetName); err != nil {
			return err
		}
		index, ok := indexByID[messageID]
		if !ok {
			continue
		}
		rows[index].DisplayContent = replaceDiscordMention(rows[index].DisplayContent, targetType, targetID, targetName)
	}
	if err := mentionRows.Err(); err != nil {
		return err
	}
	return s.resolveInlineDiscordMentions(ctx, rows)
}

func replaceDiscordMention(content, targetType, targetID, targetName string) string {
	targetID = strings.TrimSpace(targetID)
	if targetID == "" {
		return content
	}
	label := strings.TrimSpace(targetName)
	if label == "" {
		label = targetID
	}
	switch strings.TrimSpace(targetType) {
	case "role":
		return strings.ReplaceAll(content, "<@&"+targetID+">", "@"+label)
	case "channel":
		return strings.ReplaceAll(content, "<#"+targetID+">", "#"+label)
	default:
		content = strings.ReplaceAll(content, "<@"+targetID+">", "@"+label)
		return strings.ReplaceAll(content, "<@!"+targetID+">", "@"+label)
	}
}

func (s *Store) resolveInlineDiscordMentions(ctx context.Context, rows []MessageRow) error {
	userIDs := map[string]struct{}{}
	channelIDs := map[string]struct{}{}
	for _, row := range rows {
		for _, match := range discordUserMentionRE.FindAllStringSubmatch(row.DisplayContent, -1) {
			if len(match) > 1 && strings.TrimSpace(match[1]) != "" {
				userIDs[match[1]] = struct{}{}
			}
		}
		for _, match := range discordChannelMentionRE.FindAllStringSubmatch(row.DisplayContent, -1) {
			if len(match) > 1 && strings.TrimSpace(match[1]) != "" {
				channelIDs[match[1]] = struct{}{}
			}
		}
	}
	userNames, err := s.discordMemberDisplayNames(ctx, userIDs)
	if err != nil {
		return err
	}
	channelNames, err := s.discordChannelNames(ctx, channelIDs)
	if err != nil {
		return err
	}
	for index := range rows {
		guildID := strings.TrimSpace(rows[index].GuildID)
		rows[index].DisplayContent = discordUserMentionRE.ReplaceAllStringFunc(rows[index].DisplayContent, func(match string) string {
			parts := discordUserMentionRE.FindStringSubmatch(match)
			if len(parts) < 2 {
				return match
			}
			if name := firstResolvedDiscordName(userNames, guildID, parts[1]); name != "" {
				return "@" + name
			}
			return match
		})
		rows[index].DisplayContent = discordChannelMentionRE.ReplaceAllStringFunc(rows[index].DisplayContent, func(match string) string {
			parts := discordChannelMentionRE.FindStringSubmatch(match)
			if len(parts) < 2 {
				return match
			}
			if name := firstResolvedDiscordName(channelNames, guildID, parts[1]); name != "" {
				return "#" + name
			}
			return match
		})
	}
	return nil
}

func (s *Store) discordMemberDisplayNames(ctx context.Context, ids map[string]struct{}) (map[string]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := mapKeysAsAny(ids)
	query := `
		select guild_id, user_id,
			coalesce(
				nullif(display_name, ''),
				nullif(nick, ''),
				nullif(global_name, ''),
				nullif(username, ''),
				''
			)
		from members
		where deleted_at is null and user_id in (` + placeholders(len(args)) + `)
	`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var guildID, userID, name string
		if err := rows.Scan(&guildID, &userID, &name); err != nil {
			return nil, err
		}
		rememberResolvedDiscordName(out, guildID, userID, name)
	}
	return out, rows.Err()
}

func (s *Store) discordChannelNames(ctx context.Context, ids map[string]struct{}) (map[string]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := mapKeysAsAny(ids)
	query := `select guild_id, id, coalesce(nullif(name, ''), '') from channels where id in (` + placeholders(len(args)) + `)`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var guildID, channelID, name string
		if err := rows.Scan(&guildID, &channelID, &name); err != nil {
			return nil, err
		}
		rememberResolvedDiscordName(out, guildID, channelID, name)
	}
	return out, rows.Err()
}

func mapKeysAsAny(values map[string]struct{}) []any {
	out := make([]any, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	return out
}

func rememberResolvedDiscordName(out map[string]string, guildID, id, name string) {
	id = strings.TrimSpace(id)
	name = strings.TrimSpace(name)
	if id == "" || name == "" {
		return
	}
	if guildID = strings.TrimSpace(guildID); guildID != "" {
		out[guildID+"|"+id] = name
	}
	if _, ok := out["|"+id]; !ok {
		out["|"+id] = name
	}
}

func firstResolvedDiscordName(values map[string]string, guildID, id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	if guildID = strings.TrimSpace(guildID); guildID != "" {
		if value := strings.TrimSpace(values[guildID+"|"+id]); value != "" {
			return value
		}
	}
	return strings.TrimSpace(values["|"+id])
}
