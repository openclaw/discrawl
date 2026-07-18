package store

import (
	"context"
	"strings"
)

func (s *Store) ListMentions(ctx context.Context, opts MentionListOptions) ([]MentionRow, error) {
	if opts.Limit <= 0 {
		opts.Limit = 200
	}
	args := []any{}
	clauses := []string{"1=1"}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, "me.guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if channel := normalizeChannelFilter(opts.Channel); channel != "" {
		clauses = append(clauses, "(me.channel_id = ? or c.name = ? or c.name like ?)")
		args = append(args, channel, channel, "%"+channel+"%")
	}
	if author := strings.TrimSpace(opts.Author); author != "" {
		clauses = append(clauses, `(me.author_id = ? or coalesce(mem.username, '') = ? or coalesce(mem.display_name, '') = ? or coalesce(mem.username, '') like ? or coalesce(mem.display_name, '') like ?)`)
		args = append(args, author, author, author, "%"+author+"%", "%"+author+"%")
	}
	if target := strings.TrimSpace(opts.Target); target != "" {
		clauses = append(clauses, `(me.target_id = ? or me.target_name = ? or me.target_name like ?)`)
		args = append(args, target, target, "%"+target+"%")
	}
	if targetType := strings.TrimSpace(opts.TargetType); targetType != "" {
		clauses = append(clauses, "me.target_type = ?")
		args = append(args, targetType)
	}
	if !opts.Since.IsZero() {
		clauses = append(clauses, "me.event_at >= ?")
		args = append(args, opts.Since.UTC().Format(timeLayout))
	}
	if !opts.Before.IsZero() {
		clauses = append(clauses, "me.event_at < ?")
		args = append(args, opts.Before.UTC().Format(timeLayout))
	}
	args = append(args, opts.Limit)
	rows, err := s.db.QueryContext(ctx, `
		select
			me.message_id,
			me.guild_id,
			me.channel_id,
			coalesce(c.name, ''),
			coalesce(me.author_id, ''),
			coalesce(
				nullif(mem.display_name, ''),
				nullif(mem.nick, ''),
				nullif(mem.global_name, ''),
				nullif(mem.username, ''),
				nullif(json_extract(m.raw_json, '$.author.global_name'), ''),
				nullif(json_extract(m.raw_json, '$.author.username'), ''),
				''
			),
			me.target_type,
			me.target_id,
			me.target_name,
			case
				when trim(coalesce(m.content, '')) <> '' then m.content
				else m.normalized_content
			end,
			me.event_at
		from mention_events me
		left join messages m on m.id = me.message_id
		left join channels c on c.id = me.channel_id
		left join members mem on mem.guild_id = me.guild_id and mem.user_id = me.author_id and mem.deleted_at is null
		where `+strings.Join(clauses, " and ")+`
		order by me.event_at desc, me.event_id desc
		limit ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []MentionRow
	for rows.Next() {
		var row MentionRow
		var created string
		if err := rows.Scan(
			&row.MessageID,
			&row.GuildID,
			&row.ChannelID,
			&row.ChannelName,
			&row.AuthorID,
			&row.AuthorName,
			&row.TargetType,
			&row.TargetID,
			&row.TargetName,
			&row.Content,
			&created,
		); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		out = append(out, row)
	}
	return out, rows.Err()
}
