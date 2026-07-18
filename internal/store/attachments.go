package store

import (
	"context"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/store/storedb"
)

type AttachmentListOptions struct {
	GuildIDs        []string
	ExcludeGuildIDs []string
	ChannelIDs      []string
	Channel         string
	Author          string
	MessageID       string
	Filename        string
	ContentType     string
	Since           time.Time
	Before          time.Time
	Limit           int
	MissingOnly     bool
}

type AttachmentRow struct {
	AttachmentID  string    `json:"attachment_id"`
	MessageID     string    `json:"message_id"`
	GuildID       string    `json:"guild_id"`
	GuildName     string    `json:"guild_name,omitempty"`
	ChannelID     string    `json:"channel_id"`
	ChannelName   string    `json:"channel_name"`
	AuthorID      string    `json:"author_id"`
	AuthorName    string    `json:"author_name"`
	Filename      string    `json:"filename"`
	ContentType   string    `json:"content_type,omitempty"`
	Size          int64     `json:"size"`
	URL           string    `json:"url,omitempty"`
	ProxyURL      string    `json:"proxy_url,omitempty"`
	TextContent   string    `json:"text_content,omitempty"`
	MediaPath     string    `json:"media_path,omitempty"`
	ContentSHA256 string    `json:"content_sha256,omitempty"`
	ContentSize   int64     `json:"content_size,omitempty"`
	FetchedAt     time.Time `json:"fetched_at,omitzero"`
	FetchStatus   string    `json:"fetch_status,omitempty"`
	FetchError    string    `json:"fetch_error,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type AttachmentMediaUpdate struct {
	AttachmentID  string
	MediaPath     string
	ContentSHA256 string
	ContentSize   int64
	FetchedAt     string
	FetchStatus   string
	FetchError    string
}

func (s *Store) ListAttachments(ctx context.Context, opts AttachmentListOptions) ([]AttachmentRow, error) {
	args := []any{}
	clauses := []string{"1=1"}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, "a.guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if len(opts.ExcludeGuildIDs) > 0 {
		clauses = append(clauses, "a.guild_id not in ("+placeholders(len(opts.ExcludeGuildIDs))+")")
		for _, guildID := range opts.ExcludeGuildIDs {
			args = append(args, guildID)
		}
	}
	if len(opts.ChannelIDs) > 0 {
		clauses = append(clauses, "a.channel_id in ("+placeholders(len(opts.ChannelIDs))+")")
		for _, channelID := range opts.ChannelIDs {
			args = append(args, channelID)
		}
	}
	if channel := normalizeChannelFilter(opts.Channel); channel != "" {
		clauses = append(clauses, "(a.channel_id = ? or c.name = ? or c.name like ?)")
		args = append(args, channel, channel, "%"+channel+"%")
	}
	if author := strings.TrimSpace(opts.Author); author != "" {
		clauses = append(clauses, `(a.author_id = ? or coalesce(mem.username, '') = ? or coalesce(mem.display_name, '') = ? or coalesce(json_extract(m.raw_json, '$.author.username'), '') = ? or coalesce(json_extract(m.raw_json, '$.author.global_name'), '') = ? or coalesce(mem.username, '') like ? or coalesce(mem.display_name, '') like ? or coalesce(json_extract(m.raw_json, '$.author.username'), '') like ? or coalesce(json_extract(m.raw_json, '$.author.global_name'), '') like ?)`)
		args = append(args, author, author, author, author, author, "%"+author+"%", "%"+author+"%", "%"+author+"%", "%"+author+"%")
	}
	if messageID := strings.TrimSpace(opts.MessageID); messageID != "" {
		clauses = append(clauses, "a.message_id = ?")
		args = append(args, messageID)
	}
	if filename := strings.TrimSpace(opts.Filename); filename != "" {
		clauses = append(clauses, "a.filename like ?")
		args = append(args, "%"+filename+"%")
	}
	if contentType := strings.TrimSpace(opts.ContentType); contentType != "" {
		clauses = append(clauses, "coalesce(a.content_type, '') like ?")
		args = append(args, "%"+contentType+"%")
	}
	if !opts.Since.IsZero() {
		clauses = append(clauses, "m.created_at >= ?")
		args = append(args, opts.Since.UTC().Format(timeLayout))
	}
	if !opts.Before.IsZero() {
		clauses = append(clauses, "m.created_at < ?")
		args = append(args, opts.Before.UTC().Format(timeLayout))
	}
	if opts.MissingOnly {
		clauses = append(clauses, "coalesce(a.media_path, '') = ''")
	}
	query := `
		select
			a.attachment_id,
			a.message_id,
			a.guild_id,
			coalesce(g.name, ''),
			a.channel_id,
			coalesce(c.name, ''),
			coalesce(a.author_id, ''),
			coalesce(
				nullif(mem.display_name, ''),
				nullif(mem.nick, ''),
				nullif(mem.global_name, ''),
				nullif(mem.username, ''),
				nullif(json_extract(m.raw_json, '$.author.global_name'), ''),
				nullif(json_extract(m.raw_json, '$.author.username'), ''),
				''
			),
			a.filename,
			coalesce(a.content_type, ''),
			a.size,
			coalesce(a.url, ''),
			coalesce(a.proxy_url, ''),
			a.text_content,
			coalesce(a.media_path, ''),
			coalesce(a.content_sha256, ''),
			a.content_size,
			coalesce(a.fetched_at, ''),
			coalesce(a.fetch_status, ''),
			coalesce(a.fetch_error, ''),
			m.created_at
		from message_attachments a
		left join messages m on m.id = a.message_id
		left join guilds g on g.id = a.guild_id and g.deleted_at is null
		left join channels c on c.id = a.channel_id
		left join members mem on mem.guild_id = a.guild_id and mem.user_id = a.author_id and mem.deleted_at is null
		where ` + strings.Join(clauses, " and ") + `
		order by m.created_at asc, a.attachment_id asc
	`
	if opts.Limit > 0 {
		query += ` limit ?`
		args = append(args, opts.Limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []AttachmentRow{}
	for rows.Next() {
		var row AttachmentRow
		var created string
		var fetched string
		if err := rows.Scan(
			&row.AttachmentID,
			&row.MessageID,
			&row.GuildID,
			&row.GuildName,
			&row.ChannelID,
			&row.ChannelName,
			&row.AuthorID,
			&row.AuthorName,
			&row.Filename,
			&row.ContentType,
			&row.Size,
			&row.URL,
			&row.ProxyURL,
			&row.TextContent,
			&row.MediaPath,
			&row.ContentSHA256,
			&row.ContentSize,
			&fetched,
			&row.FetchStatus,
			&row.FetchError,
			&created,
		); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		row.FetchedAt = parseTime(fetched)
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) ExpandAttachmentChannelIDs(ctx context.Context, channelIDs []string) ([]string, error) {
	if len(channelIDs) == 0 {
		return nil, nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(channelIDs))
	args := make([]any, 0, len(channelIDs)*2)
	for _, channelID := range channelIDs {
		channelID = strings.TrimSpace(channelID)
		if channelID == "" {
			continue
		}
		if _, ok := seen[channelID]; ok {
			continue
		}
		seen[channelID] = struct{}{}
		out = append(out, channelID)
		args = append(args, channelID)
	}
	if len(out) == 0 {
		return nil, nil
	}
	args = append(args, args...)
	rows, err := s.db.QueryContext(ctx, `
		select id
		from channels
		where id in (`+placeholders(len(out))+`)
		   or thread_parent_id in (`+placeholders(len(out))+`)
		order by id
	`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var channelID string
		if err := rows.Scan(&channelID); err != nil {
			return nil, err
		}
		if _, ok := seen[channelID]; ok {
			continue
		}
		seen[channelID] = struct{}{}
		out = append(out, channelID)
	}
	return out, rows.Err()
}

func (s *Store) UpdateAttachmentMedia(ctx context.Context, update AttachmentMediaUpdate) error {
	return s.q.UpdateAttachmentMedia(ctx, storedb.UpdateAttachmentMediaParams{
		MediaPath:     nullString(update.MediaPath),
		ContentSha256: nullString(update.ContentSHA256),
		ContentSize:   update.ContentSize,
		FetchedAt:     nullString(update.FetchedAt),
		FetchStatus:   update.FetchStatus,
		FetchError:    update.FetchError,
		UpdatedAt:     time.Now().UTC().Format(timeLayout),
		AttachmentID:  update.AttachmentID,
	})
}

func (s *Store) UpdateAttachmentFetchStatus(ctx context.Context, attachmentID, fetchedAt, status, message string) error {
	return s.q.UpdateAttachmentFetchStatus(ctx, storedb.UpdateAttachmentFetchStatusParams{
		FetchedAt:    nullString(fetchedAt),
		FetchStatus:  status,
		FetchError:   message,
		UpdatedAt:    time.Now().UTC().Format(timeLayout),
		AttachmentID: attachmentID,
	})
}
