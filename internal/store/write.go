package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

type GuildRecord struct {
	ID      string
	Name    string
	Icon    string
	RawJSON string
}

type ChannelRecord struct {
	ID               string
	GuildID          string
	ParentID         string
	Kind             string
	Name             string
	Topic            string
	Position         int
	IsNSFW           bool
	IsArchived       bool
	IsLocked         bool
	IsPrivateThread  bool
	ThreadParentID   string
	ArchiveTimestamp string
	RawJSON          string
}

type MemberRecord struct {
	GuildID       string
	UserID        string
	Username      string
	GlobalName    string
	DisplayName   string
	Nick          string
	Discriminator string
	Avatar        string
	Bot           bool
	JoinedAt      string
	RoleIDsJSON   string
	RawJSON       string
}

type MessageRecord struct {
	ID                string
	GuildID           string
	ChannelID         string
	ChannelName       string
	AuthorID          string
	AuthorName        string
	MessageType       int
	CreatedAt         string
	EditedAt          string
	DeletedAt         string
	Content           string
	NormalizedContent string
	ReplyToMessageID  string
	Pinned            bool
	HasAttachments    bool
	RawJSON           string
}

type AttachmentRecord struct {
	AttachmentID  string
	MessageID     string
	GuildID       string
	ChannelID     string
	AuthorID      string
	Filename      string
	ContentType   string
	Size          int64
	URL           string
	ProxyURL      string
	TextContent   string
	MediaPath     string
	ContentSHA256 string
	ContentSize   int64
	FetchedAt     string
	FetchStatus   string
	FetchError    string
}

type MentionEventRecord struct {
	MessageID  string
	GuildID    string
	ChannelID  string
	AuthorID   string
	TargetType string
	TargetID   string
	TargetName string
	EventAt    string
}

type MessageMutation struct {
	Record      MessageRecord
	EventType   string
	PayloadJSON string
	Options     WriteOptions
	Attachments []AttachmentRecord
	Mentions    []MentionEventRecord
}

type WriteOptions struct {
	AppendEvent      bool
	EnqueueEmbedding bool
}

func (s *Store) UpsertGuild(ctx context.Context, guild GuildRecord) error {
	now := time.Now().UTC().Format(timeLayout)
	_, err := s.db.ExecContext(ctx, `
		insert into guilds(id, name, icon, raw_json, updated_at)
		values(?, ?, ?, ?, ?)
		on conflict(id) do update set
			name=excluded.name,
			icon=excluded.icon,
			raw_json=excluded.raw_json,
			updated_at=excluded.updated_at
	`, guild.ID, guild.Name, guild.Icon, guild.RawJSON, now)
	return err
}

func (s *Store) UpsertChannel(ctx context.Context, channel ChannelRecord) error {
	now := time.Now().UTC().Format(timeLayout)
	_, err := s.db.ExecContext(ctx, `
		insert into channels(
			id, guild_id, parent_id, kind, name, topic, position, is_nsfw,
			is_archived, is_locked, is_private_thread, thread_parent_id,
			archive_timestamp, raw_json, updated_at
		) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(id) do update set
			guild_id=excluded.guild_id,
			parent_id=excluded.parent_id,
			kind=excluded.kind,
			name=excluded.name,
			topic=excluded.topic,
			position=excluded.position,
			is_nsfw=excluded.is_nsfw,
			is_archived=excluded.is_archived,
			is_locked=excluded.is_locked,
			is_private_thread=excluded.is_private_thread,
			thread_parent_id=excluded.thread_parent_id,
			archive_timestamp=excluded.archive_timestamp,
			raw_json=excluded.raw_json,
			updated_at=excluded.updated_at
	`, channel.ID, channel.GuildID, channel.ParentID, channel.Kind, channel.Name, channel.Topic, channel.Position,
		boolInt(channel.IsNSFW), boolInt(channel.IsArchived), boolInt(channel.IsLocked), boolInt(channel.IsPrivateThread),
		channel.ThreadParentID, nullable(channel.ArchiveTimestamp), channel.RawJSON, now)
	return err
}

func (s *Store) ReplaceMembers(ctx context.Context, guildID string, members []MemberRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `delete from members where guild_id = ?`, guildID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `delete from member_fts where guild_id = ?`, guildID); err != nil {
		return err
	}
	now := time.Now().UTC().Format(timeLayout)
	stmt, err := tx.PrepareContext(ctx, `
		insert into members(
			guild_id, user_id, username, global_name, display_name, nick, discriminator,
			avatar, bot, joined_at, role_ids_json, raw_json, updated_at
		) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, member := range members {
		if _, err := stmt.ExecContext(ctx, member.GuildID, member.UserID, member.Username, nullable(member.GlobalName),
			nullable(member.DisplayName), nullable(member.Nick), nullable(member.Discriminator), nullable(member.Avatar),
			boolInt(member.Bot), nullable(member.JoinedAt), member.RoleIDsJSON, member.RawJSON, now); err != nil {
			return err
		}
		if err := upsertMemberFTSTx(ctx, tx, member); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) UpsertMember(ctx context.Context, member MemberRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	now := time.Now().UTC().Format(timeLayout)
	if _, err := tx.ExecContext(ctx, `
		insert into members(
			guild_id, user_id, username, global_name, display_name, nick, discriminator,
			avatar, bot, joined_at, role_ids_json, raw_json, updated_at
		) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(guild_id, user_id) do update set
			username=excluded.username,
			global_name=excluded.global_name,
			display_name=excluded.display_name,
			nick=excluded.nick,
			discriminator=excluded.discriminator,
			avatar=excluded.avatar,
			bot=excluded.bot,
			joined_at=excluded.joined_at,
			role_ids_json=excluded.role_ids_json,
			raw_json=excluded.raw_json,
			updated_at=excluded.updated_at
	`, member.GuildID, member.UserID, member.Username, nullable(member.GlobalName), nullable(member.DisplayName),
		nullable(member.Nick), nullable(member.Discriminator), nullable(member.Avatar), boolInt(member.Bot),
		nullable(member.JoinedAt), member.RoleIDsJSON, member.RawJSON, now); err != nil {
		return err
	}
	if err := upsertMemberFTSTx(ctx, tx, member); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) DeleteGuildData(ctx context.Context, guildID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	for _, stmt := range []string{
		`delete from embedding_jobs where message_id in (select id from messages where guild_id = ?)`,
		`delete from message_embeddings where message_id in (select id from messages where guild_id = ?)`,
		`delete from message_fts where guild_id = ?`,
		`delete from message_events where guild_id = ?`,
		`delete from message_attachments where guild_id = ?`,
		`delete from mention_events where guild_id = ?`,
		`delete from messages where guild_id = ?`,
		`delete from member_fts where guild_id = ?`,
		`delete from members where guild_id = ?`,
		`delete from channels where guild_id = ?`,
		`delete from guilds where id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, stmt, guildID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) DeleteOrphanChannels(ctx context.Context, guildID string) error {
	_, err := s.db.ExecContext(ctx, `
		delete from channels
		where guild_id = ?
		  and not exists (
			select 1
			from messages
			where messages.channel_id = channels.id
		  )
	`, guildID)
	return err
}

func (s *Store) DeleteMember(ctx context.Context, guildID, userID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `delete from members where guild_id = ? and user_id = ?`, guildID, userID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `delete from member_fts where rowid = ?`, memberFTSRowID(guildID, userID)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) UpsertMessage(ctx context.Context, message MessageRecord) error {
	return s.UpsertMessageWithOptions(ctx, message, WriteOptions{})
}

func (s *Store) UpsertMessageWithOptions(ctx context.Context, message MessageRecord, opts WriteOptions) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := upsertMessageTx(ctx, tx, message, opts); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) UpsertMessages(ctx context.Context, messages []MessageMutation) error {
	if len(messages) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := upsertMessageTx(ctx, tx, message.Record, message.Options); err != nil {
			return err
		}
		if err := replaceAttachmentsTx(ctx, tx, message.Record.ID, message.Attachments); err != nil {
			return err
		}
		if err := replaceMentionEventsTx(ctx, tx, message.Record.ID, message.Mentions); err != nil {
			return err
		}
		if message.Options.AppendEvent && message.EventType != "" {
			if err := appendEventTx(
				ctx,
				tx,
				message.Record.GuildID,
				message.Record.ChannelID,
				message.Record.ID,
				message.EventType,
				message.PayloadJSON,
			); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func upsertMessageTx(ctx context.Context, tx *sql.Tx, message MessageRecord, opts WriteOptions) error {
	now := time.Now().UTC().Format(timeLayout)
	var previousNormalized sql.NullString
	previousErr := sql.ErrNoRows
	jobExists := false
	if opts.EnqueueEmbedding {
		previousErr = tx.QueryRowContext(ctx, `
			select normalized_content
			from messages
			where id = ?
		`, message.ID).Scan(&previousNormalized)
		if previousErr != nil && previousErr != sql.ErrNoRows {
			return previousErr
		}
		if previousErr == nil {
			var existingJobs int
			if err := tx.QueryRowContext(ctx, `
				select count(*)
				from embedding_jobs
				where message_id = ?
			`, message.ID).Scan(&existingJobs); err != nil {
				return err
			}
			jobExists = existingJobs > 0
		}
	}
	if _, err := tx.ExecContext(ctx, `
		insert into messages(
			id, guild_id, channel_id, author_id, message_type, created_at, edited_at, deleted_at,
			content, normalized_content, reply_to_message_id, pinned, has_attachments, raw_json, updated_at
		) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(id) do update set
			guild_id=excluded.guild_id,
			channel_id=excluded.channel_id,
			author_id=excluded.author_id,
			message_type=excluded.message_type,
			created_at=excluded.created_at,
			edited_at=excluded.edited_at,
			deleted_at=coalesce(excluded.deleted_at, messages.deleted_at),
			content=excluded.content,
			normalized_content=excluded.normalized_content,
			reply_to_message_id=excluded.reply_to_message_id,
			pinned=excluded.pinned,
			has_attachments=excluded.has_attachments,
			raw_json=excluded.raw_json,
			updated_at=excluded.updated_at
	`, message.ID, message.GuildID, message.ChannelID, nullable(message.AuthorID), message.MessageType, message.CreatedAt,
		nullable(message.EditedAt), nullable(message.DeletedAt), message.Content, message.NormalizedContent,
		nullable(message.ReplyToMessageID), boolInt(message.Pinned), boolInt(message.HasAttachments), message.RawJSON, now); err != nil {
		return err
	}
	if rowID, ok := messageFTSRowID(message.ID); ok {
		if _, err := tx.ExecContext(ctx, `delete from message_fts where rowid = ?`, rowID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			insert into message_fts(rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content)
			values(?, ?, ?, ?, ?, ?, ?, ?)
		`, rowID, message.ID, message.GuildID, message.ChannelID, nullable(message.AuthorID), message.AuthorName, message.ChannelName, message.NormalizedContent); err != nil {
			return err
		}
	}
	queueEmbedding := opts.EnqueueEmbedding && (previousErr == sql.ErrNoRows || previousNormalized.String != message.NormalizedContent || !jobExists)
	if queueEmbedding {
		if _, err := tx.ExecContext(ctx, `
			insert into embedding_jobs(message_id, state, attempts, updated_at)
			values(?, 'pending', 0, ?)
			on conflict(message_id) do update set
				state = 'pending',
				attempts = 0,
				last_error = '',
				locked_at = null,
				updated_at = excluded.updated_at
		`, message.ID, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) MarkMessageDeleted(ctx context.Context, guildID, channelID, messageID string, payload any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	now := time.Now().UTC().Format(timeLayout)
	if _, err := tx.ExecContext(ctx, `
		update messages
		set deleted_at = ?, updated_at = ?
		where id = ?
	`, now, now, messageID); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := appendEventTx(ctx, tx, guildID, channelID, messageID, "delete", string(body)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) AppendMessageEvent(ctx context.Context, guildID, channelID, messageID, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		insert into message_events(guild_id, channel_id, message_id, event_type, event_at, payload_json)
		values(?, ?, ?, ?, ?, ?)
	`, guildID, channelID, messageID, eventType, time.Now().UTC().Format(timeLayout), string(body))
	return err
}

func appendEventTx(ctx context.Context, tx *sql.Tx, guildID, channelID, messageID, eventType, payload string) error {
	_, err := tx.ExecContext(ctx, `
		insert into message_events(guild_id, channel_id, message_id, event_type, event_at, payload_json)
		values(?, ?, ?, ?, ?, ?)
	`, guildID, channelID, messageID, eventType, time.Now().UTC().Format(timeLayout), payload)
	return err
}

func replaceAttachmentsTx(ctx context.Context, tx *sql.Tx, messageID string, attachments []AttachmentRecord) error {
	existing, err := existingAttachmentMediaTx(ctx, tx, messageID)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `delete from message_attachments where message_id = ?`, messageID); err != nil {
		return err
	}
	if len(attachments) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(timeLayout)
	stmt, err := tx.PrepareContext(ctx, `
		insert into message_attachments(
			attachment_id, message_id, guild_id, channel_id, author_id, filename,
			content_type, size, url, proxy_url, text_content, media_path, content_sha256,
			content_size, fetched_at, fetch_status, fetch_error, updated_at
		) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, attachment := range attachments {
		if media, ok := existing[attachment.AttachmentID]; ok && attachment.MediaPath == "" {
			attachment.MediaPath = media.MediaPath
			attachment.ContentSHA256 = media.ContentSHA256
			attachment.ContentSize = media.ContentSize
			attachment.FetchedAt = media.FetchedAt
			attachment.FetchStatus = media.FetchStatus
			attachment.FetchError = media.FetchError
		}
		if _, err := stmt.ExecContext(
			ctx,
			attachment.AttachmentID,
			attachment.MessageID,
			attachment.GuildID,
			attachment.ChannelID,
			nullable(attachment.AuthorID),
			attachment.Filename,
			nullable(attachment.ContentType),
			attachment.Size,
			nullable(attachment.URL),
			nullable(attachment.ProxyURL),
			attachment.TextContent,
			nullable(attachment.MediaPath),
			nullable(attachment.ContentSHA256),
			attachment.ContentSize,
			nullable(attachment.FetchedAt),
			attachment.FetchStatus,
			attachment.FetchError,
			now,
		); err != nil {
			return err
		}
	}
	return nil
}

func existingAttachmentMediaTx(ctx context.Context, tx *sql.Tx, messageID string) (map[string]AttachmentRecord, error) {
	rows, err := tx.QueryContext(ctx, `
		select attachment_id, coalesce(media_path, ''), coalesce(content_sha256, ''),
		       content_size, coalesce(fetched_at, ''), coalesce(fetch_status, ''), coalesce(fetch_error, '')
		from message_attachments
		where message_id = ?
	`, messageID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]AttachmentRecord{}
	for rows.Next() {
		var record AttachmentRecord
		if err := rows.Scan(
			&record.AttachmentID,
			&record.MediaPath,
			&record.ContentSHA256,
			&record.ContentSize,
			&record.FetchedAt,
			&record.FetchStatus,
			&record.FetchError,
		); err != nil {
			return nil, err
		}
		out[record.AttachmentID] = record
	}
	return out, rows.Err()
}

func replaceMentionEventsTx(ctx context.Context, tx *sql.Tx, messageID string, mentions []MentionEventRecord) error {
	if _, err := tx.ExecContext(ctx, `delete from mention_events where message_id = ?`, messageID); err != nil {
		return err
	}
	if len(mentions) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
		insert into mention_events(
			message_id, guild_id, channel_id, author_id, target_type, target_id, target_name, event_at
		) values(?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, mention := range mentions {
		eventAt := mention.EventAt
		if eventAt == "" {
			eventAt = time.Now().UTC().Format(timeLayout)
		}
		if _, err := stmt.ExecContext(
			ctx,
			mention.MessageID,
			mention.GuildID,
			mention.ChannelID,
			nullable(mention.AuthorID),
			mention.TargetType,
			mention.TargetID,
			mention.TargetName,
			eventAt,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) SetSyncState(ctx context.Context, scope, cursor string) error {
	_, err := s.db.ExecContext(ctx, `
		insert into sync_state(scope, cursor, updated_at)
		values(?, ?, ?)
		on conflict(scope) do update set
			cursor=excluded.cursor,
			updated_at=excluded.updated_at
	`, scope, cursor, time.Now().UTC().Format(timeLayout))
	return err
}

func (s *Store) DeleteSyncState(ctx context.Context, scope string) error {
	_, err := s.db.ExecContext(ctx, `delete from sync_state where scope = ?`, scope)
	return err
}

func rollback(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func upsertMemberFTSTx(ctx context.Context, tx *sql.Tx, member MemberRecord) error {
	rowID := memberFTSRowID(member.GuildID, member.UserID)
	if _, err := tx.ExecContext(ctx, `delete from member_fts where rowid = ?`, rowID); err != nil {
		return err
	}
	displayName := member.DisplayName
	if displayName == "" {
		displayName = member.Nick
	}
	if displayName == "" {
		displayName = member.GlobalName
	}
	if displayName == "" {
		displayName = member.Username
	}
	_, err := tx.ExecContext(ctx, `
		insert into member_fts(rowid, member_key, guild_id, user_id, username, display_name, profile_text)
		values(?, ?, ?, ?, ?, ?, ?)
	`, rowID, memberKey(member.GuildID, member.UserID), member.GuildID, member.UserID, member.Username, displayName, memberProfileSearchText(member.RawJSON))
	return err
}
