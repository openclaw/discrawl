package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/store/storedb"
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

const deleteMessageFTSByRowIDSQL = `delete from message_fts where rowid = ?`

func (s *Store) UpsertGuild(ctx context.Context, guild GuildRecord) error {
	return s.q.UpsertGuild(ctx, storedb.UpsertGuildParams{
		ID:        guild.ID,
		Name:      guild.Name,
		Icon:      nullString(guild.Icon),
		RawJson:   guild.RawJSON,
		UpdatedAt: time.Now().UTC().Format(timeLayout),
	})
}

func (s *Store) MarkGuildDeleted(ctx context.Context, guildID, source, reason string) error {
	if strings.TrimSpace(guildID) == "" || strings.TrimSpace(source) == "" || strings.TrimSpace(reason) == "" {
		return errors.New("guild tombstone requires guild id, source, and reason")
	}
	now := time.Now().UTC().Format(timeLayout)
	return s.q.MarkGuildDeleted(ctx, storedb.MarkGuildDeletedParams{
		DeletedAt:      nullString(now),
		DeletionSource: nullString(source),
		DeletionReason: nullString(reason),
		UpdatedAt:      now,
		ID:             guildID,
	})
}

func (s *Store) UpsertChannel(ctx context.Context, channel ChannelRecord) error {
	return s.q.UpsertChannel(ctx, upsertChannelParams(channel, time.Now().UTC().Format(timeLayout)))
}

// MergeMembers refreshes observed members without treating absence as deletion.
func (s *Store) MergeMembers(ctx context.Context, guildID string, members []MemberRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	now := time.Now().UTC().Format(timeLayout)
	for _, member := range members {
		if member.GuildID != guildID {
			return fmt.Errorf("member guild %q does not match refresh guild %q", member.GuildID, guildID)
		}
		if err := qtx.UpsertMember(ctx, upsertMemberParams(member, now)); err != nil {
			return err
		}
		if err := upsertMemberFTSTx(ctx, tx, member); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) MarkMemberDeleted(ctx context.Context, guildID, userID, source, reason string) error {
	if strings.TrimSpace(guildID) == "" || strings.TrimSpace(userID) == "" || strings.TrimSpace(source) == "" || strings.TrimSpace(reason) == "" {
		return errors.New("member tombstone requires guild id, user id, source, and reason")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	now := time.Now().UTC().Format(timeLayout)
	if err := s.q.WithTx(tx).MarkMemberDeleted(ctx, storedb.MarkMemberDeletedParams{
		DeletedAt:      nullString(now),
		DeletionSource: nullString(source),
		DeletionReason: nullString(reason),
		UpdatedAt:      now,
		GuildID:        guildID,
		UserID:         userID,
	}); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `delete from member_fts where rowid = ?`, memberFTSRowID(guildID, userID)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) UpsertMember(ctx context.Context, member MemberRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	now := time.Now().UTC().Format(timeLayout)
	if err := qtx.UpsertMember(ctx, upsertMemberParams(member, now)); err != nil {
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
	qtx := s.q.WithTx(tx)
	if err := qtx.DeleteEmbeddingJobsByGuild(ctx, guildID); err != nil {
		return err
	}
	if err := qtx.DeleteMessageEmbeddingsByGuild(ctx, guildID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `delete from message_fts where guild_id = ?`, guildID); err != nil {
		return err
	}
	if err := qtx.DeleteMessageEventsByGuild(ctx, guildID); err != nil {
		return err
	}
	if err := qtx.DeleteAttachmentsByGuild(ctx, guildID); err != nil {
		return err
	}
	if err := qtx.DeleteMentionEventsByGuild(ctx, guildID); err != nil {
		return err
	}
	if err := qtx.DeleteMessagesByGuild(ctx, guildID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `delete from member_fts where guild_id = ?`, guildID); err != nil {
		return err
	}
	if err := qtx.DeleteMembersByGuild(ctx, guildID); err != nil {
		return err
	}
	if err := qtx.DeleteChannelsByGuild(ctx, guildID); err != nil {
		return err
	}
	if err := qtx.DeleteGuild(ctx, guildID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) DeleteOrphanChannels(ctx context.Context, guildID string) error {
	return s.q.DeleteOrphanChannels(ctx, guildID)
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
	if err := upsertMessageTx(ctx, tx, s.q.WithTx(tx), message, opts); err != nil {
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
	qtx := s.q.WithTx(tx)
	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := upsertMessageTx(ctx, tx, qtx, message.Record, message.Options); err != nil {
			return err
		}
		if err := replaceAttachmentsTx(ctx, qtx, message.Record.ID, message.Attachments); err != nil {
			return err
		}
		if err := replaceMentionEventsTx(ctx, qtx, message.Record.ID, message.Mentions); err != nil {
			return err
		}
		if message.Options.AppendEvent && message.EventType != "" {
			if err := appendEventTx(
				ctx,
				qtx,
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

func upsertMessageTx(ctx context.Context, tx *sql.Tx, qtx *storedb.Queries, message MessageRecord, opts WriteOptions) error {
	now := time.Now().UTC().Format(timeLayout)
	var previousNormalized sql.NullString
	previousErr := sql.ErrNoRows
	jobExists := false
	if opts.EnqueueEmbedding {
		normalized, err := qtx.GetMessageNormalizedContent(ctx, message.ID)
		previousErr = err
		if previousErr != nil && !errors.Is(previousErr, sql.ErrNoRows) {
			return previousErr
		}
		if previousErr == nil {
			previousNormalized = sql.NullString{String: normalized, Valid: true}
			existingJobs, err := qtx.CountEmbeddingJobsByMessage(ctx, message.ID)
			if err != nil {
				return err
			}
			jobExists = existingJobs > 0
		}
	}
	if err := qtx.UpsertMessage(ctx, upsertMessageParams(message, now)); err != nil {
		return &RowWriteError{
			Ref: FailureRef{
				Operation: "write_message",
				GuildID:   message.GuildID,
				ChannelID: message.ChannelID,
				MessageID: message.ID,
			},
			AuthorID: message.AuthorID,
			Err:      err,
		}
	}
	if rowID, ok := messageFTSRowID(message.ID); ok {
		if _, err := tx.ExecContext(ctx, deleteMessageFTSByRowIDSQL, rowID); err != nil {
			return err
		}
		if message.DeletedAt != "" {
			if err := qtx.DeleteMessageEmbeddingsByMessage(ctx, message.ID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `delete from embedding_jobs where message_id = ?`, message.ID); err != nil {
				return err
			}
			return nil
		}
		if _, err := tx.ExecContext(ctx, `
			insert into message_fts(rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content)
			values(?, ?, ?, ?, ?, ?, ?, ?)
		`, rowID, message.ID, message.GuildID, message.ChannelID, nullable(message.AuthorID), message.AuthorName, message.ChannelName, message.NormalizedContent); err != nil {
			return err
		}
	}
	queueEmbedding := opts.EnqueueEmbedding && (errors.Is(previousErr, sql.ErrNoRows) || previousNormalized.String != message.NormalizedContent || !jobExists)
	if queueEmbedding {
		if err := qtx.UpsertEmbeddingJobPending(ctx, storedb.UpsertEmbeddingJobPendingParams{MessageID: message.ID, UpdatedAt: now}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) MarkMessageDeleted(ctx context.Context, guildID, channelID, messageID string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.markMessageDeleted(ctx, guildID, channelID, messageID, string(body), true, false)
}

// MarkMessageDeletedWithoutEvent applies delete cleanup during recovery without
// appending a duplicate message event.
func (s *Store) MarkMessageDeletedWithoutEvent(ctx context.Context, guildID, channelID, messageID string) error {
	return s.markMessageDeleted(ctx, guildID, channelID, messageID, "", false, true)
}

func (s *Store) markMessageDeleted(
	ctx context.Context,
	guildID string,
	channelID string,
	messageID string,
	eventPayload string,
	appendDeleteEvent bool,
	verifyCanonicalScope bool,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	preserveDeletionTimestamps := false
	if verifyCanonicalScope {
		var storedGuildID string
		var storedChannelID string
		var storedDeletedAt sql.NullString
		err := tx.QueryRowContext(
			ctx,
			`select guild_id, channel_id, deleted_at from messages where id = ?`,
			messageID,
		).Scan(&storedGuildID, &storedChannelID, &storedDeletedAt)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("recover message delete: message %q does not exist", messageID)
		case err != nil:
			return fmt.Errorf("recover message delete: verify message scope: %w", err)
		case storedGuildID != guildID:
			return fmt.Errorf(
				"recover message delete: guild mismatch for message %q: supplied=%q stored=%q",
				messageID,
				guildID,
				storedGuildID,
			)
		case storedChannelID != channelID:
			return fmt.Errorf(
				"recover message delete: channel mismatch for message %q: supplied=%q stored=%q",
				messageID,
				channelID,
				storedChannelID,
			)
		}
		preserveDeletionTimestamps = storedDeletedAt.Valid
	}
	if !preserveDeletionTimestamps {
		now := time.Now().UTC().Format(timeLayout)
		if err := qtx.MarkMessageDeleted(ctx, storedb.MarkMessageDeletedParams{
			DeletedAt: sql.NullString{String: now, Valid: true},
			UpdatedAt: now,
			ID:        messageID,
		}); err != nil {
			return err
		}
	}
	if rowID, ok := messageFTSRowID(messageID); ok {
		if _, err := tx.ExecContext(ctx, deleteMessageFTSByRowIDSQL, rowID); err != nil {
			return err
		}
	}
	if err := qtx.DeleteMessageEmbeddingsByMessage(ctx, messageID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `delete from embedding_jobs where message_id = ?`, messageID); err != nil {
		return err
	}
	if appendDeleteEvent {
		if err := appendEventTx(ctx, qtx, guildID, channelID, messageID, "delete", eventPayload); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) AppendMessageEvent(ctx context.Context, guildID, channelID, messageID, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return appendEventTx(ctx, s.q, guildID, channelID, messageID, eventType, string(body))
}

func appendEventTx(ctx context.Context, q *storedb.Queries, guildID, channelID, messageID, eventType, payload string) error {
	return q.InsertMessageEvent(ctx, storedb.InsertMessageEventParams{
		GuildID:     guildID,
		ChannelID:   channelID,
		MessageID:   messageID,
		EventType:   eventType,
		EventAt:     time.Now().UTC().Format(timeLayout),
		PayloadJson: payload,
	})
}

func replaceAttachmentsTx(ctx context.Context, qtx *storedb.Queries, messageID string, attachments []AttachmentRecord) error {
	existing, err := existingAttachmentMediaTx(ctx, qtx, messageID)
	if err != nil {
		return err
	}
	if err := qtx.DeleteAttachmentsByMessage(ctx, messageID); err != nil {
		return err
	}
	if len(attachments) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(timeLayout)
	for _, attachment := range attachments {
		if media, ok := existing[attachment.AttachmentID]; ok && attachment.MediaPath == "" {
			attachment.MediaPath = media.MediaPath
			attachment.ContentSHA256 = media.ContentSHA256
			attachment.ContentSize = media.ContentSize
			attachment.FetchedAt = media.FetchedAt
			attachment.FetchStatus = media.FetchStatus
			attachment.FetchError = media.FetchError
		}
		if err := qtx.InsertMessageAttachment(ctx, insertMessageAttachmentParams(attachment, now)); err != nil {
			return &RowWriteError{
				Ref: FailureRef{
					Operation:   "write_attachment",
					GuildID:     attachment.GuildID,
					ChannelID:   attachment.ChannelID,
					MessageID:   attachment.MessageID,
					RelatedKind: "attachment_id",
					RelatedID:   attachment.AttachmentID,
				},
				AuthorID:    attachment.AuthorID,
				Filename:    attachment.Filename,
				ContentType: attachment.ContentType,
				Size:        attachment.Size,
				Err:         err,
			}
		}
	}
	return nil
}

func existingAttachmentMediaTx(ctx context.Context, qtx *storedb.Queries, messageID string) (map[string]AttachmentRecord, error) {
	rows, err := qtx.ListExistingAttachmentMedia(ctx, messageID)
	if err != nil {
		return nil, err
	}
	out := map[string]AttachmentRecord{}
	for _, row := range rows {
		out[row.AttachmentID] = AttachmentRecord{
			AttachmentID:  row.AttachmentID,
			MediaPath:     row.MediaPath,
			ContentSHA256: row.ContentSha256,
			ContentSize:   row.ContentSize,
			FetchedAt:     row.FetchedAt,
			FetchStatus:   row.FetchStatus,
			FetchError:    row.FetchError,
		}
	}
	return out, nil
}

func replaceMentionEventsTx(ctx context.Context, qtx *storedb.Queries, messageID string, mentions []MentionEventRecord) error {
	if err := qtx.DeleteMentionEventsByMessage(ctx, messageID); err != nil {
		return err
	}
	if len(mentions) == 0 {
		return nil
	}
	for _, mention := range mentions {
		eventAt := normalizeStoredTime(mention.EventAt)
		if eventAt == "" {
			eventAt = time.Now().UTC().Format(timeLayout)
		}
		if err := qtx.InsertMentionEvent(ctx, storedb.InsertMentionEventParams{
			MessageID:  mention.MessageID,
			GuildID:    mention.GuildID,
			ChannelID:  mention.ChannelID,
			AuthorID:   nullString(mention.AuthorID),
			TargetType: mention.TargetType,
			TargetID:   mention.TargetID,
			TargetName: mention.TargetName,
			EventAt:    eventAt,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) SetSyncState(ctx context.Context, scope, cursor string) error {
	return s.q.SetSyncState(ctx, storedb.SetSyncStateParams{
		Scope:     scope,
		Cursor:    sql.NullString{String: cursor, Valid: true},
		UpdatedAt: time.Now().UTC().Format(timeLayout),
	})
}

func (s *Store) AdvanceChannelLatestMessageID(ctx context.Context, channelID, messageID string) error {
	if messageID == "" {
		return nil
	}
	if channelID == "" {
		return errors.New("channel id is required")
	}
	if !isCanonicalDecimalSnowflake(messageID) {
		return errors.New("channel latest message id must be a canonical decimal snowflake")
	}
	result, err := s.db.ExecContext(ctx, `
insert into sync_state(scope, cursor, updated_at)
values(?, ?, ?)
on conflict(scope) do update set
	cursor = excluded.cursor,
	updated_at = excluded.updated_at
where sync_state.cursor is null
	or sync_state.cursor = ''
	or (
		sync_state.cursor not glob '*[^0-9]*'
		and (sync_state.cursor = '0' or sync_state.cursor not glob '0*')
		and (
			length(excluded.cursor) > length(sync_state.cursor)
			or (length(excluded.cursor) = length(sync_state.cursor) and excluded.cursor > sync_state.cursor)
		)
	)
`, "channel:"+channelID+":latest_message_id", messageID, time.Now().UTC().Format(timeLayout))
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 0 {
		return err
	}
	current, err := s.GetSyncState(ctx, "channel:"+channelID+":latest_message_id")
	if err != nil {
		return err
	}
	if current != "" && !isCanonicalDecimalSnowflake(current) {
		return errors.New("stored channel latest message id is not a canonical decimal snowflake")
	}
	return nil
}

func (s *Store) EnsureChannelLatestMessageState(ctx context.Context, channelID string) error {
	if channelID == "" {
		return errors.New("channel id is required")
	}
	_, err := s.db.ExecContext(ctx, `
insert into sync_state(scope, cursor, updated_at)
values(?, '', ?)
on conflict(scope) do nothing
`, "channel:"+channelID+":latest_message_id", time.Now().UTC().Format(timeLayout))
	return err
}

func isCanonicalDecimalSnowflake(value string) bool {
	if value == "" {
		return false
	}
	if len(value) > 1 && value[0] == '0' {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (s *Store) DeleteSyncState(ctx context.Context, scope string) error {
	return s.q.DeleteSyncState(ctx, scope)
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

func nullString(v string) sql.NullString {
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}

func upsertChannelParams(channel ChannelRecord, now string) storedb.UpsertChannelParams {
	return storedb.UpsertChannelParams{
		ID:               channel.ID,
		GuildID:          channel.GuildID,
		ParentID:         nullString(channel.ParentID),
		Kind:             channel.Kind,
		Name:             channel.Name,
		Topic:            nullString(channel.Topic),
		Position:         sql.NullInt64{Int64: int64(channel.Position), Valid: true},
		IsNsfw:           int64(boolInt(channel.IsNSFW)),
		IsArchived:       int64(boolInt(channel.IsArchived)),
		IsLocked:         int64(boolInt(channel.IsLocked)),
		IsPrivateThread:  int64(boolInt(channel.IsPrivateThread)),
		ThreadParentID:   nullString(channel.ThreadParentID),
		ArchiveTimestamp: nullString(channel.ArchiveTimestamp),
		RawJson:          channel.RawJSON,
		UpdatedAt:        now,
	}
}

func upsertMemberParams(member MemberRecord, now string) storedb.UpsertMemberParams {
	return storedb.UpsertMemberParams{
		GuildID:       member.GuildID,
		UserID:        member.UserID,
		Username:      member.Username,
		GlobalName:    nullString(member.GlobalName),
		DisplayName:   nullString(member.DisplayName),
		Nick:          nullString(member.Nick),
		Discriminator: nullString(member.Discriminator),
		Avatar:        nullString(member.Avatar),
		Bot:           int64(boolInt(member.Bot)),
		JoinedAt:      nullString(member.JoinedAt),
		RoleIdsJson:   member.RoleIDsJSON,
		RawJson:       member.RawJSON,
		UpdatedAt:     now,
	}
}

func upsertMessageParams(message MessageRecord, now string) storedb.UpsertMessageParams {
	return storedb.UpsertMessageParams{
		ID:                message.ID,
		GuildID:           message.GuildID,
		ChannelID:         message.ChannelID,
		AuthorID:          nullString(message.AuthorID),
		MessageType:       int64(message.MessageType),
		CreatedAt:         normalizeStoredTime(message.CreatedAt),
		EditedAt:          nullString(normalizeStoredTime(message.EditedAt)),
		DeletedAt:         nullString(normalizeStoredTime(message.DeletedAt)),
		Content:           message.Content,
		NormalizedContent: message.NormalizedContent,
		ReplyToMessageID:  nullString(message.ReplyToMessageID),
		Pinned:            int64(boolInt(message.Pinned)),
		HasAttachments:    int64(boolInt(message.HasAttachments)),
		RawJson:           message.RawJSON,
		UpdatedAt:         now,
	}
}

func insertMessageAttachmentParams(attachment AttachmentRecord, now string) storedb.InsertMessageAttachmentParams {
	return storedb.InsertMessageAttachmentParams{
		AttachmentID:  attachment.AttachmentID,
		MessageID:     attachment.MessageID,
		GuildID:       attachment.GuildID,
		ChannelID:     attachment.ChannelID,
		AuthorID:      nullString(attachment.AuthorID),
		Filename:      attachment.Filename,
		ContentType:   nullString(attachment.ContentType),
		Size:          attachment.Size,
		Url:           nullString(attachment.URL),
		ProxyUrl:      nullString(attachment.ProxyURL),
		TextContent:   attachment.TextContent,
		MediaPath:     nullString(attachment.MediaPath),
		ContentSha256: nullString(attachment.ContentSHA256),
		ContentSize:   attachment.ContentSize,
		FetchedAt:     nullString(attachment.FetchedAt),
		FetchStatus:   attachment.FetchStatus,
		FetchError:    attachment.FetchError,
		UpdatedAt:     now,
	}
}

func normalizeStoredTime(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC().Format(timeLayout)
		}
	}
	return raw
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
