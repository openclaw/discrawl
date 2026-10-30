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
	TextPartsJSON     string
	TextVersion       int
}

type AttachmentRecord struct {
	AttachmentID    string
	MessageID       string
	GuildID         string
	ChannelID       string
	AuthorID        string
	Filename        string
	ContentType     string
	Size            int64
	URL             string
	ProxyURL        string
	TextContent     string
	MediaPath       string
	ContentSHA256   string
	ContentSize     int64
	FetchedAt       string
	FetchStatus     string
	FetchError      string
	TextStatus      string
	TextError       string
	TextAttemptedAt string
	TextSucceededAt string
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
	ObservationStartedAt time.Time
	ScopePolicy          string
	AppendEvent          bool
	EnqueueEmbedding     bool
	EmbeddingCatchUp     bool
	PreserveNewer        bool
	DeduplicateEvent     bool
}

const deleteMessageFTSByRowIDSQL = `delete from message_fts where rowid = ?`

func (s *Store) UpsertGuild(ctx context.Context, guild GuildRecord) error {
	return s.UpsertObservedGuild(ctx, guild, time.Time{})
}

// UpsertObservedGuild stores guild metadata unless a write or check recorded
// after started is newer. An observation whose canonical content equals the
// stored row writes no content row; it records a check and any changed live
// counters only.
func (s *Store) UpsertObservedGuild(ctx context.Context, guild GuildRecord, started time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := upsertGuildTx(ctx, tx, s.q.WithTx(tx), guild, started); err != nil {
		return err
	}
	return tx.Commit()
}

func upsertGuildTx(ctx context.Context, tx *sql.Tx, qtx *storedb.Queries, guild GuildRecord, started time.Time) error {
	if err := recordEvidenceTx(ctx, tx, "guilds", guild, guild.ID); err != nil {
		return err
	}
	var name, icon, raw, deleted string
	err := tx.QueryRowContext(ctx, `select name,coalesce(icon,''),raw_json,coalesce(deleted_at,'') from guilds where id=?`, guild.ID).Scan(&name, &icon, &raw, &deleted)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	exists := err == nil
	if exists && !started.IsZero() {
		last, _, err := lastObservedTx(ctx, tx, "guilds", "id=?", guild.ID)
		if err != nil {
			return err
		}
		if last.After(started) {
			return nil
		}
	}
	merged, metrics := canonicalizeRaw(entityGuild, guild.RawJSON, raw, "")
	changed := !exists || deleted != "" || name != guild.Name || icon != guild.Icon || !sameCanonicalRaw(entityGuild, raw, merged, "")
	if changed {
		if exists {
			if err := retainLegacyRawTx(ctx, tx, "guilds", raw, guild.ID); err != nil {
				return err
			}
		}
		if err := qtx.UpsertGuild(ctx, storedb.UpsertGuildParams{ID: guild.ID, Name: guild.Name, Icon: nullString(guild.Icon), RawJson: merged, UpdatedAt: time.Now().UTC().Format(timeLayout)}); err != nil {
			return err
		}
	}
	if err := recordMetricsTx(ctx, tx, "guild", guild.ID, metrics); err != nil {
		return err
	}
	return recordObservedTx(ctx, tx, "guilds", changed, guild.ID)
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

// MarkObservedGuildDeleted tombstones a guild unless a write recorded after
// the deletion evidence was requested (for example a Gateway rejoin) is newer.
func (s *Store) MarkObservedGuildDeleted(ctx context.Context, guildID, source, reason string, started time.Time) error {
	if strings.TrimSpace(guildID) == "" || strings.TrimSpace(source) == "" || strings.TrimSpace(reason) == "" {
		return errors.New("guild tombstone requires guild id, source, and reason")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	last, exists, err := lastObservedTx(ctx, tx, "guilds", "id=?", guildID)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if !started.IsZero() && last.After(started) {
		return nil
	}
	now := time.Now().UTC().Format(timeLayout)
	if err := s.q.WithTx(tx).MarkGuildDeleted(ctx, storedb.MarkGuildDeletedParams{
		DeletedAt:      nullString(now),
		DeletionSource: nullString(source),
		DeletionReason: nullString(reason),
		UpdatedAt:      now,
		ID:             guildID,
	}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) UpsertChannel(ctx context.Context, channel ChannelRecord) error {
	return s.UpsertObservedChannel(ctx, channel, time.Time{})
}

func (s *Store) UpsertObservedChannel(ctx context.Context, channel ChannelRecord, started time.Time) error {
	return s.upsertChannel(ctx, channel, started, true)
}

// UpsertChannels applies authoritative channel metadata, such as a full
// GUILD_CREATE or THREAD_LIST_SYNC catalog, in one transaction. Each record
// follows UpsertChannel semantics; the batch commits once, so a large catalog
// costs one durable commit instead of one per channel, and an interrupted
// batch leaves no partially applied catalog.
func (s *Store) UpsertChannels(ctx context.Context, channels []ChannelRecord) error {
	if len(channels) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	for _, channel := range channels {
		if err := s.upsertChannelTx(ctx, tx, channel, time.Time{}, true); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// UpsertCachedChannel stores channel metadata from a non-authoritative cache,
// such as Discord Desktop. Cached metadata never resurrects a recorded
// deletion (a tombstoned channel keeps its retained metadata and tombstone)
// and never replaces stored ancestry or a known kind.
func (s *Store) UpsertCachedChannel(ctx context.Context, channel ChannelRecord) error {
	return s.upsertChannel(ctx, channel, time.Time{}, false)
}

func (s *Store) upsertChannel(ctx context.Context, channel ChannelRecord, started time.Time, authoritative bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := s.upsertChannelTx(ctx, tx, channel, started, authoritative); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) upsertChannelTx(ctx context.Context, tx *sql.Tx, channel ChannelRecord, started time.Time, authoritative bool) error {
	if err := recordEvidenceTx(ctx, tx, "channels", channel, channel.ID); err != nil {
		return err
	}
	var stored storedChannel
	err := tx.QueryRowContext(ctx, `select guild_id,coalesce(parent_id,''),kind,name,coalesce(topic,''),coalesce(position,0),is_nsfw,is_archived,is_locked,is_private_thread,coalesce(thread_parent_id,''),coalesce(archive_timestamp,''),raw_json,coalesce(deleted_at,'') from channels where id=?`, channel.ID).
		Scan(&stored.guild, &stored.parent, &stored.kind, &stored.name, &stored.topic, &stored.position, &stored.nsfw, &stored.archived, &stored.locked, &stored.private, &stored.threadParent, &stored.archiveTimestamp, &stored.raw, &stored.deleted)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	exists := err == nil
	if exists && !started.IsZero() {
		last, _, err := lastObservedTx(ctx, tx, "channels", "id=?", channel.ID)
		if err != nil {
			return err
		}
		if last.After(started) {
			return nil
		}
	}
	if exists && !authoritative {
		if stored.deleted != "" {
			return nil
		}
		// Cached metadata may be stale or incomplete: it never replaces
		// stored ancestry or a known kind, and only fills missing values.
		if stored.parent != "" {
			channel.ParentID = stored.parent
		}
		if stored.kind != "" && stored.kind != "unknown" {
			channel.Kind = stored.kind
		}
	}
	channel.ArchiveTimestamp = normalizeProviderTime(channel.ArchiveTimestamp)
	merged, metrics := canonicalizeRaw(entityChannel, channel.RawJSON, stored.raw, channel.Kind)
	channel.RawJSON = merged
	// Repeated observations write no content row when every content column
	// and the canonical provider metadata are unchanged.
	changed := !exists || stored.deleted != "" || stored.guild != channel.GuildID || stored.parent != channel.ParentID ||
		stored.kind != channel.Kind || stored.name != channel.Name || stored.topic != channel.Topic ||
		stored.position != int64(channel.Position) || stored.nsfw != int64(boolInt(channel.IsNSFW)) ||
		stored.archived != int64(boolInt(channel.IsArchived)) || stored.locked != int64(boolInt(channel.IsLocked)) ||
		stored.private != int64(boolInt(channel.IsPrivateThread)) || stored.threadParent != channel.ThreadParentID ||
		normalizeProviderTime(stored.archiveTimestamp) != channel.ArchiveTimestamp ||
		!sameCanonicalRaw(entityChannel, stored.raw, merged, channel.Kind)
	if changed {
		if exists {
			if err := retainLegacyRawTx(ctx, tx, "channels", stored.raw, channel.ID); err != nil {
				return err
			}
		}
		if exists && (stored.parent != channel.ParentID || stored.kind != channel.Kind) {
			if err = invalidateChannelScope(ctx, tx, channel.ID); err != nil {
				return err
			}
		}
		if err = s.q.WithTx(tx).UpsertChannel(ctx, upsertChannelParams(channel, time.Now().UTC().Format(timeLayout))); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `update channels set deleted_at=null,deletion_source=null where id=? and deleted_at is not null`, channel.ID); err != nil {
			return err
		}
	}
	if err := recordMetricsTx(ctx, tx, "channel", channel.ID, metrics); err != nil {
		return err
	}
	return recordObservedTx(ctx, tx, "channels", changed, channel.ID)
}

type storedChannel struct {
	guild, parent, kind, name, topic, threadParent, archiveTimestamp, raw, deleted string
	position, nsfw, archived, locked, private                                      int64
}

// MergeMembers refreshes observed members without treating absence as deletion.
func (s *Store) MergeMembers(ctx context.Context, guildID string, members []MemberRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	for _, member := range members {
		if member.GuildID != guildID {
			return fmt.Errorf("member guild %q does not match refresh guild %q", member.GuildID, guildID)
		}
		if _, err := upsertMemberTx(ctx, tx, qtx, member); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) MarkMemberDeleted(ctx context.Context, guildID, userID, source, reason string) error {
	return s.MarkObservedMemberDeleted(ctx, guildID, userID, source, reason, time.Time{})
}

func (s *Store) MarkObservedMemberDeleted(ctx context.Context, guildID, userID, source, reason string, observed time.Time) error {
	if strings.TrimSpace(guildID) == "" || strings.TrimSpace(userID) == "" || strings.TrimSpace(source) == "" || strings.TrimSpace(reason) == "" {
		return errors.New("member tombstone requires guild id, user id, source, and reason")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if !observed.IsZero() {
		last, exists, err := lastObservedTx(ctx, tx, "members", "guild_id=? and user_id=?", guildID, userID)
		if err != nil {
			return err
		}
		if exists && last.After(observed) {
			return nil
		}
	}
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
	return s.UpsertObservedMember(ctx, member, time.Time{})
}

func (s *Store) UpsertObservedMember(ctx context.Context, member MemberRecord, started time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if !started.IsZero() {
		last, exists, err := lastObservedTx(ctx, tx, "members", "guild_id=? and user_id=?", member.GuildID, member.UserID)
		if err != nil {
			return err
		}
		if exists && last.After(started) {
			if err := recordEvidenceTx(ctx, tx, "members", member, member.GuildID, member.UserID); err != nil {
				return err
			}
			return tx.Commit()
		}
	}
	if _, err := upsertMemberTx(ctx, tx, s.q.WithTx(tx), member); err != nil {
		return err
	}
	return tx.Commit()
}

// upsertMemberTx writes a member only when a content column or the canonical
// provider metadata changed, and records the observation either way. The
// search index is refreshed only with a content write.
func upsertMemberTx(ctx context.Context, tx *sql.Tx, qtx *storedb.Queries, member MemberRecord) (bool, error) {
	if err := recordEvidenceTx(ctx, tx, "members", member, member.GuildID, member.UserID); err != nil {
		return false, err
	}
	var username, globalName, displayName, nick, discriminator, avatar, joined, roles, raw, deleted string
	var bot int64
	err := tx.QueryRowContext(ctx, `select username,coalesce(global_name,''),coalesce(display_name,''),coalesce(nick,''),coalesce(discriminator,''),coalesce(avatar,''),bot,coalesce(joined_at,''),role_ids_json,raw_json,coalesce(deleted_at,'') from members where guild_id=? and user_id=?`, member.GuildID, member.UserID).
		Scan(&username, &globalName, &displayName, &nick, &discriminator, &avatar, &bot, &joined, &roles, &raw, &deleted)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	exists := err == nil
	member.JoinedAt = normalizeProviderTime(member.JoinedAt)
	member.RawJSON, _ = canonicalizeRaw(entityMember, member.RawJSON, raw, "")
	changed := !exists || deleted != "" || username != member.Username || globalName != member.GlobalName ||
		displayName != member.DisplayName || nick != member.Nick || discriminator != member.Discriminator ||
		avatar != member.Avatar || bot != int64(boolInt(member.Bot)) || normalizeProviderTime(joined) != member.JoinedAt ||
		roles != member.RoleIDsJSON || !sameCanonicalRaw(entityMember, raw, member.RawJSON, "")
	if changed {
		if exists {
			if err := retainLegacyRawTx(ctx, tx, "members", raw, member.GuildID, member.UserID); err != nil {
				return false, err
			}
		}
		if err := qtx.UpsertMember(ctx, upsertMemberParams(member, time.Now().UTC().Format(timeLayout))); err != nil {
			return false, err
		}
		if err := upsertMemberFTSTx(ctx, tx, member); err != nil {
			return false, err
		}
	}
	return changed, recordObservedTx(ctx, tx, "members", changed, member.GuildID, member.UserID)
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
	if err := s.deleteLexicalMessagesTx(ctx, tx, "guild_id", guildID); err != nil {
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
	if err := recordEvidenceTx(ctx, tx, "messages", message, message.ID); err != nil {
		return err
	}
	if opts.PreserveNewer {
		keep, err := keepStoredMessage(ctx, tx, s.q.WithTx(tx), message, opts.ObservationStartedAt)
		if err != nil {
			return err
		}
		if keep {
			return tx.Commit()
		}
	}
	changed, err := s.upsertMessageTx(ctx, tx, s.q.WithTx(tx), message, opts)
	if err != nil {
		return err
	}
	if err := recordObservedTx(ctx, tx, "messages", changed, message.ID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notifyEmbeddingWork()
	return nil
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
		if err := requireMessageScope(ctx, tx, message.Record, message.Options.ScopePolicy); err != nil {
			return err
		}
		if err := recordEvidenceTx(ctx, tx, "messages", message, message.Record.ID); err != nil {
			return err
		}
		if err := prepareTextMutation(ctx, tx, &message); err != nil {
			return err
		}
		if message.Options.PreserveNewer {
			keep, err := keepStoredMessage(ctx, tx, qtx, message.Record, message.Options.ObservationStartedAt)
			if err != nil {
				return err
			}
			if keep {
				// Keep canonical state and its related rows, but retain a genuine
				// Gateway or snapshot observation in the same transaction.
				if err := appendMutationEventTx(ctx, qtx, message); err != nil {
					return err
				}
				continue
			}
		}
		changed, err := s.upsertMessageTx(ctx, tx, qtx, message.Record, message.Options)
		if err != nil {
			return err
		}
		attachmentsChanged, err := applyAttachmentsTx(ctx, tx, qtx, message.Record.ID, message.Attachments)
		if err != nil {
			return err
		}
		mentionsChanged, err := applyMentionEventsTx(ctx, tx, message.Record.ID, message.Mentions)
		if err != nil {
			return err
		}
		if !changed && (attachmentsChanged || mentionsChanged) {
			// Readers rebind relationship rows from the owning message's
			// updated_at, so a relationship change still advances it.
			if _, err := tx.ExecContext(ctx, `update messages set updated_at=? where id=?`, time.Now().UTC().Format(timeLayout), message.Record.ID); err != nil {
				return err
			}
			changed = true
		}
		if err := recordObservedTx(ctx, tx, "messages", changed, message.Record.ID); err != nil {
			return err
		}
		if err := appendMutationEventTx(ctx, qtx, message); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notifyEmbeddingWork()
	return nil
}

func appendMutationEventTx(ctx context.Context, qtx *storedb.Queries, message MessageMutation) error {
	if !message.Options.AppendEvent || message.EventType == "" {
		return nil
	}
	if message.Options.DeduplicateEvent {
		exists, err := qtx.MessageEventExists(ctx, storedb.MessageEventExistsParams{
			MessageID: message.Record.ID, EventType: message.EventType, PayloadJson: message.PayloadJSON,
		})
		if err != nil || exists {
			return err
		}
	}
	return appendEventTx(
		ctx,
		qtx,
		message.Record.GuildID,
		message.Record.ChannelID,
		message.Record.ID,
		message.EventType,
		message.PayloadJSON,
	)
}

func keepStoredMessage(ctx context.Context, tx *sql.Tx, qtx *storedb.Queries, message MessageRecord, started time.Time) (bool, error) {
	stored, err := qtx.GetMessageRevision(ctx, message.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Fixed UTC precision makes normalized edit timestamps sortable. Keep
	// tombstones and all related rows intact when an older cache is replayed.
	if stored.DeletedAt != "" {
		return true, nil
	}
	incoming, current := normalizeStoredTime(message.EditedAt), normalizeStoredTime(stored.EditedAt)
	if incoming != current {
		// Provider edit order decides when it exists. A strictly newer edit
		// wins even after local projection-only writes advanced updated_at.
		return incoming < current, nil
	}
	// Equal or absent provider revisions cannot be ordered by Discord. Then
	// any write after this observation started is treated as newer.
	if !started.IsZero() {
		last, _, err := lastObservedTx(ctx, tx, "messages", "id=?", message.ID)
		if err != nil {
			return false, err
		}
		if last.After(started) {
			return true, nil
		}
	}
	if message.Content != "" || message.DeletedAt != "" {
		return false, nil
	}
	var content, raw string
	if err := tx.QueryRowContext(ctx, "select content,raw_json from messages where id=?", message.ID).Scan(&content, &raw); err != nil {
		return false, err
	}
	// An unordered reread that only loses authored text is not evidence of
	// a clear. Retain the entire canonical mutation, including its related
	// rows and projections. An actual clear without a newer edit revision
	// and with identical shape is indistinguishable, so prefer known text.
	// Distinct rich-message changes keep their existing admission policy.
	// Admission must compare the same merged transport representation that
	// the writer stores, particularly inside referenced_message. Otherwise
	// retained Gateway metadata makes an empty REST reread look like an edit.
	incomingRaw, _ := canonicalizeRaw(entityMessage, message.RawJSON, raw, "")
	return content != "" && sameMessageTextShape(raw, incomingRaw), nil
}

// Compare message identity and content-bearing fields, not mutable user names,
// reactions, pin state, or SDK receipt-schema additions. Empty collections are
// equivalent whether omitted, null, or []. GuildID comes from channel scope and
// may be absent on REST receipts. A newer provider revision bypasses this check.
func sameMessageTextShape(a, b string) bool {
	var shapes [2]struct {
		ID        string    `json:"id"`
		ChannelID string    `json:"channel_id"`
		Timestamp time.Time `json:"timestamp"`
		Type      int       `json:"type"`
		Author    *struct {
			ID string `json:"id"`
		} `json:"author"`
		Attachments       []any `json:"attachments,omitempty"`
		Embeds            []any `json:"embeds,omitempty"`
		Components        []any `json:"components,omitempty"`
		StickerItems      []any `json:"sticker_items,omitempty"`
		Poll              any   `json:"poll,omitempty"`
		MessageReference  any   `json:"message_reference,omitempty"`
		ReferencedMessage any   `json:"referenced_message,omitempty"`
		MessageSnapshots  []any `json:"message_snapshots,omitempty"`
		Mentions          []struct {
			ID string `json:"id"`
		} `json:"mentions,omitempty"`
		MentionRoles    []string `json:"mention_roles,omitempty"`
		MentionEveryone bool     `json:"mention_everyone"`
		Flags           int      `json:"flags"`
	}
	var encoded [2]string
	for i, raw := range []string{a, b} {
		if json.Unmarshal([]byte(raw), &shapes[i]) != nil || shapes[i].ID == "" || shapes[i].ChannelID == "" {
			return false
		}
		body, err := json.Marshal(shapes[i])
		if err != nil {
			return false
		}
		encoded[i] = string(body)
	}
	return encoded[0] == encoded[1]
}

func (s *Store) upsertMessageTx(
	ctx context.Context,
	tx *sql.Tx,
	qtx *storedb.Queries,
	message MessageRecord,
	opts WriteOptions,
) (bool, error) {
	stored, exists, err := loadStoredMessageTx(ctx, tx, message.ID)
	if err != nil {
		return false, err
	}
	message.RawJSON, _ = canonicalizeRaw(entityMessage, message.RawJSON, stored.raw, "")
	if exists && !messageContentChanged(stored, message) {
		return false, s.refreshUnchangedMessageTx(ctx, tx, qtx, stored, message, opts)
	}
	if exists {
		if err := retainLegacyRawTx(ctx, tx, "messages", stored.raw, message.ID); err != nil {
			return false, err
		}
	}
	var tokenized map[string]string
	if message.DeletedAt == "" {
		var err error
		tokenized, err = s.tokenizeLexical(ctx, message.NormalizedContent)
		if err != nil {
			return true, err
		}
	}
	now := time.Now().UTC().Format(timeLayout)
	var previousNormalized sql.NullString
	jobExists := false
	normalized, previousErr := qtx.GetMessageNormalizedContent(ctx, message.ID)
	if previousErr != nil && !errors.Is(previousErr, sql.ErrNoRows) {
		return true, previousErr
	}
	if previousErr == nil {
		previousNormalized = sql.NullString{String: normalized, Valid: true}
		if opts.EnqueueEmbedding {
			existingJobs, err := qtx.CountEmbeddingJobsByMessage(ctx, message.ID)
			if err != nil {
				return true, err
			}
			jobExists = existingJobs > 0
		}
	}
	if previousNormalized.Valid && previousNormalized.String != message.NormalizedContent {
		if err := archiveMessageEmbeddings(ctx, tx, message.ID, previousNormalized.String); err != nil {
			return true, err
		}
		if err := qtx.DeleteMessageEmbeddingsByMessage(ctx, message.ID); err != nil {
			return true, err
		}
		if !opts.EnqueueEmbedding {
			if _, err := tx.ExecContext(ctx, `update embedding_jobs set revision=revision+1,lease_token='',lease_until='',locked_at=null where message_id=?`, message.ID); err != nil {
				return true, err
			}
		}
	}
	if err := qtx.UpsertMessage(ctx, upsertMessageParams(message, now)); err != nil {
		return true, &RowWriteError{
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
	if message.TextVersion > 0 {
		if _, err := tx.ExecContext(ctx, `update messages set text_parts_json=?,text_version=? where id=?`, message.TextPartsJSON, message.TextVersion, message.ID); err != nil {
			return true, err
		}
	} else if _, err := tx.ExecContext(ctx, `update messages set text_parts_json='[]',text_version=0 where id=? and text_version<>0`, message.ID); err != nil {
		// A writer without an attributed projection (for example a Desktop
		// import) supersedes earlier parts; never keep stale provenance.
		return true, err
	}
	if rowID, ok := messageFTSRowID(message.ID); ok {
		if _, err := tx.ExecContext(ctx, deleteMessageFTSByRowIDSQL, rowID); err != nil {
			return true, err
		}
		if err := s.upsertLexicalMessageTx(ctx, tx, message, tokenized); err != nil {
			return true, err
		}
		if message.DeletedAt != "" {
			if err := archiveMessageEmbeddings(ctx, tx, message.ID, previousNormalized.String); err != nil {
				return true, err
			}
			if err := qtx.DeleteMessageEmbeddingsByMessage(ctx, message.ID); err != nil {
				return true, err
			}
			if _, err := tx.ExecContext(ctx, `delete from embedding_jobs where message_id = ?`, message.ID); err != nil {
				return true, err
			}
			return true, nil
		}
		if _, err := tx.ExecContext(ctx, `
			insert into message_fts(rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content)
			values(?, ?, ?, ?, ?, ?, ?, ?)
		`, rowID, message.ID, message.GuildID, message.ChannelID, nullable(message.AuthorID), message.AuthorName, message.ChannelName, message.NormalizedContent); err != nil {
			return true, err
		}
	}
	queueEmbedding := opts.EnqueueEmbedding && message.GuildID != "@me" &&
		(errors.Is(previousErr, sql.ErrNoRows) || previousNormalized.String != message.NormalizedContent || !jobExists)
	if queueEmbedding {
		if err := archiveMessageEmbeddings(ctx, tx, message.ID, previousNormalized.String); err != nil {
			return true, err
		}
		if err := qtx.UpsertEmbeddingJobPending(ctx, storedb.UpsertEmbeddingJobPendingParams{MessageID: message.ID, UpdatedAt: now}); err != nil {
			return true, err
		}
		priority := 1
		if opts.EmbeddingCatchUp {
			priority = 0
		}
		if _, err := tx.ExecContext(ctx, `update embedding_jobs set revision=revision+1,lease_token='',lease_until='',available_at='',priority=?,enqueued_at=? where message_id=?`, priority, now, message.ID); err != nil {
			return true, err
		}
		if err := qtx.DeleteMessageEmbeddingsByMessage(ctx, message.ID); err != nil {
			return true, err
		}
	}
	return true, nil
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
		if err := s.deleteLexicalMessagesTx(ctx, tx, "rowid", rowID); err != nil {
			return err
		}
	}
	var retainedText string
	if err := tx.QueryRowContext(ctx, `select normalized_content from messages where id=?`, messageID).Scan(&retainedText); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := archiveMessageEmbeddings(ctx, tx, messageID, retainedText); err != nil {
		return err
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

// applyAttachmentsTx reconciles a message's attachment rows in place. A row
// is written only when one of its content columns changes; attachments that
// disappeared from the message are deleted. text_attempted_at and
// text_succeeded_at are check fields: an extraction attempt that changes no
// content is recorded in entity_checks ('attachment_text') and leaves the row
// untouched. Retained text history is unchanged.
func applyAttachmentsTx(ctx context.Context, tx *sql.Tx, qtx *storedb.Queries, messageID string, attachments []AttachmentRecord) (bool, error) {
	if err := retainAttachmentText(ctx, tx, messageID); err != nil {
		return false, err
	}
	existing, err := existingAttachmentMediaTx(ctx, qtx, messageID)
	if err != nil {
		return false, err
	}
	keep := make(map[string]struct{}, len(attachments))
	for _, attachment := range attachments {
		keep[attachment.AttachmentID] = struct{}{}
	}
	changed := false
	for id := range existing {
		if _, ok := keep[id]; ok {
			continue
		}
		if err := retainLegacyAttachmentTx(ctx, tx, id); err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(ctx, `delete from message_attachments where attachment_id=?`, id); err != nil {
			return false, err
		}
		changed = true
	}
	now := time.Now().UTC().Format(timeLayout)
	for _, attachment := range attachments {
		if err := recordEvidenceTx(ctx, tx, "message_attachments", attachment, attachment.AttachmentID); err != nil {
			return false, err
		}
		if media, ok := existing[attachment.AttachmentID]; ok && attachment.MediaPath == "" {
			attachment.MediaPath = media.MediaPath
			attachment.ContentSHA256 = media.ContentSHA256
			attachment.ContentSize = media.ContentSize
			attachment.FetchedAt = media.FetchedAt
			attachment.FetchStatus = media.FetchStatus
			attachment.FetchError = media.FetchError
		}
		if attachment.TextStatus == "" {
			attachment.TextStatus = "unknown"
		}
		same, err := sameStoredAttachmentTx(ctx, tx, attachment)
		if err != nil {
			return false, err
		}
		if !same {
			if err := retainLegacyAttachmentTx(ctx, tx, attachment.AttachmentID); err != nil {
				return false, err
			}
			if err := writeAttachmentTx(ctx, tx, qtx, attachment, now); err != nil {
				return false, err
			}
			changed = true
		}
		if attachment.TextAttemptedAt != "" {
			if err := recordCheckTx(ctx, tx, "message_attachments", checkAttachmentText, normalizeStoredTime(attachment.TextAttemptedAt), normalizeStoredTime(attachment.TextSucceededAt), attachment.TextStatus, attachment.AttachmentID); err != nil {
				return false, err
			}
		}
	}
	return changed, nil
}

// sameStoredAttachmentTx compares every content column the writer sets,
// using the upsert's own merge rules for media columns.
func sameStoredAttachmentTx(ctx context.Context, tx *sql.Tx, a AttachmentRecord) (bool, error) {
	var messageID, guildID, channelID, authorID, filename, contentType, url, proxyURL, text, mediaPath, sha, fetchedAt, fetchStatus, fetchError, textStatus, textError string
	var size, contentSize int64
	err := tx.QueryRowContext(ctx, `select message_id,guild_id,channel_id,coalesce(author_id,''),filename,coalesce(content_type,''),size,coalesce(url,''),coalesce(proxy_url,''),text_content,coalesce(media_path,''),coalesce(content_sha256,''),content_size,coalesce(fetched_at,''),fetch_status,fetch_error,text_status,text_error from message_attachments where attachment_id=?`, a.AttachmentID).
		Scan(&messageID, &guildID, &channelID, &authorID, &filename, &contentType, &size, &url, &proxyURL, &text, &mediaPath, &sha, &contentSize, &fetchedAt, &fetchStatus, &fetchError, &textStatus, &textError)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	effective := func(incoming, stored string) string {
		if incoming != "" {
			return incoming
		}
		return stored
	}
	wantContentSize := contentSize
	if a.ContentSize > 0 {
		wantContentSize = a.ContentSize
	}
	wantFetchError := fetchError
	if a.FetchStatus != "" || a.FetchError != "" {
		wantFetchError = a.FetchError
	}
	return messageID == a.MessageID && guildID == a.GuildID && channelID == a.ChannelID && authorID == a.AuthorID &&
		filename == a.Filename && contentType == a.ContentType && size == a.Size && url == a.URL && proxyURL == a.ProxyURL &&
		text == a.TextContent && mediaPath == effective(a.MediaPath, mediaPath) && sha == effective(a.ContentSHA256, sha) &&
		contentSize == wantContentSize && fetchedAt == effective(a.FetchedAt, fetchedAt) &&
		fetchStatus == effective(a.FetchStatus, fetchStatus) && fetchError == wantFetchError &&
		textStatus == a.TextStatus && textError == a.TextError, nil
}

// writeAttachmentTx writes one attachment row, including its text receipt, in
// a single statement, so one content change is one change-feed entry. Media
// columns follow the InsertMessageAttachment merge rules.
func writeAttachmentTx(ctx context.Context, tx *sql.Tx, _ *storedb.Queries, attachment AttachmentRecord, now string) error {
	p := insertMessageAttachmentParams(attachment, now)
	_, err := tx.ExecContext(ctx, `insert into message_attachments(
	attachment_id, message_id, guild_id, channel_id, author_id, filename,
	content_type, size, url, proxy_url, text_content, media_path, content_sha256,
	content_size, fetched_at, fetch_status, fetch_error, updated_at,
	text_status, text_error, text_attempted_at, text_succeeded_at
) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(attachment_id) do update set
	message_id = excluded.message_id,
	guild_id = excluded.guild_id,
	channel_id = excluded.channel_id,
	author_id = excluded.author_id,
	filename = excluded.filename,
	content_type = excluded.content_type,
	size = excluded.size,
	url = excluded.url,
	proxy_url = excluded.proxy_url,
	text_content = excluded.text_content,
	media_path = coalesce(excluded.media_path, message_attachments.media_path),
	content_sha256 = coalesce(excluded.content_sha256, message_attachments.content_sha256),
	content_size = case when excluded.content_size > 0 then excluded.content_size else message_attachments.content_size end,
	fetched_at = coalesce(excluded.fetched_at, message_attachments.fetched_at),
	fetch_status = case when excluded.fetch_status <> '' then excluded.fetch_status else message_attachments.fetch_status end,
	fetch_error = case when excluded.fetch_status <> '' or excluded.fetch_error <> '' then excluded.fetch_error else message_attachments.fetch_error end,
	updated_at = excluded.updated_at,
	text_status = excluded.text_status,
	text_error = excluded.text_error,
	text_attempted_at = excluded.text_attempted_at,
	text_succeeded_at = excluded.text_succeeded_at`,
		p.AttachmentID, p.MessageID, p.GuildID, p.ChannelID, p.AuthorID, p.Filename,
		p.ContentType, p.Size, p.Url, p.ProxyUrl, p.TextContent, p.MediaPath, p.ContentSha256,
		p.ContentSize, p.FetchedAt, p.FetchStatus, p.FetchError, p.UpdatedAt,
		attachment.TextStatus, attachment.TextError, nullString(attachment.TextAttemptedAt), nullString(attachment.TextSucceededAt))
	if err != nil {
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

// applyMentionEventsTx reconciles a message's mention events by their natural
// identity (message_id, target_type, target_id). An unchanged mention keeps
// its row and event_id; a changed target name, scope or time is updated in
// place; only mentions that appeared or disappeared are inserted or deleted.
// Duplicate rows for one identity (from older writers) collapse to the
// lowest event_id.
func applyMentionEventsTx(ctx context.Context, tx *sql.Tx, messageID string, mentions []MentionEventRecord) (bool, error) {
	type row struct {
		id                                    int64
		guild, channel, author, name, eventAt string
	}
	existing := map[[2]string]row{}
	var drop []int64
	rows, err := tx.QueryContext(ctx, `select event_id,target_type,target_id,guild_id,channel_id,coalesce(author_id,''),target_name,event_at from mention_events where message_id=? order by event_id`, messageID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var r row
		var key [2]string
		if err := rows.Scan(&r.id, &key[0], &key[1], &r.guild, &r.channel, &r.author, &r.name, &r.eventAt); err != nil {
			return false, err
		}
		if _, dup := existing[key]; dup {
			drop = append(drop, r.id)
			continue
		}
		existing[key] = r
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	changed := false
	seen := map[[2]string]struct{}{}
	for _, mention := range mentions {
		key := [2]string{mention.TargetType, mention.TargetID}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		eventAt := normalizeStoredTime(mention.EventAt)
		prior, ok := existing[key]
		if eventAt == "" {
			if ok {
				eventAt = prior.eventAt
			} else {
				eventAt = time.Now().UTC().Format(timeLayout)
			}
		}
		if !ok {
			if _, err := tx.ExecContext(ctx, `insert into mention_events(message_id,guild_id,channel_id,author_id,target_type,target_id,target_name,event_at) values(?,?,?,?,?,?,?,?)`,
				messageID, mention.GuildID, mention.ChannelID, nullString(mention.AuthorID), mention.TargetType, mention.TargetID, mention.TargetName, eventAt); err != nil {
				return false, err
			}
			changed = true
			continue
		}
		delete(existing, key)
		if prior.guild == mention.GuildID && prior.channel == mention.ChannelID && prior.author == mention.AuthorID && prior.name == mention.TargetName && prior.eventAt == eventAt {
			continue
		}
		if err := retainMentionIDTx(ctx, tx, prior.id); err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(ctx, `update mention_events set guild_id=?,channel_id=?,author_id=?,target_name=?,event_at=? where event_id=?`,
			mention.GuildID, mention.ChannelID, nullString(mention.AuthorID), mention.TargetName, eventAt, prior.id); err != nil {
			return false, err
		}
		changed = true
	}
	for _, r := range existing {
		drop = append(drop, r.id)
	}
	for _, id := range drop {
		if err := retainMentionIDTx(ctx, tx, id); err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(ctx, `delete from mention_events where event_id=?`, id); err != nil {
			return false, err
		}
		changed = true
	}
	return changed, nil
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

type storedMessage struct {
	guild, channel, author, createdAt, editedAt, deletedAt, content, normalized, replyTo, raw, textParts string
	messageType, pinned, hasAttachments, textVersion                                                     int64
}

func loadStoredMessageTx(ctx context.Context, tx *sql.Tx, id string) (storedMessage, bool, error) {
	var m storedMessage
	err := tx.QueryRowContext(ctx, `select guild_id,channel_id,coalesce(author_id,''),message_type,created_at,coalesce(edited_at,''),coalesce(deleted_at,''),content,normalized_content,coalesce(reply_to_message_id,''),pinned,has_attachments,raw_json,text_parts_json,text_version from messages where id=?`, id).
		Scan(&m.guild, &m.channel, &m.author, &m.messageType, &m.createdAt, &m.editedAt, &m.deletedAt, &m.content, &m.normalized, &m.replyTo, &m.pinned, &m.hasAttachments, &m.raw, &m.textParts, &m.textVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return m, false, nil
	}
	return m, err == nil, err
}

// messageContentChanged compares every column the message writer sets, with
// the writer's own merge rules (a tombstone is never cleared by an upsert, and
// a writer without a text projection resets only an attributed one). The
// incoming raw_json must already be the canonical merge with stored.raw.
func messageContentChanged(stored storedMessage, message MessageRecord) bool {
	deleted := normalizeStoredTime(message.DeletedAt)
	if deleted == "" {
		deleted = stored.deletedAt
	}
	parts, version := message.TextPartsJSON, int64(message.TextVersion)
	if version <= 0 {
		parts, version = stored.textParts, stored.textVersion
		if stored.textVersion != 0 {
			parts, version = "[]", 0
		}
	}
	return stored.guild != message.GuildID || stored.channel != message.ChannelID || stored.author != message.AuthorID ||
		stored.messageType != int64(message.MessageType) || stored.createdAt != normalizeStoredTime(message.CreatedAt) ||
		stored.editedAt != normalizeStoredTime(message.EditedAt) || stored.deletedAt != deleted ||
		stored.content != message.Content || stored.normalized != message.NormalizedContent ||
		stored.replyTo != message.ReplyToMessageID || stored.pinned != int64(boolInt(message.Pinned)) ||
		stored.hasAttachments != int64(boolInt(message.HasAttachments)) || stored.textParts != parts ||
		stored.textVersion != version || !sameCanonicalRaw(entityMessage, stored.raw, message.RawJSON, "")
}

// refreshUnchangedMessageTx keeps derived state current for an observation
// that changed no message content: a missing embedding job is still queued,
// and the search row picks up a newly known author or channel name. It writes
// no messages row.
func (s *Store) refreshUnchangedMessageTx(ctx context.Context, tx *sql.Tx, qtx *storedb.Queries, stored storedMessage, message MessageRecord, opts WriteOptions) error {
	if stored.deletedAt != "" {
		return nil
	}
	if rowID, ok := messageFTSRowID(message.ID); ok {
		var authorName, channelName sql.NullString
		err := tx.QueryRowContext(ctx, `select author_name,channel_name from message_fts where rowid=?`, rowID).Scan(&authorName, &channelName)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		missing := errors.Is(err, sql.ErrNoRows)
		renamed := (message.AuthorName != "" && message.AuthorName != authorName.String) ||
			(message.ChannelName != "" && message.ChannelName != channelName.String)
		if missing || renamed {
			if !missing {
				if message.AuthorName == "" {
					message.AuthorName = authorName.String
				}
				if message.ChannelName == "" {
					message.ChannelName = channelName.String
				}
			}
			if _, err := tx.ExecContext(ctx, deleteMessageFTSByRowIDSQL, rowID); err != nil {
				return err
			}
			if missing {
				tokenized, err := s.tokenizeLexical(ctx, message.NormalizedContent)
				if err != nil {
					return err
				}
				if err := s.upsertLexicalMessageTx(ctx, tx, message, tokenized); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, `
				insert into message_fts(rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content)
				values(?, ?, ?, ?, ?, ?, ?, ?)
			`, rowID, message.ID, message.GuildID, message.ChannelID, nullable(message.AuthorID), message.AuthorName, message.ChannelName, message.NormalizedContent); err != nil {
				return err
			}
		}
	}
	if !opts.EnqueueEmbedding || message.GuildID == "@me" {
		return nil
	}
	jobs, err := qtx.CountEmbeddingJobsByMessage(ctx, message.ID)
	if err != nil || jobs > 0 {
		return err
	}
	now := time.Now().UTC().Format(timeLayout)
	if err := archiveMessageEmbeddings(ctx, tx, message.ID, stored.normalized); err != nil {
		return err
	}
	if err := qtx.UpsertEmbeddingJobPending(ctx, storedb.UpsertEmbeddingJobPendingParams{MessageID: message.ID, UpdatedAt: now}); err != nil {
		return err
	}
	priority := 1
	if opts.EmbeddingCatchUp {
		priority = 0
	}
	if _, err := tx.ExecContext(ctx, `update embedding_jobs set revision=revision+1,lease_token='',lease_until='',available_at='',priority=?,enqueued_at=? where message_id=?`, priority, now, message.ID); err != nil {
		return err
	}
	return qtx.DeleteMessageEmbeddingsByMessage(ctx, message.ID)
}
