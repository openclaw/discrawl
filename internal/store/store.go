package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/discrawl/internal/store/storedb"
)

const (
	timeLayout         = "2006-01-02T15:04:05.000000000Z07:00"
	messageFTSVersion  = "2"
	memberFTSVersion   = "1"
	storeSchemaVersion = 5
)

var ErrSchemaVersionMismatch = errors.New("database schema version mismatch")

type Store struct {
	db   *sql.DB
	q    *storedb.Queries
	path string
}

type Status struct {
	DBPath             string    `json:"db_path"`
	GuildCount         int       `json:"guild_count"`
	ChannelCount       int       `json:"channel_count"`
	ThreadCount        int       `json:"thread_count"`
	MessageCount       int       `json:"message_count"`
	MemberCount        int       `json:"member_count"`
	EmbeddingBacklog   int       `json:"embedding_backlog"`
	LastSyncAt         time.Time `json:"last_sync_at,omitzero"`
	LastTailEventAt    time.Time `json:"last_tail_event_at,omitzero"`
	DefaultGuildID     string    `json:"default_guild_id,omitempty"`
	DefaultGuildName   string    `json:"default_guild_name,omitempty"`
	AccessibleGuildIDs []string  `json:"accessible_guild_ids,omitempty"`
}

type SearchOptions struct {
	Query        string
	GuildIDs     []string
	Channel      string
	Author       string
	Limit        int
	IncludeEmpty bool
}

type SearchResult struct {
	MessageID   string    `json:"message_id"`
	GuildID     string    `json:"guild_id"`
	ChannelID   string    `json:"channel_id"`
	ChannelName string    `json:"channel_name"`
	AuthorID    string    `json:"author_id"`
	AuthorName  string    `json:"author_name"`
	Content     string    `json:"content"`
	CreatedAt   time.Time `json:"created_at"`
}

type MentionRow struct {
	MessageID   string    `json:"message_id"`
	GuildID     string    `json:"guild_id"`
	ChannelID   string    `json:"channel_id"`
	ChannelName string    `json:"channel_name"`
	AuthorID    string    `json:"author_id"`
	AuthorName  string    `json:"author_name"`
	TargetType  string    `json:"target_type"`
	TargetID    string    `json:"target_id"`
	TargetName  string    `json:"target_name"`
	Content     string    `json:"content"`
	CreatedAt   time.Time `json:"created_at"`
}

type MemberRow struct {
	GuildID       string    `json:"guild_id"`
	UserID        string    `json:"user_id"`
	Username      string    `json:"username"`
	GlobalName    string    `json:"global_name,omitempty"`
	DisplayName   string    `json:"display_name,omitempty"`
	Nick          string    `json:"nick,omitempty"`
	Discriminator string    `json:"discriminator,omitempty"`
	Avatar        string    `json:"avatar,omitempty"`
	RoleIDsJSON   string    `json:"role_ids_json"`
	Bot           bool      `json:"bot"`
	JoinedAt      time.Time `json:"joined_at,omitzero"`
	Bio           string    `json:"bio,omitempty"`
	Pronouns      string    `json:"pronouns,omitempty"`
	Location      string    `json:"location,omitempty"`
	Website       string    `json:"website,omitempty"`
	XHandle       string    `json:"x_handle,omitempty"`
	GitHubLogin   string    `json:"github_login,omitempty"`
	URLs          []string  `json:"urls,omitempty"`
	RawJSON       string    `json:"-"`
}

type ChannelRow struct {
	ID               string    `json:"id"`
	GuildID          string    `json:"guild_id"`
	ParentID         string    `json:"parent_id,omitempty"`
	Kind             string    `json:"kind"`
	Name             string    `json:"name"`
	Topic            string    `json:"topic,omitempty"`
	Position         int       `json:"position"`
	IsNSFW           bool      `json:"is_nsfw"`
	IsArchived       bool      `json:"is_archived"`
	IsLocked         bool      `json:"is_locked"`
	IsPrivateThread  bool      `json:"is_private_thread"`
	ThreadParentID   string    `json:"thread_parent_id,omitempty"`
	ArchiveTimestamp time.Time `json:"archive_timestamp,omitzero"`
}

func Open(ctx context.Context, path string) (*Store, error) {
	base, err := crawlstore.Open(ctx, crawlstore.Options{Path: path})
	if err != nil {
		return nil, err
	}
	db := base.DB()
	store := &Store{db: db, q: storedb.New(db), path: path}
	if err := store.migrate(ctx); err != nil {
		_ = base.Close()
		return nil, err
	}
	return store, nil
}

func OpenReadOnly(ctx context.Context, path string) (*Store, error) {
	base, err := crawlstore.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, err
	}
	db := base.DB()
	store := &Store{db: db, q: storedb.New(db), path: path}
	if version, err := store.schemaVersion(ctx); err != nil {
		_ = base.Close()
		return nil, err
	} else if version != storeSchemaVersion {
		_ = base.Close()
		return nil, fmt.Errorf("%w: got %d want %d", ErrSchemaVersionMismatch, version, storeSchemaVersion)
	}
	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) migrate(ctx context.Context) error {
	currentVersion, err := s.schemaVersion(ctx)
	if err != nil {
		return err
	}
	if currentVersion > storeSchemaVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d", currentVersion, storeSchemaVersion)
	}
	if currentVersion < 1 {
		if err := s.applyBaselineSchema(ctx); err != nil {
			return err
		}
		if err := s.setSchemaVersion(ctx, 1); err != nil {
			return err
		}
		currentVersion = 1
	}
	if currentVersion < 2 {
		if err := s.applyQueryIndexMigration(ctx); err != nil {
			return err
		}
		if err := s.setSchemaVersion(ctx, 2); err != nil {
			return err
		}
		currentVersion = 2
	}
	if currentVersion < 3 {
		if err := s.applyAttachmentMediaMigration(ctx); err != nil {
			return err
		}
		if err := s.setSchemaVersion(ctx, 3); err != nil {
			return err
		}
		currentVersion = 3
	}
	if currentVersion < 4 {
		if err := s.applyFailureLedgerMigration(ctx); err != nil {
			return err
		}
		if err := s.setSchemaVersion(ctx, 4); err != nil {
			return err
		}
		currentVersion = 4
	}
	if currentVersion < 5 {
		if err := s.applyEntityTombstoneMigration(ctx); err != nil {
			return err
		}
		if err := s.setSchemaVersion(ctx, 5); err != nil {
			return err
		}
	}
	if version, err := s.schemaVersion(ctx); err != nil {
		return err
	} else if version != storeSchemaVersion {
		return fmt.Errorf("%w: got %d want %d", ErrSchemaVersionMismatch, version, storeSchemaVersion)
	}
	if err := s.applyQueryIndexMigration(ctx); err != nil {
		return err
	}
	if err := s.applyAttachmentMediaMigration(ctx); err != nil {
		return err
	}
	if err := s.applyFailureLedgerMigration(ctx); err != nil {
		return err
	}
	if err := s.applyEntityTombstoneMigration(ctx); err != nil {
		return err
	}
	if err := s.ensureFTSRowIDs(ctx); err != nil {
		return err
	}
	if err := s.ensureMemberFTSRowIDs(ctx); err != nil {
		return err
	}
	if err := s.ensureEmbeddingSearchIndexes(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) RebuildSearchIndexes(ctx context.Context) error {
	if err := s.rebuildFTS(ctx); err != nil {
		return err
	}
	if err := s.rebuildMemberFTS(ctx); err != nil {
		return err
	}
	return s.stampSearchIndexVersions(ctx, true, true)
}

func (s *Store) RebuildMessageSearchIndex(ctx context.Context) error {
	if err := s.rebuildFTS(ctx); err != nil {
		return err
	}
	return s.stampSearchIndexVersions(ctx, true, false)
}

func (s *Store) RebuildMemberSearchIndex(ctx context.Context) error {
	if err := s.rebuildMemberFTS(ctx); err != nil {
		return err
	}
	return s.stampSearchIndexVersions(ctx, false, true)
}

func (s *Store) stampSearchIndexVersions(ctx context.Context, message, member bool) error {
	now := time.Now().UTC().Format(timeLayout)
	switch {
	case message && member:
		if _, err := s.db.ExecContext(ctx, `
		insert into sync_state(scope, cursor, updated_at)
		values(?, ?, ?), (?, ?, ?)
		on conflict(scope) do update set
			cursor=excluded.cursor,
			updated_at=excluded.updated_at
	`, "schema:message_fts_rowid_version", messageFTSVersion, now, "schema:member_fts_rowid_version", memberFTSVersion, now); err != nil {
			return fmt.Errorf("stamp search index versions: %w", err)
		}
	case message:
		if _, err := s.db.ExecContext(ctx, `
		insert into sync_state(scope, cursor, updated_at)
		values(?, ?, ?)
		on conflict(scope) do update set
			cursor=excluded.cursor,
			updated_at=excluded.updated_at
	`, "schema:message_fts_rowid_version", messageFTSVersion, now); err != nil {
			return fmt.Errorf("stamp message search index version: %w", err)
		}
	case member:
		if _, err := s.db.ExecContext(ctx, `
		insert into sync_state(scope, cursor, updated_at)
		values(?, ?, ?)
		on conflict(scope) do update set
			cursor=excluded.cursor,
			updated_at=excluded.updated_at
	`, "schema:member_fts_rowid_version", memberFTSVersion, now); err != nil {
			return fmt.Errorf("stamp member search index version: %w", err)
		}
	}
	return nil
}

func (s *Store) schemaVersion(ctx context.Context) (int, error) {
	var version int
	if err := s.db.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	return version, nil
}

func (s *Store) setSchemaVersion(ctx context.Context, version int) error {
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("pragma user_version = %d", version)); err != nil {
		return fmt.Errorf("set schema version %d: %w", version, err)
	}
	return nil
}

func (s *Store) applyBaselineSchema(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	stmts := []string{
		`create table if not exists guilds (
			id text primary key,
			name text not null,
			icon text,
			raw_json text not null,
			updated_at text not null,
			deleted_at text,
			deletion_source text,
			deletion_reason text
		);`,
		`create table if not exists channels (
			id text primary key,
			guild_id text not null,
			parent_id text,
			kind text not null,
			name text not null,
			topic text,
			position integer,
			is_nsfw integer not null default 0,
			is_archived integer not null default 0,
			is_locked integer not null default 0,
			is_private_thread integer not null default 0,
			thread_parent_id text,
			archive_timestamp text,
			raw_json text not null,
			updated_at text not null
		);`,
		`create table if not exists members (
			guild_id text not null,
			user_id text not null,
			username text not null,
			global_name text,
			display_name text,
			nick text,
			discriminator text,
			avatar text,
			bot integer not null default 0,
			joined_at text,
			role_ids_json text not null,
			raw_json text not null,
			updated_at text not null,
			deleted_at text,
			deletion_source text,
			deletion_reason text,
			primary key (guild_id, user_id)
		);`,
		`create table if not exists messages (
			id text primary key,
			guild_id text not null,
			channel_id text not null,
			author_id text,
			message_type integer not null,
			created_at text not null,
			edited_at text,
			deleted_at text,
			content text not null,
			normalized_content text not null,
			reply_to_message_id text,
			pinned integer not null default 0,
			has_attachments integer not null default 0,
			raw_json text not null,
			updated_at text not null
		);`,
		`create table if not exists message_events (
			event_id integer primary key autoincrement,
			guild_id text not null,
			channel_id text not null,
			message_id text not null,
			event_type text not null,
			event_at text not null,
			payload_json text not null
		);`,
		`create table if not exists message_attachments (
			attachment_id text primary key,
			message_id text not null,
			guild_id text not null,
			channel_id text not null,
			author_id text,
			filename text not null,
			content_type text,
			size integer not null default 0,
			url text,
			proxy_url text,
			text_content text not null default '',
			media_path text,
			content_sha256 text,
			content_size integer not null default 0,
			fetched_at text,
			fetch_status text not null default '',
			fetch_error text not null default '',
			updated_at text not null
		);`,
		`create table if not exists mention_events (
			event_id integer primary key autoincrement,
			message_id text not null,
			guild_id text not null,
			channel_id text not null,
			author_id text,
			target_type text not null,
			target_id text not null,
			target_name text not null default '',
			event_at text not null
		);`,
		`create table if not exists sync_state (
			scope text primary key,
			cursor text,
			updated_at text not null
		);`,
		`create table if not exists embedding_jobs (
			message_id text primary key,
			state text not null,
			attempts integer not null default 0,
			provider text not null default '',
			model text not null default '',
			input_version text not null default '',
			last_error text not null default '',
			locked_at text,
			updated_at text not null
		);`,
		`create table if not exists message_embeddings (
			message_id text not null,
			provider text not null,
			model text not null,
			input_version text not null,
			dimensions integer not null,
			embedding_blob blob not null,
			embedded_at text not null,
			primary key (message_id, provider, model, input_version)
		);`,
		`create table if not exists failure_ledger (
			failure_id integer primary key autoincrement,
			operation text not null,
			source text not null,
			guild_id text not null default '',
			channel_id text not null default '',
			message_id text not null default '',
			related_kind text not null default '',
			related_id text not null default '',
			error_class text not null,
			error_message text not null,
			first_seen_at text not null,
			last_seen_at text not null,
			retry_count integer not null default 0,
			resolved_at text,
			unique(operation, source, guild_id, channel_id, message_id, related_kind, related_id)
		);`,
		// Uses SQLite FTS5's default unicode61 tokenizer; normalizeFTSQuery quotes user terms before MATCH.
		`create virtual table if not exists message_fts using fts5(
			message_id unindexed,
			guild_id unindexed,
			channel_id unindexed,
			author_id unindexed,
			author_name,
			channel_name,
			content
		);`,
		`create virtual table if not exists member_fts using fts5(
			member_key unindexed,
			guild_id unindexed,
			user_id unindexed,
			username,
			display_name,
			profile_text
		);`,
		`create index if not exists idx_channels_guild_id on channels(guild_id);`,
		`create index if not exists idx_members_guild_id on members(guild_id);`,
		`create index if not exists idx_messages_channel_id on messages(channel_id);`,
		`create index if not exists idx_messages_guild_id on messages(guild_id);`,
		`create index if not exists idx_messages_created_id on messages(created_at, id);`,
		`create index if not exists idx_messages_guild_created_id on messages(guild_id, created_at, id);`,
		`create index if not exists idx_messages_channel_created_id on messages(channel_id, created_at, id);`,
		`create index if not exists idx_messages_author_created_id on messages(author_id, created_at, id);`,
		`create index if not exists idx_events_message_id on message_events(message_id);`,
		`create index if not exists idx_attachments_message_id on message_attachments(message_id);`,
		`create index if not exists idx_attachments_channel_id on message_attachments(channel_id);`,
		`create index if not exists idx_mentions_message_id on mention_events(message_id);`,
		`create index if not exists idx_mentions_guild_event on mention_events(guild_id, event_at, event_id);`,
		`create index if not exists idx_mentions_channel_event on mention_events(channel_id, event_at, event_id);`,
		`create index if not exists idx_mentions_target on mention_events(target_type, target_id, event_at);`,
		`create index if not exists idx_mentions_author on mention_events(author_id, event_at);`,
		`create index if not exists idx_embedding_jobs_state_updated on embedding_jobs(state, updated_at);`,
		`create index if not exists idx_message_embeddings_identity on message_embeddings(provider, model, input_version, dimensions);`,
		`create index if not exists idx_failure_ledger_unresolved on failure_ledger(resolved_at, last_seen_at desc);`,
		`create index if not exists idx_failure_ledger_scope on failure_ledger(guild_id, channel_id, message_id);`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate baseline schema: %w", err)
		}
	}
	return tx.Commit()
}

func (s *Store) applyAttachmentMediaMigration(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	for _, column := range []struct {
		name string
		sql  string
	}{
		{"media_path", `alter table message_attachments add column media_path text`},
		{"content_sha256", `alter table message_attachments add column content_sha256 text`},
		{"content_size", `alter table message_attachments add column content_size integer not null default 0`},
		{"fetched_at", `alter table message_attachments add column fetched_at text`},
		{"fetch_status", `alter table message_attachments add column fetch_status text not null default ''`},
		{"fetch_error", `alter table message_attachments add column fetch_error text not null default ''`},
	} {
		ok, err := columnExists(ctx, tx, "message_attachments", column.name)
		if err != nil {
			return err
		}
		if !ok {
			if _, err := tx.ExecContext(ctx, column.sql); err != nil {
				return fmt.Errorf("add message_attachments.%s: %w", column.name, err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `create index if not exists idx_attachments_sha256 on message_attachments(content_sha256)`); err != nil {
		return fmt.Errorf("ensure attachment media index: %w", err)
	}
	return tx.Commit()
}

func (s *Store) applyFailureLedgerMigration(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	for _, stmt := range []string{
		`create table if not exists failure_ledger (
			failure_id integer primary key autoincrement,
			operation text not null,
			source text not null,
			guild_id text not null default '',
			channel_id text not null default '',
			message_id text not null default '',
			related_kind text not null default '',
			related_id text not null default '',
			error_class text not null,
			error_message text not null,
			first_seen_at text not null,
			last_seen_at text not null,
			retry_count integer not null default 0,
			resolved_at text,
			unique(operation, source, guild_id, channel_id, message_id, related_kind, related_id)
		);`,
		`create index if not exists idx_failure_ledger_unresolved on failure_ledger(resolved_at, last_seen_at desc);`,
		`create index if not exists idx_failure_ledger_scope on failure_ledger(guild_id, channel_id, message_id);`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate failure ledger: %w", err)
		}
	}
	return tx.Commit()
}

func (s *Store) applyEntityTombstoneMigration(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	for _, table := range []string{"guilds", "members"} {
		for _, column := range []string{"deleted_at", "deletion_source", "deletion_reason"} {
			exists, err := columnExists(ctx, tx, table, column)
			if err != nil {
				return err
			}
			if exists {
				continue
			}
			if _, err := tx.ExecContext(ctx, "alter table "+table+" add column "+column+" text"); err != nil {
				return fmt.Errorf("migrate %s.%s: %w", table, column, err)
			}
		}
	}
	return tx.Commit()
}

func (s *Store) applyQueryIndexMigration(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `create table if not exists embedding_jobs (
		message_id text primary key,
		state text not null,
		attempts integer not null default 0,
		provider text not null default '',
		model text not null default '',
		input_version text not null default '',
		last_error text not null default '',
		locked_at text,
		updated_at text not null
	);`); err != nil {
		return fmt.Errorf("ensure embedding_jobs: %w", err)
	}
	for _, column := range []struct {
		name string
		sql  string
	}{
		{"provider", `alter table embedding_jobs add column provider text not null default ''`},
		{"model", `alter table embedding_jobs add column model text not null default ''`},
		{"input_version", `alter table embedding_jobs add column input_version text not null default ''`},
		{"last_error", `alter table embedding_jobs add column last_error text not null default ''`},
		{"locked_at", `alter table embedding_jobs add column locked_at text`},
	} {
		ok, err := columnExists(ctx, tx, "embedding_jobs", column.name)
		if err != nil {
			return err
		}
		if !ok {
			if _, err := tx.ExecContext(ctx, column.sql); err != nil {
				return fmt.Errorf("add embedding_jobs.%s: %w", column.name, err)
			}
		}
	}
	stmts := []string{
		`create table if not exists message_embeddings (
			message_id text not null,
			provider text not null,
			model text not null,
			input_version text not null,
			dimensions integer not null,
			embedding_blob blob not null,
			embedded_at text not null,
			primary key (message_id, provider, model, input_version)
		);`,
		`create index if not exists idx_messages_guild_created_id on messages(guild_id, created_at, id);`,
		`create index if not exists idx_messages_channel_created_id on messages(channel_id, created_at, id);`,
		`create index if not exists idx_messages_author_created_id on messages(author_id, created_at, id);`,
		`create index if not exists idx_messages_created_id on messages(created_at, id);`,
		`create index if not exists idx_mentions_guild_event on mention_events(guild_id, event_at, event_id);`,
		`create index if not exists idx_mentions_channel_event on mention_events(channel_id, event_at, event_id);`,
		`create index if not exists idx_embedding_jobs_state_updated on embedding_jobs(state, updated_at);`,
		`create index if not exists idx_message_embeddings_identity on message_embeddings(provider, model, input_version, dimensions);`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate query indexes: %w", err)
		}
	}
	return tx.Commit()
}

func (s *Store) ensureEmbeddingSearchIndexes(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		create index if not exists idx_message_embeddings_identity
		on message_embeddings(provider, model, input_version, dimensions)
	`)
	if err != nil {
		return fmt.Errorf("ensure embedding search indexes: %w", err)
	}
	return nil
}

func columnExists(ctx context.Context, tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `pragma table_info(`+table+`)`)
	if err != nil {
		return false, fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (s *Store) ensureFTSRowIDs(ctx context.Context) error {
	var version sql.NullString
	err := s.db.QueryRowContext(ctx, `
		select cursor
		from sync_state
		where scope = 'schema:message_fts_rowid_version'
	`).Scan(&version)
	if err == nil && version.String == messageFTSVersion {
		return nil
	}
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("check fts schema version: %w", err)
	}
	if err := s.rebuildFTS(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		insert into sync_state(scope, cursor, updated_at)
		values(?, ?, ?)
		on conflict(scope) do update set
			cursor=excluded.cursor,
			updated_at=excluded.updated_at
	`, "schema:message_fts_rowid_version", messageFTSVersion, time.Now().UTC().Format(timeLayout)); err != nil {
		return fmt.Errorf("stamp fts schema version: %w", err)
	}
	return nil
}

func (s *Store) rebuildFTS(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)

	if _, err := tx.ExecContext(ctx, `drop table if exists message_fts`); err != nil {
		return fmt.Errorf("drop message_fts: %w", err)
	}
	// Uses SQLite FTS5's default unicode61 tokenizer; normalizeFTSQuery quotes user terms before MATCH.
	if _, err := tx.ExecContext(ctx, `
		create virtual table message_fts using fts5(
			message_id unindexed,
			guild_id unindexed,
			channel_id unindexed,
			author_id unindexed,
			author_name,
			channel_name,
			content
		)
	`); err != nil {
		return fmt.Errorf("create message_fts: %w", err)
	}
	if err := configureFTSBulkLoad(ctx, tx, "message_fts"); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `
		select
			m.id,
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
		where m.deleted_at is null
		order by cast(m.id as integer)
	`)
	if err != nil {
		return fmt.Errorf("query fts rebuild rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	stmt, err := tx.PrepareContext(ctx, `
		insert into message_fts(
			rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content
		) values(?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare fts rebuild: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var (
			messageID   string
			guildID     string
			channelID   string
			authorID    string
			authorName  string
			channelName string
			content     string
		)
		if err := rows.Scan(&messageID, &guildID, &channelID, &authorID, &authorName, &channelName, &content); err != nil {
			return fmt.Errorf("scan fts rebuild row: %w", err)
		}
		rowID, ok := messageFTSRowID(messageID)
		if !ok {
			continue
		}
		if _, err := stmt.ExecContext(ctx, rowID, messageID, guildID, channelID, nullable(authorID), authorName, channelName, content); err != nil {
			return fmt.Errorf("insert fts rebuild row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate fts rebuild rows: %w", err)
	}
	if err := optimizeFTS(ctx, tx, "message_fts"); err != nil {
		return err
	}
	return tx.Commit()
}

func configureFTSBulkLoad(ctx context.Context, tx *sql.Tx, table string) error {
	if table != "message_fts" && table != "member_fts" {
		return fmt.Errorf("unsupported fts table %q", table)
	}
	stmts := []string{
		fmt.Sprintf("insert into %s(%s, rank) values('pgsz', 32768)", table, table),
		fmt.Sprintf("insert into %s(%s, rank) values('automerge', 0)", table, table),
		fmt.Sprintf("insert into %s(%s, rank) values('crisismerge', 64)", table, table),
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("configure %s bulk load: %w", table, err)
		}
	}
	return nil
}

func optimizeFTS(ctx context.Context, tx *sql.Tx, table string) error {
	if table != "message_fts" && table != "member_fts" {
		return fmt.Errorf("unsupported fts table %q", table)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("insert into %s(%s) values('optimize')", table, table)); err != nil {
		return fmt.Errorf("optimize %s: %w", table, err)
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
