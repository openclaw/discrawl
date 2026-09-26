package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

// Additive metadata only. Existing raw payloads, cursors and vectors are retained.
func (s *Store) applyIntegrityMigration(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	for table, columns := range map[string]map[string]string{
		"messages":            {"text_parts_json": "text not null default '[]'", "text_version": "integer not null default 0"},
		"channels":            {"collection_scope": "text not null default 'unknown'", "scope_policy": "text not null default ''", "scope_updated_at": "text", "deleted_at": "text", "deletion_source": "text"},
		"message_attachments": {"text_status": "text not null default 'unknown'", "text_error": "text not null default ''", "text_attempted_at": "text", "text_succeeded_at": "text"},
		"failure_ledger":      {"resolution_reason": "text not null default ''"},
	} {
		for _, column := range slices.Sorted(maps.Keys(columns)) {
			definition := columns[column]
			exists, err := columnExists(ctx, tx, table, column)
			if err != nil {
				return err
			}
			if !exists {
				if _, err := tx.ExecContext(ctx, "alter table "+table+" add column "+column+" "+definition); err != nil {
					return err
				}
			}
		}
	}
	_, err = tx.ExecContext(ctx, `create index if not exists idx_channels_parent_id on channels(parent_id);
		create index if not exists idx_attachment_text_failure on message_attachments(text_status) where text_status='failed';
		create table if not exists message_embedding_history (
		message_id text not null, provider text not null, model text not null, input_version text not null,
		guild_id text not null, channel_id text not null,
		dimensions integer not null, embedding_blob blob not null, embedded_at text not null,
		normalized_content text not null, retained_at text not null,
		primary key(message_id,provider,model,input_version,embedded_at));`)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `create table if not exists attachment_text_history (
		attachment_id text not null,message_id text not null,guild_id text not null,channel_id text not null,
		text_sha256 text not null,text_content text not null,text_succeeded_at text,retained_at text not null,
		primary key(attachment_id,text_sha256));`)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Scope is an ingestion-policy decision, never proof of public visibility.
type ChannelScope struct {
	ChannelRow
	DeletedAt       string
	CollectionScope string
	Policy          string
	Revision        string
}

func (s *Store) ScopeChannels(ctx context.Context) ([]ChannelScope, error) {
	rows, err := s.db.QueryContext(ctx, `select id,guild_id,coalesce(parent_id,''),kind,coalesce(deleted_at,''),collection_scope,scope_policy,updated_at from channels`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ChannelScope
	for rows.Next() {
		var row ChannelScope
		if err := rows.Scan(&row.ID, &row.GuildID, &row.ParentID, &row.Kind, &row.DeletedAt, &row.CollectionScope, &row.Policy, &row.Revision); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

type ScopeDecision struct{ State, Revision string }

func (s *Store) SetChannelScopes(ctx context.Context, scopes map[string]ScopeDecision, policy string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	now := time.Now().UTC().Format(timeLayout)
	for id, decision := range scopes {
		scope := decision.State
		if scope != "allowed" && scope != "excluded" && scope != "unknown" {
			return errors.New("invalid collection scope")
		}
		if _, err = tx.ExecContext(ctx, `update channels set collection_scope=?,scope_policy=?,scope_updated_at=?,updated_at=? where id=? and updated_at=? and (collection_scope!=? or scope_policy!=?)`, scope, policy, now, now, id, decision.Revision, scope, policy); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Invalidate descendants atomically when ancestry changes. A concurrent message
// transaction must not rely on the old allowed decision while catalog work runs.
func invalidateChannelScope(ctx context.Context, tx *sql.Tx, id string) error {
	_, err := tx.ExecContext(ctx, `with recursive descendants(id) as (
		select ? union select c.id from channels c join descendants d on c.parent_id=d.id
	) update channels set collection_scope='unknown',scope_policy='',updated_at=? where id in (select id from descendants)`, id, time.Now().UTC().Format(timeLayout))
	return err
}

func (s *Store) GuildScopePayloads(ctx context.Context) ([]GuildRecord, error) {
	rows, err := s.db.QueryContext(ctx, `select id,raw_json from guilds where deleted_at is null`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []GuildRecord
	for rows.Next() {
		var g GuildRecord
		if err := rows.Scan(&g.ID, &g.RawJSON); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (s *Store) MarkChannelDeleted(ctx context.Context, guildID, id, source string) error {
	if guildID == "" || id == "" || source == "" {
		return errors.New("channel tombstone requires scope and source")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var existingGuild string
	err = tx.QueryRowContext(ctx, `select guild_id from channels where id=?`, id).Scan(&existingGuild)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && existingGuild != guildID {
		return errors.New("channel deletion guild mismatch")
	}
	if errors.Is(err, sql.ErrNoRows) {
		identity, marshalErr := json.Marshal(map[string]string{"id": id, "guild_id": guildID})
		if marshalErr != nil {
			return marshalErr
		}
		if _, err = tx.ExecContext(ctx, `insert into channels(id,guild_id,kind,name,raw_json,updated_at)values(?,?,'unknown','',?,?)`, id, guildID, string(identity), time.Now().UTC().Format(timeLayout)); err != nil {
			return err
		}
	}
	if err = invalidateChannelScope(ctx, tx, id); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `update channels set deleted_at=coalesce(deleted_at,?),deletion_source=?,collection_scope='excluded',updated_at=? where id=? and guild_id=?`, time.Now().UTC().Format(timeLayout), source, time.Now().UTC().Format(timeLayout), id, guildID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

var ErrCollectionScope = errors.New("message collection scope is not allowed")

func (s *Store) ResolveFailureWithReason(ctx context.Context, ref FailureRef, reason string) error {
	if reason == "" {
		return errors.New("resolution reason required")
	}
	_, err := s.db.ExecContext(ctx, `update failure_ledger set resolved_at=?,resolution_reason=? where resolved_at is null and operation=? and source=? and guild_id=? and channel_id=? and message_id=? and related_kind=? and related_id=?`, time.Now().UTC().Format(timeLayout), reason, ref.Operation, ref.Source, ref.GuildID, ref.ChannelID, ref.MessageID, ref.RelatedKind, ref.RelatedID)
	return err
}

func requireMessageScope(ctx context.Context, tx *sql.Tx, message MessageRecord, policy string) error {
	if policy == "" {
		var tombstones int
		if err := tx.QueryRowContext(ctx, `with recursive ancestry(id,parent_id,deleted_at) as (
			select id,parent_id,deleted_at from channels where id=?
			union select c.id,c.parent_id,c.deleted_at from channels c join ancestry a on c.id=a.parent_id
		) select count(*) from ancestry where deleted_at is not null`, message.ChannelID).Scan(&tombstones); err != nil {
			return err
		}
		if tombstones > 0 {
			return ErrCollectionScope
		}
		return nil
	}
	var scope, storedPolicy, guild string
	err := tx.QueryRowContext(ctx, `select collection_scope,scope_policy,guild_id from channels where id=? and deleted_at is null`, message.ChannelID).Scan(&scope, &storedPolicy, &guild)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrCollectionScope
	}
	if err != nil {
		return err
	}
	if scope != "allowed" || storedPolicy != policy || guild != message.GuildID {
		return fmt.Errorf("%w: channel %s", ErrCollectionScope, message.ChannelID)
	}
	return nil
}
