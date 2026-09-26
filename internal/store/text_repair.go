package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/messagetext"
	"github.com/openclaw/discrawl/internal/store/storedb"
)

type TextRepairProgress struct {
	LastID         string `json:"last_id"`
	HighWater      string `json:"high_water"`
	Scanned        int    `json:"scanned"`
	Changed        int    `json:"changed"`
	Reused         int    `json:"reused"`
	SkippedScope   int    `json:"skipped_scope"`
	SkippedReceipt int    `json:"skipped_non_provider_receipt"`
	Rejected       int    `json:"rejected"`
	Complete       bool   `json:"complete"`
}

// RepairMessageTextBatch is a local, resumable derived-data operation. Provider
// data, events and ingestion cursors are never changed. A policy-specific
// checkpoint and fixed high-water bound make process cancellation safe.
func (s *Store) RepairMessageTextBatch(ctx context.Context, policy string, limit int, embeddings bool) (TextRepairProgress, error) {
	var p TextRepairProgress
	if limit < 1 || limit > 1000 || policy == "" {
		return p, errors.New("bounded text repair requires policy and limit 1..1000")
	}
	key := "repair:text-v2:" + policy
	var raw string
	err := s.db.QueryRowContext(ctx, `select cursor from sync_state where scope=?`, key).Scan(&raw)
	if err == nil {
		if err = json.Unmarshal([]byte(raw), &p); err != nil {
			return p, err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return p, err
	}
	if p.Complete {
		return p, nil
	}
	if p.HighWater == "" {
		if err = s.db.QueryRowContext(ctx, `select coalesce(max(id),'') from messages`).Scan(&p.HighWater); err != nil {
			return p, err
		}
	}
	type candidate struct {
		record                  MessageRecord
		revision, scope, policy string
	}
	batch, err := func() ([]candidate, error) {
		rows, err := s.db.QueryContext(ctx, `select m.id,m.guild_id,m.channel_id,coalesce(m.author_id,''),m.content,m.normalized_content,m.raw_json,m.updated_at,coalesce(m.deleted_at,''),m.text_version,coalesce(c.collection_scope,'unknown'),coalesce(c.scope_policy,'')
		from messages m left join channels c on c.id=m.channel_id where m.id>? and m.id<=? order by m.id limit ?`, p.LastID, p.HighWater, limit)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rows.Close() }()
		batch := []candidate{}
		for rows.Next() {
			var c candidate
			if err = rows.Scan(&c.record.ID, &c.record.GuildID, &c.record.ChannelID, &c.record.AuthorID, &c.record.Content, &c.record.NormalizedContent, &c.record.RawJSON, &c.revision, &c.record.DeletedAt, &c.record.TextVersion, &c.scope, &c.policy); err != nil {
				return nil, err
			}
			batch = append(batch, c)
		}
		return batch, rows.Err()
	}()
	if err != nil {
		return p, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return p, err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	for _, c := range batch {
		if err := ctx.Err(); err != nil {
			return p, err
		}
		m := c.record
		p.Scanned++
		p.LastID = m.ID
		if c.scope != "allowed" || c.policy != policy {
			p.SkippedScope++
			continue
		}
		if m.TextVersion >= messagetext.Version {
			p.Reused++
			continue
		}
		var msg discordgo.Message
		if nonProviderTextReceipt(m) {
			p.SkippedReceipt++
			continue
		}
		if json.Unmarshal([]byte(m.RawJSON), &msg) != nil || msg.ID != m.ID || msg.ChannelID != m.ChannelID || (msg.GuildID != "" && msg.GuildID != m.GuildID) || msg.Content != m.Content {
			p.Rejected++
			if err := recordFailure(ctx, tx, FailureRef{Operation: "derive_text", Source: "local", GuildID: m.GuildID, ChannelID: m.ChannelID, MessageID: m.ID}, errors.New("stored provider identity/content mismatch"), time.Now().UTC()); err != nil {
				return p, err
			}
			continue
		}
		atts := make([]messagetext.Attachment, 0, len(msg.Attachments))
		for _, a := range msg.Attachments {
			if a == nil {
				continue
			}
			var text string
			err := tx.QueryRowContext(ctx, `select text_content from message_attachments where attachment_id=? and message_id=? and guild_id=? and channel_id=?`, a.ID, m.ID, m.GuildID, m.ChannelID).Scan(&text)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return p, err
			}
			atts = append(atts, messagetext.Attachment{ID: a.ID, Filename: a.Filename, Text: text})
		}
		parts := messagetext.Parts(&msg, atts)
		body, err := json.Marshal(parts)
		if err != nil {
			return p, err
		}
		normalized := messagetext.Normalize(parts)
		now := time.Now().UTC().Format(timeLayout)
		// The owner may ingest a newer edit between the bounded read and this
		// transaction. Never apply a projection derived from that older payload.
		res, err := tx.ExecContext(ctx, `update messages set normalized_content=?,text_parts_json=?,text_version=?,updated_at=? where id=? and updated_at=? and exists(select 1 from channels c where c.id=messages.channel_id and c.collection_scope='allowed' and c.scope_policy=?)`, normalized, string(body), messagetext.Version, now, m.ID, c.revision, policy)
		if err != nil {
			return p, err
		}
		changed, err := res.RowsAffected()
		if err != nil {
			return p, err
		}
		if changed == 0 {
			continue
		}
		if normalized == m.NormalizedContent {
			p.Reused++
			continue
		}
		p.Changed++
		if m.DeletedAt != "" {
			continue
		}
		if rowID, ok := messageFTSRowID(m.ID); ok {
			if _, err = tx.ExecContext(ctx, `update message_fts set content=? where rowid=?`, normalized, rowID); err != nil {
				return p, err
			}
		}
		m.NormalizedContent = normalized
		tokens, err := s.tokenizeLexical(ctx, normalized)
		if err != nil {
			return p, err
		}
		if err = s.upsertLexicalMessageTx(ctx, tx, m, tokens); err != nil {
			return p, err
		}
		if err = archiveMessageEmbeddings(ctx, tx, m.ID, c.record.NormalizedContent); err != nil {
			return p, err
		}
		if err = qtx.DeleteMessageEmbeddingsByMessage(ctx, m.ID); err != nil {
			return p, err
		}
		if embeddings {
			if err = qtx.UpsertEmbeddingJobPending(ctx, storedb.UpsertEmbeddingJobPendingParams{MessageID: m.ID, UpdatedAt: now}); err != nil {
				return p, err
			}
		}
		if _, err = tx.ExecContext(ctx, `update embedding_jobs set revision=revision+1,lease_token='',lease_until='',locked_at=null,available_at='',priority=0,enqueued_at=? where message_id=?`, now, m.ID); err != nil {
			return p, err
		}
	}
	p.Complete = len(batch) < limit || p.LastID == p.HighWater
	body, err := json.Marshal(p)
	if err != nil {
		return p, err
	}
	if _, err = tx.ExecContext(ctx, `insert into sync_state(scope,cursor,updated_at) values(?,?,?) on conflict(scope) do update set cursor=excluded.cursor,updated_at=excluded.updated_at`, key, string(body), time.Now().UTC().Format(timeLayout)); err != nil {
		return p, err
	}
	if err = tx.Commit(); err != nil {
		return p, err
	}
	s.notifyEmbeddingWork()
	return p, nil
}

func (s *Store) TextRepairStatus(ctx context.Context) ([]TextRepairProgress, error) {
	rows, err := s.db.QueryContext(ctx, `select cursor from sync_state where scope like 'repair:text-v2:%' order by updated_at desc limit 1`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []TextRepairProgress
	for rows.Next() {
		var raw string
		var p TextRepairProgress
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, fmt.Errorf("invalid text repair status: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
