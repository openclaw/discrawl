package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

func retainAttachmentText(ctx context.Context, tx *sql.Tx, messageID string) error {
	type prior struct {
		id, message, guild, channel, text string
		at                                sql.NullString
	}
	records, err := func() ([]prior, error) {
		rows, err := tx.QueryContext(ctx, `select attachment_id,message_id,guild_id,channel_id,text_content,text_succeeded_at from message_attachments where message_id=? and text_content!=''`, messageID)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rows.Close() }()
		var records []prior
		for rows.Next() {
			var r prior
			if err = rows.Scan(&r.id, &r.message, &r.guild, &r.channel, &r.text, &r.at); err != nil {
				return nil, err
			}
			records = append(records, r)
		}
		return records, rows.Err()
	}()
	if err != nil {
		return err
	}
	for _, r := range records {
		sum := sha256.Sum256([]byte(r.text))
		if _, err = tx.ExecContext(ctx, `insert or ignore into attachment_text_history values(?,?,?,?,?,?,?,?)`, r.id, r.message, r.guild, r.channel, hex.EncodeToString(sum[:]), r.text, r.at, time.Now().UTC().Format(timeLayout)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) AttachmentTextRetryCandidates(ctx context.Context, guildIDs []string, limit int) ([]MessageRecord, error) {
	if limit < 1 || limit > 25 {
		return nil, errors.New("attachment retry limit must be 1..25")
	}
	where := ""
	args := []any{}
	if len(guildIDs) > 0 {
		where = " and m.guild_id in (" + placeholders(len(guildIDs)) + ")"
		for _, id := range guildIDs {
			args = append(args, id)
		}
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `select m.id,m.guild_id,m.channel_id from message_attachments a join messages m on m.id=a.message_id join channels c on c.id=m.channel_id where a.text_status='failed' and m.deleted_at is null and c.collection_scope='allowed'`+where+` group by m.id order by min(a.text_attempted_at),m.id limit ?`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []MessageRecord
	for rows.Next() {
		var m MessageRecord
		if err := rows.Scan(&m.ID, &m.GuildID, &m.ChannelID); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) RecordAttachmentRefetchFailure(ctx context.Context, m MessageRecord, code string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	now := time.Now().UTC().Format(timeLayout)
	if _, err = tx.ExecContext(ctx, `update message_attachments set text_attempted_at=?,text_error=?,updated_at=? where message_id=? and text_status='failed'`, now, code, now, m.ID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `update messages set updated_at=? where id=?`, now, m.ID); err != nil {
		return err
	}
	return tx.Commit()
}
