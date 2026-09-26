package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/messagetext"
)

func prepareTextMutation(ctx context.Context, tx *sql.Tx, m *MessageMutation) error {
	if m.Record.TextVersion != messagetext.Version {
		return nil
	}
	for i := range m.Attachments {
		a := &m.Attachments[i]
		var text, status, attempted, succeeded, reason string
		err := tx.QueryRowContext(ctx, `select text_content,text_status,coalesce(text_attempted_at,''),coalesce(text_succeeded_at,''),text_error from message_attachments where attachment_id=? and message_id=?`, a.AttachmentID, m.Record.ID).Scan(&text, &status, &attempted, &succeeded, &reason)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && a.TextStatus != "succeeded" && a.TextStatus != "empty" {
			if text != "" {
				a.TextContent = text
				a.TextSucceededAt = succeeded
			}
			if a.TextAttemptedAt == "" {
				a.TextStatus = status
				a.TextAttemptedAt = attempted
				a.TextError = reason
			}
		}
	}
	var msg discordgo.Message
	if err := json.Unmarshal([]byte(m.Record.RawJSON), &msg); err != nil {
		return err
	}
	atts := make([]messagetext.Attachment, 0, len(m.Attachments))
	for _, a := range m.Attachments {
		atts = append(atts, messagetext.Attachment{ID: a.AttachmentID, Filename: a.Filename, Text: a.TextContent})
	}
	parts := messagetext.Parts(&msg, atts)
	body, err := json.Marshal(parts)
	if err != nil {
		return err
	}
	m.Record.TextPartsJSON = string(body)
	m.Record.NormalizedContent = messagetext.Normalize(parts)
	return nil
}

func archiveMessageEmbeddings(ctx context.Context, tx *sql.Tx, id, oldText string) error {
	_, err := tx.ExecContext(ctx, `insert or ignore into message_embedding_history
		(message_id,provider,model,input_version,guild_id,channel_id,dimensions,embedding_blob,embedded_at,normalized_content,retained_at)
		select e.message_id,e.provider,e.model,e.input_version,m.guild_id,m.channel_id,e.dimensions,e.embedding_blob,e.embedded_at,?,? from message_embeddings e join messages m on m.id=e.message_id where e.message_id=?`, oldText, time.Now().UTC().Format(timeLayout), id)
	return err
}
