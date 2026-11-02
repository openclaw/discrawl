package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ImportPersonalPackageChannel adds sent-message evidence without replacing
// richer cached messages, attachment rows, edits, or deletion tombstones.
// One channel commits atomically; callers may safely retry an interrupted package.
func (s *Store) ImportPersonalPackageChannel(ctx context.Context, channel ChannelRecord, messages []MessageMutation) (int, error) {
	if channel.ID == "" || channel.GuildID != DirectMessageGuildID || (channel.Kind != "dm" && channel.Kind != "group_dm") {
		return 0, errors.New("personal package requires a direct-message channel")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	var guild string
	err = tx.QueryRowContext(ctx, "select guild_id from channels where id=?", channel.ID).Scan(&guild)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if err == nil && guild != DirectMessageGuildID {
		return 0, errors.New("personal package channel conflicts with stored guild")
	}
	channelExists := err == nil
	qtx := s.q.WithTx(tx)
	if err := upsertGuildTx(ctx, tx, qtx, GuildRecord{ID: DirectMessageGuildID, Name: "Discord Direct Messages", RawJSON: `{}`}, time.Time{}); err != nil {
		return 0, err
	}
	if channelExists {
		// Package metadata is sparse. Preserve the complete cached row while
		// retaining this source observation independently of canonical state.
		if err := recordEvidenceTx(ctx, tx, "channels", channel, channel.ID); err != nil {
			return 0, err
		}
	} else {
		if err := s.upsertChannelTx(ctx, tx, channel, time.Time{}, false); err != nil {
			return 0, err
		}
	}
	inserted := 0
	for _, message := range messages {
		m := message.Record
		if m.ID == "" || m.AuthorID == "" || m.ChannelID != channel.ID || m.GuildID != DirectMessageGuildID || message.EventType != "data_package" {
			return 0, errors.New("invalid personal package message identity")
		}
		if err := requireMessageScope(ctx, tx, m, ""); err != nil {
			return 0, err
		}
		var storedGuild, storedChannel, storedAuthor string
		err := tx.QueryRowContext(ctx, "select guild_id,channel_id,author_id from messages where id=?", m.ID).Scan(&storedGuild, &storedChannel, &storedAuthor)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		if err == nil && (storedGuild != m.GuildID || storedChannel != m.ChannelID || storedAuthor != m.AuthorID) {
			return 0, fmt.Errorf("personal package message %s conflicts with stored identity", m.ID)
		}
		if errors.Is(err, sql.ErrNoRows) {
			if err := prepareTextMutation(ctx, tx, &message); err != nil {
				return 0, err
			}
			changed, err := s.upsertMessageTx(ctx, tx, qtx, message.Record, WriteOptions{})
			if err != nil {
				return 0, err
			}
			if err := recordObservedTx(ctx, tx, "messages", changed, m.ID); err != nil {
				return 0, err
			}
			inserted++
		}
		if err := recordEvidenceTx(ctx, tx, "messages", message, m.ID); err != nil {
			return 0, err
		}
		message.Options = WriteOptions{AppendEvent: true, DeduplicateEvent: true}
		if err := appendMutationEventTx(ctx, qtx, message); err != nil {
			return 0, err
		}
	}
	return inserted, tx.Commit()
}
