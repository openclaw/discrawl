package store

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"
)

// Desktop imports deliberately retain a metadata receipt rather than a provider
// message payload. It cannot reconstruct original rich text. Do not substitute
// canonical content into raw JSON or reproject from missing provider fields.
func nonProviderTextReceipt(m MessageRecord) bool {
	// Only top-level receipt fields emitted by the importer are understood.
	// Duplicate identity keys must not be resolved using JSON's last-key wins.
	d := json.NewDecoder(strings.NewReader(m.RawJSON))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return false
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return false
		}
		seen[key] = true
		switch key {
		case "id", "guild_id", "channel_id", "author_id", "source", "type", "timestamp", "edited_timestamp", "message_reference", "attachment_count", "mention_count", "desktop_cache_note", "author":
		default:
			return false
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return false
		}
	}
	if end, err := d.Token(); err != nil || end != json.Delim('}') {
		return false
	}
	if err := d.Decode(new(json.RawMessage)); err != io.EOF {
		return false
	}
	var r struct {
		ID        string          `json:"id"`
		GuildID   string          `json:"guild_id"`
		ChannelID string          `json:"channel_id"`
		Source    string          `json:"source"`
		Note      string          `json:"desktop_cache_note"`
		Content   json.RawMessage `json:"content"`
	}
	return json.Unmarshal([]byte(m.RawJSON), &r) == nil && r.ID == m.ID && r.GuildID == m.GuildID && r.ChannelID == m.ChannelID && r.Source == "discord_desktop" && r.Note == "raw desktop cache payload intentionally not stored" && len(r.Content) == 0
}

// ReconcileTextReceiptFailures classifies only verified metadata-only receipts
// rejected by an earlier repair pass. Raw content, derived text and embeddings
// stay untouched. Unknown formats and identity mismatches remain unresolved.
func (s *Store) ReconcileTextReceiptFailures(ctx context.Context) error {
	type receipt struct {
		failureID int64
		message   MessageRecord
	}
	var highWater, lastID int64
	if err := s.db.QueryRowContext(ctx, `select coalesce(max(failure_id),0) from failure_ledger`).Scan(&highWater); err != nil {
		return err
	}
	for lastID < highWater {
		batch, err := func() ([]receipt, error) {
			rows, err := s.db.QueryContext(ctx, `select f.failure_id,m.id,m.guild_id,m.channel_id,m.raw_json from failure_ledger f join messages m on m.id=f.message_id where f.operation='derive_text' and f.source='local' and f.resolved_at is null and f.failure_id>? and f.failure_id<=? order by f.failure_id limit 500`, lastID, highWater)
			if err != nil {
				return nil, err
			}
			defer func() { _ = rows.Close() }()
			var out []receipt
			for rows.Next() {
				var r receipt
				if err := rows.Scan(&r.failureID, &r.message.ID, &r.message.GuildID, &r.message.ChannelID, &r.message.RawJSON); err != nil {
					return nil, err
				}
				out = append(out, r)
			}
			return out, rows.Err()
		}()
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		for _, r := range batch {
			lastID = r.failureID
			if !nonProviderTextReceipt(r.message) {
				continue
			}
			if _, err := s.db.ExecContext(ctx, `update failure_ledger set resolved_at=?,resolution_reason='non_provider_receipt_retained' where failure_id=? and resolved_at is null and exists(select 1 from messages m where m.id=failure_ledger.message_id and m.raw_json=? and m.guild_id=? and m.channel_id=?)`, time.Now().UTC().Format(timeLayout), r.failureID, r.message.RawJSON, r.message.GuildID, r.message.ChannelID); err != nil {
				return err
			}
		}
	}
	return nil
}
