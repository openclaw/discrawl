package discordpackage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/messagetext"
	"github.com/openclaw/discrawl/internal/store"
)

func snowflake(raw json.RawMessage) (string, error) {
	value := strings.TrimSpace(string(raw))
	if strings.HasPrefix(value, `"`) {
		if err := json.Unmarshal(raw, &value); err != nil {
			return "", errors.New("invalid ID")
		}
	}
	if value == "" || value[0] == '0' || strings.ContainsAny(value, ".+-eE") {
		return "", errors.New("invalid ID")
	}
	if _, err := strconv.ParseUint(value, 10, 64); err != nil {
		return "", errors.New("invalid ID")
	}
	return value, nil
}

// Reject duplicate keys rather than silently accepting an ambiguous identity.
func objectFields(data []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(strings.NewReader(string(data)))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("expected JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return nil, errors.New("invalid JSON object key")
		}
		key, ok := token.(string)
		if !ok || fields[key] != nil {
			return nil, errors.New("duplicate JSON object key")
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, errors.New("invalid JSON object value")
		}
		fields[key] = value
	}
	if token, err := d.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("invalid JSON object end")
	}
	if err := d.Decode(new(json.RawMessage)); err != io.EOF {
		return nil, errors.New("trailing JSON data")
	}
	return fields, nil
}

func readChannel(files fs.FS, dir, account string) (store.ChannelRecord, bool, error) {
	data, err := readFile(files, path.Join(dir, "channel.json"))
	if err != nil {
		return store.ChannelRecord{}, false, err
	}
	fields, err := objectFields(data)
	if err != nil {
		return store.ChannelRecord{}, false, err
	}
	var kind int
	if err := json.Unmarshal(fields["type"], &kind); err != nil {
		return store.ChannelRecord{}, false, errors.New("channel type is required")
	}
	if kind != 1 && kind != 3 {
		return store.ChannelRecord{}, true, nil
	}
	if g := fields["guild"]; len(g) != 0 && string(g) != "null" {
		return store.ChannelRecord{}, false, errors.New("DM channel contains a guild")
	}
	if g := fields["guild_id"]; len(g) != 0 && string(g) != "null" && string(g) != `""` {
		return store.ChannelRecord{}, false, errors.New("DM channel contains a guild ID")
	}
	id, err := snowflake(fields["id"])
	if err != nil {
		return store.ChannelRecord{}, false, err
	}
	if name := strings.TrimPrefix(path.Base(dir), "c"); name != id {
		return store.ChannelRecord{}, false, errors.New("channel directory conflicts with channel ID")
	}
	var recipients []json.RawMessage
	if err := json.Unmarshal(fields["recipients"], &recipients); err != nil || len(recipients) == 0 {
		return store.ChannelRecord{}, false, errors.New("DM recipient IDs are required")
	}
	ids := []string{account}
	seen := map[string]bool{account: true}
	for _, recipient := range recipients {
		id, err := snowflake(recipient)
		if err != nil {
			return store.ChannelRecord{}, false, errors.New("invalid DM recipient ID")
		}
		if !seen[id] {
			ids, seen[id] = append(ids, id), true
		}
	}
	if (kind == 1 && len(ids) != 2) || (kind == 3 && len(ids) < 2) {
		return store.ChannelRecord{}, false, errors.New("DM recipient count conflicts with channel type")
	}
	channelKind := "dm"
	if kind == 3 {
		channelKind = "group_dm"
	}
	raw, err := json.Marshal(map[string]any{"id": id, "type": kind, "recipients": ids, "source": "discord_data_package", "package_channel": json.RawMessage(data)})
	return store.ChannelRecord{ID: id, GuildID: store.DirectMessageGuildID, Kind: channelKind, Name: "dm-" + id, RawJSON: string(raw)}, false, err
}

func readMessages(ctx context.Context, data []byte, channel, account string) ([]store.MessageMutation, int, error) {
	var rows []json.RawMessage
	if len(data) == 0 || strings.TrimSpace(string(data)) == "null" {
		return nil, 0, errors.New("expected message array")
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, 0, errors.New("invalid message array")
	}
	hash := sha256.Sum256(data)
	transcriptHash := hex.EncodeToString(hash[:])
	messages := make([]store.MessageMutation, 0, len(rows))
	seen := make(map[string]bool)
	attachmentCount := 0
	for i, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		fields, err := objectFields(row)
		if err != nil {
			return nil, 0, fmt.Errorf("message row %d: %w", i+1, err)
		}
		id, err := snowflake(fields["ID"])
		if err != nil || seen[id] {
			return nil, 0, fmt.Errorf("message row %d has an invalid or duplicate ID", i+1)
		}
		seen[id] = true
		var timestamp, content, attachments string
		if string(fields["Contents"]) == "null" || json.Unmarshal(fields["Timestamp"], &timestamp) != nil || json.Unmarshal(fields["Contents"], &content) != nil {
			return nil, 0, fmt.Errorf("message row %d requires Timestamp and Contents strings", i+1)
		}
		created, err := time.Parse(time.RFC3339Nano, timestamp)
		if err != nil {
			created, err = time.Parse("2006-01-02 15:04:05.999999999-07:00", timestamp)
		}
		if err != nil {
			return nil, 0, fmt.Errorf("message row %d has an invalid timestamp", i+1)
		}
		if a := fields["Attachments"]; len(a) != 0 && string(a) != "null" {
			if json.Unmarshal(a, &attachments) != nil {
				return nil, 0, fmt.Errorf("message row %d requires an Attachments string", i+1)
			}
		}
		if attachments != "" {
			attachmentCount++
		}
		raw, err := json.Marshal(map[string]any{"id": id, "guild_id": store.DirectMessageGuildID, "channel_id": channel, "author": map[string]string{"id": account}, "timestamp": created.UTC().Format(time.RFC3339Nano), "content": content, "source": "discord_data_package", "export_message": row})
		if err != nil {
			return nil, 0, err
		}
		provenance, err := json.Marshal(map[string]any{"source": "discord_data_package", "account_id": account, "channel_id": channel, "transcript_sha256": transcriptHash, "coverage": "sent_only", "message": row})
		if err != nil {
			return nil, 0, err
		}
		messages = append(messages, store.MessageMutation{
			Record:    store.MessageRecord{ID: id, GuildID: store.DirectMessageGuildID, ChannelID: channel, AuthorID: account, AuthorName: account, CreatedAt: created.UTC().Format(time.RFC3339Nano), Content: content, RawJSON: string(raw), HasAttachments: attachments != "", TextVersion: messagetext.Version},
			EventType: "data_package", PayloadJSON: string(provenance),
		})
	}
	return messages, attachmentCount, nil
}
