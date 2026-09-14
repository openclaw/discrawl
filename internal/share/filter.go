package share

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type snapshotFilter struct {
	active            bool
	publicOnly        bool
	includeChannels   map[string]struct{}
	excludeChannels   map[string]struct{}
	allowedChannels   map[string]struct{}
	allowedGuilds     map[string]struct{}
	allowedMembers    map[string]map[string]struct{}
	allowedMessageIDs map[string]bool
	channels          map[string]snapshotChannel
	guilds            map[string]string
	publicMemo        map[string]bool
	publicSeen        map[string]bool
	publicMissing     map[string]bool
}

type snapshotChannel struct {
	ID              string
	GuildID         string
	ParentID        string
	ThreadParentID  string
	Kind            string
	IsPrivateThread bool
	RawJSON         string
}

func newSnapshotFilter(ctx context.Context, db *sql.DB, opts FilterOptions) (*snapshotFilter, error) {
	f := &snapshotFilter{
		publicOnly:        opts.PublicOnly,
		includeChannels:   stringSet(opts.IncludeChannelIDs),
		excludeChannels:   stringSet(opts.ExcludeChannelIDs),
		allowedChannels:   map[string]struct{}{},
		allowedGuilds:     map[string]struct{}{},
		allowedMembers:    map[string]map[string]struct{}{},
		allowedMessageIDs: map[string]bool{},
		channels:          map[string]snapshotChannel{},
		guilds:            map[string]string{},
		publicMemo:        map[string]bool{},
		publicSeen:        map[string]bool{},
		publicMissing:     map[string]bool{},
	}
	f.active = opts.Active()
	if !f.active {
		return f, nil
	}
	if err := f.loadGuilds(ctx, db); err != nil {
		return nil, err
	}
	if err := f.loadChannels(ctx, db); err != nil {
		return nil, err
	}
	for id, ch := range f.channels {
		if f.channelAllowed(ch) {
			f.allowedChannels[id] = struct{}{}
			if ch.GuildID != "" {
				f.allowedGuilds[ch.GuildID] = struct{}{}
			}
		}
	}
	if err := f.loadAllowedMessages(ctx, db); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *snapshotFilter) loadGuilds(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `select id, raw_json from guilds`)
	if err != nil {
		return fmt.Errorf("query guilds for share filter: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return fmt.Errorf("scan guild share filter: %w", err)
		}
		f.guilds[id] = raw
	}
	return rows.Err()
}

func (f *snapshotFilter) loadChannels(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `
		select id, guild_id, coalesce(parent_id, ''), kind, is_private_thread,
		       coalesce(thread_parent_id, ''), raw_json
		from channels
	`)
	if err != nil {
		return fmt.Errorf("query channels for share filter: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var ch snapshotChannel
		if err := rows.Scan(&ch.ID, &ch.GuildID, &ch.ParentID, &ch.Kind, &ch.IsPrivateThread, &ch.ThreadParentID, &ch.RawJSON); err != nil {
			return fmt.Errorf("scan channel share filter: %w", err)
		}
		f.channels[ch.ID] = ch
	}
	return rows.Err()
}

func (f *snapshotFilter) loadAllowedMessages(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `select id, guild_id, channel_id, author_id from messages where guild_id != ?`, directMessageGuildID)
	if err != nil {
		return fmt.Errorf("query messages for share filter: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var messageID, guildID, channelID, authorID string
		if err := rows.Scan(&messageID, &guildID, &channelID, &authorID); err != nil {
			return fmt.Errorf("scan message share filter: %w", err)
		}
		if !f.allowChannelID(channelID) {
			continue
		}
		f.allowedMessageIDs[messageID] = true
		if guildID != "" && authorID != "" {
			if f.allowedMembers[guildID] == nil {
				f.allowedMembers[guildID] = map[string]struct{}{}
			}
			f.allowedMembers[guildID][authorID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows, err = db.QueryContext(ctx, `
		select guild_id, channel_id, target_id
		from mention_events
		where target_type = 'user' and guild_id != ?
	`, directMessageGuildID)
	if err != nil {
		return fmt.Errorf("query mentions for share filter: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var guildID, channelID, userID string
		if err := rows.Scan(&guildID, &channelID, &userID); err != nil {
			return fmt.Errorf("scan mention share filter: %w", err)
		}
		if !f.allowChannelID(channelID) {
			continue
		}
		if guildID != "" && userID != "" && f.allowedMembers[guildID] != nil {
			f.allowedMembers[guildID][userID] = struct{}{}
		}
	}
	return rows.Err()
}

func (f *snapshotFilter) allow(table string, row map[string]any) bool {
	if isDirectMessageSnapshotRow(table, row) {
		return false
	}
	if !f.active {
		return true
	}
	switch table {
	case "guilds":
		_, ok := f.allowedGuilds[stringValue(row["id"])]
		return ok
	case "channels":
		return f.allowChannelID(stringValue(row["id"]))
	case "members":
		guildID := stringValue(row["guild_id"])
		userID := stringValue(row["user_id"])
		_, ok := f.allowedMembers[guildID][userID]
		return ok
	case "messages", "message_events":
		return f.allowedMessageIDs[stringValue(row["message_id"])] || f.allowChannelID(stringValue(row["channel_id"]))
	case "message_attachments", "mention_events":
		return f.allowChannelID(stringValue(row["channel_id"]))
	case "sync_state":
		return f.allowSyncState(stringValue(row["scope"]))
	default:
		return true
	}
}

func (f *snapshotFilter) allowChannelID(channelID string) bool {
	if !f.active {
		return true
	}
	_, ok := f.allowedChannels[channelID]
	return ok
}

func (f *snapshotFilter) allowSyncState(scope string) bool {
	if rest, ok := strings.CutPrefix(scope, "channel:"); ok {
		channelID, _, _ := strings.Cut(rest, ":")
		return f.allowChannelID(channelID)
	}
	if strings.HasPrefix(scope, "wiretap:") || strings.HasPrefix(scope, "worker:") {
		return false
	}
	if strings.HasPrefix(scope, "share:") {
		return false
	}
	if rest, ok := strings.CutPrefix(scope, "guild:"); ok {
		if strings.HasSuffix(scope, ":members:last_success") {
			return false
		}
		guildID, _, _ := strings.Cut(rest, ":")
		_, ok := f.allowedGuilds[guildID]
		return ok
	}
	return false
}

func (f *snapshotFilter) channelAllowed(ch snapshotChannel) bool {
	if ch.ID == "" {
		return false
	}
	if _, blocked := f.excludeChannels[ch.ID]; blocked {
		return false
	}
	parentID := channelParentID(ch)
	if parentID != "" {
		if _, blocked := f.excludeChannels[parentID]; blocked {
			return false
		}
	}
	if len(f.includeChannels) > 0 {
		if _, ok := f.includeChannels[ch.ID]; !ok {
			_, parentOK := f.includeChannels[parentID]
			if !strings.HasPrefix(ch.Kind, "thread_") || !parentOK {
				return false
			}
		}
	}
	if f.publicOnly && !f.publicChannel(ch.ID) {
		return false
	}
	return true
}

func (f *snapshotFilter) publicChannel(channelID string) bool {
	if cached, ok := f.publicMemo[channelID]; ok {
		return cached
	}
	if f.publicSeen[channelID] {
		f.publicMissing[channelID] = true
		return false
	}
	f.publicSeen[channelID] = true
	defer delete(f.publicSeen, channelID)
	ch, ok := f.channels[channelID]
	if !ok {
		f.publicMissing[channelID] = true
		f.publicMemo[channelID] = false
		return false
	}
	if ch.IsPrivateThread || ch.Kind == "thread_private" {
		f.publicMemo[channelID] = false
		return false
	}
	if ch.GuildID == "" {
		f.publicMissing[channelID] = true
		f.publicMemo[channelID] = false
		return false
	}
	if strings.HasPrefix(ch.Kind, "thread_") {
		parentID := channelParentID(ch)
		allowed := parentID != "" && f.publicChannel(parentID)
		f.publicMissing[channelID] = parentID == "" || f.publicMissing[parentID]
		f.publicMemo[channelID] = allowed
		return allowed
	}
	permissions, ok := everyoneGuildPermissions(f.guilds[ch.GuildID], ch.GuildID)
	if !ok {
		f.publicMissing[channelID] = true
		f.publicMemo[channelID] = false
		return false
	}
	parent, parentOK := f.channels[ch.ParentID]
	if ch.ParentID != "" && !parentOK {
		f.publicMissing[channelID] = true
	}
	if parentOK && parent.Kind == "category" {
		permissions, ok = applyEveryoneOverwrite(permissions, parent.RawJSON, ch.GuildID)
		if !ok {
			f.publicMissing[channelID] = true
			f.publicMemo[channelID] = false
			return false
		}
	}
	permissions, ok = applyEveryoneOverwrite(permissions, ch.RawJSON, ch.GuildID)
	f.publicMissing[channelID] = f.publicMissing[channelID] || !ok
	allowed := ok && permissions&permissionViewChannel != 0
	f.publicMemo[channelID] = allowed
	return allowed
}

const permissionViewChannel int64 = 1 << 10

func everyoneGuildPermissions(rawGuild string, guildID string) (int64, bool) {
	var payload struct {
		Roles []struct {
			ID          string `json:"id"`
			Permissions any    `json:"permissions"`
		} `json:"roles"`
	}
	if err := decodeJSONUseNumber(rawGuild, &payload); err != nil {
		return 0, false
	}
	for _, role := range payload.Roles {
		if role.ID != guildID {
			continue
		}
		permissions, ok := parsePermissionBits(role.Permissions)
		if !ok {
			return 0, false
		}
		return permissions, true
	}
	return 0, false
}

func applyEveryoneOverwrite(permissions int64, rawChannel string, guildID string) (int64, bool) {
	var payload struct {
		PermissionOverwrites []struct {
			ID    string `json:"id"`
			Type  any    `json:"type"`
			Allow any    `json:"allow"`
			Deny  any    `json:"deny"`
		} `json:"permission_overwrites"`
	}
	if err := decodeJSONUseNumber(rawChannel, &payload); err != nil {
		return 0, false
	}
	if payload.PermissionOverwrites == nil {
		return 0, false
	}
	for _, overwrite := range payload.PermissionOverwrites {
		if overwrite.ID != guildID || !isRoleOverwrite(overwrite.Type) {
			continue
		}
		allow, allowOK := parsePermissionBits(overwrite.Allow)
		deny, denyOK := parsePermissionBits(overwrite.Deny)
		if !allowOK || !denyOK {
			return 0, false
		}
		permissions &^= deny
		permissions |= allow
	}
	return permissions, true
}

func decodeJSONUseNumber(raw string, value any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(value)
}

func parsePermissionBits(value any) (int64, bool) {
	switch v := value.(type) {
	case nil:
		return 0, true
	case float64:
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		return parsed, err == nil
	case json.Number:
		parsed, err := strconv.ParseInt(v.String(), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func isRoleOverwrite(value any) bool {
	switch v := value.(type) {
	case float64:
		return int(v) == 0
	case json.Number:
		parsed, err := strconv.Atoi(v.String())
		return err == nil && parsed == 0
	case string:
		return v == "0" || strings.EqualFold(v, "role")
	default:
		return false
	}
}

func channelParentID(ch snapshotChannel) string {
	if ch.ThreadParentID != "" {
		return ch.ThreadParentID
	}
	return ch.ParentID
}

func stringSet(in []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func snapshotExportQuery(table string) (string, []any) {
	switch table {
	case "guilds":
		return "select * from guilds where id != ?", []any{directMessageGuildID}
	case "channels", "members", "messages", "message_events", "message_attachments", "mention_events":
		return "select * from " + table + " where guild_id != ?", []any{directMessageGuildID}
	case "sync_state":
		return "select * from sync_state where scope not like 'wiretap:%' and scope not like 'worker:%'", nil
	default:
		return "select * from " + table, nil
	}
}

func snapshotDeleteQuery(table string) (string, []any) {
	switch table {
	case "guilds":
		return "delete from guilds where id != ?", []any{directMessageGuildID}
	case "message_events", "mention_events":
		return "delete from " + table + " where guild_id != ?", []any{directMessageGuildID}
	case "channels", "members", "messages", "message_attachments":
		return "delete from " + table + " where guild_id != ?", []any{directMessageGuildID}
	case "sync_state":
		return "delete from sync_state where scope not like 'wiretap:%' and scope not like 'worker:%'", nil
	default:
		return "delete from " + table, nil
	}
}

func isDirectMessageSnapshotRow(table string, row map[string]any) bool {
	switch table {
	case "guilds":
		return isLocalOnlyGuildID(stringValue(row["id"]))
	case "channels", "members", "messages", "message_events", "message_attachments", "mention_events":
		return isLocalOnlyGuildID(stringValue(row["guild_id"]))
	case "sync_state":
		scope := stringValue(row["scope"])
		return (strings.HasPrefix(scope, "wiretap:") || strings.HasPrefix(scope, "worker:"))
	default:
		return false
	}
}

func isLocalOnlyGuildID(guildID string) bool {
	return guildID == directMessageGuildID
}
