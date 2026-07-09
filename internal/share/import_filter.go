package share

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type mergeImportChannel struct {
	parentID string
	kind     string
}

type mergeImportFilter struct {
	excludedIDs   map[string]struct{}
	excludedKinds map[string]struct{}
	channels      map[string]mergeImportChannel
	decisions     map[string]bool
}

type mergeImportQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func newMergeImportFilter(
	ctx context.Context,
	db mergeImportQuerier,
	opts Options,
) (*mergeImportFilter, error) {
	filter := &mergeImportFilter{
		excludedIDs:   normalizedImportSet(opts.MergeExcludeChannelIDs, false),
		excludedKinds: normalizedImportSet(opts.MergeExcludeChannelKinds, true),
		channels:      map[string]mergeImportChannel{},
		decisions:     map[string]bool{},
	}
	if len(filter.excludedIDs) > 0 || len(filter.excludedKinds) > 0 {
		if err := filter.loadChannels(ctx, db); err != nil {
			return nil, err
		}
	}
	return filter, nil
}

func (f *mergeImportFilter) loadChannels(ctx context.Context, db mergeImportQuerier) error {
	rows, err := db.QueryContext(ctx, `
		select id, coalesce(parent_id, ''), coalesce(thread_parent_id, ''), kind
		from channels
	`)
	if err != nil {
		return fmt.Errorf("load channels for snapshot merge exclusions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, parentID, threadParentID, kind string
		if err := rows.Scan(&id, &parentID, &threadParentID, &kind); err != nil {
			return fmt.Errorf("scan channel for snapshot merge exclusions: %w", err)
		}
		if threadParentID != "" {
			parentID = threadParentID
		}
		f.channels[id] = mergeImportChannel{parentID: parentID, kind: kind}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate channels for snapshot merge exclusions: %w", err)
	}
	return nil
}

func (f *mergeImportFilter) allowMessageID(ctx context.Context, lookup *sql.Stmt, messageID string) (bool, error) {
	var guildID, channelID string
	err := lookup.QueryRowContext(ctx, messageID).Scan(&guildID, &channelID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load embedding message %s: %w", messageID, err)
	}
	return f.allow("messages", map[string]any{
		"guild_id":   guildID,
		"channel_id": channelID,
	})
}

func (f *mergeImportFilter) allow(table string, row map[string]any) (bool, error) {
	if isDirectMessageSnapshotRow(table, row) {
		return false, nil
	}
	switch table {
	case "channels":
		f.recordChannel(row)
		return true, nil
	case "messages", "message_events", "message_attachments", "mention_events":
		return !f.excludesChannel(stringValue(row["channel_id"]), nil), nil
	default:
		return true, nil
	}
}

func (f *mergeImportFilter) recordChannel(row map[string]any) {
	id := stringValue(row["id"])
	if id == "" {
		return
	}
	parentID := stringValue(row["thread_parent_id"])
	if parentID == "" {
		parentID = stringValue(row["parent_id"])
	}
	f.channels[id] = mergeImportChannel{
		parentID: parentID,
		kind:     stringValue(row["kind"]),
	}
	clear(f.decisions)
}

func (f *mergeImportFilter) excludesChannel(channelID string, visiting map[string]struct{}) bool {
	if channelID == "" {
		return false
	}
	if decision, ok := f.decisions[channelID]; ok {
		return decision
	}
	if _, ok := f.excludedIDs[channelID]; ok {
		f.decisions[channelID] = true
		return true
	}
	channel, ok := f.channels[channelID]
	if !ok {
		f.decisions[channelID] = false
		return false
	}
	if f.excludesKind(channel.kind) {
		f.decisions[channelID] = true
		return true
	}
	if channel.parentID == "" {
		f.decisions[channelID] = false
		return false
	}
	if visiting == nil {
		visiting = map[string]struct{}{}
	}
	if _, ok := visiting[channelID]; ok {
		return false
	}
	visiting[channelID] = struct{}{}
	excluded := f.excludesChannel(channel.parentID, visiting)
	delete(visiting, channelID)
	f.decisions[channelID] = excluded
	return excluded
}

func (f *mergeImportFilter) excludesKind(kind string) bool {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if _, ok := f.excludedKinds[kind]; ok {
		return true
	}
	switch kind {
	case "news", "thread_news", "thread_announcement":
		_, ok := f.excludedKinds["announcement"]
		return ok
	default:
		return false
	}
}

func normalizedImportSet(values []string, lower bool) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if lower {
			value = strings.ToLower(value)
		}
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}
