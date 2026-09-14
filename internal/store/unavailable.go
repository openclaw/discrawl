package store

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"time"
)

const (
	ChannelUnavailableSuffix = ":unavailable"
	UnavailableMarkerWindow  = 7 * 24 * time.Hour
)

// Invalid timestamps remain retryable so a new observation can repair the marker.
func UnavailableMarkerActive(updatedAt, now time.Time) bool {
	return !updatedAt.IsZero() && updatedAt.After(now.Add(-UnavailableMarkerWindow))
}

func (s *Store) IncompleteMessageChannelIDs(ctx context.Context, guildID string) ([]string, error) {
	ids, err := s.AllIncompleteMessageChannelIDs(ctx, guildID)
	if err != nil || len(ids) == 0 {
		return ids, err
	}
	fresh, err := s.FreshUnavailableChannelIDs(ctx)
	if err != nil {
		return nil, err
	}
	blocked := make(map[string]struct{}, len(fresh))
	for _, id := range fresh {
		blocked[id] = struct{}{}
	}
	return slices.DeleteFunc(ids, func(id string) bool { _, skip := blocked[id]; return skip }), nil
}

func (s *Store) AllIncompleteMessageChannelIDs(ctx context.Context, guildID string) ([]string, error) {
	if guildID != "" {
		return s.q.ListAllIncompleteMessageChannelIDsByGuild(ctx, guildID)
	}
	return s.q.ListAllIncompleteMessageChannelIDs(ctx)
}

type SyncStateEntry struct {
	Scope     string
	UpdatedAt time.Time
}

func (s *Store) FreshUnavailableChannelIDs(ctx context.Context) ([]string, error) {
	markers, err := s.SyncStateBySuffix(ctx, ChannelUnavailableSuffix)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	var ids []string
	for _, marker := range markers {
		id, ok := strings.CutPrefix(marker.Scope, "channel:")
		id = strings.TrimSuffix(id, ChannelUnavailableSuffix)
		if ok && id != "" && UnavailableMarkerActive(marker.UpdatedAt, now) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

// SyncStateBySuffix returns malformed timestamps as zero values for diagnostics.
func (s *Store) SyncStateBySuffix(ctx context.Context, suffix string) ([]SyncStateEntry, error) {
	if suffix == "" {
		return nil, nil
	}
	rows, err := s.q.ListSyncStateBySuffix(ctx, sql.NullString{String: suffix, Valid: true})
	if err != nil {
		return nil, err
	}
	entries := make([]SyncStateEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, SyncStateEntry{Scope: row.Scope, UpdatedAt: parseTime(row.UpdatedAt)})
	}
	return entries, nil
}
