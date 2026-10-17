package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/bwmarrin/discordgo"
)

// HistoryWindow records a completed REST traversal, including an empty window.
// It is independent of history_complete: reaching Since does not prove older history.
type HistoryWindow struct {
	GuildID           string    `json:"guild_id"`
	ChannelID         string    `json:"channel_id"`
	ChannelType       int       `json:"channel_type"`
	Since             time.Time `json:"since"`
	Until             time.Time `json:"until"`
	CompletedAt       time.Time `json:"completed_at"`
	Boundary          string    `json:"boundary"`
	BoundaryMessageID string    `json:"boundary_message_id,omitempty"`
}

type historyPermissionReader interface {
	CanReadHistory(context.Context, *discordgo.Channel) (bool, error)
}

func (s *Syncer) syncWindowChannelHistory(ctx context.Context, channel *discordgo.Channel, state channelSyncState, embeddings bool, since time.Time, progress *messageSyncProgress) (int, error) {
	reader, ok := s.client.(historyPermissionReader)
	if !ok {
		return 0, errors.New("current history permission check is unavailable")
	}
	allowed, err := reader.CanReadHistory(ctx, channel)
	if err != nil || !allowed {
		return 0, errors.Join(errors.New("history permission not proven"), err)
	}
	window := HistoryWindow{GuildID: channel.GuildID, ChannelID: channel.ID, ChannelType: int(channel.Type), Since: since.UTC(), Until: time.Now().UTC()}
	if window.Since.After(window.Until) {
		return 0, errors.New("history window begins in the future")
	}
	// Sparse retained rows and old cursors cannot certify a new window. Reuse
	// canonical pagination from the current head, stopping at the explicit bound.
	count, newest, err := s.syncBackfillPages(ctx, channel, "", state.Latest, channel.Name, embeddings, since, 0, nil, progress, &window)
	if err != nil {
		return count, err
	}
	allowed, err = reader.CanReadHistory(ctx, channel)
	if err != nil || !allowed {
		return count, errors.Join(errors.New("history permission changed during traversal"), err)
	}
	if err := ctx.Err(); err != nil {
		return count, err
	}
	if window.Boundary != "since" && window.Boundary != "end" {
		return count, errors.New("history traversal did not reach its boundary")
	}
	if window.Boundary == "since" && window.BoundaryMessageID == "" {
		return count, errors.New("history boundary message is missing")
	}
	if newest == "" {
		if err := s.store.EnsureChannelLatestMessageState(ctx, channel.ID); err != nil {
			return count, err
		}
	}
	window.CompletedAt = time.Now().UTC()
	scope := channelHistoryWindowScope(channel.ID, since)
	previous, err := s.store.GetSyncState(ctx, scope)
	if err != nil {
		return count, err
	}
	var retained HistoryWindow
	if json.Unmarshal([]byte(previous), &retained) == nil && retained.GuildID == window.GuildID &&
		retained.ChannelID == window.ChannelID && retained.ChannelType == window.ChannelType &&
		retained.Since.Equal(window.Since) && retained.Since.Before(retained.Until) && !retained.CompletedAt.Before(retained.Until) &&
		(retained.Boundary == "end" || (retained.Boundary == "since" && retained.BoundaryMessageID != "")) &&
		!retained.Until.Before(window.Until) {
		return count, nil // A clock adjustment must not regress the same window.
	}
	raw, err := json.Marshal(window)
	if err != nil {
		return count, err
	}
	return count, s.store.SetSyncState(ctx, scope, string(raw))
}

func channelHistoryWindowScope(channelID string, since time.Time) string {
	// Distinct lower bounds keep independent evidence; do not union intervals
	// or discard an older day's proof when a later window is collected.
	return "channel:" + channelID + ":history_window:v1:" + since.UTC().Format(time.RFC3339Nano)
}
