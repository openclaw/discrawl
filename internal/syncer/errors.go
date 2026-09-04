package syncer

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/bwmarrin/discordgo"
)

func (s *Syncer) skipSyncError(ctx context.Context, channel *discordgo.Channel, err error) bool {
	if s.skipUnavailableChannel(ctx, channel, err) {
		return true
	}
	if !isRetryableSyncError(ctx, err) {
		return false
	}
	s.logger.Warn("channel message crawl deferred", "channel_id", channel.ID, "err", err)
	return true
}

func (s *Syncer) skipUnavailableChannel(ctx context.Context, channel *discordgo.Channel, err error) bool {
	if channel == nil {
		return false
	}
	return s.skipUnavailableChannelByID(ctx, channel.ID, err, "channel message crawl skipped", channelMessageUnavailableScope)
}

func (s *Syncer) skipThreadCatalogUnavailableChannelByID(ctx context.Context, channelID string, err error, logMsg string) bool {
	return s.skipUnavailableChannelByID(ctx, channelID, err, logMsg, channelThreadCatalogUnavailableScope)
}

func (s *Syncer) skipUnavailableChannelByID(ctx context.Context, channelID string, err error, logMsg string, scopeForChannel func(string) string) bool {
	reason := unavailableReason(err)
	if reason == "" {
		return false
	}
	alreadyKnown := false
	if s.store != nil && channelID != "" && scopeForChannel != nil {
		scope := scopeForChannel(channelID)
		if previous, stateErr := s.store.GetSyncState(ctx, scope); stateErr == nil && previous == reason {
			alreadyKnown = true
		}
		_ = s.store.SetSyncState(ctx, scope, reason)
	}
	if alreadyKnown {
		s.logger.Debug(logMsg, "channel_id", channelID, "reason", reason)
	} else {
		s.logger.Warn(logMsg, "channel_id", channelID, "err", err)
	}
	return true
}

func isRetryableSyncError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if netErr, ok := errors.AsType[net.Error](err); ok {
		return netErr.Timeout()
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "deadline exceeded"),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "eof"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "stream error"),
		strings.Contains(msg, "goaway"),
		strings.Contains(msg, "http 429"),
		strings.Contains(msg, "http 500"),
		strings.Contains(msg, "http 502"),
		strings.Contains(msg, "http 503"),
		strings.Contains(msg, "http 504"):
		return true
	default:
		return false
	}
}

func unavailableReason(err error) string {
	switch {
	case isMissingAccess(err):
		return "missing_access"
	case isUnknownChannel(err):
		return "unknown_channel"
	default:
		return ""
	}
}

func isUnknownChannel(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown channel") ||
		(strings.Contains(msg, "http 404") && strings.Contains(msg, `"code": 10003`))
}

func isMissingAccess(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "403 Forbidden") || strings.Contains(msg, "Missing Access")
}
