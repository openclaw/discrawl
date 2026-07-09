package syncer

import (
	"context"
	"strings"

	"github.com/bwmarrin/discordgo"

	"github.com/openclaw/discrawl/internal/store"
)

type channelExclusions struct {
	ids   map[string]struct{}
	kinds map[string]struct{}
}

func newChannelExclusions(ids, kinds []string) channelExclusions {
	return channelExclusions{
		ids:   normalizedStringSet(ids, false),
		kinds: normalizedStringSet(kinds, true),
	}
}

func normalizedStringSet(values []string, lower bool) map[string]struct{} {
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

func (e channelExclusions) merged(other channelExclusions) channelExclusions {
	merged := channelExclusions{
		ids:   make(map[string]struct{}, len(e.ids)+len(other.ids)),
		kinds: make(map[string]struct{}, len(e.kinds)+len(other.kinds)),
	}
	for id := range e.ids {
		merged.ids[id] = struct{}{}
	}
	for id := range other.ids {
		merged.ids[id] = struct{}{}
	}
	for kind := range e.kinds {
		merged.kinds[kind] = struct{}{}
	}
	for kind := range other.kinds {
		merged.kinds[kind] = struct{}{}
	}
	return merged
}

func (e channelExclusions) excludesID(channelID string) bool {
	_, ok := e.ids[channelID]
	return ok
}

func (e channelExclusions) excludesKind(kind string) bool {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if _, ok := e.kinds[kind]; ok {
		return true
	}
	switch kind {
	case "news", "thread_news", "thread_announcement":
		_, ok := e.kinds["announcement"]
		return ok
	default:
		return false
	}
}

func (e channelExclusions) excludesDiscordChannel(channel *discordgo.Channel, channelByID map[string]*discordgo.Channel) bool {
	return e.excludesDiscordChannelID(channel, channelByID, nil)
}

func (e channelExclusions) excludesDiscordChannelID(
	channel *discordgo.Channel,
	channelByID map[string]*discordgo.Channel,
	visiting map[string]struct{},
) bool {
	if channel == nil {
		return false
	}
	if e.excludesID(channel.ID) || e.excludesKind(channelKind(channel)) {
		return true
	}
	if channel.ParentID == "" {
		return false
	}
	if e.excludesID(channel.ParentID) {
		return true
	}
	if visiting == nil {
		visiting = map[string]struct{}{}
	}
	if _, ok := visiting[channel.ID]; ok {
		return false
	}
	visiting[channel.ID] = struct{}{}
	defer delete(visiting, channel.ID)
	parent := channelByID[channel.ParentID]
	return parent != nil && e.excludesDiscordChannelID(parent, channelByID, visiting)
}

func (e channelExclusions) excludesStoredChannel(channel store.ChannelRow, channelByID map[string]store.ChannelRow) bool {
	return e.excludesStoredChannelID(channel, channelByID, nil)
}

func (e channelExclusions) excludesStoredChannelID(
	channel store.ChannelRow,
	channelByID map[string]store.ChannelRow,
	visiting map[string]struct{},
) bool {
	if e.excludesID(channel.ID) || e.excludesKind(channel.Kind) {
		return true
	}
	parentID := storedChannelParentID(channel)
	if parentID == "" {
		return false
	}
	if e.excludesID(parentID) {
		return true
	}
	if visiting == nil {
		visiting = map[string]struct{}{}
	}
	if _, ok := visiting[channel.ID]; ok {
		return false
	}
	visiting[channel.ID] = struct{}{}
	defer delete(visiting, channel.ID)
	parent, ok := channelByID[parentID]
	return ok && e.excludesStoredChannelID(parent, channelByID, visiting)
}

func storedChannelParentID(channel store.ChannelRow) string {
	if channel.ThreadParentID != "" {
		return channel.ThreadParentID
	}
	return channel.ParentID
}

func (s *Syncer) effectiveChannelExclusions(opts SyncOptions) channelExclusions {
	if s == nil {
		return newChannelExclusions(opts.ExcludeChannelIDs, opts.ExcludeChannelKinds)
	}
	return s.channelExclusions.merged(newChannelExclusions(opts.ExcludeChannelIDs, opts.ExcludeChannelKinds))
}

func (s *Syncer) filterExcludedStoredChannelIDs(ctx context.Context, guildID string, channelIDs []string, opts SyncOptions) ([]string, error) {
	exclusions := s.effectiveChannelExclusions(opts)
	if len(exclusions.ids) == 0 && len(exclusions.kinds) == 0 {
		return channelIDs, nil
	}
	channels, err := s.store.Channels(ctx, guildID)
	if err != nil {
		return nil, err
	}
	channelByID := make(map[string]store.ChannelRow, len(channels))
	for _, channel := range channels {
		channelByID[channel.ID] = channel
	}
	out := make([]string, 0, len(channelIDs))
	for _, channelID := range channelIDs {
		channel, ok := channelByID[channelID]
		if ok && exclusions.excludesStoredChannel(channel, channelByID) {
			continue
		}
		if !ok && exclusions.excludesID(channelID) {
			continue
		}
		out = append(out, channelID)
	}
	return out, nil
}
