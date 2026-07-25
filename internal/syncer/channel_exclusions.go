package syncer

import (
	"context"
	"strings"

	"github.com/bwmarrin/discordgo"

	"github.com/openclaw/discrawl/internal/store"
)

type channelExclusions struct {
	ids                map[string]struct{}
	kinds              map[string]struct{}
	allowedCategoryIDs map[string]struct{}
	categoryScopeSet   bool
}

func newChannelScope(ids, kinds, allowedCategoryIDs []string) channelExclusions {
	categories := normalizedStringSet(allowedCategoryIDs, false)
	return channelExclusions{
		ids:                normalizedStringSet(ids, false),
		kinds:              normalizedStringSet(kinds, true),
		allowedCategoryIDs: categories,
		categoryScopeSet:   len(categories) > 0,
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
	allowedCategoryIDs, categoryScopeSet := intersectOptionalSets(
		e.allowedCategoryIDs,
		e.categoryScopeSet,
		other.allowedCategoryIDs,
		other.categoryScopeSet,
	)
	merged := channelExclusions{
		ids:                make(map[string]struct{}, len(e.ids)+len(other.ids)),
		kinds:              make(map[string]struct{}, len(e.kinds)+len(other.kinds)),
		allowedCategoryIDs: allowedCategoryIDs,
		categoryScopeSet:   categoryScopeSet,
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

func intersectOptionalSets(
	left map[string]struct{},
	leftSet bool,
	right map[string]struct{},
	rightSet bool,
) (map[string]struct{}, bool) {
	if !leftSet && !rightSet {
		return nil, false
	}
	if !leftSet {
		return cloneStringSet(right), true
	}
	if !rightSet {
		return cloneStringSet(left), true
	}
	out := make(map[string]struct{}, min(len(left), len(right)))
	for value := range left {
		if _, ok := right[value]; ok {
			out[value] = struct{}{}
		}
	}
	return out, true
}

func cloneStringSet(in map[string]struct{}) map[string]struct{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for value := range in {
		out[value] = struct{}{}
	}
	return out
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
	if kind == "thread_announcement" {
		_, ok := e.kinds["announcement"]
		return ok
	}
	return false
}

func (e channelExclusions) excludesDiscordChannel(channel *discordgo.Channel, channelByID map[string]*discordgo.Channel) bool {
	if channel == nil {
		return false
	}
	if e.excludesID(channel.ID) || e.excludesKind(channelKind(channel)) {
		return true
	}
	if channel.ParentID == "" {
		return !e.allowsUnparentedDiscordChannel(channel)
	}
	if e.excludesID(channel.ParentID) {
		return true
	}
	parent := channelByID[channel.ParentID]
	if parent != nil && e.excludesKind(channelKind(parent)) {
		return true
	}
	return !e.allowsDiscordCategory(channel, channelByID)
}

func (e channelExclusions) excludesStoredChannel(channel store.ChannelRow, channelByID map[string]store.ChannelRow) bool {
	if e.excludesID(channel.ID) || e.excludesKind(channel.Kind) {
		return true
	}
	if channel.ParentID == "" {
		return !e.allowsUnparentedStoredChannel(channel)
	}
	if e.excludesID(channel.ParentID) {
		return true
	}
	parent, ok := channelByID[channel.ParentID]
	if ok && e.excludesKind(parent.Kind) {
		return true
	}
	return !e.allowsStoredCategory(channel, channelByID)
}

func (e channelExclusions) allowsUnparentedDiscordChannel(channel *discordgo.Channel) bool {
	if !e.categoryScopeSet {
		return true
	}
	if channel == nil {
		return false
	}
	_, ok := e.allowedCategoryIDs[channel.ID]
	return ok
}

func (e channelExclusions) allowsUnparentedStoredChannel(channel store.ChannelRow) bool {
	if !e.categoryScopeSet {
		return true
	}
	_, ok := e.allowedCategoryIDs[channel.ID]
	return ok
}

func (e channelExclusions) allowsDiscordCategory(channel *discordgo.Channel, channelByID map[string]*discordgo.Channel) bool {
	if !e.categoryScopeSet {
		return true
	}
	for current, seen := channel, map[string]struct{}{}; current != nil; {
		if current.ParentID == "" {
			return false
		}
		if _, ok := e.allowedCategoryIDs[current.ParentID]; ok {
			return true
		}
		if _, ok := seen[current.ParentID]; ok {
			return false
		}
		seen[current.ParentID] = struct{}{}
		current = channelByID[current.ParentID]
	}
	return false
}

func (e channelExclusions) allowsStoredCategory(channel store.ChannelRow, channelByID map[string]store.ChannelRow) bool {
	if !e.categoryScopeSet {
		return true
	}
	for current, seen := channel, map[string]struct{}{}; ; {
		if current.ParentID == "" {
			return false
		}
		if _, ok := e.allowedCategoryIDs[current.ParentID]; ok {
			return true
		}
		if _, ok := seen[current.ParentID]; ok {
			return false
		}
		seen[current.ParentID] = struct{}{}
		parent, ok := channelByID[current.ParentID]
		if !ok {
			return false
		}
		current = parent
	}
}

func (s *Syncer) effectiveChannelExclusions(opts SyncOptions) channelExclusions {
	if s == nil {
		return newChannelScope(opts.ExcludeChannelIDs, opts.ExcludeChannelKinds, opts.IncludeCategoryIDs)
	}
	return s.channelExclusions.merged(newChannelScope(opts.ExcludeChannelIDs, opts.ExcludeChannelKinds, opts.IncludeCategoryIDs))
}

func filterExcludedDiscordChannels(channels []*discordgo.Channel, exclusions channelExclusions) []*discordgo.Channel {
	channelByID := make(map[string]*discordgo.Channel, len(channels))
	for _, channel := range channels {
		if channel != nil {
			channelByID[channel.ID] = channel
		}
	}
	return filterExcludedDiscordChannelsWithCatalog(channels, channelByID, exclusions)
}

func filterExcludedDiscordChannelsWithCatalog(
	channels []*discordgo.Channel,
	channelByID map[string]*discordgo.Channel,
	exclusions channelExclusions,
) []*discordgo.Channel {
	if len(exclusions.ids) == 0 && len(exclusions.kinds) == 0 && !exclusions.categoryScopeSet {
		return channels
	}
	out := make([]*discordgo.Channel, 0, len(channels))
	for _, channel := range channels {
		if channel != nil && !exclusions.excludesDiscordChannel(channel, channelByID) {
			out = append(out, channel)
		}
	}
	return out
}

func (s *Syncer) filterExcludedStoredChannelIDs(ctx context.Context, guildID string, channelIDs []string, opts SyncOptions) ([]string, error) {
	exclusions := s.effectiveChannelExclusions(opts)
	if len(exclusions.ids) == 0 && len(exclusions.kinds) == 0 && !exclusions.categoryScopeSet {
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
