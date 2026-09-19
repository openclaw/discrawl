package syncer

import (
	"context"
	"fmt"

	"github.com/bwmarrin/discordgo"
)

// Desktop imports omit authoritative type, parent, and permission evidence.
// Complete every requested ancestry before storeChannelList can persist it.
func (s *Syncer) hydrateDesktopChannelMetadata(
	ctx context.Context,
	guildID string,
	channels map[string]*discordgo.Channel,
	targets map[string]struct{},
	directResults map[string]directChannelResult,
) error {
	for targetID := range targets {
		seen := map[string]struct{}{}
		for channelID := targetID; channelID != ""; {
			if _, duplicate := seen[channelID]; duplicate {
				return fmt.Errorf("channel metadata for %s has an ancestor cycle at %s", targetID, channelID)
			}
			seen[channelID] = struct{}{}
			channel := channels[channelID]
			if channel == nil {
				result := s.directChannel(ctx, channelID, directResults)
				if result.err != nil {
					return fmt.Errorf("fetch channel metadata %s for %s: %w", channelID, targetID, result.err)
				}
				channel = result.channel
			}
			if channel == nil || channel.ID != channelID {
				return fmt.Errorf("channel metadata %s for %s is missing or has a different ID", channelID, targetID)
			}
			if channel.GuildID != guildID {
				return fmt.Errorf("channel metadata %s belongs to guild %s, not %s", channelID, channel.GuildID, guildID)
			}
			if isThreadChannel(channel) {
				if channel.ParentID == "" {
					return fmt.Errorf("thread metadata %s is missing its parent", channelID)
				}
			} else if channel.PermissionOverwrites == nil {
				return fmt.Errorf("channel metadata %s is missing permission overwrites", channelID)
			}
			channels[channelID] = channel
			channelID = channel.ParentID
		}
	}
	return nil
}

func withHydratedAncestors(selected []*discordgo.Channel, catalog map[string]*discordgo.Channel, targets map[string]struct{}) []*discordgo.Channel {
	if len(targets) == 0 {
		return selected
	}
	channels := make(map[string]*discordgo.Channel, len(selected))
	for _, channel := range selected {
		channels[channel.ID] = channel
		if _, hydrated := targets[channel.ID]; !hydrated {
			continue
		}
		// Only retain ancestors of targets that survived the configured scope.
		for parent := catalog[channel.ParentID]; parent != nil; parent = catalog[parent.ParentID] {
			channels[parent.ID] = parent
		}
	}
	return mapsToSlice(channels)
}
