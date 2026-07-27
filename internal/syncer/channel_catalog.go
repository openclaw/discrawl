package syncer

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
)

type channelCatalogMode int

const (
	channelCatalogFull channelCatalogMode = iota
	channelCatalogIncremental
)

func (s *Syncer) channelList(ctx context.Context, guildID string, requested []string, mode channelCatalogMode, exclusions channelExclusions, selectedGuildIDs map[string]struct{}) ([]*discordgo.Channel, bool, error) {
	if len(requested) == 0 {
		channels, err := s.liveChannelList(ctx, guildID, mode, exclusions)
		if err != nil {
			return nil, false, err
		}
		return filterExcludedDiscordChannels(channels, exclusions), false, nil
	}

	requestedSet := makeGuildSet(requested)
	storedByID := map[string]*discordgo.Channel{}
	storedCatalog := map[string]*discordgo.Channel{}
	if s.store != nil {
		rows, err := s.store.Channels(ctx, guildID)
		if err != nil {
			return nil, false, err
		}
		storedCatalog = storedChannelCatalog(rows)
		storedByID = selectStoredChannels(rows, requestedSet)
		if canUseStoredTargets(storedByID, requestedSet) {
			selected := selectRequestedChannels(nil, storedByID, requestedSet)
			return filterExcludedDiscordChannelsWithCatalog(selected, storedCatalog, exclusions), true, nil
		}
	}

	topLevel, err := s.client.GuildChannels(ctx, guildID)
	if err != nil {
		return nil, false, fmt.Errorf("fetch channels for guild %s: %w", guildID, err)
	}
	allChannels := make(map[string]*discordgo.Channel, len(topLevel))
	for _, channel := range topLevel {
		allChannels[channel.ID] = channel
	}

	selected := selectRequestedChannels(allChannels, storedByID, requestedSet)
	requestedForums := requestedForumParents(allChannels, requestedSet)
	if len(requestedForums) > 0 {
		if err := s.appendThreadCatalog(ctx, allChannels, setToSlice(requestedForums)); err != nil {
			return nil, false, err
		}
		selected = selectRequestedChannels(allChannels, storedByID, requestedSet)
	}

	if unresolvedRequestedIDs(selected, requestedSet) > 0 {
		if err := s.appendDirectRequestedChannels(ctx, guildID, allChannels, requestedSet, selectedGuildIDs); err != nil {
			return nil, false, err
		}
		selected = selectRequestedChannels(allChannels, storedByID, requestedSet)
	}

	if unresolvedRequestedIDs(selected, requestedSet) > 0 {
		if err := s.appendThreadCatalog(ctx, allChannels, threadParentIDs(topLevel)); err != nil {
			return nil, false, err
		}
		selected = selectRequestedChannels(allChannels, storedByID, requestedSet)
	}
	return filterExcludedDiscordChannelsWithCatalog(
		selected,
		mergedChannelCatalog(storedCatalog, allChannels),
		exclusions,
	), true, nil
}

func (s *Syncer) appendDirectRequestedChannels(
	ctx context.Context,
	guildID string,
	allChannels map[string]*discordgo.Channel,
	requested map[string]struct{},
	selectedGuildIDs map[string]struct{},
) error {
	for requestedID := range requested {
		if _, ok := allChannels[requestedID]; ok {
			continue
		}
		channel, err := s.client.Channel(ctx, requestedID)
		if err != nil {
			return fmt.Errorf("fetch requested channel %s: %w", requestedID, err)
		}
		if channel == nil || channel.ID == "" {
			continue
		}
		if channel.GuildID != "" && guildID != "" && channel.GuildID != guildID {
			if _, selected := selectedGuildIDs[channel.GuildID]; !selected {
				return fmt.Errorf("requested channel %s belongs to guild %s, not %s", channel.ID, channel.GuildID, guildID)
			}
			// requested is rebuilt by channelList for each selected guild. Exclude
			// the target only from this guild's fallback discovery; its owning guild
			// gets a fresh request set and performs the same direct lookup.
			delete(requested, requestedID)
			continue
		}
		allChannels[channel.ID] = channel
	}
	return nil
}

func (s *Syncer) liveChannelList(ctx context.Context, guildID string, mode channelCatalogMode, exclusions channelExclusions) ([]*discordgo.Channel, error) {
	channels, err := s.client.GuildChannels(ctx, guildID)
	if err != nil {
		return nil, fmt.Errorf("fetch channels for guild %s: %w", guildID, err)
	}
	allChannels := make(map[string]*discordgo.Channel, len(channels))
	for _, channel := range channels {
		allChannels[channel.ID] = channel
	}
	if mode == channelCatalogIncremental {
		if err := s.appendActiveThreadCatalog(ctx, allChannels, guildID, scopedThreadParentIDs(channels, exclusions)); err != nil {
			return nil, err
		}
		return mapsToSlice(allChannels), nil
	}
	var storedRows []store.ChannelRow
	if s.store != nil {
		rows, err := s.store.Channels(ctx, guildID)
		if err != nil {
			return nil, err
		}
		storedRows = rows
		mergeStoredThreadChannels(allChannels, rows)
	}
	parentIDs := scopedThreadParentIDs(channels, exclusions)
	if len(storedThreadParentIDs(storedRows)) == 0 {
		if err := s.appendThreadCatalog(ctx, allChannels, parentIDs); err != nil {
			return nil, err
		}
	} else if err := s.appendActiveThreadCatalog(ctx, allChannels, guildID, parentIDs); err != nil {
		return nil, err
	}
	return mapsToSlice(allChannels), nil
}

func scopedThreadParentIDs(channels []*discordgo.Channel, exclusions channelExclusions) []string {
	channelByID := make(map[string]*discordgo.Channel, len(channels))
	for _, channel := range channels {
		if channel != nil {
			channelByID[channel.ID] = channel
		}
	}
	parents := make([]string, 0, len(channels))
	for _, channel := range channels {
		if isThreadParent(channel) && !exclusions.excludesDiscordChannel(channel, channelByID) {
			parents = append(parents, channel.ID)
		}
	}
	return parents
}

func (s *Syncer) appendThreadCatalog(ctx context.Context, allChannels map[string]*discordgo.Channel, parents []string) error {
	for _, parentID := range uniqueIDs(parents) {
		channel := allChannels[parentID]
		if !isThreadParent(channel) {
			continue
		}
		unavailable, err := s.appendActiveThreads(ctx, allChannels, channel.ID, false)
		if err != nil {
			return err
		}
		failed := unavailable
		for _, private := range []bool{false, true} {
			archived, err := s.client.ThreadsArchived(ctx, channel.ID, private)
			if err != nil {
				if s.skipThreadCatalogUnavailableChannelByID(ctx, channel.ID, err, "thread archive crawl failed") {
					failed = true
					continue
				}
				s.logger.Warn("thread archive crawl failed", "channel_id", channel.ID, "private", private, "err", err)
				failed = true
				continue
			}
			for _, thread := range archived {
				allChannels[thread.ID] = thread
			}
		}
		if !failed {
			if err := s.clearThreadCatalogUnavailableChannel(ctx, channel.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Syncer) appendActiveThreadCatalog(ctx context.Context, allChannels map[string]*discordgo.Channel, guildID string, parents []string) error {
	allowedParents := make(map[string]struct{}, len(parents))
	for _, parentID := range uniqueIDs(parents) {
		channel := allChannels[parentID]
		if !isThreadParent(channel) {
			continue
		}
		allowedParents[parentID] = struct{}{}
	}
	if len(allowedParents) == 0 {
		return nil
	}
	active, err := s.client.GuildThreadsActive(ctx, guildID)
	if err != nil {
		if isMissingAccess(err) {
			s.logger.Warn("guild active thread crawl skipped", "guild_id", guildID, "err", err)
			return nil
		}
		return err
	}
	for _, thread := range active {
		if thread == nil {
			continue
		}
		if _, ok := allowedParents[thread.ParentID]; !ok {
			continue
		}
		allChannels[thread.ID] = thread
	}
	return nil
}

func (s *Syncer) appendActiveThreads(ctx context.Context, allChannels map[string]*discordgo.Channel, channelID string, clearOnSuccess bool) (bool, error) {
	active, err := s.client.ThreadsActive(ctx, channelID)
	if err != nil {
		if s.skipThreadCatalogUnavailableChannelByID(ctx, channelID, err, "channel thread crawl skipped") {
			return true, nil
		}
		return false, err
	}
	if clearOnSuccess {
		if err := s.clearThreadCatalogUnavailableChannel(ctx, channelID); err != nil {
			return false, err
		}
	}
	for _, thread := range active {
		allChannels[thread.ID] = thread
	}
	return false, nil
}

func (s *Syncer) clearThreadCatalogUnavailableChannel(ctx context.Context, channelID string) error {
	if s == nil || s.store == nil || channelID == "" {
		return nil
	}
	return s.store.DeleteSyncState(ctx, channelThreadCatalogUnavailableScope(channelID))
}

func mapsToSlice(in map[string]*discordgo.Channel) []*discordgo.Channel {
	out := make([]*discordgo.Channel, 0, len(in))
	for _, channel := range in {
		out = append(out, channel)
	}
	sortChannels(out)
	return out
}

func mergeStoredThreadChannels(allChannels map[string]*discordgo.Channel, rows []store.ChannelRow) {
	for _, row := range rows {
		if !strings.HasPrefix(row.Kind, "thread_") {
			continue
		}
		if _, ok := allChannels[row.ID]; ok {
			continue
		}
		allChannels[row.ID] = channelFromRow(row)
	}
}

func selectStoredChannels(rows []store.ChannelRow, requested map[string]struct{}) map[string]*discordgo.Channel {
	if len(rows) == 0 || len(requested) == 0 {
		return nil
	}
	out := make(map[string]*discordgo.Channel, len(requested))
	for _, row := range rows {
		if _, ok := requested[row.ID]; !ok {
			continue
		}
		out[row.ID] = channelFromRow(row)
	}
	return out
}

func storedChannelCatalog(rows []store.ChannelRow) map[string]*discordgo.Channel {
	if len(rows) == 0 {
		return nil
	}
	out := make(map[string]*discordgo.Channel, len(rows))
	for _, row := range rows {
		out[row.ID] = channelFromRow(row)
	}
	return out
}

func mergedChannelCatalog(base, overlay map[string]*discordgo.Channel) map[string]*discordgo.Channel {
	out := make(map[string]*discordgo.Channel, len(base)+len(overlay))
	maps.Copy(out, base)
	maps.Copy(out, overlay)
	return out
}

func channelFromRow(row store.ChannelRow) *discordgo.Channel {
	channelType := channelTypeFromKind(row.Kind)
	var threadMeta *discordgo.ThreadMetadata
	if strings.HasPrefix(row.Kind, "thread_") {
		threadMeta = &discordgo.ThreadMetadata{
			Archived:         row.IsArchived,
			Locked:           row.IsLocked,
			ArchiveTimestamp: row.ArchiveTimestamp,
		}
	}
	return &discordgo.Channel{
		ID:             row.ID,
		GuildID:        row.GuildID,
		ParentID:       row.ParentID,
		Name:           row.Name,
		Topic:          row.Topic,
		Position:       row.Position,
		NSFW:           row.IsNSFW,
		Type:           channelType,
		ThreadMetadata: threadMeta,
	}
}

func canUseStoredTargets(storedByID map[string]*discordgo.Channel, requested map[string]struct{}) bool {
	if len(requested) == 0 || len(storedByID) != len(requested) {
		return false
	}
	for _, channel := range storedByID {
		if channel != nil && channel.Type == discordgo.ChannelTypeGuildForum {
			return false
		}
	}
	return true
}

func selectRequestedChannels(liveByID, storedByID map[string]*discordgo.Channel, requested map[string]struct{}) []*discordgo.Channel {
	if len(requested) == 0 {
		return nil
	}
	selected := make(map[string]*discordgo.Channel, len(requested))
	for requestedID := range requested {
		if channel, ok := liveByID[requestedID]; ok {
			selected[requestedID] = channel
			continue
		}
		if channel, ok := storedByID[requestedID]; ok {
			selected[requestedID] = channel
		}
	}
	for forumID := range requestedForumParents(selected, requested) {
		for _, channel := range liveByID {
			if channel == nil || channel.ParentID != forumID || !isThreadChannel(channel) {
				continue
			}
			selected[channel.ID] = channel
		}
	}
	return mapsToSlice(selected)
}

func requestedForumParents(channels map[string]*discordgo.Channel, requested map[string]struct{}) map[string]struct{} {
	if len(requested) == 0 {
		return nil
	}
	parents := make(map[string]struct{})
	for requestedID := range requested {
		channel := channels[requestedID]
		if channel != nil && channel.Type == discordgo.ChannelTypeGuildForum {
			parents[requestedID] = struct{}{}
		}
	}
	return parents
}

func setToSlice(in map[string]struct{}) []string {
	out := make([]string, 0, len(in))
	for id := range in {
		out = append(out, id)
	}
	return out
}

func unresolvedRequestedIDs(selected []*discordgo.Channel, requested map[string]struct{}) int {
	if len(requested) == 0 {
		return 0
	}
	selectedIDs := make(map[string]struct{}, len(selected))
	for _, channel := range selected {
		if channel != nil {
			selectedIDs[channel.ID] = struct{}{}
		}
	}
	missing := 0
	for requestedID := range requested {
		if _, ok := selectedIDs[requestedID]; !ok {
			missing++
		}
	}
	return missing
}

func threadParentIDs(channels []*discordgo.Channel) []string {
	parents := make([]string, 0, len(channels))
	for _, channel := range channels {
		if isThreadParent(channel) {
			parents = append(parents, channel.ID)
		}
	}
	return parents
}

func storedThreadParentIDs(rows []store.ChannelRow) map[string]struct{} {
	if len(rows) == 0 {
		return nil
	}
	parents := make(map[string]struct{})
	for _, row := range rows {
		if !strings.HasPrefix(row.Kind, "thread_") || row.ParentID == "" {
			continue
		}
		parents[row.ParentID] = struct{}{}
	}
	return parents
}

func uniqueIDs(ids []string) []string {
	set := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := set[id]; ok {
			continue
		}
		set[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func isThreadChannel(channel *discordgo.Channel) bool {
	if channel == nil {
		return false
	}
	switch channel.Type {
	case discordgo.ChannelTypeGuildPublicThread,
		discordgo.ChannelTypeGuildPrivateThread,
		discordgo.ChannelTypeGuildNewsThread:
		return true
	default:
		return false
	}
}

func sortChannels(channels []*discordgo.Channel) {
	slices.SortFunc(channels, func(a, b *discordgo.Channel) int {
		switch {
		case a.Position < b.Position:
			return -1
		case a.Position > b.Position:
			return 1
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})
}

func isThreadParent(channel *discordgo.Channel) bool {
	if channel == nil {
		return false
	}
	switch channel.Type {
	case discordgo.ChannelTypeGuildText, discordgo.ChannelTypeGuildNews, discordgo.ChannelTypeGuildForum:
		return true
	default:
		return false
	}
}

func isMessageChannel(channel *discordgo.Channel) bool {
	if channel == nil {
		return false
	}
	switch channel.Type {
	case discordgo.ChannelTypeGuildText,
		discordgo.ChannelTypeGuildNews,
		discordgo.ChannelTypeGuildPublicThread,
		discordgo.ChannelTypeGuildPrivateThread,
		discordgo.ChannelTypeGuildNewsThread:
		return true
	default:
		return false
	}
}

func toChannelRecord(channel *discordgo.Channel, raw string) store.ChannelRecord {
	record := store.ChannelRecord{
		ID:       channel.ID,
		GuildID:  channel.GuildID,
		ParentID: channel.ParentID,
		Kind:     channelKind(channel),
		Name:     channel.Name,
		Topic:    channel.Topic,
		Position: channel.Position,
		IsNSFW:   channel.NSFW,
		RawJSON:  raw,
	}
	if channel.ThreadMetadata != nil {
		record.IsArchived = channel.ThreadMetadata.Archived
		record.IsLocked = channel.ThreadMetadata.Locked
		record.ArchiveTimestamp = channel.ThreadMetadata.ArchiveTimestamp.Format(time.RFC3339Nano)
		record.ThreadParentID = channel.ParentID
	}
	if channel.Type == discordgo.ChannelTypeGuildPrivateThread {
		record.IsPrivateThread = true
	}
	return record
}

func channelKind(channel *discordgo.Channel) string {
	switch channel.Type {
	case discordgo.ChannelTypeGuildCategory:
		return "category"
	case discordgo.ChannelTypeGuildText:
		return "text"
	case discordgo.ChannelTypeGuildNews:
		return "announcement"
	case discordgo.ChannelTypeGuildForum:
		return "forum"
	case discordgo.ChannelTypeGuildPublicThread:
		return "thread_public"
	case discordgo.ChannelTypeGuildPrivateThread:
		return "thread_private"
	case discordgo.ChannelTypeGuildNewsThread:
		return "thread_announcement"
	case discordgo.ChannelTypeGuildVoice:
		return "voice"
	default:
		return fmt.Sprintf("type_%d", channel.Type)
	}
}

func channelTypeFromKind(kind string) discordgo.ChannelType {
	switch kind {
	case "category":
		return discordgo.ChannelTypeGuildCategory
	case "text":
		return discordgo.ChannelTypeGuildText
	case "announcement":
		return discordgo.ChannelTypeGuildNews
	case "forum":
		return discordgo.ChannelTypeGuildForum
	case "thread_public":
		return discordgo.ChannelTypeGuildPublicThread
	case "thread_private":
		return discordgo.ChannelTypeGuildPrivateThread
	case "thread_announcement":
		return discordgo.ChannelTypeGuildNewsThread
	case "voice":
		return discordgo.ChannelTypeGuildVoice
	default:
		return discordgo.ChannelTypeGuildText
	}
}
