package share

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/openclaw/discrawl/internal/store"
)

type FilterOptions struct {
	PublicOnly        bool
	IncludeChannelIDs []string
	ExcludeChannelIDs []string
}

func (o FilterOptions) Active() bool {
	return o.PublicOnly || len(stringSet(o.IncludeChannelIDs)) > 0 || len(stringSet(o.ExcludeChannelIDs)) > 0
}

type PublishScopeCount struct {
	Candidate int `json:"candidate"`
	Allowed   int `json:"allowed"`
	Excluded  int `json:"excluded"`
}

type PublishScopeGuild struct {
	GuildID           string `json:"guild_id"`
	GuildName         string `json:"guild_name,omitempty"`
	SourceHint        string `json:"source_hint"`
	MetadataReady     bool   `json:"metadata_ready"`
	CandidateChannels int    `json:"candidate_channels"`
	AllowedChannels   int    `json:"allowed_channels"`
	CandidateMessages int    `json:"candidate_messages"`
	AllowedMessages   int    `json:"allowed_messages"`
}

type PublishScopePreflight struct {
	Ready             bool                `json:"ready"`
	PublicOnly        bool                `json:"public_only"`
	IncludeChannelIDs []string            `json:"include_channel_ids,omitempty"`
	ExcludeChannelIDs []string            `json:"exclude_channel_ids,omitempty"`
	Guilds            []PublishScopeGuild `json:"guilds"`
	Channels          PublishScopeCount   `json:"channels"`
	Messages          PublishScopeCount   `json:"messages"`
	Empty             bool                `json:"empty"`
	EmptyReason       string              `json:"empty_reason,omitempty"`
	Warnings          []string            `json:"warnings,omitempty"`
	RepairCommand     string              `json:"repair_command,omitempty"`
}

// PreflightPublishScope evaluates the same filters used by Export without
// touching the snapshot repository or any configured remote.
func PreflightPublishScope(ctx context.Context, s *store.Store, opts FilterOptions) (PublishScopePreflight, error) {
	candidateOpts := FilterOptions{
		IncludeChannelIDs: opts.IncludeChannelIDs,
		ExcludeChannelIDs: opts.ExcludeChannelIDs,
	}
	candidateFilter, err := newSnapshotFilter(ctx, s.DB(), candidateOpts)
	if err != nil {
		return PublishScopePreflight{}, err
	}
	selectedFilter, err := newSnapshotFilter(ctx, s.DB(), opts)
	if err != nil {
		return PublishScopePreflight{}, err
	}

	report := PublishScopePreflight{
		Ready:             true,
		PublicOnly:        opts.PublicOnly,
		IncludeChannelIDs: slices.Sorted(maps.Keys(stringSet(opts.IncludeChannelIDs))),
		ExcludeChannelIDs: slices.Sorted(maps.Keys(stringSet(opts.ExcludeChannelIDs))),
		Guilds:            []PublishScopeGuild{},
	}
	guilds, err := loadPublishScopeGuilds(ctx, s.DB())
	if err != nil {
		return PublishScopePreflight{}, err
	}
	channelGuilds, err := countPublishScopeChannels(ctx, s.DB(), candidateFilter, selectedFilter, guilds, &report)
	if err != nil {
		return PublishScopePreflight{}, err
	}
	if err := countPublishScopeMessages(ctx, s.DB(), candidateFilter, selectedFilter, guilds, channelGuilds, &report); err != nil {
		return PublishScopePreflight{}, err
	}

	report.Channels.Excluded = report.Channels.Candidate - report.Channels.Allowed
	report.Messages.Excluded = report.Messages.Candidate - report.Messages.Allowed
	for _, guild := range guilds {
		if opts.PublicOnly && (guild.CandidateChannels > 0 || guild.CandidateMessages > 0) && !guild.MetadataReady {
			report.Ready = false
			report.Warnings = append(report.Warnings,
				fmt.Sprintf("guild %s lacks usable guild, channel or category permission metadata for public-only selection", guild.GuildID))
		}
		report.Guilds = append(report.Guilds, *guild)
	}
	sort.Slice(report.Guilds, func(i, j int) bool { return report.Guilds[i].GuildID < report.Guilds[j].GuildID })
	sort.Strings(report.Warnings)
	if !report.Ready {
		report.RepairCommand = "discrawl sync --source discord"
	}
	report.Empty = report.Messages.Allowed == 0
	if report.Empty {
		switch {
		case report.Messages.Candidate == 0:
			report.EmptyReason = "no_matching_messages"
		case !report.Ready:
			report.EmptyReason = "metadata_incomplete"
		default:
			report.EmptyReason = "filters_match_no_publishable_messages"
		}
	}
	return report, nil
}

func loadPublishScopeGuilds(ctx context.Context, db *sql.DB) (map[string]*PublishScopeGuild, error) {
	rows, err := db.QueryContext(ctx, `select id, name, raw_json from guilds where id != ? order by id`, directMessageGuildID)
	if err != nil {
		return nil, fmt.Errorf("query guild publish preflight: %w", err)
	}
	defer func() { _ = rows.Close() }()
	guilds := map[string]*PublishScopeGuild{}
	for rows.Next() {
		var id, name, raw string
		if err := rows.Scan(&id, &name, &raw); err != nil {
			return nil, fmt.Errorf("scan guild publish preflight: %w", err)
		}
		_, metadataReady := everyoneGuildPermissions(raw, id)
		guilds[id] = &PublishScopeGuild{
			GuildID:       id,
			GuildName:     name,
			SourceHint:    publishSourceHint(raw, metadataReady),
			MetadataReady: metadataReady,
		}
	}
	return guilds, rows.Err()
}

func countPublishScopeChannels(
	ctx context.Context,
	db *sql.DB,
	candidateFilter, selectedFilter *snapshotFilter,
	guilds map[string]*PublishScopeGuild,
	report *PublishScopePreflight,
) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, `select id, guild_id from channels where guild_id != ?`, directMessageGuildID)
	if err != nil {
		return nil, fmt.Errorf("query channel publish preflight: %w", err)
	}
	defer func() { _ = rows.Close() }()
	channelGuilds := map[string]string{}
	for rows.Next() {
		var channelID, guildID string
		if err := rows.Scan(&channelID, &guildID); err != nil {
			return nil, fmt.Errorf("scan channel publish preflight: %w", err)
		}
		channelGuilds[channelID] = guildID
		if !candidateFilter.allowChannelID(channelID) {
			continue
		}
		report.Channels.Candidate++
		guild := ensurePublishScopeGuild(guilds, guildID)
		guild.CandidateChannels++
		if selectedFilter.publicOnly && selectedFilter.publicMissing[channelID] {
			guild.MetadataReady = false
		}
		if selectedFilter.allowChannelID(channelID) {
			report.Channels.Allowed++
			guild.AllowedChannels++
		}
	}
	return channelGuilds, rows.Err()
}

func countPublishScopeMessages(
	ctx context.Context,
	db *sql.DB,
	candidateFilter, selectedFilter *snapshotFilter,
	guilds map[string]*PublishScopeGuild,
	channelGuilds map[string]string,
	report *PublishScopePreflight,
) error {
	rows, err := db.QueryContext(ctx, `select id, guild_id, channel_id from messages where guild_id != ?`, directMessageGuildID)
	if err != nil {
		return fmt.Errorf("query message publish preflight: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var messageID, messageGuildID, channelID string
		if err := rows.Scan(&messageID, &messageGuildID, &channelID); err != nil {
			return fmt.Errorf("scan message publish preflight: %w", err)
		}
		if !candidateFilter.allowChannelID(channelID) {
			continue
		}
		guildID := messageGuildID
		if channelGuildID, ok := channelGuilds[channelID]; ok {
			guildID = channelGuildID
		}
		report.Messages.Candidate++
		guild := ensurePublishScopeGuild(guilds, guildID)
		guild.CandidateMessages++
		if selectedFilter.publicOnly {
			if _, exists := selectedFilter.channels[channelID]; !exists {
				guild.MetadataReady = false
			}
		}
		if selectedFilter.allowedMessageIDs[messageID] || selectedFilter.allowChannelID(channelID) {
			report.Messages.Allowed++
			guild.AllowedMessages++
		}
	}
	return rows.Err()
}

func ensurePublishScopeGuild(guilds map[string]*PublishScopeGuild, guildID string) *PublishScopeGuild {
	if guild := guilds[guildID]; guild != nil {
		return guild
	}
	guild := &PublishScopeGuild{GuildID: guildID, SourceHint: "unknown"}
	guilds[guildID] = guild
	return guild
}

func publishSourceHint(raw string, metadataReady bool) string {
	var payload struct {
		Source string `json:"source"`
	}
	if decodeJSONUseNumber(raw, &payload) == nil && strings.EqualFold(strings.TrimSpace(payload.Source), "discord_desktop") {
		return "discord_desktop"
	}
	if metadataReady {
		return "discord_metadata"
	}
	return "unknown"
}
