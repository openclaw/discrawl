package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const wiretapStatsScope = "wiretap:last_stats:v1"

const coverageQueryTimeout = 2 * time.Minute

var messageChannelKinds = map[string]struct{}{
	"text": {}, "news": {}, "announcement": {}, "dm": {}, "group_dm": {},
	"thread_public": {}, "thread_private": {}, "thread_news": {}, "thread_announcement": {},
}

type CoverageReport struct {
	GeneratedAt   time.Time       `json:"generated_at"`
	Guilds        []CoverageGuild `json:"guilds"`
	Totals        CoverageTotals  `json:"totals"`
	LastBotSyncAt time.Time       `json:"last_bot_sync_at,omitzero"`
	Wiretap       WiretapCoverage `json:"wiretap"`
}

type CoverageGuild struct {
	ID                          string            `json:"id"`
	Name                        string            `json:"name"`
	MessageCount                int               `json:"message_count"`
	ChannelCount                int               `json:"channel_count"`
	MessageChannelCount         int               `json:"message_channel_count"`
	NamedChannelCount           int               `json:"named_channel_count"`
	SyntheticChannelCount       int               `json:"synthetic_channel_count"`
	HistoryCompleteChannelCount int               `json:"history_complete_channel_count"`
	KnownFailureCount           int               `json:"known_failure_count"`
	EarliestMessageAt           time.Time         `json:"earliest_message_at,omitzero"`
	LatestMessageAt             time.Time         `json:"latest_message_at,omitzero"`
	Channels                    []CoverageChannel `json:"channels"`
}

type CoverageChannel struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	Kind              string    `json:"kind"`
	Synthetic         bool      `json:"synthetic"`
	MessageCapable    bool      `json:"message_capable"`
	MessageCount      int       `json:"message_count"`
	EarliestMessageAt time.Time `json:"earliest_message_at,omitzero"`
	LatestMessageAt   time.Time `json:"latest_message_at,omitzero"`
	HistoryComplete   *bool     `json:"history_complete,omitempty"`
	KnownFailureCount int       `json:"known_failure_count"`
}

type CoverageTotals struct {
	GuildCount                  int `json:"guild_count"`
	MessageCount                int `json:"message_count"`
	ChannelCount                int `json:"channel_count"`
	MessageChannelCount         int `json:"message_channel_count"`
	NamedChannelCount           int `json:"named_channel_count"`
	SyntheticChannelCount       int `json:"synthetic_channel_count"`
	HistoryCompleteChannelCount int `json:"history_complete_channel_count"`
	KnownFailureCount           int `json:"known_failure_count"`
	UnscopedKnownFailureCount   int `json:"unscoped_known_failure_count"`
}

type WiretapImportStats struct {
	FilesScanned    int       `json:"files_scanned"`
	Messages        int       `json:"messages"`
	Channels        int       `json:"channels"`
	SkippedMessages int       `json:"skipped_messages"`
	SkippedChannels int       `json:"skipped_channels"`
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
}

type WiretapCoverage struct {
	LastImportAt    time.Time `json:"last_import_at,omitzero"`
	FilesScanned    int       `json:"files_scanned"`
	Messages        int       `json:"messages"`
	Channels        int       `json:"channels"`
	SkippedMessages int       `json:"skipped_messages"`
	SkippedChannels int       `json:"skipped_channels"`
}

type CoverageDelta struct {
	Messages          int `json:"messages"`
	Channels          int `json:"channels"`
	NamedChannels     int `json:"named_channels"`
	SyntheticChannels int `json:"synthetic_channels"`
}

func (s *Store) SetWiretapImportStats(ctx context.Context, stats WiretapImportStats) error {
	body, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("encode wiretap coverage stats: %w", err)
	}
	return s.SetSyncState(ctx, wiretapStatsScope, string(body))
}

func (s *Store) Coverage(ctx context.Context, guildID string, generatedAt time.Time) (CoverageReport, error) {
	report := CoverageReport{GeneratedAt: generatedAt.UTC(), Guilds: []CoverageGuild{}}
	queryCtx, cancel := context.WithTimeout(ctx, coverageQueryTimeout)
	defer cancel()

	rows, err := s.db.QueryContext(queryCtx, `
		select id, name
		from guilds
		where ? = '' or id = ?
		order by lower(name), id
	`, guildID, guildID)
	if err != nil {
		return CoverageReport{}, fmt.Errorf("list coverage guilds: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var guild CoverageGuild
		if err := rows.Scan(&guild.ID, &guild.Name); err != nil {
			return CoverageReport{}, fmt.Errorf("scan coverage guild: %w", err)
		}
		guild.Channels = []CoverageChannel{}
		report.Guilds = append(report.Guilds, guild)
	}
	if err := rows.Close(); err != nil {
		return CoverageReport{}, fmt.Errorf("close coverage guild rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return CoverageReport{}, fmt.Errorf("list coverage guilds: %w", err)
	}
	if guildID != "" && len(report.Guilds) == 0 {
		return CoverageReport{}, fmt.Errorf("guild %q not found", guildID)
	}

	guilds := make(map[string]*CoverageGuild, len(report.Guilds))
	for i := range report.Guilds {
		guilds[report.Guilds[i].ID] = &report.Guilds[i]
	}
	channelRows, err := s.db.QueryContext(queryCtx, `
		select
			c.id, c.guild_id, c.name, c.kind,
			case when exists (
				select 1 from sync_state s
				where s.scope = 'channel:' || c.id || ':history_complete'
			) then 1 else 0 end,
			count(m.id), coalesce(min(m.created_at), ''), coalesce(max(m.created_at), '')
		from channels c
		left join messages m on m.channel_id = c.id and m.deleted_at is null
		where ? = '' or c.guild_id = ?
		group by c.id, c.guild_id, c.name, c.kind
		order by c.guild_id, count(m.id) desc, lower(c.name), c.id
	`, guildID, guildID)
	if err != nil {
		return CoverageReport{}, fmt.Errorf("query channel coverage: %w", err)
	}
	defer func() { _ = channelRows.Close() }()
	for channelRows.Next() {
		var channel CoverageChannel
		var rowGuildID, earliest, latest string
		var historyComplete int
		if err := channelRows.Scan(
			&channel.ID, &rowGuildID, &channel.Name, &channel.Kind,
			&historyComplete, &channel.MessageCount, &earliest, &latest,
		); err != nil {
			return CoverageReport{}, fmt.Errorf("scan channel coverage: %w", err)
		}
		channel.Synthetic = isSyntheticChannel(channel.ID, channel.Name)
		_, channel.MessageCapable = messageChannelKinds[channel.Kind]
		channel.EarliestMessageAt = parseTime(earliest)
		channel.LatestMessageAt = parseTime(latest)
		if historyComplete == 1 {
			complete := true
			channel.HistoryComplete = &complete
		}
		guild := guilds[rowGuildID]
		if guild == nil {
			continue
		}
		guild.Channels = append(guild.Channels, channel)
		guild.ChannelCount++
		guild.MessageCount += channel.MessageCount
		if channel.MessageCapable {
			guild.MessageChannelCount++
		}
		if channel.Synthetic {
			guild.SyntheticChannelCount++
		} else {
			guild.NamedChannelCount++
		}
		if channel.HistoryComplete != nil {
			guild.HistoryCompleteChannelCount++
		}
		guild.EarliestMessageAt = earlierNonZero(guild.EarliestMessageAt, channel.EarliestMessageAt)
		guild.LatestMessageAt = later(guild.LatestMessageAt, channel.LatestMessageAt)
	}
	if err := channelRows.Err(); err != nil {
		return CoverageReport{}, fmt.Errorf("query channel coverage: %w", err)
	}

	report.Totals.GuildCount = len(report.Guilds)
	for i := range report.Guilds {
		guild := &report.Guilds[i]
		sort.SliceStable(guild.Channels, func(i, j int) bool {
			if guild.Channels[i].MessageCount != guild.Channels[j].MessageCount {
				return guild.Channels[i].MessageCount > guild.Channels[j].MessageCount
			}
			if guild.Channels[i].Name != guild.Channels[j].Name {
				return strings.ToLower(guild.Channels[i].Name) < strings.ToLower(guild.Channels[j].Name)
			}
			return guild.Channels[i].ID < guild.Channels[j].ID
		})
		report.Totals.MessageCount += guild.MessageCount
		report.Totals.ChannelCount += guild.ChannelCount
		report.Totals.MessageChannelCount += guild.MessageChannelCount
		report.Totals.NamedChannelCount += guild.NamedChannelCount
		report.Totals.SyntheticChannelCount += guild.SyntheticChannelCount
		report.Totals.HistoryCompleteChannelCount += guild.HistoryCompleteChannelCount
	}
	if err := s.loadKnownFailureCoverage(queryCtx, guildID, &report); err != nil {
		return CoverageReport{}, err
	}
	if err := s.loadCoverageState(ctx, &report); err != nil {
		return CoverageReport{}, err
	}
	return report, nil
}

func (s *Store) loadKnownFailureCoverage(ctx context.Context, guildID string, report *CoverageReport) error {
	rows, err := s.db.QueryContext(ctx, `
		select guild_id, channel_id, count(*)
		from failure_ledger
		where resolved_at is null and (? = '' or guild_id = ?)
		group by guild_id, channel_id
	`, guildID, guildID)
	if err != nil {
		return fmt.Errorf("query known failure coverage: %w", err)
	}
	defer func() { _ = rows.Close() }()
	guilds := make(map[string]*CoverageGuild, len(report.Guilds))
	channels := map[string]*CoverageChannel{}
	channelGuilds := map[string]*CoverageGuild{}
	for i := range report.Guilds {
		guild := &report.Guilds[i]
		guilds[guild.ID] = guild
		for j := range guild.Channels {
			channel := &guild.Channels[j]
			channels[channel.ID] = channel
			channelGuilds[channel.ID] = guild
		}
	}
	for rows.Next() {
		var failureGuildID, channelID string
		var count int
		if err := rows.Scan(&failureGuildID, &channelID, &count); err != nil {
			return fmt.Errorf("scan known failure coverage: %w", err)
		}
		report.Totals.KnownFailureCount += count
		if channel := channels[channelID]; channel != nil {
			channel.KnownFailureCount += count
			channelGuilds[channelID].KnownFailureCount += count
			continue
		}
		if guild := guilds[failureGuildID]; guild != nil {
			guild.KnownFailureCount += count
			continue
		}
		report.Totals.UnscopedKnownFailureCount += count
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("query known failure coverage: %w", err)
	}
	return nil
}

func (s *Store) loadCoverageState(ctx context.Context, report *CoverageReport) error {
	lastBotSync, err := s.GetSyncState(ctx, "sync:last_success")
	if err != nil {
		return fmt.Errorf("read last bot sync: %w", err)
	}
	report.LastBotSyncAt = parseTime(lastBotSync)
	lastImport, err := s.GetSyncState(ctx, "wiretap:last_import")
	if err != nil {
		return fmt.Errorf("read last wiretap import: %w", err)
	}
	report.Wiretap.LastImportAt = parseTime(lastImport)
	rawStats, err := s.GetSyncState(ctx, wiretapStatsScope)
	if err != nil {
		return fmt.Errorf("read wiretap coverage stats: %w", err)
	}
	if strings.TrimSpace(rawStats) == "" {
		return nil
	}
	var stats WiretapImportStats
	if err := json.Unmarshal([]byte(rawStats), &stats); err != nil {
		return fmt.Errorf("decode wiretap coverage stats: %w", err)
	}
	report.Wiretap.FilesScanned = stats.FilesScanned
	report.Wiretap.Messages = stats.Messages
	report.Wiretap.Channels = stats.Channels
	report.Wiretap.SkippedMessages = stats.SkippedMessages
	report.Wiretap.SkippedChannels = stats.SkippedChannels
	if !stats.FinishedAt.IsZero() {
		report.Wiretap.LastImportAt = stats.FinishedAt.UTC()
	}
	return nil
}

func CoverageDeltaSince(current, previous CoverageReport) CoverageDelta {
	return CoverageDelta{
		Messages:          current.Totals.MessageCount - previous.Totals.MessageCount,
		Channels:          current.Totals.ChannelCount - previous.Totals.ChannelCount,
		NamedChannels:     current.Totals.NamedChannelCount - previous.Totals.NamedChannelCount,
		SyntheticChannels: current.Totals.SyntheticChannelCount - previous.Totals.SyntheticChannelCount,
	}
}

func isSyntheticChannel(id, name string) bool {
	suffix := id
	if len(suffix) > 6 {
		suffix = suffix[len(suffix)-6:]
	}
	return name == "channel-"+suffix || name == "dm-"+suffix
}

func earlierNonZero(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
