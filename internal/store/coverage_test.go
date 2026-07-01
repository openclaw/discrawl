package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCoverageReportsGuildChannelAndWiretapState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild One", RawJSON: `{}`}))
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g2", Name: "Guild Two", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "channel-c2", RawJSON: `{"source":"discord_desktop"}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "v1", GuildID: "g1", Kind: "voice", Name: "Voice", RawJSON: `{}`}))
	for _, message := range []MessageRecord{
		{ID: "m1", GuildID: "g1", ChannelID: "c1", CreatedAt: "2026-06-01T10:00:00Z", Content: "one", NormalizedContent: "one", RawJSON: `{}`},
		{ID: "m2", GuildID: "g1", ChannelID: "c1", CreatedAt: "2026-06-02T10:00:00Z", Content: "two", NormalizedContent: "two", RawJSON: `{}`},
		{ID: "m3", GuildID: "g1", ChannelID: "c2", CreatedAt: "2026-06-03T10:00:00Z", Content: "three", NormalizedContent: "three", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertMessage(ctx, message))
	}
	require.NoError(t, s.SetSyncState(ctx, "channel:c1:history_complete", "1"))
	require.NoError(t, s.SetSyncState(ctx, "sync:last_success", "2026-06-04T10:00:00Z"))
	require.NoError(t, s.SetSyncState(ctx, "wiretap:last_import", "2026-06-04T11:00:00Z"))
	require.NoError(t, s.SetWiretapImportStats(ctx, WiretapImportStats{
		FilesScanned: 4, Messages: 3, Channels: 2, SkippedMessages: 5, SkippedChannels: 6,
		FinishedAt: time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC),
	}))
	require.NoError(t, s.RecordFailure(ctx, FailureRef{Operation: "sync_messages", Source: "discord", GuildID: "g1", ChannelID: "c1"}, errors.New("known channel failure")))
	require.NoError(t, s.RecordFailure(ctx, FailureRef{Operation: "embed_message", Source: "embeddings", MessageID: "missing"}, errors.New("unscoped failure")))

	generatedAt := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	report, err := s.Coverage(ctx, "", generatedAt)
	require.NoError(t, err)
	require.Equal(t, generatedAt, report.GeneratedAt)
	require.Equal(t, CoverageTotals{
		GuildCount: 2, MessageCount: 3, ChannelCount: 3, MessageChannelCount: 2,
		NamedChannelCount: 2, SyntheticChannelCount: 1, HistoryCompleteChannelCount: 1,
		KnownFailureCount: 2, UnscopedKnownFailureCount: 1,
	}, report.Totals)
	require.Equal(t, time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC), report.LastBotSyncAt)
	require.Equal(t, 5, report.Wiretap.SkippedMessages)
	require.Equal(t, 6, report.Wiretap.SkippedChannels)
	require.Equal(t, time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC), report.Wiretap.LastImportAt)
	require.Len(t, report.Guilds, 2)
	require.Equal(t, "g1", report.Guilds[0].ID)
	require.Equal(t, 3, report.Guilds[0].MessageCount)
	require.Equal(t, time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC), report.Guilds[0].EarliestMessageAt)
	require.Equal(t, time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC), report.Guilds[0].LatestMessageAt)
	require.Equal(t, "c1", report.Guilds[0].Channels[0].ID)
	require.NotNil(t, report.Guilds[0].Channels[0].HistoryComplete)
	require.True(t, *report.Guilds[0].Channels[0].HistoryComplete)
	require.Equal(t, 1, report.Guilds[0].KnownFailureCount)
	require.Equal(t, 1, report.Guilds[0].Channels[0].KnownFailureCount)
	require.True(t, report.Guilds[0].Channels[1].Synthetic)

	filtered, err := s.Coverage(ctx, "g2", generatedAt)
	require.NoError(t, err)
	require.Equal(t, CoverageTotals{GuildCount: 1}, filtered.Totals)
	require.Equal(t, "g2", filtered.Guilds[0].ID)
	_, err = s.Coverage(ctx, "missing", generatedAt)
	require.ErrorContains(t, err, `guild "missing" not found`)
}

func TestCoverageDeltaSince(t *testing.T) {
	previous := CoverageReport{Totals: CoverageTotals{MessageCount: 4, ChannelCount: 3, NamedChannelCount: 2, SyntheticChannelCount: 1}}
	current := CoverageReport{Totals: CoverageTotals{MessageCount: 7, ChannelCount: 4, NamedChannelCount: 4, SyntheticChannelCount: 0}}
	require.Equal(t, CoverageDelta{Messages: 3, Channels: 1, NamedChannels: 2, SyntheticChannels: -1}, CoverageDeltaSince(current, previous))
}

func TestSyntheticChannelClassificationUsesGeneratedPlaceholder(t *testing.T) {
	require.True(t, isSyntheticChannel("123456123456", "channel-123456"))
	require.True(t, isSyntheticChannel("123456123456", "dm-123456"))
	require.False(t, isSyntheticChannel("123456123456", "general"))
	require.False(t, isSyntheticChannel("123456123456", "channel-other"))
}
