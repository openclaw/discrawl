package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCatalogIntegrityReportsOrphanedMessageChannels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	created := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "orphan-message",
		GuildID:           "guild-1",
		ChannelID:         "missing-channel",
		MessageType:       0,
		CreatedAt:         created.Format(time.RFC3339Nano),
		Content:           "catalog witness",
		NormalizedContent: "catalog witness",
		RawJSON:           `{}`,
	}))

	integrity, err := s.CatalogIntegrity(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, integrity.OrphanedMessageCount)
	require.Equal(t, 1, integrity.OrphanedChannelCount)
	require.Equal(t, created, integrity.OldestAffectedAt)
	require.Equal(t, created, integrity.NewestAffectedAt)

	state, err := s.HasOrphanedMessageChannels(ctx)
	require.NoError(t, err)
	require.Equal(t, CatalogIncomplete, state)
	results, err := s.SearchMessages(ctx, SearchOptions{Query: "catalog witness", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Empty(t, results[0].ChannelName)
	require.False(t, results[0].ChannelMetadataPresent)
	messages, err := s.ListMessages(ctx, MessageListOptions{IncludeEmpty: true})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.False(t, messages[0].ChannelMetadataPresent)

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{
		ID: "missing-channel", GuildID: "guild-1", Kind: "thread_public", Name: "recovered-thread", RawJSON: `{}`,
	}))
	integrity, err = s.CatalogIntegrity(ctx)
	require.NoError(t, err)
	require.Equal(t, CatalogIntegrity{}, integrity)
	state, err = s.HasOrphanedMessageChannels(ctx)
	require.NoError(t, err)
	require.Equal(t, CatalogConsistent, state)
	results, err = s.SearchMessages(ctx, SearchOptions{Query: "catalog witness", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "recovered-thread", results[0].ChannelName)
	require.True(t, results[0].ChannelMetadataPresent)
	messages, err = s.ListMessages(ctx, MessageListOptions{IncludeEmpty: true})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.True(t, messages[0].ChannelMetadataPresent)
	_, rows, err := s.ReadOnlyQuery(ctx, `
		select m.id
		from messages m
		left join channels c on c.id = m.channel_id
		where m.id = 'orphan-message' and c.name = 'recovered-thread'
	`)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"orphan-message"}}, rows)
	_, rows, err = s.ReadOnlyQuery(ctx, `select message_id from message_fts where message_fts match 'catalog witness'`)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"orphan-message"}}, rows)
}

func TestCatalogIntegrityOrdersVariablePrecisionTimestampsChronologically(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, message := range []MessageRecord{
		{ID: "first", GuildID: "guild-1", ChannelID: "missing-first", MessageType: 0, CreatedAt: "2026-08-02T12:00:00Z", RawJSON: `{}`},
		{ID: "second", GuildID: "guild-1", ChannelID: "missing-second", MessageType: 0, CreatedAt: "2026-08-02T12:00:00.5Z", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertMessage(ctx, message))
	}

	integrity, err := s.CatalogIntegrity(ctx)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC), integrity.OldestAffectedAt)
	require.Equal(t, time.Date(2026, 8, 2, 12, 0, 0, 500_000_000, time.UTC), integrity.NewestAffectedAt)
}

func TestHasOrphanedMessageChannelsReturnsUndeterminedOnCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	cancel()

	state, err := s.HasOrphanedMessageChannels(ctx)
	require.NoError(t, err)
	require.Equal(t, CatalogUndetermined, state)
}

func TestCatalogIntegrityProbeLargeCompleteFixture(t *testing.T) {
	if os.Getenv("DISCRAWL_TEST_LARGE") != "1" {
		t.Skip("set DISCRAWL_TEST_LARGE=1 to run the catalog-integrity calibration fixture")
	}

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "catalog-channel", GuildID: "guild-1", Kind: "text", Name: "catalog", RawJSON: `{}`}))

	started := time.Now()
	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	stmt, err := tx.PrepareContext(ctx, `
		insert into messages(
			id, guild_id, channel_id, message_type, created_at, content,
			normalized_content, raw_json, updated_at
		) values(?, 'guild-1', 'catalog-channel', 0, '2026-08-02T00:00:00Z', '', '', '{}', '2026-08-02T00:00:00Z')
	`)
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()
	for i := range 465820 {
		_, err = stmt.ExecContext(ctx, fmt.Sprintf("%020d", i+1))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	t.Logf("fixture build duration: %s", time.Since(started))

	probeStarted := time.Now()
	state, err := s.HasOrphanedMessageChannels(ctx)
	probeDuration := time.Since(probeStarted)
	require.NoError(t, err)
	require.Equalf(t, CatalogConsistent, state, "healthy-catalog probe duration: %s", probeDuration)
	t.Logf("healthy-catalog probe duration: %s", probeDuration)
}
