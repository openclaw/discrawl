package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestDiagnosticsReportsCatalogIncompletenessWithoutChangingSQLiteSafety(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "orphan-message",
		GuildID:           "guild-1",
		ChannelID:         "missing-thread",
		MessageType:       0,
		CreatedAt:         time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		Content:           "catalog witness",
		NormalizedContent: "catalog witness",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "diagnostics", "--json"}, &out, &bytes.Buffer{}))
	var report diagnosticsReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &report))
	require.Equal(t, "warning", report.Status)
	require.True(t, report.SafeForReadOnlyInspection)
	require.False(t, report.SafeForIdentityQueries)
	require.Equal(t, 1, report.Catalog.OrphanedMessageCount)
	require.Equal(t, 1, report.Catalog.OrphanedChannelCount)
	require.NotEmpty(t, report.Catalog.OldestAffectedAt)
	require.NotEmpty(t, report.Catalog.NewestAffectedAt)
}

func TestZeroRowSQLWarnsWhenCatalogIsIncomplete(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "orphan-message",
		GuildID:           "guild-1",
		ChannelID:         "missing-thread",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "catalog witness",
		NormalizedContent: "catalog witness",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "sql", "select id from messages where id = 'absent'"}, &stdout, &stderr))
	require.Contains(t, stdout.String(), "id")
	require.Contains(t, stderr.String(), "identity joins may be incomplete")
}

func TestZeroRowSQLDoesNotWarnForCompleteCatalogAndPreservesJSON(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, cfgPath := writeTestConfig(t, dir)
	s, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID: "known-thread", GuildID: "guild-1", Kind: "thread_public", Name: "known-thread", RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "known-message",
		GuildID:           "guild-1",
		ChannelID:         "known-thread",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "catalog witness",
		NormalizedContent: "catalog witness",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "--json", "sql", "select id from messages where id = 'absent'"}, &stdout, &stderr))
	require.Empty(t, stderr.String())
	var result struct {
		Columns []string   `json:"columns"`
		Rows    [][]string `json:"rows"`
	}
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &result))
	require.Equal(t, []string{"id"}, result.Columns)
	require.Empty(t, result.Rows)
}

func TestZeroRowSQLReportsUndeterminedCatalogWithoutChangingSuccessOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dir := t.TempDir()
	cfg, _ := writeTestConfig(t, dir)
	s, err := store.Open(context.Background(), cfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	var stderr bytes.Buffer
	r := &runtime{ctx: ctx, store: s, stderr: &stderr}
	cancel()

	r.warnOnZeroRowSQL(nil)
	require.Contains(t, stderr.String(), "catalog completeness not determined")
}

func TestHumanOutputShowsChannelIDWhenMetadataIsMissing(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, printHuman(&out, []store.SearchResult{{
		GuildID: "guild-1", ChannelID: "missing-thread", AuthorName: "author", Content: "catalog witness",
	}}))
	require.Contains(t, out.String(), "channel:missing-thread (metadata missing)")
}
