package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
)

func TestResolveChannelRowsUsesStablePrecedenceAndScope(t *testing.T) {
	rows := []store.ChannelRow{
		{ID: "c1", GuildID: "g1", Name: "help", Kind: "text"},
		{ID: "c2", GuildID: "g2", Name: "Help", Kind: "text"},
		{ID: "c3", GuildID: "g1", Name: "helpers", Kind: "text"},
		{ID: "help", GuildID: "g3", Name: "random", Kind: "text"},
	}

	resolution, err := resolveChannelRows(rows, "help", nil)
	require.NoError(t, err)
	require.Equal(t, "id", resolution.Match)
	require.Equal(t, "help", resolution.Selected.ChannelID)

	resolution, err = resolveChannelRows(rows[:3], "#help", nil)
	require.Error(t, err)
	require.Equal(t, "ambiguous", resolution.Status)
	require.Equal(t, "exact_name", resolution.Match)
	require.Equal(t, []string{"c1", "c2"}, []string{resolution.Candidates[0].ChannelID, resolution.Candidates[1].ChannelID})
	require.ErrorContains(t, err, "guild=g1 channel=c1")
	require.ErrorContains(t, err, "guild=g2 channel=c2")

	resolution, err = resolveChannelRows(rows[:3], "help", []string{"g1"})
	require.NoError(t, err)
	require.Equal(t, "c1", resolution.Selected.ChannelID)

	resolution, err = resolveChannelRows(rows[:3], "pers", nil)
	require.NoError(t, err)
	require.Equal(t, "partial_name", resolution.Match)
	require.Equal(t, "c3", resolution.Selected.ChannelID)

	resolution, err = resolveChannelRows(rows, "missing", nil)
	require.Error(t, err)
	require.Equal(t, "not_found", resolution.Status)
}

func TestChannelResolverDrivesLocalQueriesAndReportsAmbiguity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "discrawl.db")
	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	for _, guild := range []store.GuildRecord{{ID: "g1", Name: "One", RawJSON: `{}`}, {ID: "g2", Name: "Two", RawJSON: `{}`}} {
		require.NoError(t, s.UpsertGuild(ctx, guild))
	}
	for _, channel := range []store.ChannelRecord{
		{ID: "c1", GuildID: "g1", Name: "help", Kind: "text", RawJSON: `{}`},
		{ID: "c2", GuildID: "g2", Name: "help", Kind: "text", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertChannel(ctx, channel))
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, message := range []store.MessageRecord{
		{ID: "m1", GuildID: "g1", ChannelID: "c1", ChannelName: "help", AuthorID: "u1", AuthorName: "One", CreatedAt: now, Content: "needle one", NormalizedContent: "needle one", RawJSON: `{}`},
		{ID: "m2", GuildID: "g2", ChannelID: "c2", ChannelName: "help", AuthorID: "u2", AuthorName: "Two", CreatedAt: now, Content: "needle two", NormalizedContent: "needle two", RawJSON: `{}`},
		{ID: "m3", GuildID: "g1", ChannelID: "700000000000000099", ChannelName: "", AuthorID: "u1", AuthorName: "One", CreatedAt: now, Content: "orphan marker", NormalizedContent: "orphan marker", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertMessage(ctx, message))
	}

	var out bytes.Buffer
	rt := &runtime{ctx: ctx, cfg: cfg, store: s, stdout: &out, stderr: &bytes.Buffer{}, logger: discardLogger()}
	err = rt.runChannelsResolve([]string{"help", "--json"})
	require.Error(t, err)
	require.ErrorContains(t, err, "channel=c1")
	require.ErrorContains(t, err, "channel=c2")
	var resolution channelResolution
	require.NoError(t, json.Unmarshal(out.Bytes(), &resolution))
	require.Equal(t, "ambiguous", resolution.Status)

	out.Reset()
	require.NoError(t, rt.runChannelsResolve([]string{"help", "--json", "--guild", "g1"}))
	require.NoError(t, json.Unmarshal(out.Bytes(), &resolution))
	require.Equal(t, "c1", resolution.Selected.ChannelID)

	err = rt.runSearch([]string{"--channel", "help", "needle"})
	require.ErrorContains(t, err, "channel=c1")
	out.Reset()
	require.NoError(t, rt.runSearch([]string{"--channel", "help", "--guild", "g1", "needle"}))
	var searchRows []store.SearchResult
	require.NoError(t, json.Unmarshal(out.Bytes(), &searchRows))
	require.Len(t, searchRows, 1)
	require.Equal(t, "c1", searchRows[0].ChannelID)

	err = rt.runMessages([]string{"--channel", "help", "--all"})
	require.ErrorContains(t, err, "channel=c2")
	out.Reset()
	require.NoError(t, rt.runMessages([]string{"--channel", "c2", "--all"}))
	var messageRows []store.MessageRow
	require.NoError(t, json.Unmarshal(out.Bytes(), &messageRows))
	require.Len(t, messageRows, 1)
	require.Equal(t, "c2", messageRows[0].ChannelID)

	out.Reset()
	require.NoError(t, rt.runSearch([]string{"--channel", "700000000000000099", "orphan"}))
	require.NoError(t, json.Unmarshal(out.Bytes(), &searchRows))
	require.Len(t, searchRows, 1)
	require.Equal(t, "700000000000000099", searchRows[0].ChannelID)
	out.Reset()
	require.NoError(t, rt.runMessages([]string{"--channel", "700000000000000099", "--all"}))
	require.NoError(t, json.Unmarshal(out.Bytes(), &messageRows))
	require.Len(t, messageRows, 1)
	require.Equal(t, "700000000000000099", messageRows[0].ChannelID)

	err = Run(ctx, []string{"--config", cfgPath, "search", "--channel", "help", "marker"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.ErrorContains(t, err, "channel=c1")
	err = Run(ctx, []string{"--config", cfgPath, "messages", "--channel", "help", "--all"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.ErrorContains(t, err, "channel=c2")
}
