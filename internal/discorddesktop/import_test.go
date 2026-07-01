package discorddesktop

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestDesktopPathAndImportHelpers(t *testing.T) {
	home := t.TempDir()
	switch runtime.GOOS {
	case "windows":
		t.Setenv("USERPROFILE", home)
		require.Equal(t, filepath.Join(home, "AppData", "Roaming", "discord"), DefaultPath())
	case "darwin":
		t.Setenv("HOME", home)
		require.Equal(t, filepath.Join(home, "Library", "Application Support", "discord"), DefaultPath())
	default:
		xdg := filepath.Join(home, "xdg")
		t.Setenv("XDG_CONFIG_HOME", xdg)
		require.Equal(t, filepath.Join(xdg, "discord"), DefaultPath())
	}

	require.Equal(t, "dm", kindForChannelType(1, true))
	require.Equal(t, "group_dm", kindForChannelType(3, true))
	require.Equal(t, "text", kindForChannelType(0, false))
	require.Equal(t, "announcement", kindForChannelType(5, false))
	require.Equal(t, "thread_announcement", kindForChannelType(10, false))
	require.Equal(t, "thread_public", kindForChannelType(11, false))
	require.Equal(t, "thread_private", kindForChannelType(12, false))
	require.Equal(t, "forum", kindForChannelType(15, false))
	require.Equal(t, "desktop", kindForChannelType(99, false))
	embedParts := embedText(map[string]any{"embeds": []any{
		map[string]any{"title": " title ", "description": "body"},
	}})
	require.Equal(t, []string{"title", "body"}, embedParts)
	require.Equal(t, time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC), snowflakeTime("0"))
	require.True(t, snowflakeTime("not-a-snowflake").IsZero())
	require.Equal(t, time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC), parseDiscordTime("2026-04-24T12:00:00Z"))
	require.True(t, parseDiscordTime("bad").IsZero())
	require.Empty(t, formatOptionalTime(time.Time{}))
	require.NotEmpty(t, formatOptionalTime(time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)))

	i, ok := intField(map[string]any{"value": float64(3)}, "value")
	require.True(t, ok)
	require.Equal(t, 3, i)
	i, ok = intField(map[string]any{"value": json.Number("4")}, "value")
	require.True(t, ok)
	require.Equal(t, 4, i)
	_, ok = intField(map[string]any{"value": json.Number("nope")}, "value")
	require.False(t, ok)
	_, ok = intField(map[string]any{}, "value")
	require.False(t, ok)

	require.Equal(t, int64(3), int64Field(map[string]any{"value": float64(3)}, "value"))
	require.Equal(t, int64(4), int64Field(map[string]any{"value": int64(4)}, "value"))
	require.Equal(t, int64(5), int64Field(map[string]any{"value": 5}, "value"))
	require.Equal(t, int64(6), int64Field(map[string]any{"value": json.Number("6")}, "value"))
	require.Zero(t, int64Field(map[string]any{"value": "6"}, "value"))
}

func TestImportExtractsDirectMessageFromDesktopCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Local Storage", "leveldb")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "000001.log"), []byte(`noise
{"id":"111111111111111111","type":1,"recipients":[{"id":"222222222222222222","username":"alice","global_name":"Alice"}]}
binary-ish {"t":"MESSAGE_CREATE","token":"do-not-store","d":{"id":"333333333333333333","channel_id":"111111111111111111","content":"launch checklist in a DM","timestamp":"2026-04-23T18:20:43.123Z","author":{"id":"222222222222222222","username":"alice","global_name":"Alice"},"attachments":[{"id":"444444444444444444","filename":"plan.txt","size":10}],"mentions":[{"id":"555555555555555555","username":"bob"}]}} tail
`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{
		Path: dir,
		Now:  func() time.Time { return time.Date(2026, 4, 23, 18, 30, 0, 0, time.UTC) },
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, stats.Channels)
	coverage, err := st.Coverage(ctx, DirectMessageGuildID, time.Date(2026, 4, 23, 18, 31, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, coverage.Wiretap.FilesScanned)
	require.Equal(t, 1, coverage.Wiretap.Messages)
	require.Zero(t, coverage.Wiretap.SkippedMessages)
	require.Equal(t, stats.FinishedAt, coverage.Wiretap.LastImportAt)
	require.Equal(t, 1, coverage.Totals.MessageChannelCount)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "launch", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, DirectMessageGuildID, results[0].GuildID)
	require.Equal(t, "Alice", results[0].ChannelName)
	require.Equal(t, "Alice", results[0].AuthorName)

	mentions, err := st.ListMentions(ctx, store.MentionListOptions{
		GuildIDs: []string{DirectMessageGuildID},
		Target:   "bob",
		Limit:    10,
	})
	require.NoError(t, err)
	require.Len(t, mentions, 1)
	require.Equal(t, "555555555555555555", mentions[0].TargetID)

	_, rows, err := st.ReadOnlyQuery(ctx, "select raw_json from messages where id = '333333333333333333'")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NotContains(t, rows[0][0], "do-not-store")
}

func TestImportSkipsUnchangedDesktopCacheFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Local Storage", "leveldb")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	channelPath := filepath.Join(cachePath, "000001.log")
	messagePath := filepath.Join(cachePath, "000002.log")
	require.NoError(t, os.WriteFile(channelPath, []byte(`{"id":"111111111111111121","guild_id":"999999999999999996","type":0,"name":"wiretap-fast"}`), 0o600))
	require.NoError(t, os.WriteFile(messagePath, []byte(`{"id":"333333333333333346","channel_id":"111111111111111121","content":"first incremental message","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 2, stats.FilesScanned)
	require.Equal(t, 1, stats.Messages)

	stats, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.FilesScanned)
	require.Equal(t, 2, stats.FilesUnchanged)
	require.Equal(t, 0, stats.Messages)

	require.NoError(t, os.WriteFile(messagePath, []byte(`{"id":"333333333333333347","channel_id":"111111111111111121","content":"second incremental message","timestamp":"2026-04-23T18:20:44Z","author":{"id":"222222222222222233","username":"bob"}}`), 0o600))
	require.NoError(t, os.Chtimes(messagePath, time.Date(2026, 4, 23, 18, 21, 0, 0, time.UTC), time.Date(2026, 4, 23, 18, 21, 0, 0, time.UTC)))

	stats, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 1, stats.FilesUnchanged)
	require.Equal(t, 1, stats.Messages)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "second incremental", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "999999999999999996", results[0].GuildID)
	require.Equal(t, "wiretap-fast", results[0].ChannelName)
}

func TestImportDryRunDoesNotWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001.log"), []byte(`{"id":"333333333333333333","channel_id":"111111111111111111","content":"dry run only","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222222","username":"alice"}}`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir, DryRun: true})
	require.NoError(t, err)
	require.True(t, stats.DryRun)
	require.Equal(t, 0, stats.Messages)
	require.Equal(t, 1, stats.SkippedMessages)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "dry", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
	rawStats, err := st.GetSyncState(ctx, "wiretap:last_stats:v1")
	require.NoError(t, err)
	require.Empty(t, rawStats)
}

func TestImportMissingDesktopPathIsEmpty(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "missing")
	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: path})
	require.NoError(t, err)
	require.Equal(t, path, stats.Path)
	require.Zero(t, stats.FilesScanned)
	require.Zero(t, stats.Messages)
	require.False(t, stats.FinishedAt.IsZero())
}

func TestImportFailureLedgerHelpersRecordAndResolve(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	ref := store.FailureRef{
		Operation: "import_messages", Source: "wiretap", GuildID: "g1", ChannelID: "c1", MessageID: "m1",
	}
	original := errors.New("forced import failure")
	require.ErrorIs(t, recordImportFailure(ctx, st, ref, original), original)
	report, err := st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "m1", report.Failures[0].MessageID)

	require.NoError(t, resolveImportMessageFailures(ctx, st, []string{"m1"}))
	report, err = st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Empty(t, report.Failures)

	require.ErrorIs(t, recordImportFailure(ctx, st, ref, original), original)
	require.NoError(t, resolveImportFailures(ctx, st, ref))

	unscoped := store.FailureRef{Operation: "import_messages", Source: "wiretap"}
	remaining := unscoped
	remaining.MessageID = "m2"
	require.ErrorIs(t, recordImportFailure(ctx, st, unscoped, original), original)
	require.ErrorIs(t, recordImportFailure(ctx, st, remaining, original), original)
	require.NoError(t, resolveImportFailureIdentity(ctx, st, unscoped))
	report, err = st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "m2", report.Failures[0].MessageID)
	require.NoError(t, resolveImportMessageFailures(ctx, st, []string{"m2"}))
}

func TestImportExtractsCompressedUnknownMessageArrayFromChromiumCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))

	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, err := zw.Write([]byte(`[{"id":"333333333333333334","channel_id":"111111111111111112","content":"compressed cache history","timestamp":"2026-04-23T18:20:43.123Z","author":{"id":"222222222222222223","username":"alice"}}]`))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	cacheBlob := append([]byte("https://discord.com/api/v9/channels/111111111111111112/messages?limit=50\x00"), compressed.Bytes()...)
	cacheBlob = append(cacheBlob, []byte("chromium trailing metadata")...)
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), cacheBlob, 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 0, stats.Messages)
	require.Equal(t, 0, stats.DMMessages)
	require.Equal(t, 1, stats.SkippedMessages)
	require.Equal(t, 1, stats.SkippedChannels)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "compressed", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestImportClassifiesCachedAPIMessageArrayFromSelectedDMRoute(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	storagePath := filepath.Join(dir, "Local Storage", "leveldb")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.MkdirAll(storagePath, 0o755))

	require.NoError(t, os.WriteFile(filepath.Join(storagePath, "000001.log"), []byte(`noise
{"_state":{"selectedGuildId":null,"selectedChannelId":"1459084628458471569","selectedChannelIds":{"null":"1459084628458471569"}}}
`), 0o600))

	messages := `[
{"id":"1499513741308461240","channel_id":"1459084628458471569","content":"changed your mind later","timestamp":"2026-04-30T20:52:15.546Z","author":{"id":"1395396685148061737","username":"onur_tc","global_name":"onur"}},
{"id":"1499513741308461241","channel_id":"1459084628458471569","content":"please correct me","timestamp":"2026-04-30T20:52:16.546Z","author":{"id":"1395396685148061737","username":"onur_tc","global_name":"onur"}},
{"id":"1499562787343278080","channel_id":"1459084628458471569","content":"I know you are going through a rough time","timestamp":"2026-05-01T00:08:34.929Z","author":{"id":"999999999999999991","username":"steipete","global_name":"Peter"}}
]`
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, err := zw.Write([]byte(messages))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	cacheBlob := append([]byte("https://discord.com/api/v9/channels/1459084628458471569/messages?limit=14\x00"), compressed.Bytes()...)
	cacheBlob = append(cacheBlob, []byte("chromium trailing metadata")...)
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), cacheBlob, 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 2, stats.FilesScanned)
	require.Equal(t, 3, stats.Messages)
	require.Equal(t, 3, stats.DMMessages)
	require.Equal(t, 1, stats.DMChannels)
	require.Equal(t, 0, stats.SkippedMessages)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "changed your mind", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, DirectMessageGuildID, results[0].GuildID)
	require.Equal(t, "onur", results[0].ChannelName)
	require.Equal(t, "onur", results[0].AuthorName)
}

func TestImportReconcilesMessagesWithLaterGuildChannelMetadata(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Local Storage", "leveldb")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "000001.log"), []byte(`{"id":"333333333333333335","channel_id":"111111111111111113","content":"guild cache message","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222224","username":"alice"},"mentions":[{"id":"555555555555555556","username":"bob"}],"attachments":[{"id":"444444444444444445","filename":"trace.txt"}]}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "000002.log"), []byte(`{"id":"111111111111111113","guild_id":"999999999999999999","type":0,"name":"backend"}`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "guild cache", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "999999999999999999", results[0].GuildID)
	require.Equal(t, "backend", results[0].ChannelName)

	mentions, err := st.ListMentions(ctx, store.MentionListOptions{
		GuildIDs: []string{"999999999999999999"},
		Target:   "bob",
		Limit:    10,
	})
	require.NoError(t, err)
	require.Len(t, mentions, 1)

	_, rows, err := st.ReadOnlyQuery(ctx, "select guild_id from message_attachments where message_id = '333333333333333335'")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "999999999999999999", rows[0][0])
}

func TestImportClassifiesMessagesFromCachedChannelRoutes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "dm_0"), []byte(`https://discord.com/channels/@me/111111111111111114
{"id":"333333333333333336","channel_id":"111111111111111114","content":"route dm message","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222225","username":"alice"}}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "guild_0"), []byte(`https://discord.com/channels/999999999999999998/111111111111111115
{"id":"333333333333333337","channel_id":"111111111111111115","content":"route guild message","timestamp":"2026-04-23T18:20:44Z","author":{"id":"222222222222222226","username":"bob"}}`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Messages)
	require.Equal(t, 1, stats.DMMessages)
	require.Equal(t, 1, stats.GuildMessages)
	require.Equal(t, 0, stats.SkippedMessages)

	dmResults, err := st.SearchMessages(ctx, store.SearchOptions{Query: "route dm", Limit: 10})
	require.NoError(t, err)
	require.Len(t, dmResults, 1)
	require.Equal(t, DirectMessageGuildID, dmResults[0].GuildID)

	guildResults, err := st.SearchMessages(ctx, store.SearchOptions{Query: "route guild", Limit: 10})
	require.NoError(t, err)
	require.Len(t, guildResults, 1)
	require.Equal(t, "999999999999999998", guildResults[0].GuildID)

	guildChannels, err := st.Channels(ctx, "999999999999999998")
	require.NoError(t, err)
	require.Len(t, guildChannels, 1)
	require.Equal(t, "111111111111111115", guildChannels[0].ID)
	require.Equal(t, "channel-111115", guildChannels[0].Name)

	_, guildRows, err := st.ReadOnlyQuery(ctx, "select name from guilds where id = '999999999999999998'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"Discord Desktop Guild 999999999999999998"}}, guildRows)
}

func TestImportClassifiesGzipCacheMessagesFromRendererRoutes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	logPath := filepath.Join(dir, "logs")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.MkdirAll(logPath, 0o755))

	require.NoError(t, os.WriteFile(filepath.Join(logPath, "renderer_js.log"), []byte(`[Routing/Utils] transitionTo - Transitioning to /channels/@me/111111111111111122
[Routing/Utils] transitionTo - Transitioning to /channels/999999999999999995/111111111111111123
`), 0o600))

	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, err := zw.Write([]byte(`[
{"id":"333333333333333348","channel_id":"111111111111111122","content":"current cache dm","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222234","username":"alice","global_name":"Alice"}},
{"id":"333333333333333349","channel_id":"111111111111111123","content":"current cache guild","timestamp":"2026-04-23T18:20:44Z","author":{"id":"222222222222222235","username":"bob","global_name":"Bob"}}
]`))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	cacheBlob := append([]byte("1/0/https://discord.com/api/v9/channels/111111111111111122/messages?limit=13\x00content-encoding\x00gzip\x00"), compressed.Bytes()...)
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), cacheBlob, 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 2, stats.FilesScanned)
	require.Equal(t, 2, stats.Messages)
	require.Equal(t, 1, stats.DMMessages)
	require.Equal(t, 1, stats.GuildMessages)
	require.Equal(t, 0, stats.SkippedMessages)

	dmResults, err := st.SearchMessages(ctx, store.SearchOptions{Query: "current cache dm", Limit: 10})
	require.NoError(t, err)
	require.Len(t, dmResults, 1)
	require.Equal(t, DirectMessageGuildID, dmResults[0].GuildID)
	require.Equal(t, "Alice", dmResults[0].ChannelName)

	guildResults, err := st.SearchMessages(ctx, store.SearchOptions{Query: "current cache guild", Limit: 10})
	require.NoError(t, err)
	require.Len(t, guildResults, 1)
	require.Equal(t, "999999999999999995", guildResults[0].GuildID)
	require.Equal(t, "channel-111123", guildResults[0].ChannelName)
}

func TestImportInfersDirectMessageNamesFromCachedUsers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), []byte(`https://discord.com/channels/@me/111111111111111119
[
{"id":"333333333333333341","channel_id":"111111111111111119","content":"self first","timestamp":"2026-04-23T18:20:43Z","author":{"id":"999999999999999991","username":"steipete","global_name":"Peter"}},
{"id":"333333333333333342","channel_id":"111111111111111119","content":"self second","timestamp":"2026-04-23T18:20:44Z","author":{"id":"999999999999999991","username":"steipete","global_name":"Peter"}},
{"id":"333333333333333343","channel_id":"111111111111111119","content":"counterparty","timestamp":"2026-04-23T18:20:45Z","author":{"id":"222222222222222230","username":"vincentkoc"}}
]
{"user":{"id":"222222222222222230","username":"vincentkoc","global_name":"Vincent K"}}
https://discord.com/channels/@me/111111111111111120
{"id":"333333333333333344","channel_id":"111111111111111120","content":"another dm","timestamp":"2026-04-23T18:20:46Z","author":{"id":"999999999999999991","username":"steipete","global_name":"Peter"}}
{"id":"333333333333333345","channel_id":"111111111111111120","content":"alice reply","timestamp":"2026-04-23T18:20:47Z","author":{"id":"222222222222222231","username":"alice","global_name":"Alice"}}
`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 5, stats.Messages)
	require.Equal(t, 2, stats.DMChannels)

	channels, err := st.Channels(ctx, DirectMessageGuildID)
	require.NoError(t, err)
	namesByID := map[string]string{}
	for _, channel := range channels {
		namesByID[channel.ID] = channel.Name
	}
	require.Equal(t, "Vincent K", namesByID["111111111111111119"])
	require.Equal(t, "Alice", namesByID["111111111111111120"])

	rows, err := st.ListMessages(ctx, store.MessageListOptions{
		GuildIDs: []string{DirectMessageGuildID},
		Channel:  "Vincent",
		Last:     1,
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "Vincent K", rows[0].ChannelName)
	require.Equal(t, "Vincent K", rows[0].AuthorName)
}

func TestImportDropsPreviousUnknownWiretapRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	require.NoError(t, st.UpsertGuild(ctx, store.GuildRecord{ID: "@unknown", Name: "Unknown", RawJSON: `{}`}))
	require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{ID: "111111111111111116", GuildID: "@unknown", Kind: "unknown", Name: "unknown", RawJSON: `{}`}))
	require.NoError(t, st.UpsertMessage(ctx, store.MessageRecord{
		ID:                "333333333333333338",
		GuildID:           "@unknown",
		ChannelID:         "111111111111111116",
		AuthorID:          "222222222222222227",
		AuthorName:        "alice",
		MessageType:       0,
		CreatedAt:         "2026-04-23T18:20:43Z",
		Content:           "stale unknown message",
		NormalizedContent: "stale unknown message",
		RawJSON:           `{}`,
	}))

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Messages)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "stale unknown", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestImportSkipsAmbiguousCachedChannelRoutes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), []byte(`https://discord.com/channels/999999999999999998/111111111111111118
https://discord.com/channels/999999999999999997/111111111111111118
{"id":"333333333333333340","channel_id":"111111111111111118","content":"ambiguous route message","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222229","username":"alice"}}`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Messages)
	require.Equal(t, 1, stats.SkippedMessages)
	require.Equal(t, 1, stats.SkippedChannels)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "ambiguous", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
}
