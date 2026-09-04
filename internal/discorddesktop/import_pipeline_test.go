package discorddesktop

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestImportFastCacheSkipsUnroutedCacheDataUnlessFullCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), []byte(`
{"id":"111111111111111121","guild_id":"999999999999999996","type":0,"name":"slow-cache"}
{"id":"333333333333333346","channel_id":"111111111111111121","content":"unrouted historical cache","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}
`), 0o600))

	fastStore, err := store.Open(ctx, filepath.Join(dir, "fast.db"))
	require.NoError(t, err)
	defer func() { _ = fastStore.Close() }()

	stats, err := Import(ctx, fastStore, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.FilesScanned)
	require.Equal(t, 1, stats.CacheFilesFastSkipped)
	require.Equal(t, 0, stats.Messages)

	results, err := fastStore.SearchMessages(ctx, store.SearchOptions{Query: "unrouted historical", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)

	stats, err = Import(ctx, fastStore, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.FilesScanned)
	require.Equal(t, 1, stats.CacheFilesFastSkipped)
	require.Equal(t, 0, stats.FilesUnchanged)

	stats, err = Import(ctx, fastStore, Options{Path: dir, FullCache: true})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 1, stats.Messages)

	fullStore, err := store.Open(ctx, filepath.Join(dir, "full.db"))
	require.NoError(t, err)
	defer func() { _ = fullStore.Close() }()

	stats, err = Import(ctx, fullStore, Options{Path: dir, FullCache: true})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 0, stats.CacheFilesFastSkipped)
	require.Equal(t, 1, stats.Messages)

	results, err = fullStore.SearchMessages(ctx, store.SearchOptions{Query: "unrouted historical", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "slow-cache", results[0].ChannelName)
}

func TestImportCheckpointsCacheBatches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	for i := range checkpointEveryFiles + 1 {
		channelID := "111111111111111121"
		messageID := 333333333333333346 + i
		body := bytesf(`https://discord.com/channels/999999999999999996/%s
{"id":"%d","channel_id":"%s","content":"checkpoint cache %d","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}
`, channelID, messageID, channelID, i)
		require.NoError(t, os.WriteFile(filepath.Join(cachePath, fmt.Sprintf("entry_%03d", i)), body, 0o600))
	}

	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, checkpointEveryFiles+1, stats.FilesScanned)
	require.Equal(t, checkpointEveryFiles+1, stats.Messages)
	require.GreaterOrEqual(t, stats.Checkpoints, 2)

	stats, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.FilesScanned)
	require.Equal(t, checkpointEveryFiles+1, stats.FilesUnchanged)
}

func TestImportUsesLaterCacheMetadataBeforeCheckpointingEarlierBatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))

	channelID := "111111111111111121"
	guildID := "999999999999999996"
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_000"), bytesf(`https://discord.com/api/v9/channels/%s/messages?limit=50
{"id":"333333333333333346","channel_id":"%s","content":"needs later channel metadata","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}
`, channelID, channelID), 0o600))
	for i := 1; i < checkpointEveryFiles; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(cachePath, fmt.Sprintf("entry_%03d", i)), bytesf(
			"https://discord.com/api/v9/channels/%s/messages?limit=50\n",
			channelID,
		), 0o600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, fmt.Sprintf("entry_%03d", checkpointEveryFiles)), bytesf(`https://discord.com/api/v9/channels/%s/messages?limit=50
{"id":"%s","guild_id":"%s","type":0,"name":"later-metadata"}
`, channelID, channelID, guildID), 0o600))

	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, checkpointEveryFiles+1+checkpointEveryFiles, stats.FilesScanned)
	require.Equal(t, 1, stats.Messages)
	require.GreaterOrEqual(t, stats.Checkpoints, 2)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "needs later channel metadata", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, guildID, results[0].GuildID)
	require.Equal(t, "later-metadata", results[0].ChannelName)
	requireMessageCount(t, ctx, st, "message_events", 1)

	stats, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.FilesScanned)
	require.Equal(t, checkpointEveryFiles+1, stats.FilesUnchanged)
	requireMessageCount(t, ctx, st, "message_events", 1)
}

func TestImportCheckpointsPartiallyResolvedRetryBatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))

	resolvedChannelID := "111111111111111121"
	unresolvedChannelID := "111111111111111122"
	guildID := "999999999999999996"
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_000"), bytesf(`https://discord.com/api/v10/channels/%s/messages?limit=50
https://discord.com/api/v9/channels/%s/messages?limit=50
{"id":"333333333333333346","channel_id":"%s","content":"partially resolved retry message","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}
{"id":"333333333333333347","channel_id":"%s","content":"still unresolved retry message","timestamp":"2026-04-23T18:20:44Z","author":{"id":"222222222222222232","username":"alice"}}
`, resolvedChannelID, unresolvedChannelID, resolvedChannelID, unresolvedChannelID), 0o600))
	for i := 1; i < checkpointEveryFiles; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(cachePath, fmt.Sprintf("entry_%03d", i)), bytesf(
			"https://discord.com/api/v9/channels/%s/messages?limit=50\n",
			resolvedChannelID,
		), 0o600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, fmt.Sprintf("entry_%03d", checkpointEveryFiles)), bytesf(`https://discord.com/api/v9/channels/%s/messages?limit=50
{"id":"%s","guild_id":"%s","type":0,"name":"partially-resolved"}
`, resolvedChannelID, resolvedChannelID, guildID), 0o600))

	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, checkpointEveryFiles+1+checkpointEveryFiles, stats.FilesScanned)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, stats.SkippedMessages)
	require.GreaterOrEqual(t, stats.Checkpoints, 1)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "partially resolved retry", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "partially-resolved", results[0].ChannelName)
	results, err = st.SearchMessages(ctx, store.SearchOptions{Query: "still unresolved retry", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
	requireMessageCount(t, ctx, st, "message_events", 0)

	require.NoError(t, os.WriteFile(filepath.Join(cachePath, fmt.Sprintf("entry_%03d", checkpointEveryFiles+1)), bytesf(`https://discord.com/api/v9/channels/%s/messages?limit=50
{"id":"%s","guild_id":"%s","type":0,"name":"later-resolved"}
`, unresolvedChannelID, unresolvedChannelID, guildID), 0o600))
	stats, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, checkpointEveryFiles+1+checkpointEveryFiles, stats.FilesScanned)
	require.Equal(t, 2, stats.Messages)

	results, err = st.SearchMessages(ctx, store.SearchOptions{Query: "still unresolved retry", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "later-resolved", results[0].ChannelName)
	requireMessageCount(t, ctx, st, "message_events", 2)

	stats, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.FilesScanned)
	require.Equal(t, checkpointEveryFiles+2, stats.FilesUnchanged)
	requireMessageCount(t, ctx, st, "message_events", 2)
}

func TestImportCheckpointsUnresolvableRouteBearingCacheMisses(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))

	channelID := "111111111111111121"
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_000"), bytesf(`https://discord.com/api/v9/channels/%s/messages?limit=50
{"id":"333333333333333346","channel_id":"%s","content":"permanent unresolved cache miss","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}
`, channelID, channelID), 0o600))

	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 1, stats.SkippedMessages)
	require.Equal(t, 1, stats.Checkpoints)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "permanent unresolved", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)

	stats, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 1, stats.SkippedMessages)
	require.Equal(t, 0, stats.FilesUnchanged)

	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_001"), bytesf(`https://discord.com/channels/999999999999999996/%s
{"id":"%s","guild_id":"999999999999999996","type":0,"name":"later-resolved"}
`, channelID, channelID), 0o600))
	stats, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 2, stats.FilesScanned)
	require.Equal(t, 1, stats.Messages)

	results, err = st.SearchMessages(ctx, store.SearchOptions{Query: "permanent unresolved", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "later-resolved", results[0].ChannelName)
}

func TestImportDoesNotAppendEventsForSkippedMixedBatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))

	guildID := "999999999999999996"
	resolvedChannelID := "111111111111111121"
	unresolvedChannelID := "111111111111111122"
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_000"), bytesf(`https://discord.com/channels/%s/%s
https://discord.com/api/v9/channels/%s/messages?limit=50
{"id":"333333333333333346","channel_id":"%s","content":"mixed resolved message","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}
{"id":"333333333333333347","channel_id":"%s","content":"mixed unresolved message","timestamp":"2026-04-23T18:20:44Z","author":{"id":"222222222222222232","username":"alice"}}
`, guildID, resolvedChannelID, unresolvedChannelID, resolvedChannelID, unresolvedChannelID), 0o600))

	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 1, stats.Checkpoints)
	requireMessageCount(t, ctx, st, "message_events", 0)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "mixed resolved", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	results, err = st.SearchMessages(ctx, store.SearchOptions{Query: "mixed unresolved", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)

	stats, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 0, stats.FilesUnchanged)
	requireMessageCount(t, ctx, st, "message_events", 0)
}

func TestImportDoesNotDuplicateEventsWhenSwitchingFullCacheModes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))

	channelID := "111111111111111121"
	guildID := "999999999999999996"
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_000"), bytesf(`https://discord.com/channels/%s/%s
{"id":"%s","guild_id":"%s","type":0,"name":"mode-switch"}
{"id":"333333333333333346","channel_id":"%s","content":"mode switch event once","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}
`, guildID, channelID, channelID, guildID, channelID), 0o600))

	t.Run("full then default", func(t *testing.T) {
		st, err := store.Open(ctx, filepath.Join(dir, "full-first.db"))
		require.NoError(t, err)
		defer func() { _ = st.Close() }()

		stats, err := Import(ctx, st, Options{Path: dir, FullCache: true})
		require.NoError(t, err)
		require.Equal(t, 1, stats.FilesScanned)
		require.Equal(t, 1, stats.Messages)
		requireMessageCount(t, ctx, st, "message_events", 1)

		stats, err = Import(ctx, st, Options{Path: dir})
		require.NoError(t, err)
		require.Equal(t, 0, stats.FilesScanned)
		require.Equal(t, 1, stats.FilesUnchanged)
		requireMessageCount(t, ctx, st, "message_events", 1)
	})

	t.Run("default then full", func(t *testing.T) {
		st, err := store.Open(ctx, filepath.Join(dir, "default-first.db"))
		require.NoError(t, err)
		defer func() { _ = st.Close() }()

		stats, err := Import(ctx, st, Options{Path: dir})
		require.NoError(t, err)
		require.Equal(t, 1, stats.FilesScanned)
		require.Equal(t, 1, stats.Messages)
		requireMessageCount(t, ctx, st, "message_events", 1)

		stats, err = Import(ctx, st, Options{Path: dir, FullCache: true})
		require.NoError(t, err)
		require.Equal(t, 0, stats.FilesScanned)
		require.Equal(t, 1, stats.FilesUnchanged)
		requireMessageCount(t, ctx, st, "message_events", 1)
	})
}

func TestImportFastCachePreservesKnownChannelMetadataAcrossBatches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	leveldbPath := filepath.Join(dir, "Local Storage", "leveldb")
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(leveldbPath, 0o755))
	require.NoError(t, os.MkdirAll(cachePath, 0o755))

	channelID := "111111111111111121"
	guildID := "999999999999999996"
	require.NoError(t, os.WriteFile(filepath.Join(leveldbPath, "000001.log"), bytesf(
		`{"id":"%s","guild_id":"%s","type":11,"name":"known-thread","thread_metadata":{"archived":false}}`,
		channelID,
		guildID,
	), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), bytesf(`https://discord.com/channels/%s/%s
{"id":"333333333333333346","channel_id":"%s","content":"thread metadata cache","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}
`, guildID, channelID, channelID), 0o600))

	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)

	channels, err := st.Channels(ctx, guildID)
	require.NoError(t, err)
	require.Len(t, channels, 1)
	require.Equal(t, "known-thread", channels[0].Name)
	require.Equal(t, "thread_public", channels[0].Kind)

	_, rows, err := st.ReadOnlyQuery(ctx, "select raw_json from channels where id = '111111111111111121'")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Contains(t, rows[0][0], `"type":11`)
}

func TestImportFastCacheRouteFiltersServiceWorkerCacheStorage(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Service Worker", "CacheStorage", "cache-id")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "unrouted"), []byte(`
{"id":"111111111111111121","guild_id":"999999999999999996","type":0,"name":"service-worker-cache"}
{"id":"333333333333333346","channel_id":"111111111111111121","content":"service worker historical cache","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}
`), 0o600))

	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.FilesScanned)
	require.Equal(t, 1, stats.CacheFilesFastSkipped)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "service worker historical", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
}

func requireMessageCount(t *testing.T, ctx context.Context, st *store.Store, table string, expected int) {
	t.Helper()
	_, rows, err := st.ReadOnlyQuery(ctx, "select count(*) from "+table)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	require.Equal(t, strconv.Itoa(expected), rows[0][0])
}

func bytesf(format string, args ...any) []byte {
	return fmt.Appendf(nil, format, args...)
}

// Upgrades must re-evaluate ambiguous checkpoints without replaying history.
func TestImportMigratesLegacyFileIndexWithoutReplayingEvents(t *testing.T) {
	for _, legacy := range []string{wiretapFileIndexScopeV1, wiretapFileIndexScopeV2} {
		t.Run(legacy, func(t *testing.T) {
			ctx, st, dir := replayStore(t)
			known := "https://discord.com/channels/" + replayGuild + "/" + replayChannel + "\n"
			replayCacheEntry(t, dir, "Cache/Cache_Data/entry_0", known+replayPayload(t, replayChannel, "already imported before upgrade", ""))
			opts := Options{Path: dir, FullCache: true}
			_, err := Import(ctx, st, opts)
			require.NoError(t, err)
			requireMessageCount(t, ctx, st, "message_events", 1)
			index, err := st.GetSyncState(ctx, wiretapFileIndexScope)
			require.NoError(t, err)
			require.NoError(t, st.SetSyncState(ctx, legacy, index))
			require.NoError(t, st.DeleteSyncState(ctx, wiretapFileIndexScope))

			// A legacy index must not trigger first-import pruning again.
			require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{ID: replayUnknownChannel, GuildID: "@unknown", Kind: "text", Name: "legacy", RawJSON: `{}`}))
			require.NoError(t, st.UpsertMessage(ctx, store.MessageRecord{ID: "333333333333333349", GuildID: "@unknown", ChannelID: replayUnknownChannel, CreatedAt: "2026-04-23T00:00:00Z", Content: "legacy retained", NormalizedContent: "legacy retained", RawJSON: `{}`}))
			stats, err := Import(ctx, st, opts)
			require.NoError(t, err)
			require.Equal(t, 1, stats.FilesScanned)
			requireMessageCount(t, ctx, st, "message_events", 1)
			requireMessageCount(t, ctx, st, "messages", 2)
			stats, err = Import(ctx, st, opts)
			require.NoError(t, err)
			require.Zero(t, stats.FilesScanned)
			requireMessageCount(t, ctx, st, "message_events", 1)
		})
	}
}
