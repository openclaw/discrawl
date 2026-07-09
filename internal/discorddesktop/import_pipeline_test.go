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
	require.Equal(t, 0, stats.FilesScanned)
	require.Equal(t, 1, stats.FilesUnchanged)
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
	require.Equal(t, 0, stats.FilesScanned)
	require.Equal(t, 1, stats.FilesUnchanged)
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

func TestImportExcludesCachedThreadRowsThroughParent(t *testing.T) {
	ctx := context.Background()
	guildID := "999999999999999996"
	parentID := "111111111111111120"
	threadID := "111111111111111121"

	tests := []struct {
		name       string
		parentType int
		options    Options
	}{
		{
			name:       "parent id",
			parentType: 0,
			options:    Options{ExcludeChannelIDs: []string{parentID}},
		},
		{
			name:       "parent kind",
			parentType: 5,
			options:    Options{ExcludeChannelKinds: []string{"announcement"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cachePath := filepath.Join(dir, "Cache", "Cache_Data")
			require.NoError(t, os.MkdirAll(cachePath, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), bytesf(`https://discord.com/api/v9/channels/%s/messages?limit=50
{"id":"%s","guild_id":"%s","type":%d,"name":"excluded-parent"}
{"id":"%s","guild_id":"%s","parent_id":"%s","type":11,"name":"cached-thread"}
{"id":"333333333333333346","channel_id":"%s","content":"excluded cached thread payload","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"},"attachments":[{"id":"444444444444444445","filename":"trace.txt"}],"mentions":[{"id":"555555555555555556","username":"bob"}]}
`, threadID, parentID, guildID, tt.parentType, threadID, guildID, parentID, threadID), 0o600))

			st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
			require.NoError(t, err)
			defer func() { _ = st.Close() }()

			opts := tt.options
			opts.Path = dir
			stats, err := Import(ctx, st, opts)
			require.NoError(t, err)
			require.Zero(t, stats.Messages)
			require.Equal(t, 1, stats.ExcludedMessages)
			require.Equal(t, 1, stats.ExcludedChannels)
			for _, table := range []string{"messages", "message_events", "message_attachments", "mention_events"} {
				requireMessageCount(t, ctx, st, table, 0)
			}

			channels, err := st.Channels(ctx, guildID)
			require.NoError(t, err)
			channelsByID := make(map[string]store.ChannelRow, len(channels))
			for _, channel := range channels {
				channelsByID[channel.ID] = channel
			}
			require.Equal(t, parentID, channelsByID[threadID].ParentID)
			require.Equal(t, parentID, channelsByID[threadID].ThreadParentID)
		})
	}
}

func TestImportDoesNotEraseStoredThreadParentFromPoorerCacheChannel(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))

	guildID := "999999999999999996"
	parentID := "111111111111111120"
	threadID := "111111111111111121"
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), bytesf(`https://discord.com/api/v9/channels/%s/messages?limit=50
{"id":"%s","guild_id":"%s","type":11,"name":"poorer-cached-thread"}
{"id":"333333333333333346","channel_id":"%s","content":"stored parent must still exclude","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"},"attachments":[{"id":"444444444444444445","filename":"trace.txt"}],"mentions":[{"id":"555555555555555556","username":"bob"}]}
`, threadID, threadID, guildID, threadID), 0o600))

	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()
	require.NoError(t, st.UpsertGuild(ctx, store.GuildRecord{ID: guildID, Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{
		ID:      parentID,
		GuildID: guildID,
		Kind:    "news",
		Name:    "legacy-announcement",
		RawJSON: `{}`,
	}))
	require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{
		ID:             threadID,
		GuildID:        guildID,
		ParentID:       parentID,
		Kind:           "thread_public",
		Name:           "stored-thread",
		ThreadParentID: parentID,
		RawJSON:        `{}`,
	}))

	stats, err := Import(ctx, st, Options{
		Path:                dir,
		ExcludeChannelKinds: []string{"announcement"},
	})
	require.NoError(t, err)
	require.Zero(t, stats.Messages)
	require.Equal(t, 1, stats.ExcludedMessages)
	require.Equal(t, 1, stats.ExcludedChannels)
	for _, table := range []string{"messages", "message_events", "message_attachments", "mention_events"} {
		requireMessageCount(t, ctx, st, table, 0)
	}

	channels, err := st.Channels(ctx, guildID)
	require.NoError(t, err)
	channelsByID := make(map[string]store.ChannelRow, len(channels))
	for _, channel := range channels {
		channelsByID[channel.ID] = channel
	}
	require.Equal(t, parentID, channelsByID[threadID].ParentID)
	require.Equal(t, parentID, channelsByID[threadID].ThreadParentID)
}

func TestImportDryRunUsesStoredThreadParentForExclusions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))

	guildID := "999999999999999996"
	parentID := "111111111111111120"
	threadID := "111111111111111121"
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), bytesf(`https://discord.com/api/v9/channels/%s/messages?limit=50
{"id":"%s","guild_id":"%s","type":11,"name":"poorer-cached-thread"}
{"id":"333333333333333346","channel_id":"%s","content":"dry run must inherit stored parent","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222232","username":"alice"}}
`, threadID, threadID, guildID, threadID), 0o600))

	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()
	require.NoError(t, st.UpsertGuild(ctx, store.GuildRecord{ID: guildID, Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{
		ID:      parentID,
		GuildID: guildID,
		Kind:    "news",
		Name:    "legacy-announcement",
		RawJSON: `{}`,
	}))
	require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{
		ID:             threadID,
		GuildID:        guildID,
		ParentID:       parentID,
		Kind:           "thread_public",
		Name:           "stored-thread",
		ThreadParentID: parentID,
		RawJSON:        `{}`,
	}))

	stats, err := Import(ctx, st, Options{
		Path:                dir,
		DryRun:              true,
		ExcludeChannelKinds: []string{"announcement"},
	})
	require.NoError(t, err)
	require.True(t, stats.DryRun)
	require.Zero(t, stats.Messages)
	require.Equal(t, 1, stats.ExcludedMessages)
	require.Equal(t, 1, stats.ExcludedChannels)
	requireMessageCount(t, ctx, st, "messages", 0)
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
