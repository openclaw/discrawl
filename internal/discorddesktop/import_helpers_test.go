package discorddesktop

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestFileFingerprintStatusHelpers(t *testing.T) {
	base := fileFingerprint{Size: 123, ModUnixNS: 456}
	require.True(t, sameFileFingerprint(base, fileFingerprint{Size: 123, ModUnixNS: 456, Status: fileStatusSkipped}))
	require.False(t, sameFileFingerprint(base, fileFingerprint{Size: 124, ModUnixNS: 456}))
	require.False(t, sameFileFingerprint(base, fileFingerprint{Size: 123, ModUnixNS: 457}))

	require.True(t, isImportedFingerprint(base))
	require.True(t, isImportedFingerprint(importedFingerprint(base)))
	require.False(t, isImportedFingerprint(skippedFingerprint(base)))
	require.Equal(t, fileStatusImported, importedFingerprint(base).Status)
	require.Equal(t, fileStatusSkipped, skippedFingerprint(base).Status)
	require.Equal(t, wiretapFileIndexScope, fileIndexScope(Options{}))
	require.Equal(t, wiretapFileIndexScope, fileIndexScope(Options{FullCache: true}))
}

func TestSnapshotCopyHelpers(t *testing.T) {
	base := newSnapshot()
	base.routes["111111111111111121"] = "999999999999999996"
	base.userLabels["222222222222222232"] = userLabel{Name: "Alice"}
	base.channels["111111111111111121"] = store.ChannelRecord{ID: "111111111111111121", GuildID: "999999999999999996", Name: "general"}

	snap := newSnapshotWithContext(base)
	require.Equal(t, base.routes, snap.routes)
	require.Equal(t, base.userLabels, snap.userLabels)
	require.Empty(t, snap.channels)

	next := newSnapshot()
	next.routes["111111111111111122"] = "999999999999999996"
	next.userLabels["222222222222222233"] = userLabel{Name: "Bob"}
	next.channels["111111111111111122"] = store.ChannelRecord{ID: "111111111111111122", GuildID: "999999999999999996", Name: "random"}
	mergeSnapshotContext(base, next)

	require.Equal(t, "999999999999999996", base.routes["111111111111111122"])
	require.Equal(t, "Bob", base.userLabels["222222222222222233"].Name)
	require.Equal(t, "random", base.channels["111111111111111122"].Name)

	lookup := copyChannelLookup(base.channels)
	lookup["111111111111111122"] = store.ChannelRecord{ID: "changed"}
	require.Equal(t, "random", base.channels["111111111111111122"].Name)
}

func TestSnapshotWithoutMessageEvents(t *testing.T) {
	snap := newSnapshot()
	snap.messages["333333333333333346"] = store.MessageMutation{
		Record: store.MessageRecord{ID: "333333333333333346"},
		Options: store.WriteOptions{
			AppendEvent:      true,
			EnqueueEmbedding: true,
		},
	}
	stripped := snapshotWithoutMessageEvents(snap)
	require.False(t, stripped.messages["333333333333333346"].Options.AppendEvent)
	require.True(t, stripped.messages["333333333333333346"].Options.EnqueueEmbedding)
	require.True(t, snap.messages["333333333333333346"].Options.AppendEvent)
}

func TestFilterExcludedMessages(t *testing.T) {
	snap := newSnapshot()
	snap.messages["m-news"] = store.MessageMutation{Record: store.MessageRecord{ID: "m-news", ChannelID: "news"}}
	snap.messages["m-logs"] = store.MessageMutation{Record: store.MessageRecord{ID: "m-logs", ChannelID: "logs"}}
	snap.messages["m-general"] = store.MessageMutation{Record: store.MessageRecord{ID: "m-general", ChannelID: "general"}}
	lookup := map[string]store.ChannelRecord{
		"news":    {ID: "news", Kind: "announcement"},
		"logs":    {ID: "logs", Kind: "text"},
		"general": {ID: "general", Kind: "text"},
	}
	totals := newScanTotals()
	stats := &Stats{}

	filterExcludedMessages(snap, lookup, Options{
		ExcludeChannelIDs:   []string{"logs"},
		ExcludeChannelKinds: []string{"announcement"},
	}, totals, stats)

	require.Len(t, snap.messages, 1)
	require.Contains(t, snap.messages, "m-general")
	require.Equal(t, 2, stats.ExcludedMessages)
	require.Equal(t, 2, stats.ExcludedChannels)
}

func TestFilterExcludedMessagesInheritsLegacyAnnouncementExclusions(t *testing.T) {
	snap := newSnapshot()
	snap.messages["m-news"] = store.MessageMutation{Record: store.MessageRecord{ID: "m-news", ChannelID: "news"}}
	snap.messages["m-thread-news"] = store.MessageMutation{Record: store.MessageRecord{ID: "m-thread-news", ChannelID: "thread-news"}}
	snap.messages["m-child"] = store.MessageMutation{Record: store.MessageRecord{ID: "m-child", ChannelID: "child"}}
	lookup := map[string]store.ChannelRecord{
		"news":        {ID: "news", Kind: "news"},
		"thread-news": {ID: "thread-news", Kind: "thread_news"},
		"child":       {ID: "child", Kind: "thread_public", ThreadParentID: "news"},
	}
	totals := newScanTotals()
	stats := &Stats{}

	filterExcludedMessages(snap, lookup, Options{
		ExcludeChannelKinds: []string{"announcement"},
	}, totals, stats)

	require.Empty(t, snap.messages)
	require.Equal(t, 3, stats.ExcludedMessages)
	require.Equal(t, 3, stats.ExcludedChannels)
}

func TestRouteFilteredCacheHelpers(t *testing.T) {
	require.Equal(t, fileSourceCacheData, sourceForPath("/tmp/discord", "/tmp/discord/Cache/Cache_Data/entry", "Cache/Cache_Data/entry"))
	require.Equal(t, fileSourceCacheData, sourceForPath("/tmp/discord", "/tmp/discord/Service Worker/CacheStorage/cache/entry", "Service Worker/CacheStorage/cache/entry"))
	require.Equal(t, fileSourceContext, sourceForPath("/tmp/discord", "/tmp/discord/Local Storage/leveldb/000001.log", "Local Storage/leveldb/000001.log"))
}

func TestCacheFileHasRouteHint(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "route"), []byte("https://discord.com/api/v9/channels/111111111111111121/messages?limit=50"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "plain"), []byte("no discord route here"), 0o600))

	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer func() { _ = root.Close() }()

	ok, err := cacheFileHasRouteHint(root, "route")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cacheFileHasRouteHint(root, "plain")
	require.NoError(t, err)
	require.False(t, ok)
	_, err = cacheFileHasRouteHint(root, "missing")
	require.Error(t, err)
}

func TestImportAndStateEdgeBranches(t *testing.T) {
	ctx := context.Background()
	_, err := Import(ctx, nil, Options{})
	require.ErrorContains(t, err, "store is required")

	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	if runtime.GOOS == "linux" {
		require.Equal(t, filepath.Join(configHome, "discord"), DefaultPath())
	}

	dir := t.TempDir()
	s, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	stats, err := Import(ctx, s, Options{
		Path: dir,
		Now:  func() time.Time { return time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC) },
	})
	require.NoError(t, err)
	require.Equal(t, dir, stats.Path)
	require.Equal(t, 1, stats.Checkpoints)

	stats, err = Import(ctx, nil, Options{Path: filepath.Join(dir, "missing"), DryRun: true})
	require.NoError(t, err)
	require.True(t, stats.DryRun)

	stats, err = Import(ctx, nil, Options{Path: dir, DryRun: true, FullCache: true})
	require.NoError(t, err)
	require.True(t, stats.FullCache)

	require.NoError(t, s.SetSyncState(ctx, fileIndexScope(Options{}), "{not-json"))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:             "c1",
		GuildID:        "g1",
		ParentID:       "parent",
		Kind:           "thread_public",
		Name:           "general",
		ThreadParentID: "parent",
		RawJSON:        `{}`,
	}))
	state, err := loadScanState(ctx, s, Options{})
	require.NoError(t, err)
	require.Empty(t, state.previous)
	require.Equal(t, "general", state.channels["c1"].Name)
	require.Equal(t, "parent", state.channels["c1"].ParentID)
	require.Equal(t, "parent", state.channels["c1"].ThreadParentID)
}

func TestSnapshotFinalizeAndCommitBranches(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	snap := newSnapshot()
	snap.messages["m-missing"] = store.MessageMutation{
		Record: store.MessageRecord{ID: "m-missing", ChannelID: "c-missing", RawJSON: `{}`},
	}
	snap.messages["m-known"] = store.MessageMutation{
		Record: store.MessageRecord{ID: "m-known", GuildID: "g1", ChannelID: "c1", ChannelName: "general", RawJSON: `{}`},
	}
	stats := &Stats{}
	totals := newScanTotals()
	unresolved := finalizeSnapshot(snap, map[string]store.ChannelRecord{
		"c1": {ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`},
	}, totals, stats, true)
	require.Equal(t, unresolvedMessages{"m-missing": "c-missing"}, unresolved)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, stats.SkippedMessages)
	require.Equal(t, "general", snap.channels["c1"].Name)
	require.Equal(t, "g1", snap.guilds["g1"].ID)

	more := unresolvedMessages{"m2": "c2"}
	mergeUnresolved(unresolved, more)
	recordUnresolved(unresolved, totals, stats)
	require.Equal(t, 2, stats.SkippedMessages)

	state := scanState{current: map[string]fileFingerprint{}}
	candidates := []fileCandidate{{relKey: "Cache_Data/entry", fingerprint: fileFingerprint{Size: 10, ModUnixNS: 20}}}
	require.NoError(t, commitSnapshot(ctx, s, Options{DryRun: true}, state, candidates, newSnapshot(), true, stats))
	require.NoError(t, commitSnapshot(ctx, s, Options{}, state, candidates, newSnapshot(), false, stats))
	require.NoError(t, commitSnapshot(ctx, s, Options{}, state, candidates, newSnapshot(), true, stats))
	require.True(t, isImportedFingerprint(state.current["Cache_Data/entry"]))

	require.NoError(t, checkpointScannedCandidates(ctx, s, Options{DryRun: true}, state, candidates, stats))
	require.NoError(t, checkpointScannedCandidates(ctx, s, Options{}, state, candidates, stats))
}

func TestRouteHintCollectionBranches(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "route"), []byte("https://discord.com/channels/123456789012/111111111111111121"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "plain"), []byte("plain"), 0o600))

	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer func() { _ = root.Close() }()

	snap := newSnapshot()
	err = collectCacheRouteHints(context.Background(), root, []fileCandidate{
		{relPath: "missing"},
		{relPath: "plain"},
		{relPath: "route"},
	}, snap)
	require.NoError(t, err)
	require.Equal(t, "123456789012", snap.routes["111111111111111121"])

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, collectCacheRouteHints(canceled, root, []fileCandidate{{relPath: "route"}}, newSnapshot()), context.Canceled)
}
