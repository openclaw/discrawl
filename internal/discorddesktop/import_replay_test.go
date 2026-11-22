package discorddesktop

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

const (
	replayChannel        = "111111111111111121"
	replayUnknownChannel = "111111111111111122"
	replayGuild          = "999999999999999996"
	replayMessage        = "333333333333333346"
)

func replayStore(t *testing.T) (context.Context, *store.Store, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return ctx, st, dir
}

func replayCacheEntry(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func replayPayload(t *testing.T, channelID, content, edited string) string {
	t.Helper()
	raw := map[string]any{
		"id": replayMessage, "channel_id": channelID, "content": content,
		"timestamp": "2026-04-23T18:20:43Z", "edited_timestamp": edited,
		"author": map[string]string{"id": "222222222222222232", "username": "alice"},
	}
	data, err := json.Marshal(raw)
	require.NoError(t, err)
	return "https://discord.com/api/v9/channels/" + channelID + "/messages?limit=50\n" + string(data)
}

func TestImportRecoversAmbiguousLegacyCheckpoints(t *testing.T) {
	for _, legacy := range []string{"wiretap:file_index:v1", "wiretap:file_index:v2"} {
		t.Run(legacy, func(t *testing.T) {
			ctx, st, dir := replayStore(t)
			rel := "Cache/Cache_Data/entry_0"
			path := replayCacheEntry(t, dir, rel, replayPayload(t, replayChannel, "legacy recovery", ""))
			info, err := os.Stat(path)
			require.NoError(t, err)
			index, err := json.Marshal(map[string]fileFingerprint{rel: {
				Size: info.Size(), ModUnixNS: info.ModTime().UnixNano(), Status: fileStatusImported,
			}})
			require.NoError(t, err)
			require.NoError(t, st.SetSyncState(ctx, legacy, string(index)))
			require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{ID: replayChannel, GuildID: replayGuild, Kind: "text", Name: "resolved", RawJSON: `{}`}))

			stats, err := Import(ctx, st, Options{Path: dir})
			require.NoError(t, err)
			require.Equal(t, 1, stats.Messages)
			requireMessageCount(t, ctx, st, "messages", 1)
			requireMessageCount(t, ctx, st, "message_events", 1)
			_, err = Import(ctx, st, Options{Path: dir})
			require.NoError(t, err)
			requireMessageCount(t, ctx, st, "message_events", 1)
		})
	}
}

func TestImportRetriesUnresolvedAcrossInputModes(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		full bool
	}{
		{"context", "payload.json", false},
		{"full-cache", "Cache/Cache_Data/entry_0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, st, dir := replayStore(t)
			replayCacheEntry(t, dir, tc.path, replayPayload(t, replayChannel, "later classified", ""))
			opts := Options{Path: dir, FullCache: tc.full}
			stats, err := Import(ctx, st, opts)
			require.NoError(t, err)
			require.Equal(t, 1, stats.SkippedMessages)
			require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{ID: replayChannel, GuildID: replayGuild, Kind: "text", Name: "resolved", RawJSON: `{}`}))
			stats, err = Import(ctx, st, opts)
			require.NoError(t, err)
			require.Equal(t, 1, stats.Messages)
			requireMessageCount(t, ctx, st, "messages", 1)
			requireMessageCount(t, ctx, st, "message_events", 1)
			stats, err = Import(ctx, st, opts)
			require.NoError(t, err)
			require.Zero(t, stats.FilesScanned)
			requireMessageCount(t, ctx, st, "message_events", 1)
		})
	}
}

func TestImportMixedRetryPreservesNewerMessageAndChildren(t *testing.T) {
	ctx, st, dir := replayStore(t)
	known := "https://discord.com/channels/" + replayGuild + "/" + replayChannel + "\n"
	unresolved := `{"id":"333333333333333347","channel_id":"` + replayUnknownChannel + `","content":"unresolved","timestamp":"2026-04-23T18:20:44Z","author":{"id":"222222222222222232","username":"alice"}}`
	path := replayCacheEntry(t, dir, "Cache/Cache_Data/entry_0", known+replayPayload(t, replayChannel, "old cache content", "")+"\n"+unresolved)
	_, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)

	newer := store.MessageMutation{
		Record:      store.MessageRecord{ID: replayMessage, GuildID: replayGuild, ChannelID: replayChannel, ChannelName: "resolved", AuthorID: "222222222222222232", AuthorName: "alice", CreatedAt: "2026-04-23T18:20:43Z", EditedAt: "2026-04-24T00:00:00Z", Content: "new authoritative content", NormalizedContent: "new authoritative content", HasAttachments: true, RawJSON: `{}`},
		Attachments: []store.AttachmentRecord{{AttachmentID: "444444444444444441", MessageID: replayMessage, GuildID: replayGuild, ChannelID: replayChannel, Filename: "new.txt", URL: "https://example.com/new.txt"}},
		Mentions:    []store.MentionEventRecord{{MessageID: replayMessage, GuildID: replayGuild, ChannelID: replayChannel, TargetType: "user", TargetID: "222222222222222233", EventAt: "2026-04-24T00:00:00Z"}},
	}
	require.NoError(t, st.UpsertMessages(ctx, []store.MessageMutation{newer}))
	_, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	var content string
	require.NoError(t, st.DB().QueryRowContext(ctx, "SELECT content FROM messages WHERE id = ?", replayMessage).Scan(&content))
	require.Equal(t, newer.Record.Content, content)
	requireMessageCount(t, ctx, st, "message_attachments", 1)
	requireMessageCount(t, ctx, st, "mention_events", 1)
	requireMessageCount(t, ctx, st, "message_events", 0)
	hits, err := st.SearchMessages(ctx, store.SearchOptions{Query: "authoritative", Limit: 10})
	require.NoError(t, err)
	require.Len(t, hits, 1)

	// A genuinely newer cache edit remains eligible.
	require.NoError(t, os.WriteFile(path, []byte(known+replayPayload(t, replayChannel, "newest cached edit", "2026-04-25T00:00:00Z")+"\n"+unresolved), 0o600))
	_, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.NoError(t, st.DB().QueryRowContext(ctx, "SELECT content FROM messages WHERE id = ?", replayMessage).Scan(&content))
	require.Equal(t, "newest cached edit", content)
	require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{ID: replayUnknownChannel, GuildID: replayGuild, Kind: "text", Name: "later", RawJSON: `{}`}))
	_, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	requireMessageCount(t, ctx, st, "message_events", 2)
	_, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	requireMessageCount(t, ctx, st, "message_events", 2)
}

func TestImportRetryPreservesDeletedMessage(t *testing.T) {
	ctx, st, dir := replayStore(t)
	known := "https://discord.com/channels/" + replayGuild + "/" + replayChannel + "\n"
	unresolved := `{"id":"333333333333333347","channel_id":"` + replayUnknownChannel + `","content":"unresolved","timestamp":"2026-04-23T18:20:44Z","author":{"id":"222222222222222232","username":"alice"}}`
	replayCacheEntry(t, dir, "Cache/Cache_Data/entry_0", known+replayPayload(t, replayChannel, "deleted cached message", "")+"\n"+unresolved)
	_, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.NoError(t, st.MarkMessageDeletedWithoutEvent(ctx, replayGuild, replayChannel, replayMessage))
	_, err = Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	var deleted string
	require.NoError(t, st.DB().QueryRowContext(ctx, "SELECT deleted_at FROM messages WHERE id = ?", replayMessage).Scan(&deleted))
	require.NotEmpty(t, deleted)
	requireMessageCount(t, ctx, st, "message_fts", 0)
	requireMessageCount(t, ctx, st, "message_events", 0)
}

func TestFullCacheRetriesOnlyUnresolvedSourceFiles(t *testing.T) {
	for _, copies := range []int{1, 2} {
		t.Run(fmt.Sprintf("unresolved-copies=%d", copies), func(t *testing.T) {
			ctx, st, dir := replayStore(t)
			known := "https://discord.com/channels/" + replayGuild + "/" + replayChannel + "\n"
			replayCacheEntry(t, dir, "Cache/Cache_Data/a-known", known+replayPayload(t, replayChannel, "completed file", ""))
			unresolved := `{"id":"333333333333333347","channel_id":"` + replayUnknownChannel + `","content":"unresolved","timestamp":"2026-04-23T18:20:44Z","author":{"id":"222222222222222232","username":"alice"}}`
			for i := range copies {
				replayCacheEntry(t, dir, fmt.Sprintf("Cache/Cache_Data/b-unknown-%d", i), unresolved)
			}
			opts := Options{Path: dir, FullCache: true}
			stats, err := Import(ctx, st, opts)
			require.NoError(t, err)
			require.Equal(t, 1, stats.Messages)
			require.Equal(t, 1, stats.SkippedMessages)
			stats, err = Import(ctx, st, opts)
			require.NoError(t, err)
			require.Equal(t, copies, stats.FilesScanned)
			require.Equal(t, 1, stats.FilesUnchanged)
			require.Zero(t, stats.Messages)
			requireMessageCount(t, ctx, st, "messages", 1)
			requireMessageCount(t, ctx, st, "message_events", 1)

			// Metadata discovered after the payloads resolves every source copy.
			replayCacheEntry(t, dir, "z-metadata.json", `{"id":"`+replayUnknownChannel+`","guild_id":"`+replayGuild+`","type":0,"name":"later"}`)
			stats, err = Import(ctx, st, opts)
			require.NoError(t, err)
			require.Equal(t, 1, stats.Messages)
			require.Zero(t, stats.SkippedMessages)
			requireMessageCount(t, ctx, st, "messages", 2)
			requireMessageCount(t, ctx, st, "message_events", 2)
			stats, err = Import(ctx, st, opts)
			require.NoError(t, err)
			require.Zero(t, stats.FilesScanned)
			requireMessageCount(t, ctx, st, "message_events", 2)
		})
	}
}

// A later cache batch resolves one channel, forcing an intra-pass retry while
// a second message is still unresolved. Frozen retries, including after a
// reopen, must not add receipts or move the feed cursor.
func TestFrozenPartialReplayDoesNotAdvanceFeed(t *testing.T) {
	ctx, st, dir := replayStore(t)
	const mixedPath = "Cache/Cache_Data/entry_000"
	const missingMessage = "333333333333333347"
	unresolved := fmt.Sprintf(`{"id":"%s","channel_id":"%s","content":"needs more context","timestamp":"2026-04-23T18:20:44Z","author":{"id":"222222222222222232","username":"alice"}}`, missingMessage, replayUnknownChannel)
	replayCacheEntry(t, dir, mixedPath, replayPayload(t, replayChannel, "first version", "")+"\n"+unresolved)
	for i := 1; i < checkpointEveryFiles; i++ {
		replayCacheEntry(t, dir, fmt.Sprintf("Cache/Cache_Data/entry_%03d", i), "https://discord.com/api/v9/channels/"+replayChannel+"/messages?limit=50\n")
	}
	metadata := func(id string) string {
		return fmt.Sprintf("https://discord.com/api/v9/channels/%s/messages?limit=50\n"+`{"id":"%s","guild_id":"%s","type":0,"name":"known"}`, id, id, replayGuild)
	}
	replayCacheEntry(t, dir, fmt.Sprintf("Cache/Cache_Data/entry_%03d", checkpointEveryFiles), metadata(replayChannel))
	marks := func(st *store.Store) [3]int64 {
		t.Helper()
		var m [3]int64
		require.NoError(t, st.DB().QueryRowContext(ctx, `select
			coalesce((select seq from sqlite_sequence where name='entity_observations'),0),
			coalesce((select seq from sqlite_sequence where name='entity_check_history'),0),
			coalesce((select seq from sqlite_sequence where name='change_log'),0)`).Scan(&m[0], &m[1], &m[2]))
		return m
	}
	opts := Options{Path: dir}
	stats, err := Import(ctx, st, opts)
	require.NoError(t, err)
	require.Equal(t, checkpointEveryFiles+2, stats.FilesScanned)
	require.Positive(t, stats.Checkpoints)
	require.Equal(t, 1, stats.SkippedMessages)
	afterFirst := marks(st)
	stats, err = Import(ctx, st, opts)
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned, "only the unresolved source remains eligible")
	require.Zero(t, stats.Checkpoints)
	require.Equal(t, afterFirst, marks(st))
	require.NoError(t, st.Close())
	st, err = store.Open(ctx, filepath.Join(dir, "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	stats, err = Import(ctx, st, opts)
	require.NoError(t, err)
	require.Equal(t, 1, stats.SkippedMessages)
	require.Equal(t, afterFirst, marks(st))

	replayCacheEntry(t, dir, "Cache/Cache_Data/zz-resolved", metadata(replayUnknownChannel))
	stats, err = Import(ctx, st, opts)
	require.NoError(t, err)
	require.Zero(t, stats.SkippedMessages)
	requireMessageCount(t, ctx, st, "messages", 2)
	requireMessageCount(t, ctx, st, "message_events", 2)
}

func TestImportRecoversUnpublishedCheckpointCoverage(t *testing.T) {
	for _, scope := range []string{"wiretap:last_import", "wiretap:last_stats:v1"} {
		for _, full := range []bool{false, true} {
			t.Run(fmt.Sprintf("scope=%s/full=%t", scope, full), func(t *testing.T) {
				ctx, st, dir := replayStore(t)
				known := "https://discord.com/channels/" + replayGuild + "/" + replayChannel + "\n"
				replayCacheEntry(t, dir, "Cache/Cache_Data/entry_0", known+replayPayload(t, replayChannel, "checkpoint coverage recovery", ""))
				trigger := fmt.Sprintf(`create trigger deny_receipt before insert on sync_state when new.scope='%s' begin select raise(abort, 'receipt refused'); end`, scope)
				_, err := st.DB().ExecContext(ctx, trigger)
				require.NoError(t, err)
				opts := Options{Path: dir, FullCache: full}
				_, err = Import(ctx, st, opts)
				require.ErrorContains(t, err, "receipt refused")
				requireMessageCount(t, ctx, st, "messages", 1)
				for _, published := range []string{"wiretap:last_import", "wiretap:last_stats:v1"} {
					val, err := st.GetSyncState(ctx, published)
					require.NoError(t, err)
					require.Empty(t, val, "a failed publish may not leave half a coverage receipt")
				}
				_, err = st.DB().ExecContext(ctx, `drop trigger deny_receipt`)
				require.NoError(t, err)
				require.NoError(t, st.Close())
				st, err = store.Open(ctx, filepath.Join(dir, "archive.db"))
				require.NoError(t, err)
				t.Cleanup(func() { _ = st.Close() })
				stats, err := Import(ctx, st, opts)
				require.NoError(t, err)
				require.Zero(t, stats.FilesScanned, "the checkpoint itself already committed")
				for _, published := range []string{"wiretap:last_import", "wiretap:last_stats:v1"} {
					val, err := st.GetSyncState(ctx, published)
					require.NoError(t, err)
					require.NotEmpty(t, val)
				}
				var seq int64
				require.NoError(t, st.DB().QueryRowContext(ctx, `select max(seq) from change_log`).Scan(&seq))
				_, err = Import(ctx, st, opts)
				require.NoError(t, err)
				var after int64
				require.NoError(t, st.DB().QueryRowContext(ctx, `select max(seq) from change_log`).Scan(&after))
				require.Equal(t, seq, after, "an idle replay after recovery cannot move the feed")
			})
		}
	}
}
