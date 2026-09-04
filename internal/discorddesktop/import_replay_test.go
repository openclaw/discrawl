package discorddesktop

import (
	"context"
	"encoding/json"
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
