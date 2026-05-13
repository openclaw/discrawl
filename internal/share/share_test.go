package share

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/mirror"
	"github.com/openclaw/crawlkit/snapshot"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestExportImportRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Tables)
	require.FileExists(t, filepath.Join(repo, ManifestName))
	require.NotEmpty(t, tableEntry(t, manifest, "messages").Files)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	var progress []ImportProgress
	imported, changed, err := ImportIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) { progress = append(progress, p) },
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, manifest.GeneratedAt, imported.GeneratedAt)
	require.Contains(t, progressPhases(progress), "start")
	require.Contains(t, progressPhases(progress), "table_start")
	require.Contains(t, progressPhases(progress), "file_done")
	require.Contains(t, progressPhases(progress), "rebuild_fts")
	require.Contains(t, progressPhases(progress), "done")

	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "launch", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m1", results[0].MessageID)

	mentions, err := dst.ListMentions(ctx, store.MentionListOptions{Target: "Ops", Limit: 10})
	require.NoError(t, err)
	require.Len(t, mentions, 1)

	lastImport, err := dst.GetSyncState(ctx, LastImportSyncScope)
	require.NoError(t, err)
	require.NotEmpty(t, lastImport)
	lastManifest, err := dst.GetSyncState(ctx, LastImportManifestSyncScope)
	require.NoError(t, err)
	require.Equal(t, manifest.GeneratedAt.Format(time.RFC3339Nano), lastManifest)
	require.False(t, NeedsImport(ctx, dst, 15*time.Minute))

	imported, changed, err = ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, manifest.GeneratedAt, imported.GeneratedAt)
}

func TestImportIfChangedUsesIncrementalTailImport(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotEmpty(t, tableEntry(t, manifest, "messages").FileManifests)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, changed, err := ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "delta landed fast",
			NormalizedContent: "delta landed fast",
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
	}}))
	updated, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotEqual(t, manifest.GeneratedAt, updated.GeneratedAt)

	var progress []ImportProgress
	imported, changed, err := ImportIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) { progress = append(progress, p) },
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, updated.GeneratedAt, imported.GeneratedAt)
	require.Contains(t, progressPhases(progress), "table_start")
	require.NotContains(t, progressPhases(progress), "rebuild_fts")

	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "delta landed", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m2", results[0].MessageID)
	state, err := dst.GetSyncState(ctx, LastImportManifestJSONScope)
	require.NoError(t, err)
	require.Contains(t, state, `"file_manifests"`)
}

func TestImportIfChangedUsesMixedIncrementalPlanForMetadataChanges(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, changed, err := ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, src.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "launch", RawJSON: `{}`}))
	require.NoError(t, src.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Launch Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"bio":"delta member"}`,
	}))
	require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "launch",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "mixed delta landed",
			NormalizedContent: "mixed delta landed",
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"m2"}`,
		Options:     store.WriteOptions{AppendEvent: true},
		Attachments: []store.AttachmentRecord{{
			AttachmentID: "a2",
			MessageID:    "m2",
			GuildID:      "g1",
			ChannelID:    "c1",
			AuthorID:     "u1",
			Filename:     "delta.txt",
			TextContent:  "attached delta",
		}},
		Mentions: []store.MentionEventRecord{{
			MessageID:  "m2",
			GuildID:    "g1",
			ChannelID:  "c1",
			AuthorID:   "u1",
			TargetType: "role",
			TargetID:   "r2",
			TargetName: "Launch",
			EventAt:    now,
		}},
	}}))
	updated, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotEqual(t, manifest.GeneratedAt, updated.GeneratedAt)

	previous, ok := PreviousImportedManifest(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.True(t, ok)
	planned, supported := shareIncrementalPlan(snapshot.PlanIncrementalImport(snapshotManifest(previous), snapshotManifest(updated)))
	require.True(t, supported, "%+v", planned)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "channels").Mode)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "members").Mode)
	require.Equal(t, snapshot.TableImportFiles, importPlanTable(t, planned, "messages").Mode)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "message_events").Mode)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "message_attachments").Mode)
	require.Equal(t, snapshot.TableImportReplace, importPlanTable(t, planned, "mention_events").Mode)

	var progress []ImportProgress
	imported, changed, err := ImportIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) { progress = append(progress, p) },
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, updated.GeneratedAt, imported.GeneratedAt)
	require.Contains(t, progressPhases(progress), "rebuild_fts")
	require.Contains(t, progressPhases(progress), "rebuild_member_fts")
	require.Equal(t, importPlanRowCount(planned), progressTotalRows(t, progress, "start"))
	require.Positive(t, progressTotalRows(t, progress, "start"))

	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "checklist", Channel: "launch", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m1", results[0].MessageID)

	results, err = dst.SearchMessages(ctx, store.SearchOptions{Query: "mixed delta", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m2", results[0].MessageID)
	_, rows, err := dst.ReadOnlyQuery(ctx, "select name from channels where id = 'c1'")
	require.NoError(t, err)
	require.Equal(t, "launch", rows[0][0])
	_, rows, err = dst.ReadOnlyQuery(ctx, "select count(*) from mention_events")
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])
	_, rows, err = dst.ReadOnlyQuery(ctx, "select count(*) from message_events")
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])
	_, rows, err = dst.ReadOnlyQuery(ctx, "select count(*) from member_fts where member_fts match 'delta'")
	require.NoError(t, err)
	require.Equal(t, "1", rows[0][0])
}

func TestImportIfChangedInfersLegacyManifestFilesFromGit(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	require.NoError(t, exec.CommandContext(ctx, "git", "init", repo).Run())
	configureGitUser(t, repo)
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	writeShareManifest(t, repo, stripFileManifests(manifest))
	require.NoError(t, exec.CommandContext(ctx, "git", "-C", repo, "add", ".").Run())
	require.NoError(t, exec.CommandContext(ctx, "git", "-C", repo, "commit", "-m", "initial snapshot").Run())

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, changed, err := ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "legacy git delta",
			NormalizedContent: "legacy git delta",
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
	}}))
	updated, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	writeShareManifest(t, repo, stripFileManifests(updated))
	require.NoError(t, exec.CommandContext(ctx, "git", "-C", repo, "add", ".").Run())
	require.NoError(t, exec.CommandContext(ctx, "git", "-C", repo, "commit", "-m", "tail snapshot").Run())

	previous, ok := PreviousImportedManifest(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.True(t, ok)
	planned, supported := shareIncrementalPlan(snapshot.PlanIncrementalImport(snapshotManifest(previous), snapshotManifest(enrichManifestFromGit(ctx, repo, "HEAD", stripFileManifests(updated)))))
	require.True(t, supported, "%+v", planned)
	require.True(t, planned.Changed(), "%+v", planned)

	var progress []ImportProgress
	_, changed, err = ImportIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) { progress = append(progress, p) },
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.NotContains(t, progressPhases(progress), "rebuild_fts")
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "legacy git delta", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestApplyImportPragmasBoundImportMemory(t *testing.T) {
	ctx := context.Background()
	s := seedStore(t, filepath.Join(t.TempDir(), "dst.db"))
	defer func() { _ = s.Close() }()

	restore, err := applyImportPragmas(ctx, s.DB())
	require.NoError(t, err)
	defer func() { require.NoError(t, restore(ctx)) }()

	var tempStore int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma temp_store`).Scan(&tempStore))
	require.Equal(t, 1, tempStore, "snapshot imports should use file-backed temporary storage")

	var cacheSize int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma cache_size`).Scan(&cacheSize))
	require.GreaterOrEqual(t, cacheSize, -65536)

	var journalMode string
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma journal_mode`).Scan(&journalMode))
	require.NotEqual(t, "off", strings.ToLower(journalMode))

	var synchronous int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma synchronous`).Scan(&synchronous))
	require.NotZero(t, synchronous)
}

func TestImportRepairsBlankMessageGuildIDs(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	_, err := src.DB().ExecContext(ctx, `update messages set guild_id = '' where id = 'm1'`)
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `update message_events set guild_id = '' where message_id = 'm1'`)
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `update mention_events set guild_id = '' where message_id = 'm1'`)
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	_, err = Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.Contains(t, snapshotTableText(t, repo, tableEntry(t, mustReadManifest(t, repo), "messages")), `"guild_id":""`)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	var guildID string
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select guild_id from messages where id = 'm1'`).Scan(&guildID))
	require.Equal(t, "g1", guildID)
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select guild_id from message_events where message_id = 'm1'`).Scan(&guildID))
	require.Equal(t, "g1", guildID)
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select guild_id from mention_events where message_id = 'm1'`).Scan(&guildID))
	require.Equal(t, "g1", guildID)
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "launch", GuildIDs: []string{"g1"}, Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "g1", results[0].GuildID)
}

func TestSnapshotExcludesLocalEmbeddingState(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	_, err := src.DB().ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, provider, model, input_version, updated_at)
		values ('m1', 'done', 0, 'ollama', 'nomic-embed-text', ?, ?)
	`, store.EmbeddingInputVersion, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values ('m1', 'ollama', 'nomic-embed-text', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, []byte{0, 0, 0, 0, 0, 0, 0, 0}, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotContains(t, tableNames(manifest), "embedding_jobs")
	require.NotContains(t, tableNames(manifest), "message_embeddings")
	require.Empty(t, manifest.Embeddings)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = dst.DB().ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, provider, model, input_version, updated_at)
		values ('m1', 'pending', 0, 'ollama', 'nomic-embed-text', ?, ?)
	`, store.EmbeddingInputVersion, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	var state string
	require.NoError(t, dst.DB().QueryRowContext(ctx, `
		select state from embedding_jobs where message_id = 'm1'
	`).Scan(&state))
	require.Equal(t, "pending", state)
}

func TestSnapshotExcludesAndPreservesDirectMessages(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	seedDirectMessageData(t, ctx, src)

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.Equal(t, 1, tableEntry(t, manifest, "guilds").Rows)
	require.Equal(t, 1, tableEntry(t, manifest, "channels").Rows)
	require.Equal(t, 1, tableEntry(t, manifest, "messages").Rows)
	require.NotContains(t, snapshotTableText(t, repo, tableEntry(t, manifest, "guilds")), directMessageGuildID)
	require.NotContains(t, snapshotTableText(t, repo, tableEntry(t, manifest, "channels")), directMessageGuildID)
	require.NotContains(t, snapshotTableText(t, repo, tableEntry(t, manifest, "messages")), "private dm content")
	require.NotContains(t, snapshotTableText(t, repo, tableEntry(t, manifest, "sync_state")), "wiretap:last_import")
	manifest = appendSnapshotRow(t, repo, manifest, "messages", map[string]any{
		"id":                 "hostile-dm",
		"guild_id":           directMessageGuildID,
		"channel_id":         "dm-c2",
		"author_id":          "u9",
		"message_type":       0,
		"created_at":         "2026-04-24T16:00:00Z",
		"content":            "hostile imported dm",
		"normalized_content": "hostile imported dm",
		"pinned":             0,
		"has_attachments":    0,
		"raw_json":           `{}`,
		"updated_at":         "2026-04-24T16:00:00Z",
	})
	manifest = appendSnapshotRow(t, repo, manifest, "sync_state", map[string]any{
		"scope":      "wiretap:hostile",
		"cursor":     "private",
		"updated_at": "2026-04-24T16:00:00Z",
	})
	writeShareManifest(t, repo, manifest)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	seedDirectMessageData(t, ctx, dst)

	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	dmResults, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "private dm content", Limit: 10})
	require.NoError(t, err)
	require.Len(t, dmResults, 1)
	require.Equal(t, directMessageGuildID, dmResults[0].GuildID)
	guildResults, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "launch checklist", Limit: 10})
	require.NoError(t, err)
	require.Len(t, guildResults, 1)
	wiretapState, err := dst.GetSyncState(ctx, "wiretap:last_import")
	require.NoError(t, err)
	require.Equal(t, "2026-04-24T15:33:17Z", wiretapState)
	hostileResults, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "hostile imported dm", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, hostileResults)
	_, rows, err := dst.ReadOnlyQuery(ctx, "select count(*) from sync_state where scope = 'wiretap:hostile'")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestSnapshotFilterKeepsOnlyIncludedPublicChannels(t *testing.T) {
	ctx := context.Background()
	src, err := store.Open(ctx, filepath.Join(t.TempDir(), "src.db"))
	require.NoError(t, err)
	defer func() { _ = src.Close() }()

	require.NoError(t, src.UpsertGuild(ctx, store.GuildRecord{
		ID:      "g1",
		Name:    "Guild",
		RawJSON: `{"roles":[{"id":"g1","permissions":"1024"}]}`,
	}))
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "public", RawJSON: `{}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "private", RawJSON: `{"permission_overwrites":[{"id":"g1","type":0,"allow":"0","deny":"1024"}]}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "cat1", GuildID: "g1", Kind: "category", Name: "private-category", RawJSON: `{"permission_overwrites":[{"id":"g1","type":0,"allow":"0","deny":"1024"}]}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "c3", GuildID: "g1", ParentID: "cat1", Kind: "text", Name: "inherits-private", RawJSON: `{}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "f1", GuildID: "g1", Kind: "forum", Name: "public-forum", RawJSON: `{}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "t1", GuildID: "g1", ParentID: "f1", ThreadParentID: "f1", Kind: "thread_public", Name: "public-thread", RawJSON: `{}`})
	upsertSnapshotFilterChannel(t, ctx, src, store.ChannelRecord{ID: "tp1", GuildID: "g1", ParentID: "f1", ThreadParentID: "f1", Kind: "thread_private", Name: "private-thread", IsPrivateThread: true, RawJSON: `{}`})

	for _, userID := range []string{"u1", "u2", "u3", "u4"} {
		require.NoError(t, src.UpsertMember(ctx, store.MemberRecord{
			GuildID:     "g1",
			UserID:      userID,
			Username:    userID,
			RoleIDsJSON: `[]`,
			RawJSON:     `{}`,
		}))
	}
	upsertSnapshotFilterMessage(t, ctx, src, "m1", "c1", "u1", "public content")
	upsertSnapshotFilterMessage(t, ctx, src, "m2", "c2", "u2", "private content")
	upsertSnapshotFilterMessage(t, ctx, src, "m3", "c3", "u3", "category private content")
	upsertSnapshotFilterMessage(t, ctx, src, "mt1", "t1", "u4", "thread content")
	upsertSnapshotFilterMessage(t, ctx, src, "mtp1", "tp1", "u2", "private thread content")
	require.NoError(t, src.SetSyncState(ctx, "channel:c1:latest_message_id", "m1"))
	require.NoError(t, src.SetSyncState(ctx, "channel:c2:latest_message_id", "m2"))
	require.NoError(t, src.SetSyncState(ctx, "guild:g1:members:last_success", time.Now().UTC().Format(time.RFC3339Nano)))
	require.NoError(t, src.SetSyncState(ctx, "guild:g1:custom", "ok"))
	require.NoError(t, src.SetSyncState(ctx, LastImportManifestJSONScope, `{"tables":[{"name":"messages","rows":999}],"leak":"private content"}`))

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{
		RepoPath: repo,
		Branch:   "main",
		Filter: FilterOptions{
			PublicOnly:        true,
			IncludeChannelIDs: []string{"c1", "c2", "c3", "f1"},
		},
	})
	require.NoError(t, err)

	channelText := snapshotTableText(t, repo, tableEntry(t, manifest, "channels"))
	require.Contains(t, channelText, `"id":"c1"`)
	require.Contains(t, channelText, `"id":"f1"`)
	require.Contains(t, channelText, `"id":"t1"`)
	require.NotContains(t, channelText, `"id":"c2"`)
	require.NotContains(t, channelText, `"id":"c3"`)
	require.NotContains(t, channelText, `"id":"tp1"`)

	messageText := snapshotTableText(t, repo, tableEntry(t, manifest, "messages"))
	require.Contains(t, messageText, "public content")
	require.Contains(t, messageText, "thread content")
	require.NotContains(t, messageText, "private content")
	require.NotContains(t, messageText, "category private content")
	require.NotContains(t, messageText, "private thread content")

	memberText := snapshotTableText(t, repo, tableEntry(t, manifest, "members"))
	require.Contains(t, memberText, `"user_id":"u1"`)
	require.Contains(t, memberText, `"user_id":"u4"`)
	require.NotContains(t, memberText, `"user_id":"u2"`)
	require.NotContains(t, memberText, `"user_id":"u3"`)

	syncText := snapshotTableText(t, repo, tableEntry(t, manifest, "sync_state"))
	require.Contains(t, syncText, "channel:c1:latest_message_id")
	require.Contains(t, syncText, "guild:g1:custom")
	require.NotContains(t, syncText, "channel:c2:latest_message_id")
	require.NotContains(t, syncText, "guild:g1:members:last_success")
	require.NotContains(t, syncText, "share:last_import_manifest_json")
	require.NotContains(t, syncText, "private content")
	require.NotContains(t, syncText, `"rows":999`)

	_, rows, err := src.ReadOnlyQuery(ctx, "select count(*) from messages where content like '%private%'")
	require.NoError(t, err)
	require.Equal(t, "3", rows[0][0])
}

func TestExportImportEmbeddingsOptIn(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	vector := []float32{1, 0.5}
	blob, err := store.EncodeEmbeddingVector(vector)
	require.NoError(t, err)
	embeddedAt := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values ('m1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, blob, embeddedAt)
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{
		RepoPath:              repo,
		Branch:                "main",
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "text-embedding-3-small",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
	manifest, err := Export(ctx, src, opts)
	require.NoError(t, err)
	require.Len(t, manifest.Embeddings, 1)
	require.Equal(t, 1, manifest.Embeddings[0].Rows)
	require.NotEmpty(t, manifest.Embeddings[0].Files)
	require.FileExists(t, filepath.Join(repo, filepath.FromSlash(manifest.Embeddings[0].Files[0])))

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = Import(ctx, dst, opts)
	require.NoError(t, err)

	var gotBlob []byte
	var gotDimensions int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `
		select dimensions, embedding_blob
		from message_embeddings
		where message_id = 'm1'
		  and provider = 'openai'
		  and model = 'text-embedding-3-small'
		  and input_version = ?
	`, store.EmbeddingInputVersion).Scan(&gotDimensions, &gotBlob))
	require.Equal(t, 2, gotDimensions)
	gotVector, err := store.DecodeEmbeddingVector(gotBlob)
	require.NoError(t, err)
	require.Equal(t, vector, gotVector)
}

func TestExportEmbeddingsExcludesDirectMessages(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	seedDirectMessageData(t, ctx, src)

	blob, err := store.EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values
			('m1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?),
			('dm1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, blob, time.Now().UTC().Format(time.RFC3339Nano), store.EmbeddingInputVersion, blob, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{
		RepoPath:              repo,
		Branch:                "main",
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "text-embedding-3-small",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	})
	require.NoError(t, err)
	require.Len(t, manifest.Embeddings, 1)
	require.Equal(t, 1, manifest.Embeddings[0].Rows)
	text := snapshotFilesText(t, repo, manifest.Embeddings[0].Files)
	require.Contains(t, text, `"message_id":"m1"`)
	require.NotContains(t, text, "dm1")
}

func TestArchiveExportDropsEmbeddingBundleUnlessOptedIn(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	blob, err := store.EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values ('m1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, blob, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	embeddingOpts := Options{
		RepoPath:              repo,
		Branch:                "main",
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "text-embedding-3-small",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
	manifest, err := Export(ctx, src, embeddingOpts)
	require.NoError(t, err)
	require.Len(t, manifest.Embeddings, 1)
	embeddingFile := filepath.Join(repo, filepath.FromSlash(manifest.Embeddings[0].Files[0]))
	require.FileExists(t, embeddingFile)

	archiveManifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.Empty(t, archiveManifest.Embeddings)
}

func TestImportEmbeddingsFiltersByConfiguredIdentity(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	blob, err := store.EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values ('m1', 'openai', 'text-embedding-3-small', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, blob, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	exportOpts := Options{
		RepoPath:              repo,
		Branch:                "main",
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "text-embedding-3-small",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
	manifest, err := Export(ctx, src, exportOpts)
	require.NoError(t, err)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	require.NoError(t, ImportEmbeddings(ctx, dst, Options{
		RepoPath:              repo,
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "ollama",
		EmbeddingModel:        "nomic-embed-text",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}, manifest))

	_, rows, err := dst.ReadOnlyQuery(ctx, "select count(*) from message_embeddings")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestImportIfChangedSkipsSameManifest(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	importedManifest, imported, err := ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, imported)
	require.Equal(t, manifest.GeneratedAt, importedManifest.GeneratedAt)

	require.NoError(t, dst.UpsertMessage(ctx, store.MessageRecord{
		ID:                "local-only",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "live delta preserved",
		NormalizedContent: "live delta preserved",
		RawJSON:           `{}`,
	}))

	_, imported, err = ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.False(t, imported)
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "live delta", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "local-only", results[0].MessageID)
}

func TestExportShardsLargeTables(t *testing.T) {
	ctx := context.Background()
	prevMaxShardBytes := maxShardBytes
	maxShardBytes = 150
	t.Cleanup(func() { maxShardBytes = prevMaxShardBytes })

	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	now := time.Now().UTC()
	for i := range 25 {
		id := "extra-" + strconv.Itoa(i)
		require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
			Record: store.MessageRecord{
				ID:                id,
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "general",
				AuthorID:          "u1",
				AuthorName:        "Peter",
				MessageType:       0,
				CreatedAt:         now.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
				Content:           strings.Repeat("unique launch shard payload "+id+" ", 8),
				NormalizedContent: strings.Repeat("unique launch shard payload "+id+" ", 8),
				RawJSON:           `{}`,
			},
			EventType:   "upsert",
			PayloadJSON: `{"id":"` + id + `"}`,
			Options:     store.WriteOptions{AppendEvent: true},
		}}))
	}

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	messages := tableEntry(t, manifest, "messages")
	require.Greater(t, len(messages.Files), 1)
	require.Empty(t, messages.File)
	for _, rel := range messages.Files {
		info, err := os.Stat(filepath.Join(repo, filepath.FromSlash(rel)))
		require.NoError(t, err)
		require.Less(t, info.Size(), int64(100*1024*1024))
	}

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "shard payload", Limit: 50})
	require.NoError(t, err)
	require.Len(t, results, 25)
}

func TestGitCommitDetectsNoChanges(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{RepoPath: repo, Branch: "main"}
	_, err := Export(ctx, src, opts)
	require.NoError(t, err)
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", repo, "config", "user.name", "discrawl test").Run())
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", repo, "config", "user.email", "discrawl@example.com").Run())

	committed, err := Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)

	committed, err = Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.False(t, committed)
}

func TestPullAndPushWithBareRemote(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	dir := t.TempDir()
	remote := filepath.Join(dir, "remote.git")
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", dir, "init", "--bare", remote).Run())

	publisher := filepath.Join(dir, "publisher")
	opts := Options{RepoPath: publisher, Remote: remote, Branch: "main"}
	_, err := Export(ctx, src, opts)
	require.NoError(t, err)
	configureGitUser(t, publisher)
	committed, err := Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, Push(ctx, opts))

	subscriber := filepath.Join(dir, "subscriber")
	subOpts := Options{RepoPath: subscriber, Remote: remote, Branch: "main"}
	require.NoError(t, Pull(ctx, subOpts))
	require.FileExists(t, filepath.Join(subscriber, ManifestName))
}

func TestPushRebasesRemoteReadmeUpdates(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	dir := t.TempDir()
	remote := filepath.Join(dir, "remote.git")
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", dir, "init", "--bare", remote).Run())

	publisher := filepath.Join(dir, "publisher")
	opts := Options{RepoPath: publisher, Remote: remote, Branch: "main"}
	_, err := Export(ctx, src, opts)
	require.NoError(t, err)
	configureGitUser(t, publisher)
	require.NoError(t, os.WriteFile(filepath.Join(publisher, "README.md"), []byte("report: first\n\nfield notes: old\n"), 0o600))
	committed, err := Commit(ctx, opts, "test: initial snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, Push(ctx, opts))

	reporter := filepath.Join(dir, "reporter")
	require.NoError(t, run(ctx, dir, "git", "clone", "--branch", "main", remote, reporter))
	configureGitUser(t, reporter)
	require.NoError(t, os.WriteFile(filepath.Join(reporter, "README.md"), []byte("report: first\n\nfield notes: fresh\n"), 0o600))
	require.NoError(t, run(ctx, reporter, "git", "commit", "-am", "docs: update field notes"))
	require.NoError(t, run(ctx, reporter, "git", "push", "-u", "origin", "main"))

	require.NoError(t, os.WriteFile(filepath.Join(publisher, "README.md"), []byte("report: second\n\nfield notes: old\n"), 0o600))
	committed, err = Commit(ctx, opts, "test: update report")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, Push(ctx, opts))

	subscriber := filepath.Join(dir, "subscriber")
	require.NoError(t, Pull(ctx, Options{RepoPath: subscriber, Remote: remote, Branch: "main"}))
	body, err := os.ReadFile(filepath.Join(subscriber, "README.md"))
	require.NoError(t, err)
	require.Contains(t, string(body), "report: second")
	require.Contains(t, string(body), "field notes: fresh")
}

func TestImportValueConvertsJSONNumbers(t *testing.T) {
	t.Parallel()

	require.Equal(t, int64(42), importValue(json.Number("42")))
	require.InDelta(t, 3.5, importValue(json.Number("3.5")), 0)
	require.Equal(t, "not-a-number", importValue(json.Number("not-a-number")))
	require.Equal(t, "plain", importValue("plain"))
}

func TestManifestStateAndReadEdges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	_, err = ReadManifest(t.TempDir())
	require.ErrorIs(t, err, ErrNoManifest)

	repo := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(repo, ManifestName), []byte(`{`), 0o600))
	_, err = ReadManifest(repo)
	require.ErrorContains(t, err, "parse share manifest")
	require.NoError(t, os.WriteFile(filepath.Join(repo, ManifestName), []byte(`{"version":99}`), 0o600))
	_, err = ReadManifest(repo)
	require.ErrorContains(t, err, "unsupported share manifest version 99")

	now := time.Now().UTC().Truncate(time.Nanosecond)
	manifest := Manifest{Version: 1, GeneratedAt: now}
	require.False(t, ManifestAlreadyImported(ctx, s, Manifest{}))
	require.False(t, ManifestAlreadyImported(ctx, s, manifest))
	require.NoError(t, s.SetSyncState(ctx, LastImportManifestSyncScope, "not-time"))
	require.False(t, ManifestAlreadyImported(ctx, s, manifest))
	require.NoError(t, MarkImported(ctx, s, Manifest{}))
	require.False(t, ManifestAlreadyImported(ctx, s, manifest))
	require.NoError(t, MarkImported(ctx, s, manifest))
	require.True(t, ManifestAlreadyImported(ctx, s, manifest))

	require.False(t, NeedsImport(ctx, s, 15*time.Minute))
	require.NoError(t, s.SetSyncState(ctx, LastImportSyncScope, "bad-time"))
	require.True(t, NeedsImport(ctx, s, 15*time.Minute))
	require.NoError(t, s.SetSyncState(ctx, LastImportSyncScope, time.Now().UTC().Add(-20*time.Minute).Format(time.RFC3339Nano)))
	require.True(t, NeedsImport(ctx, s, 15*time.Minute))
	require.NoError(t, s.SetSyncState(ctx, LastImportSyncScope, time.Now().UTC().Format(time.RFC3339Nano)))
	require.False(t, NeedsImport(ctx, s, 0))
}

func TestRepoCommandEdges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	require.ErrorContains(t, EnsureRepo(ctx, Options{}), "repo path is empty")
	require.NoError(t, Pull(ctx, Options{}))

	repo := filepath.Join(t.TempDir(), "repo")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, ".git"), 0o755))
	require.NoError(t, EnsureRepo(ctx, Options{RepoPath: repo}))

	err := Push(ctx, Options{RepoPath: repo, Branch: "main"})
	require.ErrorContains(t, err, "git push -u origin main")
	require.ErrorContains(t, run(ctx, repo, "git", "definitely-not-a-command"), "git definitely-not-a-command")
}

func TestShareSmallHelpersAndValidation(t *testing.T) {
	t.Parallel()

	require.Equal(t, "_", safePathSegment(" "))
	require.Equal(t, "OpenAI_compatible-v1.2", safePathSegment("OpenAI compatible-v1.2"))
	require.Equal(t, `"weird""table"`, quoteIdent(`weird"table`))
	require.Equal(t, `insert into "messages"("id","weird""column") values(?,?)`, insertSQL("messages", []string{"id", `weird"column`}))
	require.Equal(t, "blob", exportValue([]byte("blob")))
	require.Equal(t, "plain", exportValue("plain"))
	require.Equal(t, int64(42), importValue(json.Number("42")))
	require.InDelta(t, 3.5, importValue(json.Number("3.5")), 0)
	require.Equal(t, "nope", importValue(json.Number("nope")))
	require.Equal(t, "plain", importValue("plain"))
	require.Equal(t, "plain", stringValue("plain"))
	require.Equal(t, "42", stringValue(json.Number("42")))
	require.Empty(t, stringValue(42))
	require.True(t, isNonFastForwardPush("failed to push some refs; fetch first"))
	require.True(t, isNonFastForwardPush("non-fast-forward"))
	require.False(t, isNonFastForwardPush("everything up-to-date"))

	query, args := snapshotExportQuery("messages")
	require.Equal(t, "select * from messages where guild_id != ?", query)
	require.Equal(t, []any{directMessageGuildID}, args)
	query, args = snapshotExportQuery("guilds")
	require.Equal(t, "select * from guilds where id != ?", query)
	require.Equal(t, []any{directMessageGuildID}, args)
	query, args = snapshotExportQuery("sync_state")
	require.Equal(t, "select * from sync_state where scope not like 'wiretap:%'", query)
	require.Nil(t, args)
	query, args = snapshotExportQuery("custom")
	require.Equal(t, "select * from custom", query)
	require.Nil(t, args)

	query, args = snapshotDeleteQuery("channels")
	require.Equal(t, "delete from channels where guild_id != ?", query)
	require.Equal(t, []any{directMessageGuildID}, args)
	query, args = snapshotDeleteQuery("guilds")
	require.Equal(t, "delete from guilds where id != ?", query)
	require.Equal(t, []any{directMessageGuildID}, args)
	query, args = snapshotDeleteQuery("message_events")
	require.Equal(t, "delete from message_events where guild_id != ?", query)
	require.Equal(t, []any{directMessageGuildID}, args)
	query, args = snapshotDeleteQuery("sync_state")
	require.Equal(t, "delete from sync_state where scope not like 'wiretap:%'", query)
	require.Nil(t, args)
	query, args = snapshotDeleteQuery("custom")
	require.Equal(t, "delete from custom", query)
	require.Nil(t, args)

	require.True(t, isDirectMessageSnapshotRow("guilds", map[string]any{"id": directMessageGuildID}))
	require.True(t, isDirectMessageSnapshotRow("channels", map[string]any{"guild_id": directMessageGuildID}))
	require.True(t, isDirectMessageSnapshotRow("sync_state", map[string]any{"scope": "wiretap:last_import"}))
	require.False(t, isDirectMessageSnapshotRow("sync_state", map[string]any{"scope": "share:last_import"}))
	require.False(t, isDirectMessageSnapshotRow("custom", map[string]any{"guild_id": directMessageGuildID}))
	require.True(t, isLocalOnlyGuildID(directMessageGuildID))
	require.False(t, isLocalOnlyGuildID("g1"))

	require.Equal(t, []string{"message_id", "guild_id"}, importColumns(TableManifest{Name: "message_events", Columns: []string{"event_id", "message_id", "guild_id"}}))
	require.Equal(t, []string{"event_id", "message_id"}, importColumns(TableManifest{Name: "messages", Columns: []string{"event_id", "message_id"}}))
	require.Equal(t, 7, manifestRowCount(Manifest{
		Tables:     []TableManifest{{Rows: 2}, {Rows: 3}},
		Embeddings: []EmbeddingManifest{{Rows: 2}},
	}))
	var seen []ImportProgress
	Options{Progress: func(progress ImportProgress) { seen = append(seen, progress) }}.reportProgress(ImportProgress{Phase: "phase"})
	require.Equal(t, []ImportProgress{{Phase: "phase"}}, seen)
	Options{}.reportProgress(ImportProgress{Phase: "ignored"})
	require.Equal(t, mirror.Options{RepoPath: "repo", Remote: "origin", Branch: "main"}, mirrorOptions(Options{RepoPath: "repo", Remote: "origin", Branch: "main"}))

	var buf bytes.Buffer
	cw := &countingWriter{w: &buf}
	n, err := cw.Write([]byte("abc"))
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, int64(3), cw.n)

	require.True(t, embeddingManifestMatches(Options{EmbeddingProvider: " OPENAI ", EmbeddingModel: "model"}, EmbeddingManifest{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
	}))
	require.False(t, embeddingManifestMatches(Options{EmbeddingProvider: "ollama"}, EmbeddingManifest{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
	}))

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorContains(t, importTable(ctx, tx, Options{RepoPath: t.TempDir()}, TableManifest{Name: "messages", Columns: []string{"id"}}), "has no files")
	require.NoError(t, tx.Rollback())

	require.ErrorContains(t, ImportEmbeddings(ctx, s, Options{
		RepoPath:              t.TempDir(),
		IncludeEmbeddings:     true,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "model",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}, Manifest{Embeddings: []EmbeddingManifest{{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
	}}}), "has no files")
}

func TestTableShardWriterRotates(t *testing.T) {
	oldMax := maxShardBytes
	maxShardBytes = 1
	t.Cleanup(func() { maxShardBytes = oldMax })

	writer := tableShardWriter{rootDir: t.TempDir(), relDir: "tables/messages", label: "messages"}
	require.NoError(t, os.MkdirAll(filepath.Join(writer.rootDir, filepath.FromSlash(writer.relDir)), 0o755))
	require.NoError(t, writer.open())
	_, err := writer.Write([]byte(`{"id":"m1"}` + "\n"))
	require.NoError(t, err)
	require.NoError(t, writer.finishRow())
	require.NoError(t, writer.rotateIfNeeded())
	_, err = writer.Write([]byte(`{"id":"m2"}` + "\n"))
	require.NoError(t, err)
	require.NoError(t, writer.finishRow())
	require.NoError(t, writer.close())
	require.Len(t, writer.files, 2)
	for _, rel := range writer.files {
		require.FileExists(t, filepath.Join(writer.rootDir, filepath.FromSlash(rel)))
	}
}

func TestLegacyManifestFileImportAndEmbeddingDecodeErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	repo := t.TempDir()
	tableRel := filepath.ToSlash(filepath.Join("tables", "guilds", "legacy.jsonl.gz"))
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "tables", "guilds"), 0o755))
	writeGzipJSONLines(t, filepath.Join(repo, filepath.FromSlash(tableRel)), []string{
		`{"id":"g1","name":"Guild","icon":null,"raw_json":"{}","updated_at":"2026-04-22T12:00:00Z"}`,
	})
	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, importTable(ctx, tx, Options{RepoPath: repo}, TableManifest{
		Name:    "guilds",
		File:    tableRel,
		Columns: []string{"id", "name", "icon", "raw_json", "updated_at"},
	}))
	require.NoError(t, tx.Commit())

	_, rows, err := s.ReadOnlyQuery(ctx, "select id, name from guilds")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"g1", "Guild"}}, rows)

	embeddingRel := filepath.ToSlash(filepath.Join("embeddings", "bad.jsonl.gz"))
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "embeddings"), 0o755))
	writeGzipJSONLines(t, filepath.Join(repo, filepath.FromSlash(embeddingRel)), []string{
		`{"message_id":"m1","provider":"openai","model":"model","input_version":"` + store.EmbeddingInputVersion + `","dimensions":3.5,"embedding_blob":"AAAA","embedded_at":"2026-04-22T12:00:00Z"}`,
	})
	tx, err = s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorContains(t, importEmbeddings(ctx, tx, Options{
		RepoPath:              repo,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "model",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}, []EmbeddingManifest{{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
		Files:        []string{embeddingRel},
	}}), "decode dimensions")
	require.NoError(t, tx.Rollback())

	writeGzipJSONLines(t, filepath.Join(repo, filepath.FromSlash(embeddingRel)), []string{
		`{"message_id":"m1","provider":"openai","model":"model","input_version":"` + store.EmbeddingInputVersion + `","dimensions":2,"embedding_blob":"not-base64","embedded_at":"2026-04-22T12:00:00Z"}`,
	})
	tx, err = s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorContains(t, importEmbeddings(ctx, tx, Options{
		RepoPath:              repo,
		EmbeddingProvider:     "openai",
		EmbeddingModel:        "model",
		EmbeddingInputVersion: store.EmbeddingInputVersion,
	}, []EmbeddingManifest{{
		Provider:     "openai",
		Model:        "model",
		InputVersion: store.EmbeddingInputVersion,
		Files:        []string{embeddingRel},
	}}), "decode embedding blob")
	require.NoError(t, tx.Rollback())
}

func writeGzipJSONLines(t *testing.T, path string, lines []string) {
	t.Helper()
	file, err := os.Create(path)
	require.NoError(t, err)
	gz := gzip.NewWriter(file)
	for _, line := range lines {
		_, err = gz.Write([]byte(line + "\n"))
		require.NoError(t, err)
	}
	require.NoError(t, gz.Close())
	require.NoError(t, file.Close())
}

func appendSnapshotRow(t *testing.T, repo string, manifest Manifest, tableName string, row map[string]any) Manifest {
	t.Helper()
	for i := range manifest.Tables {
		if manifest.Tables[i].Name != tableName {
			continue
		}
		rel := filepath.ToSlash(filepath.Join("tables", tableName, "hostile-"+strconv.Itoa(len(manifest.Tables[i].Files))+".jsonl.gz"))
		full := filepath.Join(repo, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		body, err := json.Marshal(row)
		require.NoError(t, err)
		writeGzipJSONLines(t, full, []string{string(body)})
		manifest.Tables[i].Files = append(manifest.Tables[i].Files, rel)
		manifest.Tables[i].Rows++
		return manifest
	}
	t.Fatalf("table %s not found", tableName)
	return manifest
}

func writeShareManifest(t *testing.T, repo string, manifest Manifest) {
	t.Helper()
	body, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(repo, ManifestName), append(body, '\n'), 0o600))
}

func stripFileManifests(manifest Manifest) Manifest {
	for i := range manifest.Tables {
		manifest.Tables[i].FileManifests = nil
	}
	return manifest
}

func snapshotTableText(t *testing.T, repo string, table TableManifest) string {
	t.Helper()
	return snapshotFilesText(t, repo, table.Files)
}

func snapshotFilesText(t *testing.T, repo string, files []string) string {
	t.Helper()
	var out strings.Builder
	for _, rel := range files {
		file, err := os.Open(filepath.Join(repo, filepath.FromSlash(rel)))
		require.NoError(t, err)
		gz, err := gzip.NewReader(file)
		require.NoError(t, err)
		_, err = io.Copy(&out, gz)
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, file.Close())
	}
	return out.String()
}

func seedStore(t *testing.T, path string) *store.Store {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, path)
	require.NoError(t, err)
	now := time.Now().UTC()
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"bio":"Runs launch ops"}`,
	}))
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now.Format(time.RFC3339Nano),
			Content:           "launch checklist ready",
			NormalizedContent: "launch checklist ready",
			RawJSON:           `{}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"m1"}`,
		Options:     store.WriteOptions{AppendEvent: true},
		Mentions: []store.MentionEventRecord{{
			MessageID:  "m1",
			GuildID:    "g1",
			ChannelID:  "c1",
			AuthorID:   "u1",
			TargetType: "role",
			TargetID:   "r1",
			TargetName: "Ops",
			EventAt:    now.Format(time.RFC3339Nano),
		}},
	}}))
	return s
}

func upsertSnapshotFilterChannel(t *testing.T, ctx context.Context, s *store.Store, channel store.ChannelRecord) {
	t.Helper()
	require.NoError(t, s.UpsertChannel(ctx, channel))
}

func upsertSnapshotFilterMessage(t *testing.T, ctx context.Context, s *store.Store, id, channelID, authorID, content string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                id,
			GuildID:           "g1",
			ChannelID:         channelID,
			ChannelName:       channelID,
			AuthorID:          authorID,
			AuthorName:        authorID,
			MessageType:       0,
			CreatedAt:         now,
			Content:           content,
			NormalizedContent: content,
			RawJSON:           `{}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"` + id + `"}`,
		Options:     store.WriteOptions{AppendEvent: true},
	}}))
}

func seedDirectMessageData(t *testing.T, ctx context.Context, s *store.Store) {
	t.Helper()
	now := time.Date(2026, 4, 24, 15, 33, 17, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: directMessageGuildID, Name: "Discord Direct Messages", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "dm-c1", GuildID: directMessageGuildID, Kind: "dm", Name: "Alice", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "dm1",
			GuildID:           directMessageGuildID,
			ChannelID:         "dm-c1",
			ChannelName:       "Alice",
			AuthorID:          "u2",
			AuthorName:        "Alice",
			MessageType:       0,
			CreatedAt:         now.Format(time.RFC3339Nano),
			Content:           "private dm content",
			NormalizedContent: "private dm content",
			RawJSON:           `{}`,
		},
		EventType:   "wiretap",
		PayloadJSON: `{"id":"dm1"}`,
		Options:     store.WriteOptions{AppendEvent: true},
		Attachments: []store.AttachmentRecord{{
			AttachmentID: "att-dm1",
			MessageID:    "dm1",
			GuildID:      directMessageGuildID,
			ChannelID:    "dm-c1",
			AuthorID:     "u2",
			Filename:     "private.txt",
		}},
		Mentions: []store.MentionEventRecord{{
			MessageID:  "dm1",
			GuildID:    directMessageGuildID,
			ChannelID:  "dm-c1",
			AuthorID:   "u2",
			TargetType: "user",
			TargetID:   "u3",
			TargetName: "Bob",
			EventAt:    now.Format(time.RFC3339Nano),
		}},
	}}))
	require.NoError(t, s.SetSyncState(ctx, "wiretap:last_import", now.Format(time.RFC3339)))
}

func configureGitUser(t *testing.T, repo string) {
	t.Helper()
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", repo, "config", "user.name", "discrawl test").Run())
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", repo, "config", "user.email", "discrawl@example.com").Run())
}

func mustReadManifest(t *testing.T, repo string) Manifest {
	t.Helper()
	manifest, err := ReadManifest(repo)
	require.NoError(t, err)
	return manifest
}

func tableEntry(t *testing.T, manifest Manifest, name string) TableManifest {
	t.Helper()
	for _, table := range manifest.Tables {
		if table.Name == name {
			return table
		}
	}
	t.Fatalf("table %s not found", name)
	return TableManifest{}
}

func importPlanTable(t *testing.T, plan snapshot.ImportPlan, name string) snapshot.TableImportPlan {
	t.Helper()
	for _, table := range plan.Tables {
		if table.Table.Name == name {
			return table
		}
	}
	t.Fatalf("plan table %s not found", name)
	return snapshot.TableImportPlan{}
}

func tableNames(manifest Manifest) []string {
	names := make([]string, 0, len(manifest.Tables))
	for _, table := range manifest.Tables {
		names = append(names, table.Name)
	}
	return names
}

func progressPhases(progress []ImportProgress) []string {
	phases := make([]string, 0, len(progress))
	for _, item := range progress {
		phases = append(phases, item.Phase)
	}
	return phases
}

func progressTotalRows(t *testing.T, progress []ImportProgress, phase string) int {
	t.Helper()
	for _, item := range progress {
		if item.Phase == phase {
			return item.TotalRows
		}
	}
	t.Fatalf("progress phase %s not found", phase)
	return 0
}
