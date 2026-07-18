package share

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/snapshot"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestMergeIfChangedPreservesLocalRowsUntilForcedReplacement(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	initial, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, changed, err := MergeIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, ManifestAlreadyMerged(ctx, dst, initial))

	require.NoError(t, dst.UpsertMessage(ctx, store.MessageRecord{
		ID:                "local-only",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "local",
		AuthorName:        "Local",
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "must survive safe refresh",
		NormalizedContent: "must survive safe refresh",
		RawJSON:           `{}`,
	}))
	require.NoError(t, dst.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "local",
		AuthorName:        "Local",
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "newer local edit",
		NormalizedContent: "newer local edit",
		RawJSON:           `{}`,
	}))
	_, err = dst.DB().ExecContext(ctx, `update messages set updated_at = ? where id = 'm1'`, time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano))
	require.NoError(t, err)
	now := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			CreatedAt:         now,
			Content:           "remote merge delta",
			NormalizedContent: "remote merge delta",
			RawJSON:           `{}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"m2"}`,
		Options:     store.WriteOptions{AppendEvent: true},
	}}))
	updated, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	var progress []ImportProgress
	_, changed, err = MergeIfChanged(ctx, dst, Options{
		RepoPath: repo,
		Branch:   "main",
		Progress: func(p ImportProgress) { progress = append(progress, p) },
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.NotContains(t, progressPhases(progress), "rebuild_fts")
	require.NotContains(t, progressPhases(progress), "rebuild_member_fts")

	_, rows, err := dst.ReadOnlyQuery(ctx, `select id from messages where id in ('local-only', 'm2') order by id`)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"local-only"}, {"m2"}}, rows)
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "remote merge delta", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	results, err = dst.SearchMessages(ctx, store.SearchOptions{Query: "newer local edit", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1, "routine merge must not overwrite a newer local version of the same row")
	_, rows, err = dst.ReadOnlyQuery(ctx, `select count(*) from message_events`)
	require.NoError(t, err)
	require.Equal(t, "1", rows[0][0], "routine merge must not replay generated event IDs")
	require.True(t, ManifestAlreadyMerged(ctx, dst, updated))
	lastExact, err := dst.GetSyncState(ctx, LastImportManifestSyncScope)
	require.NoError(t, err)
	require.Empty(t, lastExact)

	_, changed, err = Replace(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)
	_, rows, err = dst.ReadOnlyQuery(ctx, `select count(*) from messages where id = 'local-only'`)
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
	_, rows, err = dst.ReadOnlyQuery(ctx, `select count(*) from message_events`)
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])
	require.True(t, ManifestAlreadyImported(ctx, dst, updated))

	require.NoError(t, dst.UpsertMessage(ctx, store.MessageRecord{
		ID:                "local-after-force",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		CreatedAt:         now,
		Content:           "must be reconciled",
		NormalizedContent: "must be reconciled",
		RawJSON:           `{}`,
	}))
	_, changed, err = Replace(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)
	_, rows, err = dst.ReadOnlyQuery(ctx, `select count(*) from messages where id = 'local-after-force'`)
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0], "force must reconcile even when the manifest is unchanged")
}

func TestMemberAndGuildTombstonesMergeByRevisionAndRestore(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	_, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	dst := seedStore(t, filepath.Join(t.TempDir(), "dst.db"))
	defer func() { _ = dst.Close() }()
	_, _, err = MergeIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	offsetRevision := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	offsetText := offsetRevision.In(time.FixedZone("proof-offset", -2*60*60)).Format(time.RFC3339Nano)
	_, err = src.DB().ExecContext(ctx, `
		update guilds set name = 'offset revision guild', updated_at = ? where id = 'g1';
		update members set display_name = 'Offset Revision Member', updated_at = ? where guild_id = 'g1' and user_id = 'u1';
	`, offsetText, offsetText)
	require.NoError(t, err)
	_, err = dst.DB().ExecContext(ctx, `
		update guilds set updated_at = ? where id = 'g1';
		update members set updated_at = ? where guild_id = 'g1' and user_id = 'u1';
	`, offsetRevision.Add(-time.Minute).Format(time.RFC3339Nano), offsetRevision.Add(-time.Minute).Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	_, _, err = MergeIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	var guildName, memberName, guildRevision, memberRevision string
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select name, updated_at from guilds where id = 'g1'`).Scan(&guildName, &guildRevision))
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select display_name, updated_at from members where guild_id = 'g1' and user_id = 'u1'`).Scan(&memberName, &memberRevision))
	require.Equal(t, "offset revision guild", guildName)
	require.Equal(t, "Offset Revision Member", memberName)
	require.Equal(t, offsetRevision.Format(time.RFC3339Nano), guildRevision)
	require.Equal(t, offsetRevision.Format(time.RFC3339Nano), memberRevision)

	equalRevision := offsetRevision.Add(time.Second)
	_, err = src.DB().ExecContext(ctx, `
		update guilds set name = 'stale live guild', updated_at = ? where id = 'g1';
		update members set display_name = 'Stale Live Member', updated_at = ? where guild_id = 'g1' and user_id = 'u1';
	`, equalRevision.Format(time.RFC3339Nano), equalRevision.Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = dst.DB().ExecContext(ctx, `
		update guilds set deleted_at = ?, deletion_source = 'local-test', deletion_reason = 'explicit-local-delete', updated_at = ? where id = 'g1';
		update members set deleted_at = ?, deletion_source = 'local-test', deletion_reason = 'explicit-local-delete', updated_at = ? where guild_id = 'g1' and user_id = 'u1';
	`, equalRevision.Format(time.RFC3339Nano), equalRevision.Format(time.RFC3339Nano), equalRevision.Format(time.RFC3339Nano), equalRevision.Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	_, changed, err := MergeIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, changed)
	assertEntityTombstones(t, ctx, dst, true, "local-test")

	newerRevision := equalRevision.Add(time.Second)
	_, err = src.DB().ExecContext(ctx, `
		update guilds set name = 'restored guild', deleted_at = null, deletion_source = null, deletion_reason = null, updated_at = ? where id = 'g1';
		update members set display_name = 'Restored Member', deleted_at = null, deletion_source = null, deletion_reason = null, updated_at = ? where guild_id = 'g1' and user_id = 'u1';
	`, newerRevision.Format(time.RFC3339Nano), newerRevision.Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	_, _, err = MergeIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	assertEntityTombstones(t, ctx, dst, false, "")
	members, err := dst.Members(ctx, "g1", "Restored", 10)
	require.NoError(t, err)
	require.Len(t, members, 1)

	tombstoneRevision := newerRevision.Add(time.Second)
	_, err = src.DB().ExecContext(ctx, `
		update guilds set deleted_at = ?, deletion_source = 'remote-share', deletion_reason = 'guild-delete-event', updated_at = ? where id = 'g1';
		update members set deleted_at = ?, deletion_source = 'remote-share', deletion_reason = 'member-remove-event', updated_at = ? where guild_id = 'g1' and user_id = 'u1';
	`, tombstoneRevision.Format(time.RFC3339Nano), tombstoneRevision.Format(time.RFC3339Nano), tombstoneRevision.Format(time.RFC3339Nano), tombstoneRevision.Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	_, _, err = MergeIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	assertEntityTombstones(t, ctx, dst, true, "remote-share")

	restoreRevision := tombstoneRevision.Add(time.Second)
	_, err = src.DB().ExecContext(ctx, `
		update guilds set deleted_at = null, deletion_source = null, deletion_reason = null, updated_at = ? where id = 'g1';
		update members set deleted_at = null, deletion_source = null, deletion_reason = null, updated_at = ? where guild_id = 'g1' and user_id = 'u1';
	`, restoreRevision.Format(time.RFC3339Nano), restoreRevision.Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NoError(t, dst.UpsertGuild(ctx, store.GuildRecord{ID: "local-only", Name: "Local only", RawJSON: `{}`}))
	_, _, err = Replace(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	assertEntityTombstones(t, ctx, dst, false, "")
	var localOnly int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from guilds where id = 'local-only'`).Scan(&localOnly))
	require.Zero(t, localOnly)
}

func assertEntityTombstones(t *testing.T, ctx context.Context, s *store.Store, deleted bool, source string) {
	t.Helper()
	var guildDeleted, memberDeleted bool
	var guildSource, memberSource string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select deleted_at is not null, coalesce(deletion_source, '') from guilds where id = 'g1'`).Scan(&guildDeleted, &guildSource))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select deleted_at is not null, coalesce(deletion_source, '') from members where guild_id = 'g1' and user_id = 'u1'`).Scan(&memberDeleted, &memberSource))
	require.Equal(t, deleted, guildDeleted)
	require.Equal(t, deleted, memberDeleted)
	require.Equal(t, source, guildSource)
	require.Equal(t, source, memberSource)
}

func TestMergeIfChangedMarksReplacementPendingWithoutChangingRows(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, _, err = MergeIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	manifest.GeneratedAt = manifest.GeneratedAt.Add(time.Minute)
	manifest.Tables = removeManifestTable(manifest.Tables, "messages")
	writeShareManifest(t, repo, manifest)
	_, changed, err := MergeIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.ErrorIs(t, err, ErrReplacementRequired)
	require.False(t, changed)
	require.True(t, HasPendingReplacement(ctx, dst))
	_, rows, queryErr := dst.ReadOnlyQuery(ctx, `select count(*) from messages`)
	require.NoError(t, queryErr)
	require.Equal(t, "1", rows[0][0])
	require.False(t, NeedsImport(ctx, dst, 15*time.Minute), "the failed check is still fresh")
}

func TestShareMergePlanKeepsGeneratedHistoryForceOnly(t *testing.T) {
	plan, err := shareMergePlan(snapshot.ImportPlan{Tables: []snapshot.TableImportPlan{
		{Table: snapshot.TableManifest{Name: "messages"}, Mode: snapshot.TableImportFiles},
		{Table: snapshot.TableManifest{Name: "message_events"}, Mode: snapshot.TableImportFiles},
		{Table: snapshot.TableManifest{Name: "mention_events"}, Mode: snapshot.TableImportFiles},
		{Table: snapshot.TableManifest{Name: "sync_state"}, Mode: snapshot.TableImportFiles},
	}}, snapshot.Manifest{}, false)
	require.NoError(t, err)
	require.Equal(t, snapshot.TableImportFiles, importPlanTable(t, plan, "messages").Mode)
	require.Equal(t, snapshot.TableImportSkip, importPlanTable(t, plan, "message_events").Mode)
	require.Equal(t, snapshot.TableImportSkip, importPlanTable(t, plan, "mention_events").Mode)
	require.Equal(t, snapshot.TableImportSkip, importPlanTable(t, plan, "sync_state").Mode)

	bootstrap, err := shareMergePlan(snapshot.ImportPlan{Tables: []snapshot.TableImportPlan{
		{Table: snapshot.TableManifest{Name: "message_events"}, Mode: snapshot.TableImportFiles},
		{Table: snapshot.TableManifest{Name: "sync_state"}, Mode: snapshot.TableImportFiles},
	}}, snapshot.Manifest{}, true)
	require.NoError(t, err)
	require.Equal(t, snapshot.TableImportFiles, importPlanTable(t, bootstrap, "message_events").Mode)
	require.Equal(t, snapshot.TableImportSkip, importPlanTable(t, bootstrap, "sync_state").Mode)

	_, err = shareMergePlan(snapshot.ImportPlan{Tables: []snapshot.TableImportPlan{{
		Table: snapshot.TableManifest{Name: "messages"},
		Mode:  snapshot.TableImportReplace,
	}}}, snapshot.Manifest{}, false)
	var replacement *ReplacementRequiredError
	require.ErrorAs(t, err, &replacement)
	require.Equal(t, []string{"messages"}, replacement.Tables)
	legacyGuildColumns := []string{"id", "name", "icon", "raw_json", "updated_at"}
	legacyMemberColumns := []string{"guild_id", "user_id", "username", "updated_at"}
	entityPlan, err := shareMergePlan(snapshot.ImportPlan{Tables: []snapshot.TableImportPlan{
		{Table: snapshot.TableManifest{Name: "guilds", Columns: append(append([]string{}, legacyGuildColumns...), "deleted_at", "deletion_source", "deletion_reason")}, Mode: snapshot.TableImportReplace, Reason: "columns changed"},
		{Table: snapshot.TableManifest{Name: "members", Columns: append(append([]string{}, legacyMemberColumns...), "deleted_at", "deletion_source", "deletion_reason")}, Mode: snapshot.TableImportReplace, Reason: "columns changed"},
	}}, snapshot.Manifest{Tables: []snapshot.TableManifest{
		{Name: "guilds", Columns: legacyGuildColumns},
		{Name: "members", Columns: legacyMemberColumns},
	}}, false)
	require.NoError(t, err)
	require.Equal(t, snapshot.TableImportFiles, importPlanTable(t, entityPlan, "guilds").Mode)
	require.Equal(t, snapshot.TableImportFiles, importPlanTable(t, entityPlan, "members").Mode)

	_, err = shareMergePlan(snapshot.ImportPlan{Tables: []snapshot.TableImportPlan{{
		Table:  snapshot.TableManifest{Name: "guilds", Columns: []string{"id", "name", "updated_at", "deleted_at", "deletion_source", "deletion_reason"}},
		Mode:   snapshot.TableImportReplace,
		Reason: "columns changed",
	}}}, snapshot.Manifest{Tables: []snapshot.TableManifest{{
		Name: "guilds", Columns: []string{"id", "renamed_name", "updated_at"},
	}}}, false)
	require.ErrorAs(t, err, &replacement)
	require.Equal(t, []string{"guilds"}, replacement.Tables)

	_, err = shareMergePlan(snapshot.ImportPlan{Full: true, Reason: "manifest version changed"}, snapshot.Manifest{}, false)
	require.ErrorContains(t, err, "manifest version changed")
	_, err = shareMergePlan(snapshot.ImportPlan{Tables: []snapshot.TableImportPlan{{
		Table: snapshot.TableManifest{Name: "unknown"},
		Mode:  snapshot.TableImportFiles,
	}}}, snapshot.Manifest{}, false)
	require.ErrorContains(t, err, "unknown")
}

func TestMergeStateTracksChecksAndPendingReplacement(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	manifest := Manifest{Version: 1, GeneratedAt: time.Now().UTC().Truncate(time.Nanosecond)}

	require.False(t, ManifestAlreadyMerged(ctx, s, Manifest{}))
	require.False(t, ManifestAlreadyMerged(ctx, s, manifest))
	require.NoError(t, MarkReplacementPending(ctx, s, manifest, "table removed"))
	require.True(t, HasPendingReplacement(ctx, s))
	require.NoError(t, MarkMerged(ctx, s, manifest))
	require.False(t, HasPendingReplacement(ctx, s))
	require.True(t, ManifestAlreadyMerged(ctx, s, manifest))
	require.False(t, NeedsImport(ctx, s, 15*time.Minute))

	require.NoError(t, s.SetSyncState(ctx, LastMergeManifestSyncScope, "not-a-time"))
	require.False(t, ManifestAlreadyMerged(ctx, s, manifest))
	empty, err := eventTablesEmpty(ctx, s)
	require.NoError(t, err)
	require.True(t, empty)
}

func removeManifestTable(tables []snapshot.TableManifest, name string) []snapshot.TableManifest {
	out := make([]snapshot.TableManifest, 0, len(tables))
	for _, table := range tables {
		if table.Name != name {
			out = append(out, table)
		}
	}
	return out
}
