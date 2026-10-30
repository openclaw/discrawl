package share

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestUnchangedRoleRevocationFencesDelayedRepairPublication(t *testing.T) {
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	role := &discordgo.Role{ID: "guild", Permissions: 0}
	guild := &discordgo.Guild{ID: "guild", Name: "Fixture", Roles: []*discordgo.Role{role}}
	body, err := json.Marshal(guild)
	require.NoError(t, err)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "guild", Name: "Fixture", RawJSON: string(body)}))
	require.NoError(t, s.ApplyGuildRole(ctx, "guild", "guild", role))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "selected", GuildID: "guild", Kind: "text", RawJSON: `{"id":"selected","permission_overwrites":[]}`}))
	upsertSnapshotFilterMessage(t, ctx, s, "m", "selected", "u", "private")
	allowPublicFixtureChannels(t, s)
	var before, after string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from guilds where id='guild'`).Scan(&before))
	started := time.Now()
	time.Sleep(time.Millisecond)
	require.NoError(t, s.ApplyGuildRole(ctx, "guild", "guild", role))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from guilds where id='guild'`).Scan(&after))
	require.Equal(t, before, after)
	guild.Roles[0] = &discordgo.Role{ID: "guild", Permissions: 1024}
	body, err = json.Marshal(guild)
	require.NoError(t, err)
	require.NoError(t, s.UpsertObservedGuild(ctx, store.GuildRecord{ID: "guild", Name: "Fixture", RawJSON: string(body)}, started))
	report, err := PreflightPublishScope(ctx, s, FilterOptions{PublicOnly: true})
	require.NoError(t, err)
	require.True(t, report.Ready)
	require.Zero(t, report.Messages.Allowed)
	filter, err := newSnapshotFilter(ctx, s.DB(), FilterOptions{PublicOnly: true})
	require.NoError(t, err)
	require.False(t, filter.allowChannelID("selected"))
}

func TestPermissionEvidenceNullToEmptyRecoversPublication(t *testing.T) {
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "guild", RawJSON: `{"id":"guild","roles":[{"id":"guild","permissions":"1024"}]}`}))
	c := store.ChannelRecord{ID: "selected", GuildID: "guild", Kind: "text", RawJSON: `{"id":"selected","permission_overwrites":null}`}
	require.NoError(t, s.UpsertChannel(ctx, c))
	upsertSnapshotFilterMessage(t, ctx, s, "m", "selected", "u", "public")
	allowPublicFixtureChannels(t, s)
	report, err := PreflightPublishScope(ctx, s, FilterOptions{PublicOnly: true})
	require.NoError(t, err)
	require.False(t, report.Ready)
	c.RawJSON = `{"id":"selected","permission_overwrites":[]}`
	require.NoError(t, s.UpsertChannel(ctx, c))
	report, err = PreflightPublishScope(ctx, s, FilterOptions{PublicOnly: true})
	require.NoError(t, err)
	require.True(t, report.Ready)
	require.Equal(t, 1, report.Messages.Allowed)
	filter, err := newSnapshotFilter(ctx, s.DB(), FilterOptions{PublicOnly: true})
	require.NoError(t, err)
	require.True(t, filter.allowChannelID("selected"))
}

func TestExactImportMapsHistoricalMentionIDReuse(t *testing.T) {
	ctx := t.Context()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	dst := seedStore(t, filepath.Join(t.TempDir(), "dst.db"))
	defer func() { _ = dst.Close() }()
	var oldID int64
	var oldMessage string
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select event_id,message_id from mention_events order by event_id limit 1`).Scan(&oldID, &oldMessage))
	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo})
	require.NoError(t, err)
	manifest = appendSnapshotRow(t, repo, manifest, "mention_events", map[string]any{"event_id": oldID, "message_id": "different", "guild_id": "g1", "channel_id": "c1", "target_type": "user", "target_id": "different", "target_name": "different", "event_at": "2026-10-30T00:00:00Z"})
	writeShareManifest(t, repo, manifest)
	_, err = Import(ctx, dst, Options{RepoPath: repo})
	require.NoError(t, err)
	var after string
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select message_id from mention_events where event_id=?`, oldID).Scan(&after))
	require.Equal(t, oldMessage, after)
	// Both original local and imported source identities remain reconstructible.
	var retained int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from mention_event_history`).Scan(&retained))
	require.Positive(t, retained)
	var mapped int64
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select local_event_id from mention_event_id_map where source_event_id=? and identity_key='["different","user","different"]'`, oldID).Scan(&mapped))
	require.NotEqual(t, oldID, mapped)
}

func TestRoutineMergeHonorsNativeObservationFences(t *testing.T) {
	ctx := t.Context()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	dst := seedStore(t, filepath.Join(t.TempDir(), "dst.db"))
	defer func() { _ = dst.Close() }()
	// Pin source revisions around an unchanged local observation, avoiding wall
	// clock sleeps while exercising the public MergeIfChanged entrypoint.
	for _, s := range []*store.Store{src, dst} {
		_, err := s.DB().ExecContext(ctx, `update guilds set raw_json='{"id":"g1","roles":[{"id":"g1","permissions":"0"}]}'; update channels set raw_json='{"permission_overwrites":[]}',collection_scope='allowed'`)
		require.NoError(t, err)
	}
	for _, table := range []string{"guilds", "channels", "members", "messages"} {
		_, err := dst.DB().ExecContext(ctx, "update "+table+" set updated_at='2026-10-30T10:00:00Z'")
		require.NoError(t, err)
		_, err = src.DB().ExecContext(ctx, "update "+table+" set updated_at='2026-10-30T10:01:00Z'")
		require.NoError(t, err)
	}
	_, err := src.DB().ExecContext(ctx, `update guilds set raw_json='{"id":"g1","roles":[{"id":"g1","permissions":"1024"}]}',name='stale guild';update channels set name='stale channel';update members set username='stale member';update messages set content='stale message',normalized_content='stale message'`)
	require.NoError(t, err)
	_, err = dst.DB().ExecContext(ctx, `update entity_checks set checked_at='2026-10-30T10:02:00.000000000Z' where check_kind='observed'`)
	require.NoError(t, err)
	repo := filepath.Join(t.TempDir(), "share")
	_, err = Export(ctx, src, Options{RepoPath: repo})
	require.NoError(t, err)
	_, changed, err := MergeIfChanged(ctx, dst, Options{RepoPath: repo})
	require.NoError(t, err)
	require.True(t, changed)
	for _, q := range []string{`select count(*) from guilds where name='stale guild'`, `select count(*) from channels where name='stale channel'`, `select count(*) from members where username='stale member'`, `select count(*) from messages where content='stale message'`} {
		var n int
		require.NoError(t, dst.DB().QueryRowContext(ctx, q).Scan(&n))
		require.Zero(t, n)
	}
	report, err := PreflightPublishScope(ctx, dst, FilterOptions{PublicOnly: true})
	require.NoError(t, err)
	require.True(t, report.Ready)
	require.Zero(t, report.Messages.Allowed)
}

func TestExactReplacementResetsOnlySupersededObservationFences(t *testing.T) {
	ctx := t.Context()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	dst := seedStore(t, filepath.Join(t.TempDir(), "dst.db"))
	defer func() { _ = dst.Close() }()
	require.NoError(t, dst.UpsertGuild(ctx, store.GuildRecord{ID: "@me", RawJSON: `{}`}))
	require.NoError(t, dst.UpsertChannel(ctx, store.ChannelRecord{ID: "dm", GuildID: "@me", Kind: "dm", RawJSON: `{}`}))
	require.NoError(t, dst.UpsertMessage(ctx, store.MessageRecord{ID: "private", GuildID: "@me", ChannelID: "dm", Content: "private", NormalizedContent: "private", RawJSON: `{}`}))
	var checksBefore int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from entity_check_history`).Scan(&checksBefore))
	repo := filepath.Join(t.TempDir(), "share")
	_, err := Export(ctx, src, Options{RepoPath: repo})
	require.NoError(t, err)
	_, err = Import(ctx, dst, Options{RepoPath: repo})
	require.NoError(t, err)
	var checksAfter, overlays, private int
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from entity_check_history`).Scan(&checksAfter))
	require.Equal(t, checksBefore, checksAfter)
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from entity_checks where table_name='messages'`).Scan(&overlays))
	require.Equal(t, 1, overlays)
	require.NoError(t, dst.DB().QueryRowContext(ctx, `select count(*) from entity_checks where table_name='messages' and row_key='["private"]'`).Scan(&private))
	require.Equal(t, 1, private)
}
