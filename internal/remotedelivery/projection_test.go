package remotedelivery

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func projectionFixture(t *testing.T) (context.Context, *store.Store, string, store.NativeBaseline) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native capture identity is supported on macOS and Linux")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	path := filepath.Join(t.TempDir(), "source.db")
	s, err := store.Open(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	for _, guild := range []string{"g1", "g2", "@me"} {
		require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: guild, Name: guild, RawJSON: "{}"}))
		require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c" + guild, GuildID: guild, Name: guild, Kind: "text", RawJSON: "{}"}))
	}
	baseline, err := store.ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	return ctx, s, path, baseline
}

func capture(t *testing.T, ctx context.Context, path string, baseline store.NativeBaseline) store.NativeCapture {
	t.Helper()
	captured, err := store.CaptureNativeChanges(ctx, path, baseline, store.NativeCaptureLimits{MaxEvents: 1000, MaxBytes: 1 << 20})
	require.NoError(t, err)
	return captured
}

func writeMessage(t *testing.T, ctx context.Context, s *store.Store, id, guild string) {
	t.Helper()
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{Record: store.MessageRecord{
		ID: id, GuildID: guild, ChannelID: "c" + guild, AuthorID: "u", CreatedAt: "2026-11-14T00:00:00Z",
		Content: "text\x00preserved", NormalizedContent: "text", RawJSON: "{}",
	}}}))
}

func ownershipFor(t *testing.T, captured store.NativeCapture, owned map[string]bool) OwnershipSnapshot {
	t.Helper()
	hash, err := ScopeSHA256(Scope{GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	answers := map[string]bool{}
	for _, change := range captured.Changes {
		key, err := OwnershipKey(change.Table, change.Key)
		require.NoError(t, err)
		answers[key] = owned[key]
	}
	return OwnershipSnapshot{Baseline: captured.From, ScopeSHA256: hash, Keys: answers}
}

func TestProjectNativeCaptureScopesAndTypedWire(t *testing.T) {
	ctx, s, path, baseline := projectionFixture(t)
	for _, guild := range []string{"g1", "g2", "@me"} {
		writeMessage(t, ctx, s, "m"+guild, guild)
		require.NoError(t, s.SetSyncState(ctx, "channel:c"+guild+":history_complete", "complete"))
	}
	require.NoError(t, s.SetSyncState(ctx, "wiretap:private", "local path"))
	require.NoError(t, s.SetSyncState(ctx, "share:last_import", "local receipt"))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u", Username: "person", RoleIDsJSON: "[]", RawJSON: "{}"}))
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "updated", RawJSON: "{}"}))
	_, err := s.DB().ExecContext(ctx, `update channels set position=9007199254740993 where id='cg1'`)
	require.NoError(t, err)
	captured := capture(t, ctx, path, baseline)
	channelKey, err := OwnershipKey("channels", []store.NativeValue{{Kind: "text", Text: "cg1"}})
	require.NoError(t, err)
	projected, err := ProjectCapture(captured, Scope{GuildIDs: []string{"g1"}}, ownershipFor(t, captured, map[string]bool{channelKey: true}))
	require.NoError(t, err)
	require.Len(t, projected.Changes, 5)
	for _, change := range projected.Changes {
		require.Equal(t, "upsert", change.Operation)
		switch change.Table {
		case "messages":
			require.Equal(t, "mg1", *change.Key[0].Value)
			require.Equal(t, "text\x00preserved", *change.Row[8].Value)
			require.Equal(t, "null", change.Row[6].Kind)
		case "channels":
			require.Equal(t, "9007199254740993", *change.Row[6].Value)
		case "sync_state":
			require.Equal(t, "channel:cg1:history_complete", *change.Key[0].Value)
		}
	}
	anchor, err := NativeAnchor(captured.Through.Cursor)
	require.NoError(t, err)
	require.Less(t, len(anchor), 512)
	batch := remote.ChangeBatch{Contract: remote.ChangeContract, App: "discrawl", Archive: "fixture", BindingID: "binding", CaptureID: strings.Repeat("a", 32), PartCount: 1, NativeFrom: "qualified-seed", NativeThrough: anchor, Changes: projected.Changes}
	body, err := remote.EncodeChangeBatch(batch)
	require.NoError(t, err)
	decoded, err := remote.DecodeChangeBatch(body)
	require.NoError(t, err)
	require.Equal(t, projected.Changes, decoded.Changes)
	require.NotContains(t, string(body), "private")
	require.NotContains(t, string(body), "SourceID")
	require.NotContains(t, string(body), "@me")
}

func TestProjectScopeExitAndOwnedRemoval(t *testing.T) {
	ctx, s, path, baseline := projectionFixture(t)
	writeMessage(t, ctx, s, "exit", "g1")
	writeMessage(t, ctx, s, "delete", "g1")
	writeMessage(t, ctx, s, "private-delete", "@me")
	initial := capture(t, ctx, path, baseline)
	owned := map[string]bool{}
	first, err := ProjectCapture(initial, Scope{GuildIDs: []string{"g1"}}, ownershipFor(t, initial, nil))
	require.NoError(t, err)
	for _, entry := range first.Ownership {
		owned[entry.Key] = entry.Owned
	}
	_, err = s.DB().ExecContext(ctx, `update messages set guild_id='g2',channel_id='cg2' where id='exit'; delete from messages where id in ('delete','private-delete')`)
	require.NoError(t, err)
	next := capture(t, ctx, path, initial.Through)
	projected, err := ProjectCapture(next, Scope{GuildIDs: []string{"g1"}}, ownershipFor(t, next, owned))
	require.NoError(t, err)
	require.Len(t, projected.Changes, 2)
	var removed []string
	for _, change := range projected.Changes {
		require.Equal(t, "remove", change.Operation)
		require.Nil(t, change.Row)
		removed = append(removed, *change.Key[0].Value)
	}
	require.ElementsMatch(t, []string{"exit", "delete"}, removed)
	for _, entry := range projected.Ownership {
		require.False(t, entry.Owned)
	}
}

func TestProjectRequiresDependencyReconciliation(t *testing.T) {
	ctx, s, path, baseline := projectionFixture(t)
	_, err := s.DB().ExecContext(ctx, `update channels set guild_id='g2' where id='cg1'`)
	require.NoError(t, err)
	captured := capture(t, ctx, path, baseline)
	key, err := OwnershipKey("channels", []store.NativeValue{{Kind: "text", Text: "cg1"}})
	require.NoError(t, err)
	projected, err := ProjectCapture(captured, Scope{GuildIDs: []string{"g1"}}, ownershipFor(t, captured, map[string]bool{key: true}))
	require.ErrorIs(t, err, ErrReconcile)
	require.Equal(t, Projection{}, projected)
	baseline, err = store.ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	require.NoError(t, s.SetSyncState(ctx, "channel:unknown:history_complete", "yes"))
	missing := capture(t, ctx, path, baseline)
	projected, err = ProjectCapture(missing, Scope{GuildIDs: []string{"g1"}}, ownershipFor(t, missing, nil))
	require.ErrorIs(t, err, ErrReconcile)
	require.Equal(t, Projection{}, projected)
}

func TestProjectionRejectsInvalidScopeAndRow(t *testing.T) {
	for _, scope := range []Scope{{}, {GuildIDs: []string{"@me"}}, {GuildIDs: []string{"g1", "g1"}}, {GuildIDs: []string{" g1"}}} {
		_, err := ProjectCapture(store.NativeCapture{}, scope, OwnershipSnapshot{})
		require.ErrorIs(t, err, ErrProjection)
	}
	ctx, s, path, baseline := projectionFixture(t)
	writeMessage(t, ctx, s, "m", "g1")
	captured := capture(t, ctx, path, baseline)
	captured.Changes[0].Columns[0] = "unexpected"
	_, err := ProjectCapture(captured, Scope{GuildIDs: []string{"g1"}}, ownershipFor(t, captured, nil))
	require.ErrorIs(t, err, ErrProjection)
}

func TestProjectionRejectsMissingOrStaleOwnership(t *testing.T) {
	ctx, s, path, baseline := projectionFixture(t)
	writeMessage(t, ctx, s, "m", "g1")
	captured := capture(t, ctx, path, baseline)
	for _, mode := range []string{"missing", "baseline", "scope"} {
		t.Run(mode, func(t *testing.T) {
			prior := ownershipFor(t, captured, nil)
			switch mode {
			case "missing":
				prior.Keys = nil
			case "baseline":
				prior.Baseline.Cursor.Sequence++
			case "scope":
				prior.ScopeSHA256 = "different"
			}
			result, err := ProjectCapture(captured, Scope{GuildIDs: []string{"g1"}}, prior)
			require.ErrorIs(t, err, ErrOwnership)
			require.Equal(t, Projection{}, result)
		})
	}
}

func TestProjectChannelEntryRequiresCheckpointReconciliation(t *testing.T) {
	ctx, s, path, _ := projectionFixture(t)
	require.NoError(t, s.SetSyncState(ctx, "channel:cg2:history_complete", "old checkpoint"))
	baseline, err := store.ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `update channels set guild_id='g1' where id='cg2'`)
	require.NoError(t, err)
	captured := capture(t, ctx, path, baseline)
	require.Len(t, captured.Changes, 1, "the old checkpoint is absent from the changed-key batch")
	projected, err := ProjectCapture(captured, Scope{GuildIDs: []string{"g1"}}, ownershipFor(t, captured, nil))
	require.ErrorIs(t, err, ErrReconcile)
	require.Equal(t, Projection{}, projected)
}
