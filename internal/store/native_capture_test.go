package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func nativeTestContext(t *testing.T) context.Context {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native physical file identity is supported on macOS and Linux")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestNativeCaptureReplaysOnlyChangedRows(t *testing.T) {
	s, path := openChangeFeedStore(t)
	ctx := nativeTestContext(t)
	seedChangeFeedGuild(t, s)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("untouched", "c1", "old", nil, 0)}))
	// A malformed untouched payload must not be read by an incremental capture.
	_, err := s.DB().ExecContext(ctx, `update messages set content=x'ff' where id='untouched'`)
	require.NoError(t, err)
	baseline, err := ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("changed", "c1", "first", nil, 0)}))
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("changed", "c1", "final\x00text", nil, 0)}))
	require.NoError(t, s.SetSyncState(ctx, "coverage:old", "remove me"))
	require.NoError(t, s.DeleteSyncState(ctx, "coverage:old"))
	require.NoError(t, s.SetSyncState(ctx, "coverage:current", "proof\x00cursor"))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{GuildID: "g", UserID: "u", Username: "member", RoleIDsJSON: "[]", RawJSON: "{}"}))
	_, err = s.DB().ExecContext(ctx, `update channels set position=9007199254740993 where id='c1'; update guilds set name='renamed' where id='g'`)
	require.NoError(t, err)
	limits := NativeCaptureLimits{MaxEvents: 100, MaxBytes: 1 << 20}
	capture, err := CaptureNativeChanges(ctx, path, baseline, limits)
	require.NoError(t, err)
	require.Equal(t, baseline, capture.From)
	require.Greater(t, capture.Through.Cursor.Sequence, baseline.Cursor.Sequence)
	require.Greater(t, capture.EventsRead, len(capture.Changes))
	var message, channel, checkpoint, removal, guild, member *NativeChange
	for i := range capture.Changes {
		change := &capture.Changes[i]
		switch change.Table + ":" + change.Key[0].Text {
		case "messages:changed":
			message = change
		case "channels:c1":
			channel = change
		case "sync_state:coverage:current":
			checkpoint = change
		case "sync_state:coverage:old":
			removal = change
		case "guilds:g":
			guild = change
		case "members:g":
			member = change
		default:
			t.Fatalf("unexpected change: %s", change.Table)
		}
	}
	require.NotNil(t, message)
	require.Equal(t, NativeValue{Kind: "text", Text: "final\x00text"}, nativeColumn(t, *message, "content"))
	require.Equal(t, NativeValue{Kind: "null"}, nativeColumn(t, *message, "edited_at"))
	require.NotNil(t, channel)
	require.Equal(t, NativeValue{Kind: "integer", Integer: 9007199254740993}, nativeColumn(t, *channel, "position"))
	require.NotNil(t, checkpoint)
	require.Equal(t, NativeValue{Kind: "text", Text: "proof\x00cursor"}, nativeColumn(t, *checkpoint, "cursor"))
	require.NotNil(t, removal)
	require.Equal(t, "remove", removal.Operation)
	require.Nil(t, removal.Row)
	require.NotNil(t, guild)
	require.Equal(t, NativeValue{Kind: "text", Text: "renamed"}, nativeColumn(t, *guild, "name"))
	require.NotNil(t, member)
	require.Equal(t, []NativeValue{{Kind: "text", Text: "g"}, {Kind: "text", Text: "u"}}, member.Key)
	// Capturing does not acknowledge or write anything: the same input replays.
	replayed, err := CaptureNativeChanges(ctx, path, baseline, limits)
	require.NoError(t, err)
	require.Equal(t, capture, replayed)
	empty, err := CaptureNativeChanges(ctx, path, capture.Through, limits)
	require.NoError(t, err)
	require.Empty(t, empty.Changes)
	require.Equal(t, capture.Through, empty.Through)
}

func TestNativeCaptureKeyReplacementAndCompaction(t *testing.T) {
	s, path := openChangeFeedStore(t)
	ctx := nativeTestContext(t)
	require.NoError(t, s.SetSyncState(ctx, "old\x00key", "retained"))
	// A cursor exactly at the compaction floor remains valid.
	_, err := s.DB().ExecContext(ctx, `update change_log set recorded_at_ms=0`)
	require.NoError(t, err)
	baseline, err := ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	_, err = s.CompactChangeFeed(ctx, time.Now(), time.Hour)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `update sync_state set scope=? where scope=?`, "new\x00key", "old\x00key")
	require.NoError(t, err)
	capture, err := CaptureNativeChanges(ctx, path, baseline, NativeCaptureLimits{MaxEvents: 100, MaxBytes: 10000})
	require.NoError(t, err)
	require.Len(t, capture.Changes, 2)
	require.Equal(t, "remove", capture.Changes[0].Operation)
	require.Equal(t, "old\x00key", capture.Changes[0].Key[0].Text)
	require.Equal(t, "upsert", capture.Changes[1].Operation)
	require.Equal(t, "new\x00key", capture.Changes[1].Key[0].Text)
}

func nativeColumn(t *testing.T, change NativeChange, name string) NativeValue {
	t.Helper()
	for i, column := range change.Columns {
		if column == name {
			return change.Row[i]
		}
	}
	t.Fatalf("missing column %s", name)
	return NativeValue{}
}

func TestNativeCaptureBoundsDoNotAdvance(t *testing.T) {
	s, path := openChangeFeedStore(t)
	ctx := nativeTestContext(t)
	baseline, err := ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	require.NoError(t, s.SetSyncState(ctx, "first", "one"))
	require.NoError(t, s.SetSyncState(ctx, "second", "two"))
	for _, limits := range []NativeCaptureLimits{{MaxEvents: 1, MaxBytes: 10000}, {MaxEvents: 100, MaxBytes: 1}} {
		capture, err := CaptureNativeChanges(ctx, path, baseline, limits)
		require.ErrorIs(t, err, ErrNativeLimit)
		require.Equal(t, NativeCapture{}, capture)
	}
	capture, err := CaptureNativeChanges(ctx, path, baseline, NativeCaptureLimits{MaxEvents: 100, MaxBytes: 10000})
	require.NoError(t, err)
	require.Len(t, capture.Changes, 2)
}

func TestNativeCaptureBoundsGlobalCursorEntry(t *testing.T) {
	s, path := openChangeFeedStore(t)
	ctx := nativeTestContext(t)
	baseline, err := ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `insert into attachment_text_history(attachment_id,message_id,guild_id,channel_id,text_sha256,text_content,retained_at) values(?,'m','g','c',?,'body','now')`, strings.Repeat("key", 1000), "hash")
	require.NoError(t, err)
	capture, err := CaptureNativeChanges(ctx, path, baseline, NativeCaptureLimits{MaxEvents: 100, MaxBytes: 1024})
	require.ErrorIs(t, err, ErrNativeLimit)
	require.Equal(t, NativeCapture{}, capture)
}

func TestNativeCaptureUsesOneSnapshot(t *testing.T) {
	s, path := openChangeFeedStore(t)
	ctx := nativeTestContext(t)
	baseline, err := ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	require.NoError(t, s.SetSyncState(ctx, "checkpoint", "before"))
	reader, err := OpenReadOnly(ctx, path)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	tx, err := reader.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	head, _, err := readNativeCursor(ctx, tx, nil) // Pins the SQLite read snapshot.
	require.NoError(t, err)
	require.NoError(t, s.SetSyncState(ctx, "checkpoint", "after"))
	capture, err := captureNativeTx(ctx, tx, baseline, NativeCaptureLimits{MaxEvents: 100, MaxBytes: 10000})
	require.NoError(t, err)
	require.Equal(t, head, capture.Through.Cursor)
	require.Len(t, capture.Changes, 1)
	require.Equal(t, NativeValue{Kind: "text", Text: "before"}, nativeColumn(t, capture.Changes[0], "cursor"))
	require.NoError(t, tx.Commit())
	next, err := CaptureNativeChanges(ctx, path, capture.Through, NativeCaptureLimits{MaxEvents: 100, MaxBytes: 10000})
	require.NoError(t, err)
	require.Len(t, next.Changes, 1)
	require.Equal(t, NativeValue{Kind: "text", Text: "after"}, nativeColumn(t, next.Changes[0], "cursor"))
}

func TestNativeCaptureCoverageUsesSameSnapshot(t *testing.T) {
	s, path := openChangeFeedStore(t)
	ctx := nativeTestContext(t)
	seedChangeFeedGuild(t, s)
	baseline, err := ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	require.NoError(t, s.SetSyncState(ctx, "channel:c1:history_complete", "yes"))
	reader, err := OpenReadOnly(ctx, path)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	tx, err := reader.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, _, err = readNativeCursor(ctx, tx, nil)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `update channels set guild_id='other' where id='c1'`)
	require.NoError(t, err)
	captured, err := captureNativeTx(ctx, tx, baseline, NativeCaptureLimits{MaxEvents: 100, MaxBytes: 10000})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"channel:c1:history_complete": "g"}, captured.CoverageGuilds)
}

func TestNativeCaptureRejectsStaleBaseline(t *testing.T) {
	for _, mode := range []string{"generation", "anchor", "source", "trigger", "floor", "blob"} {
		t.Run(mode, func(t *testing.T) {
			s, path := openChangeFeedStore(t)
			ctx := nativeTestContext(t)
			require.NoError(t, s.SetSyncState(ctx, "checkpoint", "before"))
			baseline, err := ObserveNativeBaseline(ctx, path)
			require.NoError(t, err)
			want := ErrNativeBaseline
			switch mode {
			case "generation":
				tx, err := s.DB().BeginTx(ctx, nil)
				require.NoError(t, err)
				require.NoError(t, BumpChangeFeedGenerationTx(ctx, tx, "snapshot_import", "sync_state"))
				require.NoError(t, tx.Commit())
			case "anchor":
				_, err = s.DB().ExecContext(ctx, `update change_log set recorded_at_ms=recorded_at_ms+1 where seq=?`, baseline.Cursor.Sequence)
			case "source":
				other := filepath.Join(t.TempDir(), "other.db")
				require.NoError(t, s.Close())
				body, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(other, body, 0o600))
				path = other
			case "trigger":
				_, err = s.DB().ExecContext(ctx, `drop trigger change_feed_sync_state_au`)
			case "floor":
				require.NoError(t, s.SetSyncState(ctx, "later", "after"))
				_, err = s.DB().ExecContext(ctx, `update change_feed_meta set compacted_through=(select max(seq) from change_log)`)
			case "blob":
				_, err = s.DB().ExecContext(ctx, `update sync_state set cursor=x'ff' where scope='checkpoint'`)
				want = ErrNativeData
			}
			require.NoError(t, err)
			capture, err := CaptureNativeChanges(ctx, path, baseline, NativeCaptureLimits{MaxEvents: 100, MaxBytes: 10000})
			require.ErrorIs(t, err, want)
			require.Equal(t, NativeCapture{}, capture)
		})
	}
}
