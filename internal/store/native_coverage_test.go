package store

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeChannelCoverageUsesCaptureSnapshot(t *testing.T) {
	s, path := openChangeFeedStore(t)
	ctx := nativeTestContext(t)
	seedChangeFeedGuild(t, s)
	require.NoError(t, s.SetSyncState(ctx, "channel:c1:history_complete", "before"))
	baseline, err := ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `update channels set name='changed' where id='c1'`)
	require.NoError(t, err)
	reader, err := OpenReadOnly(ctx, path)
	require.NoError(t, err)
	defer reader.Close()
	tx, err := reader.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer tx.Rollback()
	head, _, err := readNativeCursor(ctx, tx, nil)
	require.NoError(t, err)
	require.NoError(t, s.SetSyncState(ctx, "channel:c1:history_complete", "after"))
	require.NoError(t, s.SetSyncState(ctx, "channel:c1:new", "later"))
	captured, err := captureNativeTx(ctx, tx, baseline, NativeCaptureLimits{MaxEvents: 10, MaxBytes: 10000})
	require.NoError(t, err)
	require.Equal(t, head, captured.Through.Cursor)
	entry, present := captured.ChannelCoverage["c1"]
	require.True(t, present)
	require.True(t, entry.Present)
	require.Equal(t, "g", entry.GuildID)
	require.Len(t, entry.Checkpoints, 1)
	require.Equal(t, NativeValue{Kind: "text", Text: "before"}, nativeColumn(t, entry.Checkpoints[0], "cursor"))
	require.NoError(t, tx.Commit())
	next, err := CaptureNativeChanges(ctx, path, captured.Through, NativeCaptureLimits{MaxEvents: 10, MaxBytes: 10000})
	require.NoError(t, err)
	require.Len(t, next.Changes, 2)
	require.Empty(t, next.ChannelCoverage, "unchanged channels do not trigger dependency reads")
}

func TestNativeChannelCoverageAbsentAndLiteralPrefix(t *testing.T) {
	s, path := openChangeFeedStore(t)
	ctx := nativeTestContext(t)
	seedChangeFeedGuild(t, s)
	channel := "c_%\"\\tail"
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: channel, GuildID: "g", Name: "literal", Kind: "text", RawJSON: "{}"}))
	for _, scope := range []string{"channel:" + channel + ":history_complete", "channel:c_XX:history_complete", "channel:" + channel + "x:history_complete"} {
		require.NoError(t, s.SetSyncState(ctx, scope, "retained"))
	}
	baseline, err := ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `delete from channels where id=?`, channel)
	require.NoError(t, err)
	captured, err := CaptureNativeChanges(ctx, path, baseline, NativeCaptureLimits{MaxEvents: 10, MaxBytes: 10000})
	require.NoError(t, err)
	entry, present := captured.ChannelCoverage[channel]
	require.True(t, present)
	require.False(t, entry.Present)
	require.Empty(t, entry.GuildID)
	require.Len(t, entry.Checkpoints, 1)
	require.Equal(t, "channel:"+channel+":history_complete", entry.Checkpoints[0].Key[0].Text)
}

func TestNativeChannelCoverageBoundsUnchangedDependencies(t *testing.T) {
	s, path := openChangeFeedStore(t)
	ctx := nativeTestContext(t)
	seedChangeFeedGuild(t, s)
	require.NoError(t, s.SetSyncState(ctx, "channel:c1:history_complete", strings.Repeat("large", 1000)))
	baseline, err := ObserveNativeBaseline(ctx, path)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `update channels set name='changed' where id='c1'`)
	require.NoError(t, err)
	for _, limits := range []NativeCaptureLimits{{MaxEvents: 1, MaxBytes: 10000}, {MaxEvents: 10, MaxBytes: 1024}} {
		captured, err := CaptureNativeChanges(ctx, path, baseline, limits)
		require.ErrorIs(t, err, ErrNativeLimit)
		require.Equal(t, NativeCapture{}, captured)
	}
	captured, err := CaptureNativeChanges(ctx, path, baseline, NativeCaptureLimits{MaxEvents: 2, MaxBytes: 10000})
	require.NoError(t, err)
	require.Len(t, captured.ChannelCoverage["c1"].Checkpoints, 1)
}
