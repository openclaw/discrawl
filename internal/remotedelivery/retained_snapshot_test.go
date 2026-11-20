package remotedelivery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/crawlkit/snapshot"
	"github.com/stretchr/testify/require"
)

func retainedFixture(t *testing.T) (context.Context, string, string, map[string]string) {
	t.Helper()
	ctx, source, path, _ := projectionFixture(t)
	for _, guild := range []string{"g1", "g2", "@me"} {
		writeMessage(t, ctx, source, "m-"+guild, guild)
	}
	for _, scope := range []string{"guild:g1:members", "channel:cg1:history_complete", "channel:cg2:history_complete", "channel:missing:history_complete", "worker:private"} {
		require.NoError(t, source.SetSyncState(ctx, scope, "cursor\x00exact"))
	}
	ddl := map[string]string{}
	for table := range columns {
		var sql string
		require.NoError(t, source.DB().QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", table).Scan(&sql))
		ddl[table] = sql
	}
	_, err := source.DB().ExecContext(ctx, "DROP TABLE change_feed_meta")
	require.NoError(t, err)
	require.NoError(t, source.Close())
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	hash := sha256.Sum256(body)
	return ctx, path, hex.EncodeToString(hash[:]), ddl
}

func retainedLimits() RetainedSnapshotLimits {
	return RetainedSnapshotLimits{MaxSourceBytes: 64 << 20, MaxRows: 100, MaxBytes: 1 << 20}
}

func TestRetainedSnapshotRoundtripWithoutJournal(t *testing.T) {
	ctx, path, hash, ddl := retainedFixture(t)
	escaped := filepath.Join(filepath.Dir(path), "source ? #.db")
	require.NoError(t, os.Rename(path, escaped))
	path = escaped
	// A stopped SQLite database may retain an empty WAL and a nonempty SHM.
	require.NoError(t, os.WriteFile(path+"-wal", nil, 0o600))
	require.NoError(t, os.WriteFile(path+"-shm", make([]byte, 32768), 0o600))
	before, err := retainedSourceState(path, 64<<20)
	require.NoError(t, err)
	root := filepath.Join(t.TempDir(), "retained ? #")
	prepared, qualification, err := PrepareRetainedSnapshot(ctx, path, root, hash, Scope{GuildIDs: []string{"g1"}}, retainedLimits())
	require.NoError(t, err)
	require.Equal(t, RetainedSnapshotFormat, prepared.Format)
	require.Equal(t, int64(5), prepared.Rows)
	require.Equal(t, hash, prepared.SourceFileSHA256)
	receipt, err := os.ReadFile(filepath.Join(root, "preparation.json"))
	require.NoError(t, err)
	digest := sha256.Sum256(receipt)
	require.Equal(t, hex.EncodeToString(digest[:]), qualification)
	require.NotContains(t, string(receipt), "baseline")
	require.NotContains(t, string(receipt), "native_sequence")
	after, err := retainedSourceState(path, 64<<20)
	require.NoError(t, err)
	require.True(t, sameRetainedSource(before, after))
	require.NoError(t, retainedSourceHash(ctx, path, hash, 64<<20))
	target, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer func() { _ = target.Close() }()
	for _, sql := range ddl {
		_, err = target.ExecContext(ctx, sql)
		require.NoError(t, err)
	}
	_, err = snapshot.Import(ctx, snapshot.ImportOptions{DB: target, RootDir: filepath.Join(root, "snapshot")})
	require.NoError(t, err)
	for table, want := range map[string]int{"guilds": 1, "channels": 1, "messages": 1, "members": 0, "sync_state": 2} {
		var count int
		require.NoError(t, target.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count))
		require.Equal(t, want, count, table)
	}
	var content, cursor string
	require.NoError(t, target.QueryRowContext(ctx, "SELECT content FROM messages WHERE id='m-g1'").Scan(&content))
	require.Equal(t, "text\x00preserved", content)
	require.NoError(t, target.QueryRowContext(ctx, "SELECT cursor FROM sync_state WHERE scope='channel:cg1:history_complete'").Scan(&cursor))
	require.Equal(t, "cursor\x00exact", cursor)
	_, _, err = PrepareRetainedSnapshot(ctx, path, root, hash, prepared.Scope, retainedLimits())
	require.Error(t, err)
	unchanged, err := os.ReadFile(filepath.Join(root, "preparation.json"))
	require.NoError(t, err)
	require.Equal(t, receipt, unchanged)
}

func TestRetainedSnapshotFailsClosed(t *testing.T) {
	ctx, path, hash, _ := retainedFixture(t)
	for _, tc := range []struct {
		name, sha string
		scope     Scope
		limits    RetainedSnapshotLimits
	}{
		{"wrong-hash", strings.Repeat("0", 64), Scope{GuildIDs: []string{"g1"}}, retainedLimits()},
		{"dm", hash, Scope{GuildIDs: []string{"@me"}}, retainedLimits()},
		{"missing-guild", hash, Scope{GuildIDs: []string{"missing"}}, retainedLimits()},
		{"rows", hash, Scope{GuildIDs: []string{"g1"}}, RetainedSnapshotLimits{64 << 20, 1, 1 << 20}},
		{"bytes", hash, Scope{GuildIDs: []string{"g1"}}, RetainedSnapshotLimits{64 << 20, 100, 1}},
		{"source-size", hash, Scope{GuildIDs: []string{"g1"}}, RetainedSnapshotLimits{1, 100, 1 << 20}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "out")
			_, _, err := PrepareRetainedSnapshot(ctx, path, root, tc.sha, tc.scope, tc.limits)
			require.Error(t, err)
			require.NoFileExists(t, filepath.Join(root, "preparation.json"))
		})
	}
	require.NoError(t, os.WriteFile(path+"-wal", []byte("pending"), 0o600))
	root := filepath.Join(t.TempDir(), "wal")
	_, _, err := PrepareRetainedSnapshot(ctx, path, root, hash, Scope{GuildIDs: []string{"g1"}}, retainedLimits())
	require.Error(t, err)
	require.NoDirExists(t, root)
	require.NoError(t, os.Remove(path+"-wal"))
	root = filepath.Join(t.TempDir(), "deadline")
	_, _, err = PrepareRetainedSnapshot(context.Background(), path, root, hash, Scope{GuildIDs: []string{"g1"}}, retainedLimits())
	require.Error(t, err)
	require.NoDirExists(t, root)
}

func TestRetainedSnapshotRejectsIncompatibleEmptyTable(t *testing.T) {
	ctx, path, _, _ := retainedFixture(t)
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "ALTER TABLE members ADD COLUMN unrepresented TEXT")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	hash := sha256.Sum256(body)
	root := filepath.Join(t.TempDir(), "incompatible")
	_, _, err = PrepareRetainedSnapshot(ctx, path, root, hex.EncodeToString(hash[:]), Scope{GuildIDs: []string{"g1"}}, retainedLimits())
	require.ErrorIs(t, err, ErrProjection)
	require.NoFileExists(t, filepath.Join(root, "preparation.json"))
}

func TestRetainedSnapshotRejectsEscapedRowAboveLimit(t *testing.T) {
	ctx, path, _, _ := retainedFixture(t)
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	// Raw text remains under 4 MiB; JSON encodes every NUL as six bytes.
	content := strings.Repeat("\x00", 800000)
	_, err = db.ExecContext(ctx, "UPDATE messages SET content=? WHERE id='m-g1'", content)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	hash := sha256.Sum256(body)
	root := filepath.Join(t.TempDir(), "escaped")
	limits := retainedLimits()
	limits.MaxBytes = 16 << 20
	_, _, err = PrepareRetainedSnapshot(ctx, path, root, hex.EncodeToString(hash[:]), Scope{GuildIDs: []string{"g1"}}, limits)
	require.ErrorContains(t, err, "retained encoded row too large")
	require.NoFileExists(t, filepath.Join(root, "preparation.json"))
}
