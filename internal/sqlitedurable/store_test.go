package sqlitedurable

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/stretchr/testify/require"
)

func requireDurable(t *testing.T, conn *sql.Conn) {
	t.Helper()
	ctx := context.Background()
	for pragma, want := range map[string]int{"synchronous": 2, "foreign_keys": 1, "busy_timeout": 5000} {
		var got int
		require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&got))
		require.Equal(t, want, got, pragma)
	}
	if runtime.GOOS == "darwin" {
		for _, pragma := range []string{"fullfsync", "checkpoint_fullfsync"} {
			var got int
			require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&got))
			require.Equal(t, 1, got, pragma)
		}
	}
}

func TestDurabilityOnEveryPhysicalConnection(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.sqlite")
	s, err := Open(ctx, crawlstore.Options{Path: path, MaxOpenConns: 2})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	s.DB().SetMaxIdleConns(0)
	first, err := s.DB().Conn(ctx)
	require.NoError(t, err)
	second, err := s.DB().Conn(ctx)
	require.NoError(t, err)
	requireDurable(t, first)
	requireDurable(t, second)
	var mode string
	require.NoError(t, first.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode))
	require.Equal(t, "wal", mode)
	// Closing a deliberately changed fixture connection must not leak its
	// settings into a fresh one, nor rely on Open's initial readback alone.
	_, err = first.ExecContext(ctx, "PRAGMA synchronous=OFF; PRAGMA fullfsync=OFF; PRAGMA checkpoint_fullfsync=OFF")
	require.NoError(t, err)
	require.NoError(t, first.Close())
	require.NoError(t, second.Close())
	replacement, err := s.DB().Conn(ctx)
	require.NoError(t, err)
	requireDurable(t, replacement)
	require.NoError(t, replacement.Close())
	_, err = s.DB().ExecContext(ctx, "CREATE TABLE retained(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO retained VALUES(1,'retained')")
	require.NoError(t, err)
	reader, err := crawlstore.OpenReadOnly(ctx, path)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	var queryOnly int
	require.NoError(t, reader.DB().QueryRowContext(ctx, "PRAGMA query_only").Scan(&queryOnly))
	require.Equal(t, 1, queryOnly)
	_, err = reader.DB().ExecContext(ctx, "DELETE FROM retained")
	require.Error(t, err)
	var retained string
	require.NoError(t, reader.DB().QueryRowContext(ctx, "SELECT value FROM retained WHERE id=1").Scan(&retained))
	require.Equal(t, "retained", retained)
}

func TestWriterURIPathAndOverrides(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "spaces ?#% ü.sqlite")
	s, err := Open(ctx, crawlstore.Options{Path: path})
	require.NoError(t, err)
	require.NoError(t, s.Close())
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		require.NoError(t, os.Chmod(path, 0o644))
	}
	u := &url.URL{Scheme: "file", Path: filepath.ToSlash(path)}
	if runtime.GOOS == "windows" {
		u.Path = "/" + u.Path
	}
	q := u.Query()
	q.Set("_synchronous", "OFF")
	q.Set("_sync", "NORMAL")
	q.Add("_pragma", "fullfsync=OFF")
	q.Add("_pragma", "checkpoint_fullfsync(0)")
	u.RawQuery = q.Encode()
	s, err = Open(ctx, crawlstore.Options{Path: u.String()})
	require.NoError(t, err)
	conn, err := s.DB().Conn(ctx)
	require.NoError(t, err)
	requireDurable(t, conn)
	require.NoError(t, conn.Close())
	require.NoError(t, s.Close())
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	q.Set("mode", "ro")
	u.RawQuery = q.Encode()
	_, err = Open(ctx, crawlstore.Options{Path: u.String()})
	require.ErrorContains(t, err, "read-only")
}

func TestMemoryAndUncreatableWriter(t *testing.T) {
	s, err := Open(context.Background(), crawlstore.Options{Path: ":memory:"})
	require.NoError(t, err)
	require.NoError(t, s.Close())
	_, err = Open(context.Background(), crawlstore.Options{Path: ""})
	require.Error(t, err)
	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	_, err = Open(context.Background(), crawlstore.Options{Path: filepath.Join(file, "child")})
	require.Error(t, err)
}

func TestWriterPreservesSymlinkParentPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlink traversal; Windows uses its full-path resolver")
	}
	root := t.TempDir()
	target := filepath.Join(root, "target")
	require.NoError(t, os.MkdirAll(filepath.Join(target, "child"), 0o700))
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(filepath.Join(target, "child"), alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	// Keep the .. component: the kernel follows alias before processing it.
	path := alias + string(filepath.Separator) + ".." + string(filepath.Separator) + "retained.sqlite"
	original, err := crawlstore.Open(t.Context(), crawlstore.Options{Path: path})
	require.NoError(t, err)
	_, err = original.DB().ExecContext(t.Context(), "CREATE TABLE evidence(value TEXT); INSERT INTO evidence VALUES('original')")
	require.NoError(t, err)
	require.NoError(t, original.Close())
	upgraded, err := Open(t.Context(), crawlstore.Options{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, upgraded.Close()) })
	var value string
	require.NoError(t, upgraded.DB().QueryRowContext(t.Context(), "SELECT value FROM evidence").Scan(&value))
	require.Equal(t, "original", value)
	require.NoFileExists(t, filepath.Join(root, "retained.sqlite"))
}
