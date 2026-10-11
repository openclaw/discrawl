//go:build windows

package sqlitedurable

import (
	"path/filepath"
	"strings"
	"testing"

	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/stretchr/testify/require"
)

func TestWindowsVolumeRelativeWriterPaths(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	path := filepath.Join(root, "retained.sqlite")
	volume := filepath.VolumeName(path)
	if len(volume) != 2 || volume[1] != ':' {
		t.Skip("requires a drive-letter temporary directory")
	}
	s, err := Open(t.Context(), crawlstore.Options{Path: path})
	require.NoError(t, err)
	_, err = s.DB().ExecContext(t.Context(), "CREATE TABLE evidence(value TEXT); INSERT INTO evidence VALUES('retained')")
	require.NoError(t, err)
	require.NoError(t, s.Close())
	for _, candidate := range []string{strings.TrimPrefix(path, volume), volume + "retained.sqlite"} {
		t.Run(candidate, func(t *testing.T) {
			s, err := Open(t.Context(), crawlstore.Options{Path: candidate})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			var value string
			require.NoError(t, s.DB().QueryRowContext(t.Context(), "SELECT value FROM evidence").Scan(&value))
			require.Equal(t, "retained", value)
		})
	}
}
