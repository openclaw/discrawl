//go:build !windows

package store

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

type tailFailureOwnerFileInfo struct {
	os.FileInfo
	stat *syscall.Stat_t
}

func (i tailFailureOwnerFileInfo) Sys() any {
	return i.stat
}

func TestTailFailureUnixOwnerMustBeCurrentUser(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record.json")
	require.NoError(t, os.WriteFile(path, []byte("fixture"), 0o600))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, validateTailFailureFallbackOwner(info))

	stat, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	foreign := *stat
	foreign.Uid++
	require.ErrorContains(t, validateTailFailureFallbackOwner(tailFailureOwnerFileInfo{
		FileInfo: info,
		stat:     &foreign,
	}), "owner must be the current user")
}
