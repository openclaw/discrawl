//go:build windows

package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestTailFailureWindowsACLProtectsDirectoryAndFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fallback")
	require.NoError(t, createTailFailureFallbackDir(path))
	root, err := os.OpenRoot(path)
	require.NoError(t, err)
	defer func() { _ = root.Close() }()
	dir, err := root.Open(".")
	require.NoError(t, err)
	defer func() { _ = dir.Close() }()
	dirInfo, err := dir.Stat()
	require.NoError(t, err)
	require.NoError(t, validateTailFailureFallbackDir(dir, dirInfo))

	file, err := root.OpenFile("record.json", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()
	require.NoError(t, secureTailFailureFallbackTempFile(dir, "record.json"))
	fileInfo, err := file.Stat()
	require.NoError(t, err)
	require.NoError(t, validateTailFailureFallbackFile(file, fileInfo))

	permissive, err := windows.SecurityDescriptorFromString("D:(A;;FA;;;WD)")
	require.NoError(t, err)
	dacl, _, err := permissive.DACL()
	require.NoError(t, err)
	extendedPath, err := tailFailureExtendedWindowsPath(path)
	require.NoError(t, err)
	pathPtr, err := windows.UTF16PtrFromString(extendedPath)
	require.NoError(t, err)
	writeDACL, err := windows.CreateFile(
		pathPtr,
		windows.WRITE_DAC,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	require.NoError(t, err)
	defer func() { _ = windows.CloseHandle(writeDACL) }()
	require.NoError(t, windows.SetSecurityInfo(
		writeDACL,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.UNPROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		dacl,
		nil,
	))
	require.Error(t, validateTailFailureFallbackDir(dir, dirInfo))
}

func TestRenameTailFailureWindowsIsNoReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fallback")
	require.NoError(t, createTailFailureFallbackDir(path))
	root, err := os.OpenRoot(path)
	require.NoError(t, err)
	defer func() { _ = root.Close() }()
	dir, err := root.Open(".")
	require.NoError(t, err)
	defer func() { _ = dir.Close() }()

	for _, name := range []string{"first.tmp", "second.tmp"} {
		file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		require.NoError(t, err)
		_, err = file.WriteString(name)
		require.NoError(t, err)
		require.NoError(t, file.Sync())
		require.NoError(t, file.Close())
	}
	require.NoError(t, renameTailFailureNoReplace(root, dir, "first.tmp", "record.json"))
	require.Error(t, renameTailFailureNoReplace(root, dir, "second.tmp", "record.json"))
	content, err := root.ReadFile("record.json")
	require.NoError(t, err)
	require.Equal(t, "first.tmp", string(content))
}

func TestTailFailureWindowsLongPath(t *testing.T) {
	parent := t.TempDir()
	for len(filepath.Join(parent, "fallback")) < 300 {
		parent = filepath.Join(parent, strings.Repeat("a", 30))
	}
	require.NoError(t, os.MkdirAll(parent, 0o700))
	path := filepath.Join(parent, "fallback")
	require.NoError(t, createTailFailureFallbackDir(path))
	root, err := os.OpenRoot(path)
	require.NoError(t, err)
	defer func() { _ = root.Close() }()
	dir, err := root.Open(".")
	require.NoError(t, err)
	defer func() { _ = dir.Close() }()
	dirInfo, err := dir.Stat()
	require.NoError(t, err)
	require.NoError(t, validateTailFailureFallbackDir(dir, dirInfo))

	file, err := root.OpenFile("record.tmp", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	require.NoError(t, err)
	require.NoError(t, secureTailFailureFallbackTempFile(dir, "record.tmp"))
	require.NoError(t, file.Close())
	require.NoError(t, renameTailFailureNoReplace(root, dir, "record.tmp", "record.json"))
	committed, err := root.Open("record.json")
	require.NoError(t, err)
	defer func() { _ = committed.Close() }()
	fileInfo, err := committed.Stat()
	require.NoError(t, err)
	require.NoError(t, validateTailFailureFallbackFile(committed, fileInfo))
}
