//go:build windows

package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

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

	insecurePath := filepath.Join(t.TempDir(), "insecure")
	permissive, err := windows.SecurityDescriptorFromString("D:(A;;FA;;;WD)")
	require.NoError(t, err)
	extendedPath, err := tailFailureExtendedWindowsPath(insecurePath)
	require.NoError(t, err)
	pathPtr, err := windows.UTF16PtrFromString(extendedPath)
	require.NoError(t, err)
	require.NoError(t, windows.CreateDirectory(pathPtr, &windows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
		SecurityDescriptor: permissive,
	}))
	insecureRoot, err := os.OpenRoot(insecurePath)
	require.NoError(t, err)
	defer func() { _ = insecureRoot.Close() }()
	insecureDir, err := insecureRoot.Open(".")
	require.NoError(t, err)
	defer func() { _ = insecureDir.Close() }()
	insecureInfo, err := insecureDir.Stat()
	require.NoError(t, err)
	require.Error(t, validateTailFailureFallbackDir(insecureDir, insecureInfo))

	insecureFile, err := insecureRoot.OpenFile("record.json", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer func() { _ = insecureFile.Close() }()
	permissiveDACL, _, err := permissive.DACL()
	require.NoError(t, err)
	insecureHandle, err := openTailFailureWindowsFile(
		insecureDir,
		"record.json",
		windows.WRITE_DAC|windows.READ_CONTROL|windows.SYNCHRONIZE,
		0,
	)
	require.NoError(t, err)
	defer func() { _ = windows.CloseHandle(insecureHandle) }()
	require.NoError(t, windows.SetSecurityInfo(
		insecureHandle,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
		nil,
		nil,
		permissiveDACL,
		nil,
	))
	insecureFileInfo, err := insecureFile.Stat()
	require.NoError(t, err)
	require.Error(t, validateTailFailureFallbackFile(insecureFile, insecureFileInfo))
}

func TestTailFailureWindowsOpenedDirectoryCannotBeReplaced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fallback")
	require.NoError(t, createTailFailureFallbackDir(path))
	root, err := os.OpenRoot(path)
	require.NoError(t, err)
	defer func() { _ = root.Close() }()
	require.Error(t, os.Rename(path, path+".replaced"))
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
