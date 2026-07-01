//go:build windows

package cli

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func syncLockState(path string) (held bool, known bool, err error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, true, nil
		}
		return false, true, err
	}
	defer func() { _ = file.Close() }()
	handle := windows.Handle(file.Fd())
	overlapped := syncLockWindowsOverlapped()
	if err := windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped); err != nil {
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return true, true, nil
		}
		return false, true, err
	}
	if err := windows.UnlockFileEx(handle, 0, 1, 0, overlapped); err != nil {
		return false, true, err
	}
	return false, true, nil
}
