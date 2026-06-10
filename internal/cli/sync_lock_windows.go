//go:build windows

package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

func acquireSyncLock(ctx context.Context, path string) (func() error, error) {
	return acquireSyncLockWithMetadata(ctx, path, fmt.Sprintf("pid=%d\n", os.Getpid()))
}

func acquireSyncLockWithMetadata(ctx context.Context, path string, metadata string) (func() error, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open sync lock: %w", err)
	}
	locked := false
	defer func() {
		if !locked {
			_ = file.Close()
		}
	}()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	handle := windows.Handle(file.Fd())
	overlapped := syncLockWindowsOverlapped()
	for {
		err = windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
		if err == nil {
			locked = true
			if err := writeSyncLockMetadataRecord(file, path, []byte(metadata)); err != nil {
				_ = windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
				locked = false
				return nil, fmt.Errorf("write sync lock metadata: %w", err)
			}
			return func() error {
				unlockErr := windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
				closeErr := file.Close()
				if unlockErr != nil {
					return unlockErr
				}
				return closeErr
			}, nil
		}
		select {
		case <-ctx.Done():
			return nil, syncLockErr(ctx, path)
		case <-ticker.C:
		}
	}
}

func tryAcquireSyncLock(path string) (func() error, bool, error) {
	return tryAcquireSyncLockWithMetadata(path, fmt.Sprintf("pid=%d\n", os.Getpid()))
}

func tryAcquireSyncLockWithMetadata(path string, metadata string) (func() error, bool, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, fmt.Errorf("open sync lock: %w", err)
	}
	handle := windows.Handle(file.Fd())
	overlapped := syncLockWindowsOverlapped()
	err = windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
	if err != nil {
		_ = file.Close()
		return nil, false, nil
	}
	if err := writeSyncLockMetadataRecord(file, path, []byte(metadata)); err != nil {
		_ = windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
		_ = file.Close()
		return nil, false, fmt.Errorf("write sync lock metadata: %w", err)
	}
	return func() error {
		unlockErr := windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
		closeErr := file.Close()
		if unlockErr != nil {
			return unlockErr
		}
		return closeErr
	}, true, nil
}

func syncLockWindowsOverlapped() *windows.Overlapped {
	return &windows.Overlapped{}
}
