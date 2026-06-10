//go:build unix

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
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
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			locked = true
			if err := writeSyncLockMetadataRecord(file, path, []byte(metadata)); err != nil {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
				locked = false
				return nil, fmt.Errorf("write sync lock metadata: %w", err)
			}
			return func() error {
				unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
				closeErr := file.Close()
				if unlockErr != nil {
					return unlockErr
				}
				return closeErr
			}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("acquire sync lock: %w", err)
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
	err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("acquire sync lock: %w", err)
	}
	if err := writeSyncLockMetadataRecord(file, path, []byte(metadata)); err != nil {
		_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
		_ = file.Close()
		return nil, false, fmt.Errorf("write sync lock metadata: %w", err)
	}
	return func() error {
		unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
		closeErr := file.Close()
		if unlockErr != nil {
			return unlockErr
		}
		return closeErr
	}, true, nil
}
