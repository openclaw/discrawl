//go:build unix

package cli

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func syncLockState(path string) (held bool, known bool, err error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, true, nil
		}
		return false, true, err
	}
	defer func() { _ = file.Close() }()
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return true, true, nil
		}
		return false, true, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
		return false, true, err
	}
	return false, true, nil
}
