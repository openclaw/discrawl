//go:build unix

package cli

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

func lockedFreeKiB(dbPath string) (uint64, error) {
	var details unix.Statfs_t
	if err := unix.Statfs(filepath.Dir(dbPath), &details); err != nil {
		return 0, err
	}
	return details.Bavail * uint64(details.Bsize) / 1024, nil
}
