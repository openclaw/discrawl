//go:build linux

package store

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func renameTailFailureNoReplace(root *os.Root, dir *os.File, oldName, newName string) error {
	err := unix.Renameat2(
		int(dir.Fd()),
		oldName,
		int(dir.Fd()),
		newName,
		unix.RENAME_NOREPLACE,
	)
	if !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) {
		return err
	}
	return linkTailFailureNoReplaceRoot(root, oldName, newName)
}
