//go:build darwin

package store

import (
	"os"

	"golang.org/x/sys/unix"
)

func renameTailFailureNoReplace(_ *os.Root, dir *os.File, oldName, newName string) error {
	return unix.RenameatxNp(
		int(dir.Fd()),
		oldName,
		int(dir.Fd()),
		newName,
		unix.RENAME_EXCL,
	)
}
