//go:build !darwin && !linux && !windows

package store

import (
	"os"
)

func renameTailFailureNoReplace(root *os.Root, _ *os.File, oldName, newName string) error {
	return linkTailFailureNoReplaceRoot(root, oldName, newName)
}
