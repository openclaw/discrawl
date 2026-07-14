//go:build !darwin && !linux

package store

import (
	"os"
)

func renameTailFailureNoReplace(root *os.Root, _ *os.File, oldName, newName string) error {
	return linkTailFailureNoReplaceRoot(root, oldName, newName)
}
