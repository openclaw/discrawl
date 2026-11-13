//go:build !darwin && !linux

package store

import "io/fs"

func nativePhysicalIdentity(fs.FileInfo) (string, bool) {
	return "", false
}
