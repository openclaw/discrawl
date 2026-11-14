//go:build darwin || linux

package store

import (
	"io/fs"
	"runtime"
	"strconv"
	"syscall"
)

func nativePhysicalIdentity(info fs.FileInfo) (string, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", false
	}
	//nolint:unconvert // Device and inode integer types differ across Unix platforms.
	return runtime.GOOS + ":" + strconv.FormatUint(uint64(stat.Dev), 10) + ":" + strconv.FormatUint(uint64(stat.Ino), 10), true
}
