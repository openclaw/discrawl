//go:build !windows

package store

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func createTailFailureFallbackDir(path string) error {
	return os.Mkdir(path, 0o700)
}

func validateTailFailureFallbackDir(_ *os.File, info os.FileInfo) error {
	if err := validateTailFailureFallbackOwner(info); err != nil {
		return err
	}
	if info.Mode().Perm() != 0o700 {
		return errors.New("tail message failure fallback directory permissions must be 0700")
	}
	return nil
}

func validateTailFailureFallbackFile(_ *os.File, info os.FileInfo) error {
	if err := validateTailFailureFallbackOwner(info); err != nil {
		return err
	}
	if info.Mode().Perm()&0o077 != 0 || info.Mode().Perm()&0o400 == 0 {
		return errors.New("committed tail message failure fallback permissions are insecure")
	}
	return nil
}

func validateTailFailureFallbackOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil || int(stat.Uid) != os.Geteuid() {
		return errors.New("tail message failure fallback owner must be the current user")
	}
	return nil
}

func secureTailFailureFallbackTempFile(_ *os.File, _ string) error {
	return nil
}

func syncTailFailureDirectory(dir *os.File) error {
	return dir.Sync()
}

func normalizeTailFailureFileURLPath(path string) string {
	return filepath.FromSlash(path)
}
