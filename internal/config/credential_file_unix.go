//go:build darwin || linux

package config

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const maxCredentialFileBytes = 16 * 1024

// readProtectedCredential walks from the filesystem root using directory FDs.
// Every component is opened without following symlinks. The final file is read
// through the same descriptor whose owner, mode and link count were checked.
// Errors deliberately omit OS diagnostics, paths and credential bytes.
func readProtectedCredential(path string) (string, error) {
	fail := errors.New("credential file must be an absolute protected file: owner 0600, parent 0700, no symlinks or writable ancestors")
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(os.PathSeparator) {
		return "", fail
	}
	parts := strings.Split(strings.TrimPrefix(path, string(os.PathSeparator)), string(os.PathSeparator))
	dir, err := unix.Open(string(os.PathSeparator), credentialDirectoryFlags(), 0)
	if err != nil {
		return "", fail
	}
	defer func() { _ = unix.Close(dir) }()
	for i := 0; ; i++ {
		var st unix.Stat_t
		if unix.Fstat(dir, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Mode&0o022 != 0 || (int(st.Uid) != os.Geteuid() && st.Uid != 0) {
			return "", fail
		}
		if !credentialACLProtected(dir) {
			return "", fail
		}
		if i == len(parts)-1 {
			if int(st.Uid) != os.Geteuid() || st.Mode&0o7777 != 0o700 {
				return "", fail
			}
			break
		}
		next, openErr := unix.Openat(dir, parts[i], credentialDirectoryFlags(), 0)
		if openErr != nil {
			return "", fail
		}
		_ = unix.Close(dir)
		dir = next
	}
	fd, err := unix.Openat(dir, parts[len(parts)-1], unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", fail
	}
	f := os.NewFile(uintptr(fd), "credential")
	defer func() { _ = f.Close() }()
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || int(st.Uid) != os.Geteuid() || st.Mode&0o7777 != 0o600 || st.Nlink != 1 || st.Size > maxCredentialFileBytes {
		return "", fail
	}
	if !credentialACLProtected(fd) {
		return "", fail
	}
	body, err := io.ReadAll(io.LimitReader(f, maxCredentialFileBytes+1))
	if err != nil || len(body) > maxCredentialFileBytes {
		return "", fail
	}
	return string(body), nil
}
