package config

import "golang.org/x/sys/unix"

func credentialDirectoryFlags() int {
	return unix.O_PATH | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
}

func credentialACLProtected(int) bool {
	// Linux POSIX ACL named-user/group permissions are capped by the group
	// class mask reflected in st_mode. The existing 0600 file/0700 parent and
	// non-writable ancestor checks therefore also constrain effective ACLs.
	return true
}
