//go:build unix

package cli

import "golang.org/x/sys/unix"

func syncLockPIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := unix.Kill(pid, 0)
	return err == nil || err == unix.EPERM
}
