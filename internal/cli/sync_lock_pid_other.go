//go:build !unix && !windows

package cli

func syncLockPIDAlive(pid int) bool {
	return pid > 0
}
