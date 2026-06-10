//go:build windows

package cli

import "testing"

func TestSyncLockWindowsOverlappedKeepsByteZeroCompatibility(t *testing.T) {
	overlapped := syncLockWindowsOverlapped()
	if overlapped.Offset != 0 || overlapped.OffsetHigh != 0 {
		t.Fatal("windows sync lock must keep byte 0 compatibility with older binaries")
	}
}
