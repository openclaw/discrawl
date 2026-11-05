//go:build !unix && !windows

package cli

import "context"

func acquireSyncLockWithMetadata(context.Context, string, string) (func() error, error) {
	return func() error { return nil }, nil
}

func tryAcquireSyncLock(string) (func() error, bool, error) {
	return func() error { return nil }, true, nil
}

func tryAcquireSyncLockWithMetadata(string, string) (func() error, bool, error) {
	return func() error { return nil }, true, nil
}
