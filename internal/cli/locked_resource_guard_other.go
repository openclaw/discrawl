//go:build !unix

package cli

import "errors"

func lockedFreeKiB(string) (uint64, error) {
	return 0, errors.New("locked free-space guard is unsupported on this platform")
}
