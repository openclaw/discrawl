//go:build !unix && !windows

package cli

func syncLockState(string) (held bool, known bool, err error) {
	return false, false, nil
}
