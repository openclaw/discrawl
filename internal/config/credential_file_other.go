//go:build !darwin && !linux

package config

import "errors"

func readProtectedCredential(string) (string, error) {
	return "", errors.New("protected credential files are supported on macOS and Linux")
}
