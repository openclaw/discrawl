//go:build !windows

package store

import (
	"os/exec"
)

func newPythonLexicalCommand(
	python string,
	language string,
) (*exec.Cmd, error) {
	return &exec.Cmd{
		Path: "/usr/bin/env",
		Args: []string{
			"/usr/bin/env",
			python,
			"-u",
			"-c",
			pythonLexicalWorker,
			language,
		},
	}, nil
}
