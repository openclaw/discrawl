//go:build windows

package store

import (
	"fmt"
	"os/exec"
)

func newPythonLexicalCommand(
	python string,
	language string,
) (*exec.Cmd, error) {
	args := []string{"-u", "-c", pythonLexicalWorker, language}
	switch python {
	case "python", "python.exe":
		return exec.Command("python", args...), nil
	case "python3", "python3.exe":
		return exec.Command("python3", args...), nil
	case "py", "py.exe":
		return exec.Command("py", append([]string{"-3"}, args...)...), nil
	default:
		return nil, fmt.Errorf(
			"unsupported Windows lexical Python launcher %q; use python, python3, or py",
			python,
		)
	}
}
