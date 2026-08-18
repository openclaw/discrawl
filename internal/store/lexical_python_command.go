package store

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

type lexicalCommandSpec struct {
	Path       string
	ArgsPrefix []string
}

func pythonCommandSpec(python string, windows bool) (lexicalCommandSpec, error) {
	python = strings.TrimSpace(python)
	if python == "" {
		python = "python3"
	}
	if isAbsolutePythonPath(python, windows) {
		return lexicalCommandSpec{Path: python}, nil
	}
	allowed := map[string][]string{
		"python":  nil,
		"python3": nil,
	}
	if windows {
		allowed["python.exe"] = nil
		allowed["python3.exe"] = nil
		allowed["py"] = []string{"-3"}
		allowed["py.exe"] = []string{"-3"}
	}
	prefix, ok := allowed[python]
	if !ok {
		return lexicalCommandSpec{}, fmt.Errorf(
			"unsupported lexical Python interpreter %q; use an absolute path or python/python3",
			python,
		)
	}
	path, err := exec.LookPath(python)
	if err != nil {
		return lexicalCommandSpec{}, fmt.Errorf("find lexical Python interpreter %q: %w", python, err)
	}
	return lexicalCommandSpec{Path: path, ArgsPrefix: prefix}, nil
}

func newPythonLexicalCommand(python string, language string) (*exec.Cmd, error) {
	spec, err := pythonCommandSpec(python, runtime.GOOS == "windows")
	if err != nil {
		return nil, err
	}
	args := append([]string{spec.Path}, spec.ArgsPrefix...)
	args = append(args, "-u", "-c", pythonLexicalWorker, language)
	return &exec.Cmd{
		Path: spec.Path,
		Args: args,
		Env:  lexicalWorkerEnvironment(os.Environ()),
	}, nil
}

func isAbsolutePythonPath(path string, windows bool) bool {
	if windows {
		if strings.HasPrefix(path, `\\`) {
			return true
		}
		return len(path) >= 3 &&
			((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) &&
			path[1] == ':' &&
			(path[2] == '\\' || path[2] == '/')
	}
	return filepath.IsAbs(path)
}

func lexicalWorkerEnvironment(parent []string) []string {
	allowed := map[string]struct{}{
		"HOME":        {},
		"LANG":        {},
		"LC_ALL":      {},
		"PATH":        {},
		"PATHEXT":     {},
		"SYSTEMROOT":  {},
		"TEMP":        {},
		"TMP":         {},
		"TMPDIR":      {},
		"VIRTUAL_ENV": {},
		"WINDIR":      {},
	}
	environment := make([]string, 0, len(allowed)+2)
	for _, entry := range parent {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, ok := allowed[strings.ToUpper(key)]; ok {
			environment = append(environment, entry)
		}
	}
	environment = append(environment, "PYTHONNOUSERSITE=1", "PYTHONUTF8=1")
	return environment
}
