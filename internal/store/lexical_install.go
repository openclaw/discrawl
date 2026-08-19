package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

type LexicalInstallResult struct {
	Packages []string
	Output   string
}

type lexicalCommandRunner func(context.Context, string, ...string) ([]byte, error)

func InstallLexicalPackages(
	ctx context.Context,
	python string,
	languages []string,
) (LexicalInstallResult, error) {
	return installLexicalPackagesWithRunner(ctx, python, languages, runLexicalCommand)
}

func installLexicalPackagesWithRunner(
	ctx context.Context,
	python string,
	languages []string,
	run lexicalCommandRunner,
) (LexicalInstallResult, error) {
	packages, err := lexicalPythonPackages(languages)
	if err != nil {
		return LexicalInstallResult{}, err
	}
	if len(packages) == 0 {
		if len(languages) == 0 {
			return LexicalInstallResult{}, errors.New("no search.lexical languages are configured")
		}
		return LexicalInstallResult{}, nil
	}
	python, err = expandLexicalPython(python)
	if err != nil {
		return LexicalInstallResult{}, err
	}
	spec, err := pythonCommandSpec(python, runtime.GOOS == "windows")
	if err != nil {
		return LexicalInstallResult{}, err
	}
	probeArgs := append([]string{}, spec.ArgsPrefix...)
	probeArgs = append(probeArgs, "-c", "import sys; print(sys.prefix != sys.base_prefix)")
	output, err := run(ctx, spec.Path, probeArgs...)
	if err != nil {
		return LexicalInstallResult{}, fmt.Errorf("check lexical Python virtual environment: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(string(output)), "true") {
		return LexicalInstallResult{}, errors.New(
			"lexical package installation requires a virtual environment; configure search.lexical.python to its interpreter",
		)
	}
	installArgs := append([]string{}, spec.ArgsPrefix...)
	installArgs = append(
		installArgs,
		"-m",
		"pip",
		"install",
		"--disable-pip-version-check",
		"--no-input",
		"--require-virtualenv",
	)
	installArgs = append(installArgs, packages...)
	output, err = run(ctx, spec.Path, installArgs...)
	if err != nil {
		return LexicalInstallResult{}, fmt.Errorf("install lexical Python packages: %w", err)
	}
	return LexicalInstallResult{
		Packages: packages,
		Output:   strings.TrimSpace(string(output)),
	}, nil
}

func lexicalPythonPackages(languages []string) ([]string, error) {
	packages := make([]string, 0, len(languages)+1)
	seen := make(map[string]struct{}, len(languages))
	for _, language := range languages {
		if _, ok := seen[language]; ok {
			continue
		}
		seen[language] = struct{}{}
		switch language {
		case "ko":
			continue
		case "ja":
			packages = append(packages, "sudachipy==0.6.11", "sudachidict_core==20260723")
		case "zh":
			packages = append(packages, "jieba==0.42.1")
		case "ar":
			packages = append(packages, "snowballstemmer==3.1.1")
		default:
			return nil, fmt.Errorf("unsupported lexical language %q", language)
		}
	}
	return packages, nil
}

func runLexicalCommand(ctx context.Context, path string, args ...string) ([]byte, error) {
	var output bytes.Buffer
	cmd := &exec.Cmd{
		Path:   path,
		Args:   append([]string{path}, args...),
		Env:    lexicalWorkerEnvironment(os.Environ()),
		Stdout: &output,
		Stderr: &output,
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	wait := make(chan error, 1)
	go func() {
		wait <- cmd.Wait()
	}()
	select {
	case err := <-wait:
		if err != nil {
			return output.Bytes(), fmt.Errorf("%w: %s", err, strings.TrimSpace(output.String()))
		}
		return output.Bytes(), nil
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		<-wait
		return output.Bytes(), ctx.Err()
	}
}
