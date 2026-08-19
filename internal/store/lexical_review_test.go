package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexicalPythonPackagesSelectOnlyConfiguredLanguages(t *testing.T) {
	packages, err := lexicalPythonPackages([]string{"ko", "zh"})
	require.NoError(t, err)
	require.Equal(t, []string{
		"jieba==0.42.1",
	}, packages)
	require.NotContains(t, packages, "kiwipiepy==0.23.2")
	require.NotContains(t, packages, "sudachipy==0.6.11")
	require.NotContains(t, packages, "snowballstemmer==3.1.1")
}

func TestLexicalPythonPackagesRejectsEmptyAndUnknownLanguages(t *testing.T) {
	packages, err := lexicalPythonPackages(nil)
	require.NoError(t, err)
	require.Empty(t, packages)

	_, err = lexicalPythonPackages([]string{"ko", "unknown"})
	require.ErrorContains(t, err, `unsupported lexical language "unknown"`)
}

func TestInstallLexicalPackagesRejectsNoConfiguredLanguages(t *testing.T) {
	_, err := InstallLexicalPackages(context.Background(), "python3", nil)
	require.ErrorContains(t, err, "no search.lexical languages")
}

func TestInstallLexicalPackagesSkipsKiwiOnlyConfiguration(t *testing.T) {
	result, err := InstallLexicalPackages(context.Background(), "python3", []string{"ko"})
	require.NoError(t, err)
	require.Empty(t, result.Packages)
}

func TestInstallLexicalPackagesRequiresVirtualEnvironment(t *testing.T) {
	_, err := installLexicalPackagesWithRunner(
		context.Background(),
		"/tmp/python",
		[]string{"zh"},
		func(context.Context, string, ...string) ([]byte, error) {
			return []byte("false\n"), nil
		},
	)
	require.ErrorContains(t, err, "virtual environment")
}

func TestInstallLexicalPackagesUsesPinnedSelectedPackages(t *testing.T) {
	var commands [][]string
	result, err := installLexicalPackagesWithRunner(
		context.Background(),
		"/tmp/python",
		[]string{"ja", "ar"},
		func(_ context.Context, path string, args ...string) ([]byte, error) {
			commands = append(commands, append([]string{path}, args...))
			if slices.Contains(args, "import sys; print(sys.prefix != sys.base_prefix)") {
				return []byte("true\n"), nil
			}
			return []byte("installed\n"), nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		"sudachipy==0.6.11",
		"sudachidict_core==20260723",
		"snowballstemmer==3.1.1",
	}, result.Packages)
	require.Len(t, commands, 2)
	require.Equal(t, []string{
		"/tmp/python",
		"-m",
		"pip",
		"install",
		"--disable-pip-version-check",
		"--no-input",
		"--require-virtualenv",
		"sudachipy==0.6.11",
		"sudachidict_core==20260723",
		"snowballstemmer==3.1.1",
	}, commands[1])
}

func TestInstallLexicalPackagesReportsBoundaryFailures(t *testing.T) {
	_, err := installLexicalPackagesWithRunner(
		context.Background(),
		"/tmp/python",
		nil,
		func(context.Context, string, ...string) ([]byte, error) {
			return nil, errors.New("must not run")
		},
	)
	require.ErrorContains(t, err, "no search.lexical languages")

	_, err = installLexicalPackagesWithRunner(
		context.Background(),
		"sh",
		[]string{"zh"},
		func(context.Context, string, ...string) ([]byte, error) {
			return nil, errors.New("must not run")
		},
	)
	require.ErrorContains(t, err, "unsupported lexical Python interpreter")

	_, err = installLexicalPackagesWithRunner(
		context.Background(),
		"/tmp/python",
		[]string{"zh"},
		func(context.Context, string, ...string) ([]byte, error) {
			return nil, errors.New("probe failed")
		},
	)
	require.ErrorContains(t, err, "check lexical Python virtual environment")

	calls := 0
	_, err = installLexicalPackagesWithRunner(
		context.Background(),
		"/tmp/python",
		[]string{"zh"},
		func(context.Context, string, ...string) ([]byte, error) {
			calls++
			if calls == 1 {
				return []byte("True\n"), nil
			}
			return []byte("pip failed"), errors.New("install failed")
		},
	)
	require.ErrorContains(t, err, "install lexical Python packages")
}

func TestPythonCommandSpecAllowsKnownLaunchersAndAbsolutePaths(t *testing.T) {
	spec, err := pythonCommandSpec("python3", false)
	require.NoError(t, err)
	require.NotEmpty(t, spec.Path)

	spec, err = pythonCommandSpec("/opt/discrawl/tokenizers/bin/python", false)
	require.NoError(t, err)
	require.Equal(t, "/opt/discrawl/tokenizers/bin/python", spec.Path)

	spec, err = pythonCommandSpec(`C:\discrawl-tokenizers\Scripts\python.exe`, true)
	require.NoError(t, err)
	require.Equal(t, `C:\discrawl-tokenizers\Scripts\python.exe`, spec.Path)

	_, err = pythonCommandSpec("sh", false)
	require.ErrorContains(t, err, "unsupported lexical Python interpreter")

	require.True(t, isAbsolutePythonPath(`\\server\share\python.exe`, true))
	require.True(t, isAbsolutePythonPath(`D:/venv/python.exe`, true))
	require.False(t, isAbsolutePythonPath(`venv\python.exe`, true))
	require.False(t, isAbsolutePythonPath("venv/python", false))
}

func TestLexicalWorkerEnvironmentDropsParentSecrets(t *testing.T) {
	environment := lexicalWorkerEnvironment([]string{
		"PATH=/usr/bin",
		"HOME=/tmp/home",
		"LANG=en_US.UTF-8",
		"VIRTUAL_ENV=/tmp/venv",
		"DISCORD_BOT_TOKEN=secret",
		"OPENAI_API_KEY=secret",
		"PIP_INDEX_URL=https://user:password@example.invalid/simple",
	})
	joined := strings.Join(environment, "\n")
	require.Contains(t, joined, "PATH=/usr/bin")
	require.Contains(t, joined, "HOME=/tmp/home")
	require.Contains(t, joined, "LANG=en_US.UTF-8")
	require.Contains(t, joined, "VIRTUAL_ENV=/tmp/venv")
	require.Contains(t, joined, "PYTHONNOUSERSITE=1")
	require.NotContains(t, joined, "DISCORD_BOT_TOKEN")
	require.NotContains(t, joined, "OPENAI_API_KEY")
	require.NotContains(t, joined, "PIP_INDEX_URL")
	require.NotContains(t, joined, "password")
}

func TestKiwiCommandUsesGoHelperAndConfiguredModel(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	command, err := newKiwiLexicalCommand(
		"/opt/discrawl/bin/discrawl-kiwi",
		"~/models/kiwi/base",
	)
	require.NoError(t, err)
	require.Equal(t, "/opt/discrawl/bin/discrawl-kiwi", command.Path)
	require.Equal(t, []string{
		"/opt/discrawl/bin/discrawl-kiwi",
		"--model",
		filepath.Join(home, "models/kiwi/base"),
	}, command.Args)
}

func TestKiwiCommandRejectsArbitraryRelativeCommand(t *testing.T) {
	_, err := newKiwiLexicalCommand("sh", "/tmp/model")
	require.ErrorContains(t, err, "unsupported Kiwi helper")
}

func TestKiwiTokenizerCommandProtocol(t *testing.T) {
	tokenizer, err := startKiwiLexicalTokenizerCommand(
		lexicalHelperCommand("ready"),
	)
	require.NoError(t, err)

	tokens, err := tokenizer.Tokenize(context.Background(), "오늘 저녁먹음 기록")
	require.NoError(t, err)
	require.Equal(t, "오늘 저녁 먹 음 기록", tokens)
	require.NoError(t, tokenizer.Close())
	_, err = tokenizer.Tokenize(context.Background(), "text")
	require.ErrorContains(t, err, "write ko tokenizer request")
}

func TestKiwiTokenizerCommandStartupFailures(t *testing.T) {
	tokenizer, err := startKiwiLexicalTokenizerCommand(
		lexicalHelperCommand("startup-error"),
	)
	require.Nil(t, tokenizer)
	require.ErrorContains(t, err, "missing tokenizer package")

	tokenizer, err = startKiwiLexicalTokenizerCommand(
		lexicalHelperCommand("malformed-startup"),
	)
	require.Nil(t, tokenizer)
	require.ErrorContains(t, err, "decode Kiwi tokenizer response")

	tokenizer, err = startKiwiLexicalTokenizerCommand(
		lexicalHelperCommand("stderr-startup"),
	)
	require.Nil(t, tokenizer)
	require.ErrorContains(t, err, "tokenizer stderr")
}

func TestKiwiTokenizerCommandResponseErrorAndCancellation(t *testing.T) {
	tokenizer, err := startKiwiLexicalTokenizerCommand(
		lexicalHelperCommand("response-error"),
	)
	require.NoError(t, err)
	defer func() { _ = tokenizer.Close() }()

	_, err = tokenizer.Tokenize(context.Background(), "text")
	require.ErrorContains(t, err, "tokenization failed")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = tokenizer.Tokenize(ctx, "text")
	require.ErrorIs(t, err, context.Canceled)
}

func TestKiwiTokenizerCommandMalformedResponse(t *testing.T) {
	tokenizer, err := startKiwiLexicalTokenizerCommand(
		lexicalHelperCommand("malformed-response"),
	)
	require.NoError(t, err)
	defer func() { _ = tokenizer.Close() }()

	_, err = tokenizer.Tokenize(context.Background(), "text")
	require.ErrorContains(t, err, "decode Kiwi tokenizer response")

	var nilTokenizer *externalLexicalTokenizer
	require.NoError(t, nilTokenizer.Close())
}

func TestKiwiCommandDefaultHelperAndOptionalModel(t *testing.T) {
	bin := t.TempDir()
	helper := filepath.Join(bin, "discrawl-kiwi")
	require.NoError(t, os.WriteFile(helper, []byte("#!/bin/sh\n"), 0o700))
	t.Setenv("PATH", bin)

	command, err := newKiwiLexicalCommand("", "")
	require.NoError(t, err)
	require.Equal(t, helper, command.Path)
	require.Equal(t, []string{helper}, command.Args)
}

func TestRunLexicalCommandCapturesOutputAndFailure(t *testing.T) {
	t.Setenv("DISCRAWL_INSTALL_HELPER", "1")
	output, err := runLexicalCommand(
		context.Background(),
		os.Args[0],
		"-test.run=TestLexicalInstallCommandHelperProcess",
		"--",
		"success",
	)
	require.NoError(t, err)
	require.Contains(t, string(output), "installed\n")

	output, err = runLexicalCommand(
		context.Background(),
		os.Args[0],
		"-test.run=TestLexicalInstallCommandHelperProcess",
		"--",
		"failure",
	)
	require.ErrorContains(t, err, "exit status")
	require.Contains(t, string(output), "install failed")

	_, err = runLexicalCommand(context.Background(), "/definitely/missing/discrawl-command")
	require.Error(t, err)
}

func TestRunLexicalCommandDropsParentSecrets(t *testing.T) {
	t.Setenv("DISCORD_BOT_TOKEN", "discord-secret")
	t.Setenv("OPENAI_API_KEY", "openai-secret")
	t.Setenv("PIP_INDEX_URL", "https://user:password@example.invalid/simple")
	output, err := runLexicalCommand(
		context.Background(),
		os.Args[0],
		"-test.run=TestLexicalInstallCommandHelperProcess",
		"--",
		"environment",
	)
	require.NoError(t, err)
	require.Contains(t, string(output), "clean\n")
}

func TestLexicalInstallCommandHelperProcess(t *testing.T) {
	if !slices.Contains(os.Args, "-test.run=TestLexicalInstallCommandHelperProcess") {
		return
	}
	switch os.Args[len(os.Args)-1] {
	case "success":
		fmt.Println("installed")
		os.Exit(0)
	case "failure":
		fmt.Println("install failed")
		os.Exit(2)
	case "environment":
		for _, key := range []string{"DISCORD_BOT_TOKEN", "OPENAI_API_KEY", "PIP_INDEX_URL"} {
			if os.Getenv(key) != "" {
				fmt.Println(key)
				os.Exit(4)
			}
		}
		fmt.Println("clean")
		os.Exit(0)
	default:
		os.Exit(3)
	}
}
