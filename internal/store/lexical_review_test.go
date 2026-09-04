package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
	require.NotContains(t, joined, "VIRTUAL_ENV")
	require.NotContains(t, joined, "PYTHONNOUSERSITE")
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

func TestHelperCommandsRejectArbitraryRelativeCommands(t *testing.T) {
	_, err := newKiwiLexicalCommand("sh", "/tmp/model")
	require.ErrorContains(t, err, "unsupported discrawl-kiwi helper")
	_, err = newHelperLexicalCommand("sh", "discrawl-ja", nil)
	require.ErrorContains(t, err, "unsupported discrawl-ja helper")
	_, err = newHelperLexicalCommand("sh", "discrawl-zh", nil)
	require.ErrorContains(t, err, "unsupported discrawl-zh helper")
}

func TestKiwiTokenizerCommandProtocol(t *testing.T) {
	tokenizer, err := startKiwiLexicalTokenizerCommand(lexicalHelperCommand("ready"))
	require.NoError(t, err)
	tokens, err := tokenizer.Tokenize(context.Background(), "오늘 저녁먹음 기록")
	require.NoError(t, err)
	require.Equal(t, "오늘 저녁 먹 음 기록", tokens)
	require.NoError(t, tokenizer.Close())
	_, err = tokenizer.Tokenize(context.Background(), "text")
	require.ErrorContains(t, err, "write ko tokenizer request")
}

func TestKiwiTokenizerCommandStartupFailures(t *testing.T) {
	tokenizer, err := startKiwiLexicalTokenizerCommand(lexicalHelperCommand("startup-error"))
	require.Nil(t, tokenizer)
	require.ErrorContains(t, err, "missing tokenizer package")

	tokenizer, err = startKiwiLexicalTokenizerCommand(lexicalHelperCommand("malformed-startup"))
	require.Nil(t, tokenizer)
	require.ErrorContains(t, err, "decode lexical tokenizer response")

	tokenizer, err = startKiwiLexicalTokenizerCommand(lexicalHelperCommand("stderr-startup"))
	require.Nil(t, tokenizer)
	require.ErrorContains(t, err, "tokenizer stderr")
}

func TestKiwiTokenizerCommandResponseErrorAndCancellation(t *testing.T) {
	tokenizer, err := startKiwiLexicalTokenizerCommand(lexicalHelperCommand("response-error"))
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
	tokenizer, err := startKiwiLexicalTokenizerCommand(lexicalHelperCommand("malformed-response"))
	require.NoError(t, err)
	defer func() { _ = tokenizer.Close() }()

	_, err = tokenizer.Tokenize(context.Background(), "text")
	require.ErrorContains(t, err, "decode lexical tokenizer response")

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
