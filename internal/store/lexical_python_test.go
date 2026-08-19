package store

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPythonLexicalTokenizerCommandProtocol(t *testing.T) {
	tokenizer, err := startPythonLexicalTokenizerCommand(
		lexicalHelperCommand("ready"),
		"test",
	)
	require.NoError(t, err)

	tokens, err := tokenizer.Tokenize(context.Background(), "mixed text")
	require.NoError(t, err)
	require.Equal(t, "mixed text tokenized", tokens)
	require.NoError(t, tokenizer.Close())
}

func TestLockedBuffer(t *testing.T) {
	var buffer lockedBuffer
	written, err := buffer.Write([]byte("tokenizer stderr"))
	require.NoError(t, err)
	require.Equal(t, len("tokenizer stderr"), written)
	require.Equal(t, "tokenizer stderr", buffer.String())
}

func TestPythonLexicalTokenizerCommandStartupError(t *testing.T) {
	tokenizer, err := startPythonLexicalTokenizerCommand(
		lexicalHelperCommand("startup-error"),
		"test",
	)
	require.Nil(t, tokenizer)
	require.ErrorContains(t, err, "missing tokenizer package")
}

func TestPythonLexicalTokenizerCommandMalformedStartup(t *testing.T) {
	tokenizer, err := startPythonLexicalTokenizerCommand(
		lexicalHelperCommand("malformed-startup"),
		"test",
	)
	require.Nil(t, tokenizer)
	require.ErrorContains(t, err, "decode tokenizer response")
}

func TestPythonLexicalTokenizerCommandStartupStderr(t *testing.T) {
	tokenizer, err := startPythonLexicalTokenizerCommand(
		lexicalHelperCommand("stderr-startup"),
		"test",
	)
	require.Nil(t, tokenizer)
	require.ErrorContains(t, err, "tokenizer stderr")
}

func TestPythonLexicalTokenizerCommandResponseError(t *testing.T) {
	tokenizer, err := startPythonLexicalTokenizerCommand(
		lexicalHelperCommand("response-error"),
		"test",
	)
	require.NoError(t, err)
	defer func() { _ = tokenizer.Close() }()

	_, err = tokenizer.Tokenize(context.Background(), "mixed text")
	require.ErrorContains(t, err, "tokenization failed")
}

func TestPythonLexicalTokenizerHonorsCanceledContext(t *testing.T) {
	tokenizer, err := startPythonLexicalTokenizerCommand(
		lexicalHelperCommand("ready"),
		"test",
	)
	require.NoError(t, err)
	defer func() { _ = tokenizer.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = tokenizer.Tokenize(ctx, "mixed text")
	require.ErrorIs(t, err, context.Canceled)
}

func TestPythonLexicalTokenizerReportsWriteAfterClose(t *testing.T) {
	tokenizer, err := startPythonLexicalTokenizerCommand(
		lexicalHelperCommand("ready"),
		"test",
	)
	require.NoError(t, err)
	require.NoError(t, tokenizer.Close())

	_, err = tokenizer.Tokenize(context.Background(), "mixed text")
	require.ErrorContains(t, err, "write tokenizer request")
}

func TestNewLexicalTokenizersDisabled(t *testing.T) {
	tokenizers, err := newLexicalTokenizers(OpenOptions{})
	require.NoError(t, err)
	require.Nil(t, tokenizers)
}

func TestPythonLexicalTokenizerDefaultWorker(t *testing.T) {
	tokenizer, err := startPythonLexicalTokenizer("python3", "default")
	require.NoError(t, err)
	defer func() { _ = tokenizer.Close() }()

	tokens, err := tokenizer.Tokenize(context.Background(), "Mixed CASE 123")
	require.NoError(t, err)
	require.Equal(t, "mixed case", tokens)
}

func TestNewLexicalTokenizersExpandsPythonHomePath(t *testing.T) {
	python, err := exec.LookPath("python3")
	require.NoError(t, err)
	home := t.TempDir()
	require.NoError(t, os.Symlink(python, filepath.Join(home, "python3")))
	t.Setenv("HOME", home)

	tokenizers, err := newLexicalTokenizers(OpenOptions{
		LexicalLanguages: []string{"default"},
		LexicalPython:    "~/python3",
	})
	require.NoError(t, err)
	require.Contains(t, tokenizers, "default")
	closeLexicalTokenizers(tokenizers)
}

func TestOpenWithOptionsLoadsKiwiHelperLazily(t *testing.T) {
	ctx := context.Background()
	s, err := OpenWithOptions(ctx, filepath.Join(t.TempDir(), "discrawl.db"), OpenOptions{
		LexicalLanguages:   []string{"ko"},
		LexicalKiwiCommand: "/definitely/missing/discrawl-kiwi",
		LexicalKiwiModel:   "/definitely/missing/kiwi-model",
	})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	err = s.UpsertMessage(ctx, MessageRecord{
		ID: "ko", GuildID: "g1", ChannelID: "c1",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content:   "저녁먹음", NormalizedContent: "저녁먹음", RawJSON: `{}`,
	})
	require.ErrorContains(t, err, "start ko lexical tokenizer")
	require.NotContains(t, err.Error(), "Python")
}

func lexicalHelperCommand(mode string) *exec.Cmd {
	return &exec.Cmd{
		Path: os.Args[0],
		Args: []string{os.Args[0], "-test.run=TestLexicalTokenizerHelperProcess", "--", mode},
		Env:  append(os.Environ(), "DISCRAWL_LEXICAL_HELPER=1"),
	}
}

func TestLexicalTokenizerHelperProcess(t *testing.T) {
	if os.Getenv("DISCRAWL_LEXICAL_HELPER") != "1" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	if mode == "startup-error" {
		fmt.Println(`{"error":"missing tokenizer package"}`)
		os.Exit(2)
	}
	if mode == "malformed-startup" {
		fmt.Println(`not-json`)
		os.Exit(2)
	}
	if mode == "stderr-startup" {
		_, _ = fmt.Fprintln(os.Stderr, "tokenizer stderr")
		os.Exit(2)
	}
	fmt.Println(`{"ready":true}`)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var request map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			fmt.Printf("{\"error\":%q}\n", err.Error())
			continue
		}
		if mode == "response-error" {
			fmt.Println(`{"error":"tokenization failed"}`)
			continue
		}
		if mode == "malformed-response" {
			fmt.Println(`not-json`)
			continue
		}
		response, err := json.Marshal(map[string]string{
			"tokens": func() string {
				if request["text"] == "오늘 저녁먹음 기록" {
					return "오늘 저녁 먹 음 기록"
				}
				return request["text"] + " tokenized"
			}(),
		})
		if err != nil {
			fmt.Printf("{\"error\":%q}\n", err.Error())
			continue
		}
		fmt.Println(string(response))
	}
	os.Exit(0)
}
