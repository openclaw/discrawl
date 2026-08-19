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

func TestNewLexicalTokenizersDisabled(t *testing.T) {
	tokenizers, err := newLexicalTokenizers(OpenOptions{})
	require.NoError(t, err)
	require.Nil(t, tokenizers)
}

func TestNewLexicalTokenizersArabicIsInProcess(t *testing.T) {
	tokenizers, err := newLexicalTokenizers(OpenOptions{LexicalLanguages: []string{"ar"}})
	require.NoError(t, err)
	require.Contains(t, tokenizers, "ar")
	tokens, err := tokenizers["ar"].Tokenize(context.Background(), "والكتاب")
	require.NoError(t, err)
	require.Contains(t, tokens, "كتاب")
}

func TestNewLexicalTokenizersRejectsUnknownLanguage(t *testing.T) {
	_, err := newLexicalTokenizers(OpenOptions{LexicalLanguages: []string{"default"}})
	require.ErrorContains(t, err, "unsupported lexical language")
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
	switch mode {
	case "startup-error":
		fmt.Println(`{"error":"missing tokenizer package"}`)
		os.Exit(2)
	case "malformed-startup":
		fmt.Println("not-json")
		os.Exit(2)
	case "stderr-startup":
		fmt.Fprintln(os.Stderr, "tokenizer stderr")
		os.Exit(2)
	case "ready", "response-error", "malformed-response":
		fmt.Println(`{"ready":true}`)
	default:
		fmt.Println(`{"ready":true}`)
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var request map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			fmt.Println(`{"error":"bad request"}`)
			continue
		}
		if mode == "response-error" {
			fmt.Println(`{"error":"tokenization failed"}`)
			continue
		}
		if mode == "malformed-response" {
			fmt.Println("not-json")
			continue
		}
		tokens := request["text"] + " tokenized"
		if request["text"] == "오늘 저녁먹음 기록" {
			tokens = "오늘 저녁 먹 음 기록"
		}
		response, err := json.Marshal(map[string]string{"tokens": tokens})
		if err != nil {
			fmt.Println(`{"error":"encode response"}`)
			continue
		}
		fmt.Println(string(response))
	}
}

func TestNewLexicalTokenizersCreatesLazyHelpers(t *testing.T) {
	tokenizers, err := newLexicalTokenizers(OpenOptions{
		LexicalLanguages:   []string{"ko", "ja", "zh", "ar"},
		LexicalKiwiCommand: "/definitely/missing/discrawl-kiwi",
		LexicalJaCommand:   "/definitely/missing/discrawl-ja",
		LexicalZhCommand:   "/definitely/missing/discrawl-zh",
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"ko", "ja", "zh", "ar"}, keys(tokenizers))
}

func keys(tokenizers map[string]LexicalTokenizer) []string {
	out := make([]string, 0, len(tokenizers))
	for language := range tokenizers {
		out = append(out, language)
	}
	return out
}

func TestStartHelperLexicalTokenizerUsesReadyScript(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "discrawl-ja")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
echo '{"ready":true}'
read line
echo '{"tokens":"tokyo"}'
`), 0o700))
	tokenizer, err := startHelperLexicalTokenizer("ja", script, "discrawl-ja", nil)
	require.NoError(t, err)
	defer func() { _ = tokenizer.Close() }()
	tokens, err := tokenizer.Tokenize(context.Background(), "東京")
	require.NoError(t, err)
	require.Equal(t, "tokyo", tokens)
}

func TestOpenReadOnlyWithOptionsRejectsUnknownLanguage(t *testing.T) {
	_, err := OpenReadOnlyWithOptions(context.Background(), filepath.Join(t.TempDir(), "discrawl.db"), OpenOptions{
		LexicalLanguages: []string{"nope"},
	})
	require.ErrorContains(t, err, "unsupported lexical language")
}
