package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type kiwiLexicalResponse struct {
	Ready  bool   `json:"ready,omitempty"`
	Tokens string `json:"tokens,omitempty"`
	Error  string `json:"error,omitempty"`
}

func startKiwiLexicalTokenizer(command, model string) (LexicalTokenizer, error) {
	cmd, err := newKiwiLexicalCommand(command, model)
	if err != nil {
		return nil, err
	}
	return startKiwiLexicalTokenizerCommand(cmd)
}

func startKiwiLexicalTokenizerCommand(cmd *exec.Cmd) (LexicalTokenizer, error) {
	tokenizer := &externalLexicalTokenizer{
		language: "ko",
		command:  cmd,
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	cmd.Stderr = &tokenizer.stderr
	tokenizer.stdin = stdin
	tokenizer.stdout = bufio.NewScanner(stdout)
	tokenizer.stdout.Buffer(make([]byte, 4096), 8*1024*1024)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start Korean Kiwi tokenizer: %w", err)
	}
	response, err := tokenizer.readStartup()
	if err != nil {
		_ = tokenizer.Close()
		return nil, fmt.Errorf("initialize Korean Kiwi tokenizer: %w", err)
	}
	if !response.Ready {
		_ = tokenizer.Close()
		return nil, errors.New(response.Error)
	}
	return tokenizer, nil
}

func newKiwiLexicalCommand(command, model string) (*exec.Cmd, error) {
	command, err := expandLexicalPath(command, "discrawl-kiwi")
	if err != nil {
		return nil, fmt.Errorf("resolve Kiwi helper: %w", err)
	}
	if !filepath.IsAbs(command) {
		if command != "discrawl-kiwi" {
			return nil, fmt.Errorf(
				"unsupported Kiwi helper %q; use an absolute path or discrawl-kiwi",
				command,
			)
		}
		command, err = exec.LookPath(command)
		if err != nil {
			return nil, fmt.Errorf("find Kiwi helper: %w", err)
		}
	}
	model, err = expandLexicalPath(model, "")
	if err != nil {
		return nil, fmt.Errorf("resolve Kiwi model: %w", err)
	}
	args := []string{command}
	if model != "" {
		args = append(args, "--model", model)
	}
	return &exec.Cmd{
		Path: command,
		Args: args,
		Env:  lexicalWorkerEnvironment(os.Environ()),
	}, nil
}

func expandLexicalPath(path, fallback string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return fallback, nil
	}
	if !strings.HasPrefix(path, "~/") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, strings.TrimPrefix(path, "~/")), nil
}

type externalLexicalTokenizer struct {
	language string
	command  *exec.Cmd
	stdin    io.WriteCloser
	stdout   *bufio.Scanner
	stderr   lockedBuffer
	mutex    sync.Mutex
}

func (t *externalLexicalTokenizer) readStartup() (kiwiLexicalResponse, error) {
	result := make(chan struct {
		response kiwiLexicalResponse
		err      error
	}, 1)
	go func() {
		response, err := t.readResponse()
		result <- struct {
			response kiwiLexicalResponse
			err      error
		}{response, err}
	}()
	select {
	case output := <-result:
		return output.response, output.err
	case <-time.After(30 * time.Second):
		_ = t.command.Process.Kill()
		return kiwiLexicalResponse{}, errors.New("kiwi tokenizer startup timed out after 30s")
	}
}

func (t *externalLexicalTokenizer) Tokenize(ctx context.Context, text string) (string, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	request, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return "", err
	}
	if _, err := t.stdin.Write(append(request, '\n')); err != nil {
		return "", fmt.Errorf("write %s tokenizer request: %w", t.language, err)
	}
	result := make(chan struct {
		response kiwiLexicalResponse
		err      error
	}, 1)
	go func() {
		response, err := t.readResponse()
		result <- struct {
			response kiwiLexicalResponse
			err      error
		}{response, err}
	}()
	select {
	case output := <-result:
		if output.err != nil {
			return "", output.err
		}
		if output.response.Error != "" {
			return "", errors.New(output.response.Error)
		}
		return output.response.Tokens, nil
	case <-ctx.Done():
		_ = t.command.Process.Kill()
		return "", ctx.Err()
	case <-time.After(30 * time.Second):
		_ = t.command.Process.Kill()
		return "", errors.New("kiwi tokenizer response timed out after 30s")
	}
}

func (t *externalLexicalTokenizer) readResponse() (kiwiLexicalResponse, error) {
	if !t.stdout.Scan() {
		if err := t.stdout.Err(); err != nil {
			return kiwiLexicalResponse{}, err
		}
		if detail := strings.TrimSpace(t.stderr.String()); detail != "" {
			return kiwiLexicalResponse{}, errors.New(detail)
		}
		return kiwiLexicalResponse{}, io.EOF
	}
	var response kiwiLexicalResponse
	if err := json.Unmarshal(t.stdout.Bytes(), &response); err != nil {
		return kiwiLexicalResponse{}, fmt.Errorf("decode Kiwi tokenizer response: %w", err)
	}
	return response, nil
}

func (t *externalLexicalTokenizer) Close() error {
	if t == nil || t.command == nil || t.command.Process == nil {
		return nil
	}
	_ = t.stdin.Close()
	err := t.command.Wait()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
