package store

import (
	"bufio"
	"bytes"
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

type helperLexicalResponse struct {
	Ready  bool   `json:"ready,omitempty"`
	Tokens string `json:"tokens,omitempty"`
	Error  string `json:"error,omitempty"`
}

type lockedBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

func startHelperLexicalTokenizer(language, command, defaultName string, extraArgs []string) (LexicalTokenizer, error) {
	cmd, err := newHelperLexicalCommand(command, defaultName, extraArgs)
	if err != nil {
		return nil, err
	}
	return startHelperLexicalTokenizerCommand(language, cmd)
}

func startHelperLexicalTokenizerCommand(language string, cmd *exec.Cmd) (LexicalTokenizer, error) {
	tokenizer := &externalLexicalTokenizer{
		language: language,
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
		return nil, fmt.Errorf("start %s lexical tokenizer: %w", language, err)
	}
	response, err := tokenizer.readStartup()
	if err != nil {
		_ = tokenizer.Close()
		return nil, fmt.Errorf("initialize %s lexical tokenizer: %w", language, err)
	}
	if !response.Ready {
		_ = tokenizer.Close()
		if response.Error == "" {
			return nil, fmt.Errorf("initialize %s lexical tokenizer", language)
		}
		return nil, errors.New(response.Error)
	}
	return tokenizer, nil
}

func newHelperLexicalCommand(command, defaultName string, extraArgs []string) (*exec.Cmd, error) {
	command, err := expandLexicalPath(command, defaultName)
	if err != nil {
		return nil, fmt.Errorf("resolve %s helper: %w", defaultName, err)
	}
	if !filepath.IsAbs(command) {
		if command != defaultName {
			return nil, fmt.Errorf(
				"unsupported %s helper %q; use an absolute path or %s",
				defaultName,
				command,
				defaultName,
			)
		}
		command, err = exec.LookPath(command)
		if err != nil {
			return nil, fmt.Errorf("find %s helper: %w", defaultName, err)
		}
	}
	args := append([]string{command}, extraArgs...)
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

func (t *externalLexicalTokenizer) readStartup() (helperLexicalResponse, error) {
	result := make(chan struct {
		response helperLexicalResponse
		err      error
	}, 1)
	go func() {
		response, err := t.readResponse()
		result <- struct {
			response helperLexicalResponse
			err      error
		}{response, err}
	}()
	select {
	case output := <-result:
		return output.response, output.err
	case <-time.After(30 * time.Second):
		_ = t.command.Process.Kill()
		return helperLexicalResponse{}, errors.New("lexical tokenizer startup timed out after 30s")
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
		response helperLexicalResponse
		err      error
	}, 1)
	go func() {
		response, err := t.readResponse()
		result <- struct {
			response helperLexicalResponse
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
		return "", errors.New("lexical tokenizer response timed out after 30s")
	}
}

func (t *externalLexicalTokenizer) readResponse() (helperLexicalResponse, error) {
	if !t.stdout.Scan() {
		if err := t.stdout.Err(); err != nil {
			return helperLexicalResponse{}, err
		}
		if detail := strings.TrimSpace(t.stderr.String()); detail != "" {
			return helperLexicalResponse{}, errors.New(detail)
		}
		return helperLexicalResponse{}, io.EOF
	}
	var response helperLexicalResponse
	if err := json.Unmarshal(t.stdout.Bytes(), &response); err != nil {
		return helperLexicalResponse{}, fmt.Errorf("decode lexical tokenizer response: %w", err)
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

func lexicalWorkerEnvironment(parent []string) []string {
	allowed := map[string]struct{}{
		"HOME":       {},
		"LANG":       {},
		"LC_ALL":     {},
		"PATH":       {},
		"PATHEXT":    {},
		"SYSTEMROOT": {},
		"TEMP":       {},
		"TMP":        {},
		"TMPDIR":     {},
		"WINDIR":     {},
	}
	environment := make([]string, 0, len(allowed))
	for _, entry := range parent {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, ok := allowed[strings.ToUpper(key)]; ok {
			environment = append(environment, entry)
		}
	}
	return environment
}
