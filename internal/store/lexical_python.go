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

type OpenOptions struct {
	LexicalLanguages []string
	LexicalPython    string
}

type pythonLexicalTokenizer struct {
	language string
	command  *exec.Cmd
	stdin    io.WriteCloser
	stdout   *bufio.Scanner
	stderr   lockedBuffer
	mutex    sync.Mutex
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

type pythonLexicalResponse struct {
	Ready  bool   `json:"ready,omitempty"`
	Tokens string `json:"tokens,omitempty"`
	Error  string `json:"error,omitempty"`
}

func OpenWithOptions(ctx context.Context, path string, opts OpenOptions) (*Store, error) {
	tokenizers, err := newPythonLexicalTokenizers(opts)
	if err != nil {
		return nil, err
	}
	return openWithLexicalTokenizers(ctx, path, tokenizers)
}

func OpenReadOnlyWithOptions(ctx context.Context, path string, opts OpenOptions) (*Store, error) {
	tokenizers, err := newPythonLexicalTokenizers(opts)
	if err != nil {
		return nil, err
	}
	return openReadOnlyWithLexicalTokenizers(ctx, path, tokenizers)
}

func newPythonLexicalTokenizers(opts OpenOptions) (map[string]LexicalTokenizer, error) {
	if len(opts.LexicalLanguages) == 0 {
		return nil, nil
	}
	python, err := expandLexicalPython(opts.LexicalPython)
	if err != nil {
		return nil, err
	}
	tokenizers := make(map[string]LexicalTokenizer, len(opts.LexicalLanguages))
	for _, language := range opts.LexicalLanguages {
		tokenizers[language] = newLazyLexicalTokenizer(func() (LexicalTokenizer, error) {
			return startPythonLexicalTokenizer(python, language)
		})
	}
	return tokenizers, nil
}

func expandLexicalPython(python string) (string, error) {
	python = strings.TrimSpace(python)
	if python == "" {
		return "python3", nil
	}
	if !strings.HasPrefix(python, "~/") {
		return python, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve lexical Python home directory: %w", err)
	}
	return filepath.Join(home, strings.TrimPrefix(python, "~/")), nil
}

func startPythonLexicalTokenizer(
	python string,
	language string,
) (*pythonLexicalTokenizer, error) {
	command, err := newPythonLexicalCommand(python, language)
	if err != nil {
		return nil, err
	}
	return startPythonLexicalTokenizerCommand(command, language)
}

func startPythonLexicalTokenizerCommand(
	command *exec.Cmd,
	language string,
) (*pythonLexicalTokenizer, error) {
	tokenizer := &pythonLexicalTokenizer{language: language, command: command}
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	command.Stderr = &tokenizer.stderr
	tokenizer.stdin = stdin
	tokenizer.stdout = bufio.NewScanner(stdout)
	tokenizer.stdout.Buffer(make([]byte, 4096), 8*1024*1024)
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start %s lexical tokenizer with %s: %w", language, command.Path, err)
	}
	response, err := tokenizer.readStartupResponse()
	if err != nil {
		_ = tokenizer.Close()
		return nil, fmt.Errorf("initialize %s lexical tokenizer: %w", language, err)
	}
	if !response.Ready {
		_ = tokenizer.Close()
		return nil, fmt.Errorf("initialize %s lexical tokenizer: %s", language, response.Error)
	}
	return tokenizer, nil
}

func (p *pythonLexicalTokenizer) readStartupResponse() (pythonLexicalResponse, error) {
	type startupResult struct {
		response pythonLexicalResponse
		err      error
	}
	result := make(chan startupResult, 1)
	go func() {
		response, err := p.readResponse()
		result <- startupResult{response: response, err: err}
	}()
	select {
	case startup := <-result:
		return startup.response, startup.err
	case <-time.After(30 * time.Second):
		_ = p.command.Process.Kill()
		return pythonLexicalResponse{}, errors.New("startup timed out after 30s")
	}
}

func (p *pythonLexicalTokenizer) Tokenize(ctx context.Context, text string) (string, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	request, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return "", err
	}
	if _, err := p.stdin.Write(append(request, '\n')); err != nil {
		return "", fmt.Errorf("write tokenizer request: %w", err)
	}
	response, err := p.readResponseContext(ctx)
	if err != nil {
		return "", err
	}
	if response.Error != "" {
		return "", errors.New(response.Error)
	}
	return response.Tokens, nil
}

func (p *pythonLexicalTokenizer) readResponseContext(ctx context.Context) (pythonLexicalResponse, error) {
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	type responseResult struct {
		response pythonLexicalResponse
		err      error
	}
	result := make(chan responseResult, 1)
	go func() {
		response, err := p.readResponse()
		result <- responseResult{response: response, err: err}
	}()
	select {
	case response := <-result:
		return response.response, response.err
	case <-readCtx.Done():
		_ = p.command.Process.Kill()
		return pythonLexicalResponse{}, fmt.Errorf("tokenizer response: %w", readCtx.Err())
	}
}

func (p *pythonLexicalTokenizer) readResponse() (pythonLexicalResponse, error) {
	if !p.stdout.Scan() {
		err := p.stdout.Err()
		if err == nil {
			err = io.EOF
		}
		detail := strings.TrimSpace(p.stderr.String())
		if detail != "" {
			return pythonLexicalResponse{}, fmt.Errorf("%w: %s", err, detail)
		}
		return pythonLexicalResponse{}, err
	}
	var response pythonLexicalResponse
	if err := json.Unmarshal(p.stdout.Bytes(), &response); err != nil {
		return pythonLexicalResponse{}, fmt.Errorf("decode tokenizer response: %w", err)
	}
	return response, nil
}

func (p *pythonLexicalTokenizer) Close() error {
	if p == nil || p.command == nil || p.command.Process == nil {
		return nil
	}
	_ = p.stdin.Close()
	err := p.command.Wait()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

const pythonLexicalWorker = `
import json
import re
import sys
import unicodedata

language = sys.argv[1]

try:
    if language == "default":
        engine = None
    elif language == "ko":
        from kiwipiepy import Kiwi
        engine = Kiwi()
    elif language == "ja":
        from sudachipy import dictionary, tokenizer as sudachi_tokenizer
        engine = dictionary.Dictionary().create()
        split_mode = sudachi_tokenizer.Tokenizer.SplitMode.A
    elif language == "zh":
        import jieba
        engine = jieba
    elif language == "ar":
        import snowballstemmer
        engine = snowballstemmer.stemmer("arabic")
    else:
        raise RuntimeError("unsupported lexical language: " + language)
except Exception as error:
    print(json.dumps({"ready": False, "error": str(error)}, ensure_ascii=False), flush=True)
    raise SystemExit(2)

print(json.dumps({"ready": True}, ensure_ascii=False), flush=True)

def unique(tokens):
    output = []
    seen = set()
    for token in tokens:
        token = unicodedata.normalize("NFKC", token).strip().lower()
        if token and token not in seen:
            seen.add(token)
            output.append(token)
    return output

def tokenize(text):
    if language == "default":
        return unique(re.findall(r"[^\W\d_]+", text, flags=re.UNICODE))
    if language == "ko":
        return unique(token.form for token in engine.tokenize(text) if not token.tag.startswith("S"))
    if language == "ja":
        output = []
        for token in engine.tokenize(text, split_mode):
            output.append(token.surface())
            base = token.dictionary_form()
            if base != "*":
                output.append(base)
        return unique(output)
    if language == "zh":
        return unique(engine.cut_for_search(text))
    words = re.findall(r"[^\W\d_]+", text, flags=re.UNICODE)
    normalized = [re.sub(r"[\u064b-\u065f\u0670\u0640]", "", word) for word in words]
    variants = list(normalized)
    for word in normalized:
        if word.startswith(("وال", "فال", "بال", "كال", "لال")) and len(word) > 4:
            variants.append(word[3:])
        elif word.startswith("ال") and len(word) > 3:
            variants.append(word[2:])
        elif word[:1] in ("و", "ف", "ب", "ك", "ل") and len(word) > 3:
            variants.append(word[1:])
    return unique(variants + engine.stemWords(variants))

for line in sys.stdin:
    try:
        request = json.loads(line)
        print(json.dumps({"tokens": " ".join(tokenize(request.get("text", "")))}, ensure_ascii=False), flush=True)
    except Exception as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False), flush=True)
`
