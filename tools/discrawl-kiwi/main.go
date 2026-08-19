package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	kiwi "github.com/codingpot/kiwigo"
)

type request struct {
	Text string `json:"text"`
}

type response struct {
	Ready   bool   `json:"ready,omitempty"`
	Tokens  string `json:"tokens,omitempty"`
	Error   string `json:"error,omitempty"`
	Version string `json:"version,omitempty"`
}

func main() {
	model := flag.String("model", "", "path to the Kiwi base model directory")
	flag.Parse()
	if strings.TrimSpace(*model) == "" {
		writeResponse(response{Error: "Kiwi model path is required; pass --model"})
		os.Exit(2)
	}
	analyzer, err := kiwi.New(*model, kiwi.WithNumThread(0))
	if err != nil {
		writeResponse(response{Error: err.Error()})
		os.Exit(2)
	}
	defer analyzer.Close()

	writeResponse(response{Ready: true, Version: kiwi.KiwiVersion()})
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 8*1024*1024)
	for scanner.Scan() {
		var input request
		if err := json.Unmarshal(scanner.Bytes(), &input); err != nil {
			writeResponse(response{Error: fmt.Sprintf("decode request: %v", err)})
			continue
		}
		tokens, err := tokenize(analyzer, input.Text)
		if err != nil {
			writeResponse(response{Error: err.Error()})
			continue
		}
		writeResponse(response{Tokens: strings.Join(tokens, " ")})
	}
	if err := scanner.Err(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type kiwiAnalyzer interface {
	Analyze(string, ...kiwi.AnalyzeOptionFunc) ([]kiwi.TokenResult, error)
}

func tokenize(analyzer kiwiAnalyzer, text string) ([]string, error) {
	results, err := analyzer.Analyze(text, kiwi.WithTopN(1))
	if err != nil {
		return nil, fmt.Errorf("analyze Korean text: %w", err)
	}
	if len(results) == 0 {
		return nil, errors.New("Kiwi returned no analysis")
	}
	tokens := make([]string, 0, len(results[0].Tokens))
	seen := make(map[string]struct{}, len(results[0].Tokens))
	for _, token := range results[0].Tokens {
		if strings.HasPrefix(string(token.Tag), "S") {
			continue
		}
		form := strings.ToLower(strings.TrimSpace(token.Form))
		if form == "" {
			continue
		}
		if _, ok := seen[form]; ok {
			continue
		}
		seen[form] = struct{}{}
		tokens = append(tokens, form)
	}
	return tokens, nil
}

func writeResponse(output response) {
	_ = json.NewEncoder(os.Stdout).Encode(output)
}
