package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/ikawaha/kagome-dict/ipa"
	"github.com/ikawaha/kagome/v2/tokenizer"
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
	analyzer, err := tokenizer.New(ipa.Dict(), tokenizer.OmitBosEos())
	if err != nil {
		writeResponse(response{Error: err.Error()})
		os.Exit(2)
	}
	writeResponse(response{Ready: true, Version: "kagome-ipa-search"})
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 8*1024*1024)
	for scanner.Scan() {
		var input request
		if err := json.Unmarshal(scanner.Bytes(), &input); err != nil {
			writeResponse(response{Error: fmt.Sprintf("decode request: %v", err)})
			continue
		}
		writeResponse(response{Tokens: strings.Join(tokenize(analyzer, input.Text), " ")})
	}
	if err := scanner.Err(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func tokenize(analyzer *tokenizer.Tokenizer, text string) []string {
	seen := make(map[string]struct{})
	var tokens []string
	add := func(form string) {
		form = strings.ToLower(strings.TrimSpace(form))
		if form == "" || form == "*" {
			return
		}
		if _, ok := seen[form]; ok {
			return
		}
		seen[form] = struct{}{}
		tokens = append(tokens, form)
	}
	for _, token := range analyzer.Analyze(text, tokenizer.Search) {
		add(token.Surface)
		if base := tokenBaseForm(token); base != "" {
			add(base)
		}
	}
	return tokens
}

func tokenBaseForm(token tokenizer.Token) string {
	features := token.Features()
	if len(features) > 6 {
		return features[6]
	}
	return ""
}

func writeResponse(output response) {
	_ = json.NewEncoder(os.Stdout).Encode(output)
}
