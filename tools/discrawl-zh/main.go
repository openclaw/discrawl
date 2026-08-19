package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/go-ego/gse"
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
	var seg gse.Segmenter
	if err := seg.LoadDictEmbed(); err != nil {
		writeResponse(response{Error: err.Error()})
		os.Exit(2)
	}
	writeResponse(response{Ready: true, Version: "gse-search"})
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 8*1024*1024)
	for scanner.Scan() {
		var input request
		if err := json.Unmarshal(scanner.Bytes(), &input); err != nil {
			writeResponse(response{Error: fmt.Sprintf("decode request: %v", err)})
			continue
		}
		writeResponse(response{Tokens: strings.Join(unique(seg.CutSearch(input.Text, true)), " ")})
	}
	if err := scanner.Err(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func unique(forms []string) []string {
	seen := make(map[string]struct{}, len(forms))
	tokens := make([]string, 0, len(forms))
	for _, form := range forms {
		form = strings.ToLower(strings.TrimSpace(form))
		if form == "" {
			continue
		}
		if _, ok := seen[form]; ok {
			continue
		}
		seen[form] = struct{}{}
		tokens = append(tokens, form)
	}
	return tokens
}

func writeResponse(output response) {
	_ = json.NewEncoder(os.Stdout).Encode(output)
}
