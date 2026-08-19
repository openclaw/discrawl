package store

import (
	"fmt"
	"os/exec"
)

func startKiwiLexicalTokenizer(command, model string) (LexicalTokenizer, error) {
	cmd, err := newKiwiLexicalCommand(command, model)
	if err != nil {
		return nil, err
	}
	return startKiwiLexicalTokenizerCommand(cmd)
}

func startKiwiLexicalTokenizerCommand(cmd *exec.Cmd) (LexicalTokenizer, error) {
	return startHelperLexicalTokenizerCommand("ko", cmd)
}

func newKiwiLexicalCommand(command, model string) (*exec.Cmd, error) {
	model, err := expandLexicalPath(model, "")
	if err != nil {
		return nil, fmt.Errorf("resolve Kiwi model: %w", err)
	}
	var extra []string
	if model != "" {
		extra = []string{"--model", model}
	}
	return newHelperLexicalCommand(command, "discrawl-kiwi", extra)
}
