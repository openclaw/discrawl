package store

import (
	"context"
	"sync"
)

type lazyLexicalTokenizer struct {
	start     func() (LexicalTokenizer, error)
	once      sync.Once
	tokenizer LexicalTokenizer
	err       error
}

func newLazyLexicalTokenizer(start func() (LexicalTokenizer, error)) LexicalTokenizer {
	return &lazyLexicalTokenizer{start: start}
}

func (l *lazyLexicalTokenizer) Tokenize(ctx context.Context, text string) (string, error) {
	l.once.Do(func() {
		l.tokenizer, l.err = l.start()
	})
	if l.err != nil {
		return "", l.err
	}
	return l.tokenizer.Tokenize(ctx, text)
}

func (l *lazyLexicalTokenizer) Close() error {
	if l == nil || l.tokenizer == nil {
		return nil
	}
	return l.tokenizer.Close()
}
