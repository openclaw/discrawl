package store

import (
	"context"
	"sync"
)

type lazyLexicalTokenizer struct {
	identity  string
	start     func() (LexicalTokenizer, error)
	once      sync.Once
	tokenizer LexicalTokenizer
	err       error
}

func newLazyLexicalTokenizer(identity string, start func() (LexicalTokenizer, error)) LexicalTokenizer {
	return &lazyLexicalTokenizer{identity: identity, start: start}
}

func (l *lazyLexicalTokenizer) Identity() string { return l.identity }

func (l *lazyLexicalTokenizer) Tokenize(ctx context.Context, text string) (string, error) {
	l.once.Do(func() {
		l.tokenizer, l.err = l.start()
	})
	if l.err != nil {
		return "", l.err
	}
	return l.tokenizer.Tokenize(ctx, text)
}

func (l *lazyLexicalTokenizer) QueryGroups(ctx context.Context, text string) ([][]string, error) {
	l.once.Do(func() { l.tokenizer, l.err = l.start() })
	if l.err != nil {
		return nil, l.err
	}
	return l.tokenizer.QueryGroups(ctx, text)
}

func (l *lazyLexicalTokenizer) Close() error {
	if l == nil || l.tokenizer == nil {
		return nil
	}
	return l.tokenizer.Close()
}
