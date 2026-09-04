package store

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLazyLexicalTokenizerStartsOnce(t *testing.T) {
	starts := 0
	tokenizer := newLazyLexicalTokenizer(func() (LexicalTokenizer, error) {
		starts++
		return stubLexicalTokenizer{tokenize: func(text string) string {
			return text + " tokenized"
		}}, nil
	})

	first, err := tokenizer.Tokenize(context.Background(), "first")
	require.NoError(t, err)
	require.Equal(t, "first tokenized", first)
	second, err := tokenizer.Tokenize(context.Background(), "second")
	require.NoError(t, err)
	require.Equal(t, "second tokenized", second)
	require.Equal(t, 1, starts)
	require.NoError(t, tokenizer.Close())
}

func TestLazyLexicalTokenizerCachesStartupFailure(t *testing.T) {
	starts := 0
	tokenizer := newLazyLexicalTokenizer(func() (LexicalTokenizer, error) {
		starts++
		return nil, errors.New("startup failed")
	})

	_, err := tokenizer.Tokenize(context.Background(), "first")
	require.ErrorContains(t, err, "startup failed")
	_, err = tokenizer.Tokenize(context.Background(), "second")
	require.ErrorContains(t, err, "startup failed")
	require.Equal(t, 1, starts)
	require.NoError(t, tokenizer.Close())
}

func TestLazyLexicalTokenizerCloseBeforeStart(t *testing.T) {
	tokenizer := newLazyLexicalTokenizer(func() (LexicalTokenizer, error) {
		return nil, errors.New("must not start")
	})
	require.NoError(t, tokenizer.Close())
}
