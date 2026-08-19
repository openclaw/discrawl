package store

import (
	"context"
	"fmt"
)

type OpenOptions struct {
	LexicalLanguages   []string
	LexicalKiwiCommand string
	LexicalKiwiModel   string
	LexicalJaCommand   string
	LexicalZhCommand   string
}

func OpenWithOptions(ctx context.Context, path string, opts OpenOptions) (*Store, error) {
	tokenizers, err := newLexicalTokenizers(opts)
	if err != nil {
		return nil, err
	}
	return openWithLexicalTokenizers(ctx, path, tokenizers)
}

func OpenReadOnlyWithOptions(ctx context.Context, path string, opts OpenOptions) (*Store, error) {
	tokenizers, err := newLexicalTokenizers(opts)
	if err != nil {
		return nil, err
	}
	return openReadOnlyWithLexicalTokenizers(ctx, path, tokenizers)
}

func newLexicalTokenizers(opts OpenOptions) (map[string]LexicalTokenizer, error) {
	if len(opts.LexicalLanguages) == 0 {
		return nil, nil
	}
	tokenizers := make(map[string]LexicalTokenizer, len(opts.LexicalLanguages))
	for _, language := range opts.LexicalLanguages {
		switch language {
		case "ko":
			command := opts.LexicalKiwiCommand
			model := opts.LexicalKiwiModel
			tokenizers[language] = newLazyLexicalTokenizer(func() (LexicalTokenizer, error) {
				tokenizer, err := startKiwiLexicalTokenizer(command, model)
				if err != nil {
					return nil, fmt.Errorf("start ko lexical tokenizer: %w", err)
				}
				return tokenizer, nil
			})
		case "ja":
			command := opts.LexicalJaCommand
			tokenizers[language] = newLazyLexicalTokenizer(func() (LexicalTokenizer, error) {
				tokenizer, err := startHelperLexicalTokenizer("ja", command, "discrawl-ja", nil)
				if err != nil {
					return nil, fmt.Errorf("start ja lexical tokenizer: %w", err)
				}
				return tokenizer, nil
			})
		case "zh":
			command := opts.LexicalZhCommand
			tokenizers[language] = newLazyLexicalTokenizer(func() (LexicalTokenizer, error) {
				tokenizer, err := startHelperLexicalTokenizer("zh", command, "discrawl-zh", nil)
				if err != nil {
					return nil, fmt.Errorf("start zh lexical tokenizer: %w", err)
				}
				return tokenizer, nil
			})
		case "ar":
			tokenizers[language] = newArabicLexicalTokenizer()
		default:
			return nil, fmt.Errorf("unsupported lexical language %q", language)
		}
	}
	return tokenizers, nil
}
