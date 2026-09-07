package store

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
)

type OpenOptions struct {
	RebuildLexicalIndexes bool
	LexicalLanguages      []string
	LexicalKiwiCommand    string
	LexicalKiwiModel      string
	LexicalJaCommand      string
	LexicalZhCommand      string
}

func OpenWithOptions(ctx context.Context, path string, opts OpenOptions) (*Store, error) {
	tokenizers, err := newLexicalTokenizers(opts)
	if err != nil {
		return nil, err
	}
	return openLexicalStore(ctx, path, tokenizers, opts.RebuildLexicalIndexes)
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
		identity, err := lexicalAnalyzerIdentity(opts, language)
		if err != nil {
			closeLexicalTokenizers(tokenizers)
			return nil, err
		}
		switch language {
		case "ko":
			command := opts.LexicalKiwiCommand
			model := opts.LexicalKiwiModel
			tokenizers[language] = newLazyLexicalTokenizer(identity, func() (LexicalTokenizer, error) {
				tokenizer, err := startKiwiLexicalTokenizer(command, model)
				if err != nil {
					return nil, fmt.Errorf("start ko lexical tokenizer: %w", err)
				}
				return tokenizer, nil
			})
		case "ja":
			command := opts.LexicalJaCommand
			tokenizers[language] = newLazyLexicalTokenizer(identity, func() (LexicalTokenizer, error) {
				tokenizer, err := startHelperLexicalTokenizer("ja", command, "discrawl-ja", nil)
				if err != nil {
					return nil, fmt.Errorf("start ja lexical tokenizer: %w", err)
				}
				return tokenizer, nil
			})
		case "zh":
			command := opts.LexicalZhCommand
			tokenizers[language] = newLazyLexicalTokenizer(identity, func() (LexicalTokenizer, error) {
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

func lexicalAnalyzerIdentity(opts OpenOptions, language string) (string, error) {
	var command, fallback, model string
	switch language {
	case "ko":
		command, fallback, model = opts.LexicalKiwiCommand, "discrawl-kiwi", opts.LexicalKiwiModel
	case "ja":
		command, fallback = opts.LexicalJaCommand, "discrawl-ja"
	case "zh":
		command, fallback = opts.LexicalZhCommand, "discrawl-zh"
	case "ar":
		return newArabicLexicalTokenizer().Identity(), nil
	default:
		return "", fmt.Errorf("unsupported lexical language %q", language)
	}
	command, err := expandLexicalPath(command, fallback)
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(command) {
		if command != fallback {
			return "", fmt.Errorf("unsupported %s helper %q; use an absolute path or %s", fallback, command, fallback)
		}
		// Missing helpers remain lazy so metadata-only opens still work.
		if resolved, err := exec.LookPath(command); err == nil {
			command = resolved
		}
	}
	model, err = expandLexicalPath(model, "")
	if err != nil {
		return "", err
	}
	if model != "" {
		model, err = filepath.Abs(model)
		if err != nil {
			return "", err
		}
	}
	return lexicalFingerprint(language, command, model), nil
}
