package store

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Arabic light stemming follows the Lucene/Bleve prefix-and-suffix
// contract so attached proclitics remain searchable as independent terms.
var arabicPrefixes = []string{"وال", "فال", "بال", "كال", "لال", "ال", "لل", "و", "ف", "ب", "ك", "ل"}

var arabicSuffixes = []string{"ها", "ان", "ات", "ون", "ين", "يه", "ية", "ه", "ة", "ي"}

type arabicLexicalTokenizer struct{}

func newArabicLexicalTokenizer() LexicalTokenizer {
	return arabicLexicalTokenizer{}
}

func (arabicLexicalTokenizer) Tokenize(_ context.Context, text string) (string, error) {
	return strings.Join(tokenizeArabic(text), " "), nil
}

func (arabicLexicalTokenizer) Close() error {
	return nil
}

func tokenizeArabic(text string) []string {
	seen := make(map[string]struct{})
	var tokens []string
	add := func(token string) {
		token = strings.ToLower(strings.TrimSpace(token))
		if token == "" {
			return
		}
		if _, ok := seen[token]; ok {
			return
		}
		seen[token] = struct{}{}
		tokens = append(tokens, token)
	}
	for _, word := range splitArabicWords(normalizeArabic(text)) {
		add(word)
		stripped := stripArabicPrefix(word)
		add(stripped)
		add(stemArabic(word))
		add(stemArabic(stripped))
	}
	return tokens
}

func splitArabicWords(text string) []string {
	var words []string
	var current []rune
	flush := func() {
		if len(current) == 0 {
			return
		}
		words = append(words, string(current))
		current = current[:0]
	}
	for _, r := range text {
		if unicode.IsLetter(r) {
			current = append(current, r)
			continue
		}
		flush()
	}
	flush()
	return words
}

func normalizeArabic(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, r := range strings.ToValidUTF8(text, "") {
		switch r {
		case '\u064B', '\u064C', '\u064D', '\u064E', '\u064F', '\u0650', '\u0651', '\u0652', '\u0670', '\u0640':
			continue
		default:
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

func stripArabicPrefix(word string) string {
	for _, prefix := range arabicPrefixes {
		if !strings.HasPrefix(word, prefix) {
			continue
		}
		rest := strings.TrimPrefix(word, prefix)
		if utf8.RuneCountInString(rest) < 2 {
			continue
		}
		if prefix == "و" && utf8.RuneCountInString(word) < 4 {
			continue
		}
		return rest
	}
	return word
}

func stemArabic(word string) string {
	runes := []rune(stripArabicPrefix(word))
	for _, suffix := range arabicSuffixes {
		suffixRunes := []rune(suffix)
		if len(runes)-len(suffixRunes) < 2 {
			continue
		}
		if string(runes[len(runes)-len(suffixRunes):]) != suffix {
			continue
		}
		runes = runes[:len(runes)-len(suffixRunes)]
	}
	return string(runes)
}
