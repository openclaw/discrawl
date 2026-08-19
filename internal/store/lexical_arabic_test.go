package store

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenizeArabicSplitsProcliticsForSearch(t *testing.T) {
	cases := map[string]string{
		"والكتاب":   "كتاب",
		"والمدرسة":  "مدرسة",
		"فالاجتماع": "اجتماع",
		"بالسجل":    "سجل",
		"كالبرنامج": "برنامج",
	}
	for input, want := range cases {
		tokens := tokenizeArabic(input)
		require.Containsf(t, tokens, want, "input %q tokens=%v", input, tokens)
	}
}

func TestArabicTokenizerIsInProcessAndIdempotent(t *testing.T) {
	tokenizer := newArabicLexicalTokenizer()
	tokens, err := tokenizer.Tokenize(context.Background(), "والكتاب في المدرسة")
	require.NoError(t, err)
	require.Contains(t, strings.Split(tokens, " "), "كتاب")
	require.NoError(t, tokenizer.Close())
	require.NoError(t, tokenizer.Close())
}
