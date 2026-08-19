package main

import (
	"errors"
	"strings"
	"testing"

	kiwi "github.com/codingpot/kiwigo"
	"github.com/stretchr/testify/require"
)

type fakeAnalyzer struct {
	results []kiwi.TokenResult
	err     error
}

func (f fakeAnalyzer) Analyze(string, ...kiwi.AnalyzeOptionFunc) ([]kiwi.TokenResult, error) {
	return f.results, f.err
}

func TestTokenizeFiltersPunctuationAndDuplicates(t *testing.T) {
	tokens, err := tokenize(fakeAnalyzer{results: []kiwi.TokenResult{{Tokens: []kiwi.TokenInfo{
		{Form: "오늘", Tag: kiwi.POS_NNG},
		{Form: ".", Tag: kiwi.POS_SF},
		{Form: "오늘", Tag: kiwi.POS_NNG},
		{Form: "먹", Tag: kiwi.POS_VV},
	}}}}, "오늘먹음")
	require.NoError(t, err)
	require.Equal(t, []string{"오늘", "먹"}, tokens)
}

func TestTokenizeReportsAnalyzerFailures(t *testing.T) {
	_, err := tokenize(fakeAnalyzer{err: errors.New("failed")}, "text")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "failed"))

	_, err = tokenize(fakeAnalyzer{}, "text")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "no analysis"))
}
