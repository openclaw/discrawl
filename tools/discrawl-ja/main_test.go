package main

import (
	"testing"

	"github.com/ikawaha/kagome-dict/ipa"
	"github.com/ikawaha/kagome/v2/tokenizer"
)

func TestTokenizeSearchSplitsCompounds(t *testing.T) {
	analyzer, err := tokenizer.New(ipa.Dict(), tokenizer.OmitBosEos())
	if err != nil {
		t.Fatal(err)
	}
	tokens := tokenize(analyzer, "東京都庁に行きます")
	found := false
	for _, token := range tokens {
		if token == "東京" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected 東京 in %v", tokens)
	}
}
