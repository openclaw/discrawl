package main

import (
	"testing"

	"github.com/go-ego/gse"
)

func TestCutSearchKeepsCompoundParts(t *testing.T) {
	var seg gse.Segmenter
	if err := seg.LoadDictEmbed(); err != nil {
		t.Fatal(err)
	}
	tokens := unique(seg.CutSearch("自然语言处理很有趣", true))
	found := false
	for _, token := range tokens {
		if token == "语言" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected 语言 in %v", tokens)
	}
}

func TestQueryGroupsExcludePunctuation(t *testing.T) {
	var seg gse.Segmenter
	if err := seg.LoadDictEmbed(); err != nil {
		t.Fatal(err)
	}
	groups := queryGroups(&seg, "搜索，功能。7")
	if len(groups) != 3 || groups[0][0] != "搜索" || groups[1][0] != "功能" || groups[2][0] != "7" {
		t.Fatalf("unexpected query groups: %v", groups)
	}
}
