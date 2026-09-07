# discrawl-ja

Optional Japanese lexical helper. It uses the pure-Go
[`github.com/ikawaha/kagome/v2`](https://pkg.go.dev/github.com/ikawaha/kagome/v2)
tokenizer in Search mode with the embedded MeCab-IPADIC dictionary.

This binary is not linked into the default Discrawl release. Build it only when
Japanese lexical fields are enabled.

```bash
go build -o discrawl-ja .
```

The helper speaks the same newline-delimited JSON protocol as `discrawl-kiwi`.

Query requests add `"query":true` to the JSON request. The response includes
`"groups":[["surface","base"],["required-term"]]`: groups are conjoined,
while equivalent terms within one group are alternatives. Index requests
continue to return the flat `tokens` string. Build helpers from the same
Discrawl source revision as the CLI.
