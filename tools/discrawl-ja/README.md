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
