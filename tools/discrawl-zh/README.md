# discrawl-zh

Optional Chinese lexical helper. It uses the pure-Go
[`github.com/go-ego/gse`](https://pkg.go.dev/github.com/go-ego/gse)
search-mode segmenter with the embedded default dictionary.

This binary is not linked into the default Discrawl release. Build it only when
Chinese lexical fields are enabled.

```bash
go build -o discrawl-zh .
```

The helper speaks the same newline-delimited JSON protocol as `discrawl-kiwi`.
