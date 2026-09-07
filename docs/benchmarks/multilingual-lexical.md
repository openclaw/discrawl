# Multilingual lexical benchmark

This targeted microbenchmark checks the failure mode the optional multilingual
fields are designed to fix: a query matching a useful subword inside an
unsegmented Korean, Japanese, Chinese, or Arabic surface form.

It is not a general relevance benchmark. The fixtures deliberately contain
compound forms and attached Arabic proclitics that SQLite FTS5 `unicode61`
cannot retrieve as independent terms.

## Reproduce

```bash
# Korean helper: official Kiwi 0.23.2 + discrawl-kiwi
# Japanese helper: go build ./tools/discrawl-ja
# Chinese helper: go build ./tools/discrawl-zh
# Arabic: in-process, no helper

DISCRAWL_TOKENIZER_E2E=1 \
DISCRAWL_KIWI_HELPER=/tmp/discrawl-kiwi \
DISCRAWL_KIWI_MODEL=/tmp/kiwi-model/models/cong/base \
DISCRAWL_JA_HELPER=/tmp/discrawl-ja \
DISCRAWL_ZH_HELPER=/tmp/discrawl-zh \
go test ./internal/store \
  -run TestMultilingualLexicalQualityBenchmark \
  -count=1 -v
```

## Result

Measured on macOS arm64 with native Kiwi 0.23.2, Kagome Search, GSE CutSearch,
and the in-process Arabic analyzer:

| Language | `unicode61` recall@5 | Multilingual recall@5 |
| --- | ---: | ---: |
| Korean / Kiwi via kiwigo | 0/5 | 5/5 |
| Japanese / Kagome Search | 0/5 | 5/5 |
| Chinese / GSE search mode | 0/5 | 5/5 |
| Arabic / in-process light stem | 0/5 | 5/5 |
| **Total** | **0/20** | **20/20** |

The benchmark therefore supports a narrow claim: configured language fields
substantially improve recall for these segmentation cases. It does not claim a
universal 100-point gain on natural Discord query distributions. No Python
tokenizer is used.
