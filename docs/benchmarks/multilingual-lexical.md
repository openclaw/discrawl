# Multilingual lexical benchmark

This targeted microbenchmark checks the failure mode the optional multilingual
fields are designed to fix: a query matching a useful subword inside an
unsegmented Korean, Japanese, Chinese, or Arabic surface form.

It is not a general relevance benchmark. The fixtures deliberately contain
compound forms and attached Arabic proclitics that SQLite FTS5 `unicode61`
cannot retrieve as independent terms.

## Reproduce

```bash
python3 -m venv /tmp/discrawl-tokenizer-e2e
/tmp/discrawl-tokenizer-e2e/bin/python -m pip install \
  kiwipiepy==0.23.2 \
  sudachipy==0.6.11 \
  sudachidict_core==20260723 \
  jieba==0.42.1 \
  snowballstemmer==3.1.1

DISCRAWL_TOKENIZER_E2E=1 \
DISCRAWL_TOKENIZER_PYTHON=/tmp/discrawl-tokenizer-e2e/bin/python \
go test ./internal/store \
  -run TestMultilingualLexicalQualityBenchmark \
  -count=1 -v
```

## Result

Measured on macOS arm64 with Python 3.13.2:

| Language | `unicode61` recall@5 | Multilingual recall@5 |
| --- | ---: | ---: |
| Korean / Kiwi | 0/5 | 5/5 |
| Japanese / Sudachi A | 0/5 | 5/5 |
| Chinese / Jieba search mode | 0/5 | 5/5 |
| Arabic / Snowball + proclitics | 0/5 | 5/5 |
| **Total** | **0/20** | **20/20** |

Across repeated runs of this 20-message fixture, the database with four extra
FTS tables used 1.36-1.37x the SQLite pages of the baseline. Real corpora will
have different ratios because base tables, attachments, and metadata are not
duplicated while postings scale with enabled analyzers.

The benchmark therefore supports a narrow claim: configured language fields
substantially improve recall for these segmentation cases. It does not claim a
universal 100-point gain on natural Discord query distributions.
