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
  sudachipy==0.6.11 \
  sudachidict_core==20260723 \
  jieba==0.42.1 \
  snowballstemmer==3.1.1

DISCRAWL_TOKENIZER_E2E=1 \
DISCRAWL_TOKENIZER_PYTHON=/tmp/discrawl-tokenizer-e2e/bin/python \
DISCRAWL_KIWI_HELPER=/tmp/discrawl-kiwi \
DISCRAWL_KIWI_MODEL=/tmp/kiwi-model/models/cong/base \
go test ./internal/store \
  -run TestMultilingualLexicalQualityBenchmark \
  -count=1 -v
```

## Result

Measured on macOS arm64 with Kiwi 0.23.2 through `kiwigo`; the remaining
analyzers used Python 3.13.2:

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

## Live verification transcript

Captured from the command above:

```text
=== RUN   TestMultilingualLexicalQualityBenchmark
database pages: unicode61=233472 bytes multilingual=319488 bytes (1.37x)
ko recall@5: unicode61=0/5 multilingual=5/5
ja recall@5: unicode61=0/5 multilingual=5/5
zh recall@5: unicode61=0/5 multilingual=5/5
ar recall@5: unicode61=0/5 multilingual=5/5
--- PASS: TestMultilingualLexicalQualityBenchmark
=== RUN   TestMultilingualLexicalSearchE2E
--- PASS: TestMultilingualLexicalSearchE2E
PASS
```

The Korean path now uses the published `github.com/codingpot/kiwigo` Go binding
against Kiwi 0.23.2. The live helper returned `오늘 저녁 먹 음 기록` for
`오늘 저녁먹음 기록`; a subsequent CLI search for `저녁` returned that fixture.
No Python process or `kiwipiepy` package is involved in Korean indexing or
query analysis.
