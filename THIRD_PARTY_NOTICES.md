# Third-party notices

## Optional Korean lexical helper

The separately built `tools/discrawl-kiwi` helper depends on:

- [Kiwi](https://github.com/bab2min/Kiwi), copyright Minchul Lee,
  licensed under GNU LGPL 2.1 or later.
- [github.com/codingpot/kiwigo](https://github.com/codingpot/kiwigo), a Go
  binding for Kiwi, licensed under GNU LGPL 2.1.

These dependencies are optional and are not linked into the default Discrawl
binary. Distributors who provide the helper or Kiwi native binaries must
satisfy their applicable LGPL notice, source-access, and relinking
requirements. Kiwi's full license text is available from its source
repository and the GNU project:

https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html

## Optional Japanese lexical helper

`tools/discrawl-ja` depends on:

- [Kagome](https://github.com/ikawaha/kagome), MIT
- [kagome-dict IPA](https://github.com/ikawaha/kagome-dict), MIT wrapper around
  mecab-ipadic-2.7.0-20070801 / ICOT Free Software

These dependencies are optional and are not linked into the default Discrawl
binary. Preserve the IPADIC/ICOT notice when distributing the helper.

## Optional Chinese lexical helper

`tools/discrawl-zh` depends on [GSE](https://github.com/go-ego/gse), Apache-2.0.
It is optional and is not linked into the default Discrawl binary.

## In-process Arabic analyzer

The Arabic light stemmer follows the Lucene/Bleve prefix-and-suffix contract.
It is implemented in Discrawl itself and does not depend on Python.
