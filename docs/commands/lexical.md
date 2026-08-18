# `lexical`

Installs only the optional tokenizer packages selected by
`search.lexical.languages`.

## Usage

```bash
discrawl lexical install
```

The configured `search.lexical.python` interpreter must belong to a virtual
environment. Discrawl refuses to install packages into a system Python.

Package versions are pinned by Discrawl:

| Language | Packages |
| --- | --- |
| `ko` | `kiwipiepy==0.23.2` |
| `ja` | `sudachipy==0.6.11`, `sudachidict_core==20260723` |
| `zh` | `jieba==0.42.1` |
| `ar` | `snowballstemmer==3.1.1` |

Installation is always explicit. Opening an archive or running a search never
downloads or installs code. Tokenizer workers start lazily only when an enabled
language is first used for indexing or search.
