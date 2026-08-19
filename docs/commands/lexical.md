# `lexical`

Installs only the optional Python tokenizer packages selected by
`search.lexical.languages`. Korean is provided by the native
`discrawl-kiwi` Go helper and is not installed by this command.

## Usage

```bash
discrawl lexical install
```

The configured `search.lexical.python` interpreter must belong to a virtual
environment when Japanese, Chinese, or Arabic is selected. Discrawl refuses to
install packages into a system Python. With a Korean-only configuration this
command is a no-op because Korean uses the separately built Go helper.

Package versions are pinned by Discrawl:

| Language | Packages |
| --- | --- |
| `ko` | none; uses `github.com/codingpot/kiwigo` + Kiwi 0.23.2 |
| `ja` | `sudachipy==0.6.11`, `sudachidict_core==20260723` |
| `zh` | `jieba==0.42.1` |
| `ar` | `snowballstemmer==3.1.1` |

Installation is always explicit. Opening an archive or running a search never
downloads or installs code. Tokenizer workers start lazily only when an enabled
language is first used for indexing or search.

For Korean, configure:

```toml
[search.lexical]
languages = ["ko"]
kiwi_command = "~/.local/share/discrawl/bin/discrawl-kiwi"
kiwi_model = "~/.local/share/discrawl/models/kiwi/base"
```

The helper source is under `tools/discrawl-kiwi`. It uses the existing
[`github.com/codingpot/kiwigo`](https://pkg.go.dev/github.com/codingpot/kiwigo)
binding and dynamically linked Kiwi library; it does not use Python or
`kiwipiepy`.
