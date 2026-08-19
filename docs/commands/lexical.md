# `lexical`

Shows the Go helpers required by `search.lexical.languages`. Discrawl never
downloads or installs tokenizer packages.

## Usage

```bash
discrawl lexical install
```

| Language | Runtime | Command |
| --- | --- | --- |
| `ko` | helper | `discrawl-kiwi` (`github.com/codingpot/kiwigo` + Kiwi 0.23.2) |
| `ja` | helper | `discrawl-ja` (`github.com/ikawaha/kagome/v2` Search) |
| `zh` | helper | `discrawl-zh` (`github.com/go-ego/gse` CutSearch) |
| `ar` | in-process | none |

Tokenizer helpers start lazily only when an enabled language is first used for
indexing or search. Opening an archive never starts a helper.

```toml
[search.lexical]
languages = ["ko", "ja", "zh", "ar"]
kiwi_command = "~/.local/share/discrawl/bin/discrawl-kiwi"
kiwi_model = "~/.local/share/discrawl/models/kiwi/base"
ja_command = "~/.local/share/discrawl/bin/discrawl-ja"
zh_command = "~/.local/share/discrawl/bin/discrawl-zh"
```

Helper sources live under `tools/discrawl-kiwi`, `tools/discrawl-ja`, and
`tools/discrawl-zh`. None of those helpers use Python.
