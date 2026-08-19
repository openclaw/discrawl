# Search modes

`discrawl search` has three modes. FTS is the default and works with no embeddings.

## Modes

- **`fts`** (default) - searches the local SQLite FTS5 index, returns newest matching messages first
- **`semantic`** - embeds the query, scores against locally stored message vectors; errors out cleanly if embeddings are disabled or no compatible vectors exist
- **`hybrid`** - runs FTS and semantic, deduplicates by message id, falls back to FTS when semantic is unavailable

## FTS details

- backed by SQLite FTS5 with the default `unicode61` tokenizer
- optional `[search.lexical]` languages add independent tokenizer-specific FTS tables and merge their ranked results with reciprocal rank fusion
- supported presets are Korean with native Kiwi through `kiwigo`, Japanese with Kagome Search, Chinese with GSE search mode, and Arabic with in-process light stemming
- user query terms are parameterized and quoted before `MATCH`, so tokens like `AND`, `OR`, `NOT`, `NEAR`, and `*` are searched as input terms instead of FTS operators
- punctuation still follows FTS5 tokenization rules
- by default, `search` skips rows with no searchable content (attachment text, attachment filenames, embeds, and replies still count as content); use `--include-empty` to opt back in

### Optional multilingual lexical fields

Install Kiwi 0.23.2's dynamic library and base model, then build the Go helper:

```bash
git clone https://github.com/openclaw/discrawl
cd discrawl/tools/discrawl-kiwi
go build -o ~/.local/share/discrawl/bin/discrawl-kiwi .
```

`github.com/codingpot/kiwigo` links to the system Kiwi C API. Its upstream
installation expects Kiwi headers and dynamic libraries under `/usr/local`;
the model is the `kiwi_model_v0.23.2_base.tgz` release asset.

Build the optional Japanese and Chinese helpers:

```bash
cd tools/discrawl-ja && go build -o ~/.local/share/discrawl/bin/discrawl-ja .
cd ../discrawl-zh && go build -o ~/.local/share/discrawl/bin/discrawl-zh .
```

Configure the fields:

```toml
[search.lexical]
languages = ["ko", "ja", "zh", "ar"]
kiwi_command = "~/.local/share/discrawl/bin/discrawl-kiwi"
kiwi_model = "~/.local/share/discrawl/models/kiwi/base"
ja_command = "~/.local/share/discrawl/bin/discrawl-ja"
zh_command = "~/.local/share/discrawl/bin/discrawl-zh"
```

`discrawl lexical install` reports those helper paths. It does not download
packages.

Every message is analyzed into each configured field. This deliberately avoids
language detection, so mixed-language Discord messages remain searchable
through every enabled analyzer. Disk usage and indexing work increase with the
number of fields; query-time RRF deduplicates message ids without mixing the
different BM25 term statistics into one field.

None of the lexical analyzers use Python. Korean, Japanese, and Chinese run as
persistent Go helpers; Arabic is in-process. Helpers load lazily on first use
and are not linked into the default Discrawl binary.

See [Multilingual lexical benchmark](../benchmarks/multilingual-lexical.html)
for the reproducible targeted quality check and its storage tradeoff.

## Semantic and hybrid prerequisites

- `[search.embeddings]` configured in the Discrawl config file
- local `message_embeddings` rows for the configured provider, model, and input version
- input version is currently `message_normalized_v1`, so vectors are tied to normalized message text rather than raw Discord payloads

Two-phase embedding creation:

1. `discrawl sync --with-embeddings` queues changed messages by writing `embedding_jobs` rows. New messages, changed normalized text, and messages without an existing job are queued. This phase does not call the embedding provider.
2. `discrawl embed` drains pending jobs in bounded batches, calls the configured provider, and writes vectors to `message_embeddings`.

## Provider/model identity

The provider/model/input-version identity is stored on each job and vector. If you change provider or model, pending jobs are retargeted to the new identity and prior attempts are reset. Existing vectors for another identity remain in SQLite, but semantic search only reads vectors compatible with the current config.

Use `--rebuild` when changing provider, model, or input settings and you want to regenerate vectors for the existing archive.

## Local vs remote providers

Local providers like Ollama keep both message and query embedding on the same machine. With remote providers (OpenAI, etc.), message text is sent during `discrawl embed`, and search query text is sent when using `--mode semantic` or `--mode hybrid`. Stored message text is not sent during local vector scoring.

## Examples

```bash
discrawl search "panic: nil pointer"
discrawl search --mode fts "panic: nil pointer"
discrawl search --mode semantic "missing launch checklist"
discrawl search --mode hybrid "database timeout"
discrawl search --guild 123456789012345678 "payment failed"
discrawl search --dm "launch checklist"
discrawl search --channel billing --author steipete --limit 50 "invoice"
discrawl search --include-empty "GitHub"
discrawl --json search "websocket closed"
```

## See also

- [`search`](../commands/search.html)
- [`embed`](../commands/embed.html)
- [Embeddings](embeddings.html)
