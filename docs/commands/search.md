# `search`

Searches archived messages. FTS is the default mode and works without embeddings.

## Usage

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

## Modes

- `fts` (default) - SQLite FTS5 with `unicode61` tokenizer; newest matches first
- `semantic` - embeds the query, scores against locally stored vectors; errors out if embeddings are disabled or no compatible vectors exist
- `hybrid` - runs both, deduplicates by message id, falls back to FTS when semantic is unavailable

## Flags

- `--mode <fts|semantic|hybrid>` - search mode
- `--guild <id>` / `--guilds <id,id>` - restrict the guild scope
- `--dm` - shorthand for `--guild @me`
- `--channel <id|name|#name>` - restrict to one channel (id, exact name, `#name`, or partial match)
- `--author <name>` - restrict to one author
- `--limit <n>` - cap result count
- `--include-empty` - include rows with no searchable content (attachment text/filenames, embeds, and replies still count as content)

## FTS behavior

User query terms are parameterized and quoted before `MATCH`, so tokens like `AND`, `OR`, `NOT`, `NEAR`, and `*` are searched as input terms instead of FTS operators. Punctuation still follows FTS5 tokenization rules.

## Semantic prerequisites

- `[search.embeddings]` configured in the Discrawl config file
- local `message_embeddings` rows for the configured provider, model, and input version
- input version is currently `message_normalized_v1`

Run `discrawl sync --with-embeddings` to enqueue, then `discrawl embed` to generate vectors.

## See also

- [Search modes](../guides/search-modes.html)
- [Embeddings](../guides/embeddings.html)
- [`messages`](messages.html) - exact slices, not search
