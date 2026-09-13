# `embed`

Drains pending `embedding_jobs` rows by calling the configured embedding provider and writing vectors to `message_embeddings`.

## Usage

```bash
discrawl embed
discrawl embed --limit 1000
discrawl embed --rebuild --limit 1000
```

## Flags

- `--limit <n>` - cap how many jobs this run drains
- `--batch-size <n>` - provider request batch size
- `--rebuild` - regenerate vectors for the existing archive after a provider/model/input-version change

## Behavior

- claims jobs with a short lock so overlapping runs do not process the same batch
- rate limits requeue the batch and stop that drain run cleanly
- provider or validation failures retry up to three attempts before the job is marked failed
- messages with no normalized text are marked done and any stale vector for that message is removed

## Identity

Provider, model, and input version are stored on each job and vector. Changing any of them retargets pending jobs to the new identity and resets prior attempts. Existing vectors for another identity remain in SQLite but are not used by semantic search.

## When to use `--rebuild`

After changing `[search.embeddings]` provider, model, or any input setting, when you want to regenerate vectors for messages already in the archive.

## Pairing with `sync`

`sync --with-embeddings` enqueues; `embed` drains. The two phases are intentionally separate so a slow provider does not block the hot sync path.

For continuous capture with background embedding, use `discrawl tail --embed-live`.
Do not run a separate drain against the tail owner; live mode coordinates both
inside one process.

## See also

- [Embeddings guide](../guides/embeddings.html)
- [Search modes](../guides/search-modes.html)
- [`search`](search.html)
