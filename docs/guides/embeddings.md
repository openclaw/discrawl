# Embeddings

Embeddings are optional. FTS is the default search path and the primary verification target. Embeddings enrich recall in background batches; they do not block the hot sync path.

## Quick path

```bash
export OPENAI_API_KEY="..."
discrawl init --with-embeddings
discrawl sync --with-embeddings
discrawl tail --with-embeddings
discrawl embed --limit 1000
discrawl search --mode semantic "launch checklist"
discrawl search --mode hybrid "launch checklist"
```

## Two-phase pipeline

1. **Queue** - `sync --with-embeddings` writes `embedding_jobs` rows for changed archive messages. `tail --with-embeddings` queues live events, replayed failures, and periodic repair messages. The embedding provider is **not** called in this phase.
2. **Drain** - `discrawl embed` claims pending jobs with a short lock so overlapping runs do not process the same batch. It calls the configured provider, writes vectors to `message_embeddings` with provider, model, input version, dimensions, and binary vector data.

Behavior during drain:

- rate limits requeue the batch and stop that drain run cleanly
- provider or validation failures retry up to three attempts before marking the job failed
- messages with no normalized text are marked done and any stale vector for that message is removed

## Identity (provider, model, input version)

Stored on each job and vector. If you change provider or model:

- pending jobs are retargeted to the new identity
- prior attempts are reset
- existing vectors for another identity remain in SQLite but are not used for semantic search

Use `--rebuild` when you want to regenerate vectors for the existing archive after a config change:

```bash
discrawl embed --rebuild --limit 1000
```

For OpenAI `text-embedding-3-small`, `dimensions` can project vectors to a smaller size. Leave it unset for the provider default, or use a positive value such as `512` to reduce local vector storage. Run `embed --rebuild` after changing it.

## Credentials

By default, credentials come from `api_key_env`; no OS keyring is queried.
Credential-free local providers keep working with an empty `api_key_env`.
To use an existing OS keyring item instead, configure:

```toml
[search.embeddings]
api_key_source = "keyring"
api_key_keyring_service = "discrawl/embeddings"
api_key_keyring_account = "api-key"
```

Keep your other embedding settings. The service and account identify an existing
item; the API key itself is never written to the configuration or exported to the
environment. Keyring selection is explicit and does not fall back to an
environment variable. `api_key_source = "env"` restores the default behavior.

`tail --embed-live` keeps capturing while a credential is missing, empty, or
locked. Native background work reports `embedding_provider_configuration` and
retries initialization after one minute. A pending keyring prompt does not block
worker cancellation, and only one lookup is outstanding. Once initialized, the
provider retains its credential until the process restarts.

## Local provider example

```toml
[search.embeddings]
enabled = true
provider = "ollama"
model = "nomic-embed-text"
```

With local providers, message and query embedding both happen on the same machine. With remote providers, message text is sent during `discrawl embed`, and search query text is sent during `--mode semantic` or `--mode hybrid` calls.

## Git snapshot interaction

By default, `publish` does not export embeddings. Use `--with-embeddings`:

```bash
discrawl publish --with-embeddings --push
discrawl subscribe --with-embeddings https://github.com/example/discord-archive.git
discrawl update --with-embeddings
```

The snapshot stores vectors under `embeddings/<provider>/<model>/<input_version>/...` and records that identity in `manifest.json`. Only vectors for exported non-DM messages are included, so publish filters also filter embeddings. Import only restores matching embedding manifests, so an Ollama/nomic subscriber does not accidentally import OpenAI/text-embedding vectors. `embedding_jobs` is never exported; subscribers that want fresh local vectors run `discrawl embed --rebuild`. Publishing without `--with-embeddings` omits embedding manifests instead of carrying forward an older bundle.

## See also

- [Search modes](search-modes.html)
- [`embed`](../commands/embed.html)
- [Configuration](../configuration.html)

## Continuous embedding during capture

`discrawl tail --embed-live` opts into crawlkit's background worker runtime and
also queues new messages, edits, replay and repair work. Embeddings must already
be enabled in the configuration. Ordinary `tail --with-embeddings` still only
queues work; `embed` remains the bounded one-shot drain.

Two workers honor the configured batch size up to a maximum of 64 inputs, with a 250 ms batching window
and one-second fallback polling. The worker deadline includes the configured
provider request timeout plus 30 seconds for preparation and result handling. Fresh Gateway content takes priority over sync
catch-up, with capacity reserved for catch-up. Healthy-provider freshness targets
are measured from the source transaction to the committed vector; ten seconds
is a target under normal load, not a promise during provider outages or overload.

The tail process retains its normal exclusive writer ownership. Background
workers share it in-process, make provider calls outside database transactions,
and prepare content on a separate read connection. Do not schedule another
`embed` command against a running tail. No stop/embed/restart cycle is needed in
live mode.

Queue revisions and expiring random claim tokens fence results, retries and
release. Edits invalidate queued claims and stale vectors; deletes cannot be
resurrected by a late provider response. Completion also checks the current source
text and eligibility. Interrupted work is recovered by the next exclusive tail
owner. Model/provider changes continue to use the existing rebuild contract.

Provider outages, missing credentials and throttling leave capture running.
Transient provider failures pause processing and retry; rate-limit Retry-After
values are respected. Invalid input/results remain visible as failed jobs.
`status --json` adds `background_work`, including lifecycle, pending/in-flight
counts, oldest pending input, last success and safe error codes. Worker status
is local-only and is never included in published snapshots.

This feature upgrades the local schema from 5 to 6 without regenerating existing
vectors. Back up before upgrading. To roll back the feature, keep the
schema-compatible binary, omit `--embed-live`, and return to serialized one-shot
embedding. Older binaries cannot open schema 6; do not overwrite newly captured
data with a pre-upgrade database merely to disable the worker.
