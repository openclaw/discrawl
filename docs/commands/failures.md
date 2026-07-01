# `failures`

Queries the local operational failure ledger without authenticating to Discord
or running a sync.

## Usage

```bash
discrawl failures
discrawl failures --source discord --guild 123456789012345678
discrawl failures --all --limit 200
discrawl failures --json
```

By default, the command returns up to 100 unresolved rows. Filters accept an
exact source, guild id, or channel id. `--all` also returns resolved history;
the maximum limit is 1000.

Each row includes the operation and source, available guild/channel/message or
attachment identifiers, a bounded error class/message, first and last seen
times, retry count, and resolved time. Repeated failures update the same row.
A successful later channel, message, attachment, or embedding operation marks
the matching failure resolved.

Resolved history is retained for 90 days; unresolved rows are not aged out.
The ledger is local-only and excluded from Git snapshot exports/imports. Error
text is bounded and common bearer/query/JSON secret forms are redacted.

## Sources

- `discord` - bot sync and Gateway tail operations
- `wiretap` - Discord Desktop cache imports
- `media` - attachment fetches and cache writes
- `embeddings` - embedding provider and local vector writes

## See also

- [`coverage`](coverage.html) - known-failure counts beside archive coverage
- [`diagnostics`](diagnostics.html) - SQLite integrity, WAL, freshness, and writer-lock checks
- [Data layout](../guides/data-storage.html) - local storage and snapshot boundaries
