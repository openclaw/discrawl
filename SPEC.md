# Engineering contract

Discrawl is a local-first Discord archive: bot-visible guild history and
classifiable Discord Desktop cache messages are stored in SQLite for offline
search, SQL and terminal browsing. Current commands and defaults are documented
in [README.md](README.md) and the [command reference](docs/README.md).

## Boundaries

- Discord network access uses a bot account. Personal DMs are supported only
  through local Desktop cache import; no user-token or selfbot flows.
- Wiretap reads bounded local files, stores sanitized metadata, and skips
  messages whose channel/guild route cannot be established.
- Proven DMs use the synthetic guild ID `@me`. They and their media, vectors
  and operational state stay local and are excluded from shared snapshots.
- Read commands do not migrate the archive. Snapshot auto-update is explicit
  configuration; diagnostics and publication preflight remain read-only.
- Writers serialize archive changes with the shared writer lock. Tail event
  handling, history repair, failure replay and embeddings must respect their
  existing cursor, shutdown and persistence ownership.
- SQLite holds canonical messages, event history, indexes and metadata.
  Attachment bytes are stored in the media cache. Secrets are resolved at
  runtime from configured environment variables or OS keyring items.
- Preserve released config, CLI and snapshot contracts, including legacy
  filesystem discovery, checkpoint migration, raw-media imports, and local DM
  preservation during shared-archive updates.

## Implementation map

| Package | Responsibility |
| --- | --- |
| `cmd/discrawl` | Process entrypoint and signal handling |
| `internal/cli` | Command parsing, runtime ownership and output |
| `internal/config` | TOML defaults, normalization and credential resolution |
| `internal/discord` | Bot REST client and Gateway lifecycle |
| `internal/discorddesktop` | Local cache scanning, classification and import |
| `internal/syncer` | Catalog discovery, history pagination, live writes and repair |
| `internal/store` | SQLite schema, migrations, queries, FTS, embeddings and failures |
| `internal/store/storedb` | Generated sqlc queries; edit `internal/store/sqlc` and regenerate |
| `internal/media` | Bounded attachment fetch and local cache |
| `internal/share` | Privacy-filtered Git snapshots, import and publication |
| `internal/report` | Activity, digest, trends and aggregate field notes |
| `tools/discrawl-{ja,kiwi,zh}` | Separately built optional lexical helpers |

Shared storage, mirror, embedding and terminal infrastructure comes from
CrawlKit. The optional lexical helpers remain separate modules and processes;
Kiwi's native dependency has its own licensing and installation boundary.

## Sources of truth

- [Configuration](docs/configuration.md): current defaults, paths and overrides.
- [Data layout](docs/guides/data-storage.md): archive tables and indexes.
- [Sync sources](docs/guides/sync-sources.md): routine, full and targeted capture.
- [Search modes](docs/guides/search-modes.md) and
  [embeddings](docs/guides/embeddings.md): indexing and query behavior.
- [Git snapshots](docs/guides/git-snapshots.md): merge/exact semantics,
  privacy, media and compatibility.
- [Security](docs/security.md): credential and disclosure boundaries.
- [Release workflow](docs/RELEASING.md): signed artifact and publication contracts.

Use the Go versions declared in each module and the tools pinned in `Makefile`.
`make check` runs the local gates; CI additionally exercises optional helpers
and container workflows. Behavior changes need focused regression coverage,
command documentation and an `Unreleased` changelog entry. Pure internal
refactors should preserve output and data formats.
