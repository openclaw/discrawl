# Data layout

Everything lives in one local SQLite file. By default, Discrawl stores it at
`${XDG_DATA_HOME:-~/.local/share}/discrawl/discrawl.db` on Linux and
`~/Library/Application Support/discrawl/discrawl.db` on macOS. Setting `XDG_DATA_HOME` overrides
the new default base directory on any OS.

Existing `~/.discrawl/discrawl.db` installs continue to use that database until
the new default database file exists. Discrawl does not copy or migrate the
SQLite file during upgrade. Copy it yourself before creating the new database
path if you want to move an existing archive to the platform-native location.

## What is stored

- guild metadata
- channels and threads in one table (Discord models threads as channels)
- current member snapshot
- canonical message rows
- append-only message event records
- FTS5 index rows
- optional local embedding queue metadata and vectors
- local-only sync/import/media/embedding failure history

Messages imported from Discord Desktop use the same message, attachment, mention, and FTS paths as bot-synced messages.

## DMs

Proven DMs use the synthetic guild id `@me`. Unclassifiable desktop-cache payloads are skipped instead of being stored as unknown synthetic data.

## Attachments

Attachment binaries are not stored in SQLite. SQLite stores attachment metadata, filenames, optional extracted text, and media cache bookkeeping.

`discrawl attachments fetch` and `discrawl sync --with-media` download media into `cache_dir/media` and record the relative media path, SHA-256, byte size, fetch time, and fetch status on the attachment row.

Discord attachment URLs are not guaranteed to stay fetchable forever. Expired or removed CDN objects are recorded as failed fetches with their HTTP status, commonly `404`; already cached files remain usable and can still be exported.

Set `sync.attachment_text = false` if you want to keep attachment metadata and filenames but disable attachment body fetches for text indexing.

## Multi-guild ready

The schema is multi-guild ready even when the common UX stays single-guild simple. Threads are stored as channels because that matches the Discord model. Archived threads are part of the sync surface.

## Schema migrations

SQLite schema migrations are versioned with `PRAGMA user_version`. Startup fails fast when a local DB schema is newer than the supported binary - that means you have a binary older than the database.

## Failure history

`failure_ledger` records bounded error text and available Discord row
identifiers. Unresolved failures are retained; resolved failures age out after
90 days. The table is intentionally absent from Git snapshot exports and
imports, so one machine's operational diagnostics never become published
archive content. Query it through [`failures`](../commands/failures.html).

## Querying directly

Anything you want, with read-only SQL:

```bash
discrawl sql 'select count(*) as messages from messages'
echo 'select guild_id, count(*) from messages group by guild_id' | discrawl sql -
```

See [`sql`](../commands/sql.html).

## See also

- [`status`](../commands/status.html) - high-level archive status
- [`coverage`](../commands/coverage.html) - per-guild/channel archive readiness and wiretap progress counters
- [`diagnostics`](../commands/diagnostics.html) - SQLite integrity, WAL, freshness, and writer-lock state
- [`failures`](../commands/failures.html) - local ingestion and write failure history
- [`channels`](../commands/channels.html) - channel directory
- [`members`](../commands/members.html) - member directory
- [Security](../security.html)
