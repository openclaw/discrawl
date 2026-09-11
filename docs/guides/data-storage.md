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

Index maintenance also runs on writable opens of an already-versioned database.
An unchanged `user_version` therefore does not mean that a writer open will do
no schema work. Read-only opens do not run this maintenance.

### Channel update-cursor index

After v0.14.1, Discrawl adds `idx_messages_channel_updated_id` on
`messages(channel_id, updated_at, id)` for per-channel changed-message queries.
It preserves the existing creation-time indexes and does not change
`user_version`. The first writable open that finds the index missing builds it
synchronously before returning; later opens retain the existing index.

That first build can delay the initiating command and other writers. Build
duration, peak memory, temporary disk requirements, persistent index size and
ongoing message-write overhead remain **unmeasured for the target archive**.
Small synthetic read timings and successful CI are not production capacity
estimates or acceptance of those costs. The historical operational decision in
[PR214](https://github.com/openclaw/discrawl/pull/214) is not resolved by this
documentation.

### Before broader index rollout

Require an explicitly approved, backup-first canary and a decision on its
measurements before expanding adoption. These are operator prerequisites, not
an automatic software gate or authorization to run a canary:

1. Have the owner approve the candidate revision, isolated target, maintenance coordination and rollback plan. Define elapsed-time, peak-memory, disk headroom and write-impact limits, with stop criteria, before starting.
2. Coordinate archive writers and take a verified SQLite-consistent backup under that plan. Retain the matching pre-upgrade binary and configuration. Do not copy only the main database file while uncheckpointed WAL data may exist; use an approved consistent-backup method and verify restoration.
3. Use a separately approved disposable copy, with explicit isolated config, database, cache, log and share paths. Prevent provider, Git-share and backend requests. Do not run collection or publication, and leave the source archive and other deployments untouched.
4. On that copy only, measure the first writable open, peak memory, temporary and persistent disk growth, remaining headroom, and a bounded local write workload before and after the index. Keep archive contents private; record aggregate measurements and the exact candidate revision.
5. Stop expansion if a limit is exceeded or evidence is incomplete. The owner must explicitly approve the measured tradeoff for the intended rollout; successful publication elsewhere does not establish acceptable local costs.
6. For rollback, restore the matching pre-upgrade binary, configuration and database from the verified backup, accounting for any writes since backup. A binary downgrade alone is not a verified rollback plan.

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
