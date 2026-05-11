# Data layout

Everything lives in one local SQLite file. By default, Discrawl stores it at
`${XDG_DATA_HOME:-~/.local/share}/discrawl/discrawl.db` on Linux and
`~/Library/Application Support/discrawl/discrawl.db` on macOS. Setting `XDG_DATA_HOME` overrides
the new default base directory on any OS.

Existing `~/.discrawl/discrawl.db` installs continue to use that database until the new default
database file exists. This keeps old installs working on systems that already set XDG variables.

## What is stored

- guild metadata
- channels and threads in one table (Discord models threads as channels)
- current member snapshot
- canonical message rows
- append-only message event records
- FTS5 index rows
- optional local embedding queue metadata and vectors

Messages imported from Discord Desktop use the same message, attachment, mention, and FTS paths as bot-synced messages.

## DMs

Proven DMs use the synthetic guild id `@me`. Unclassifiable desktop-cache payloads are skipped instead of being stored as unknown synthetic data.

## Attachments

Attachment binaries are not stored in SQLite. Only attachment metadata, filenames, and (optionally) extracted text.

Set `sync.attachment_text = false` if you want to keep attachment metadata and filenames but disable attachment body fetches for text indexing.

## Multi-guild ready

The schema is multi-guild ready even when the common UX stays single-guild simple. Threads are stored as channels because that matches the Discord model. Archived threads are part of the sync surface.

## Schema migrations

SQLite schema migrations are versioned with `PRAGMA user_version`. Startup fails fast when a local DB schema is newer than the supported binary - that means you have a binary older than the database.

## Querying directly

Anything you want, with read-only SQL:

```bash
discrawl sql 'select count(*) as messages from messages'
echo 'select guild_id, count(*) from messages group by guild_id' | discrawl sql -
```

See [`sql`](../commands/sql.html).

## See also

- [`status`](../commands/status.html) - high-level archive status
- [`channels`](../commands/channels.html) - channel directory
- [`members`](../commands/members.html) - member directory
- [Security](../security.html)
