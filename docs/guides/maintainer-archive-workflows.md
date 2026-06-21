# Maintainer archive workflows

Use the local archive first. A Discrawl maintainer workflow should answer most
questions from SQLite, a Git snapshot, or a cloud remote before it reaches for
live Discord. Live bot sync is still the right tool when the question depends on
current permissions, fresh channel metadata, or messages missing from the local
archive.

## Start with health and freshness

Run read-only checks before asking an agent, script, or report to trust the
archive:

```bash
discrawl status --json
discrawl doctor
```

[`status`](../commands/status.html) reports where the database lives, archive
counts, latest message times, Git snapshot freshness, and cloud remote metadata
when `[remote].mode = "cloud"` is configured. [`doctor`](../commands/doctor.html)
checks config, token source, bot reachability, database compatibility, and FTS
wiring without printing secrets.

These checks decide the next source.

Query the local archive directly when it is fresh enough.

Run [`update`](../commands/update.html) when a configured Git snapshot is stale,
or let read commands auto-update according to the configured stale window.

Use `status --json` and [`remote`](../commands/remote.html) to inspect a
configured cloud remote without opening the local SQLite database.

Run a bot sync when bot-visible metadata or latest messages matter.

## No-bot path with wiretap

When bot access is unavailable, use the Discord Desktop cache importer:

```bash
discrawl sync --source wiretap
```

This reads only the desktop-cache source. It works without a bot token,
credential extraction, or user-account Discord API calls. See
[`sync`](../commands/sync.html), [sync sources](sync-sources.html), and
[wiretap](wiretap.html).

`wiretap` can import classifiable cached guild messages and proven direct
messages. Proven DMs are stored under the synthetic guild id `@me`. Treat that
data as incomplete local cache evidence.

## Manual browsing plus watch mode

For cache-driven investigations, open Discord Desktop and browse the channels or
DMs you need. Then keep Discrawl importing while you scroll:

```bash
discrawl wiretap --watch-every 2m
```

Run [`wiretap`](../commands/wiretap.html) directly for desktop-cache import. Run
`sync --source wiretap` when the same source should fit the normal sync workflow.

Watch mode is a local importer loop. Stop it when the browsing/import pass is
done, especially before running metadata repair, publishing checks, or tests that
expect a quiet database.

## Check coverage before querying

After any sync or import, repeat the status check:

```bash
discrawl status --json
```

For exact coverage questions, use read-only SQL:

```bash
discrawl sql 'select count(*) as messages from messages'
discrawl sql 'select guild_id, count(*) from messages group by guild_id'
printf '%s\n' \
  'select channel_id, count(*) as messages' \
  'from messages group by channel_id' \
  'order by messages desc limit 20' |
  discrawl sql -
```

[`sql`](../commands/sql.html) opens a read-only connection by default. Use it for
counts, rankings, and coverage checks when high-level command output is too
coarse. If quoting gets awkward, pass SQL on stdin:

```bash
printf '%s\n' 'select guild_id, count(*) from messages group by guild_id;' |
  discrawl sql -
```

Inspect the schema before writing ad hoc queries that depend on column names. See
[data layout](data-storage.html) for the stable model and the `@me` boundary.

## Use stable channel ids

Prefer numeric channel ids for repeatable maintainer queries:

```bash
discrawl messages --channel 1458141495701012561 --hours 24
discrawl search --channel 1458141495701012561 "release checklist"
discrawl sync --channels 1458141495701012561 --since 2026-06-01T00:00:00Z
```

Names can collide, change, or mean different things across guilds. Numeric ids
make agent prompts, scripts, and follow-up sessions easier to replay.

Use [`channels`](../commands/channels.html) to discover ids, then keep the ids in
the local notes or workflow that needs repeatability.

## Bot metadata vs desktop cache data

Bot sync and wiretap import complement each other.

`discrawl sync --source discord` reads bot-visible guilds, channels, threads,
members, permissions, and live message history. It needs a real bot token and
guild access.

`discrawl sync --source wiretap` reads local Discord Desktop cache data when bot
access is unavailable. It is cache-only and makes no live Discord calls.

`discrawl wiretap --watch-every 2m` repeats local import while you browse
Discord Desktop. Stop the loop when the import pass is done.

`discrawl subscribe` and `discrawl update` are Git snapshot reader-mode tools for
shared archive data. They run without Discord credentials.

Cloud remote mode reads a Worker-fronted archive for remote metadata and
read-only queries.

Run a Discord-source sync when publish filters or public/private classification
depend on current bot-visible metadata:

```bash
discrawl sync --source discord
```

That repair pass refreshes bot-owned guild, channel, member, and permission data
missing from desktop cache import.

## Public/private publish preflight

Before privacy-sensitive publishing, refresh the bot-visible metadata and inspect
the intended scope:

```bash
discrawl sync --source discord
discrawl status --json
discrawl publish --public-only --no-media --no-commit
```

[`publish`](../commands/publish.html) always excludes local-only DM data. With
`--public-only`, it exports only channels visible to the guild `@everyone` role
after category and channel permission overwrites. Add `--include-channels` or
`--exclude-channels` with numeric ids when the shared snapshot should be narrower
than the public archive.

Git snapshots exclude `@me` rows, DM media, wiretap sync state, and vectors for
DM messages. Snapshot imports preserve local DM search during shared guild mirror
refreshes.

## Stop importers and check database health

Background importers and readers can overlap with SQLite. Before metadata edits,
publish preflight checks, or tests that need deterministic output, confirm watch
loops and long syncs have stopped.

On macOS or Linux:

```bash
pgrep -fl 'discrawl (wiretap|sync|tail)' || true
discrawl doctor
discrawl status --json
```

If SQLite reports a busy or locked database, stop the background importer you
started and repeat the health checks. Rule out a running `wiretap --watch-every`,
`sync`, or `tail` process before treating the archive as corrupt.

## See also

- [Sync sources](sync-sources.html)
- [Desktop wiretap](wiretap.html)
- [Git-backed snapshots](git-snapshots.html)
- [Data layout](data-storage.html)
- [`status`](../commands/status.html)
- [`doctor`](../commands/doctor.html)
- [`sync`](../commands/sync.html)
- [`wiretap`](../commands/wiretap.html)
- [`sql`](../commands/sql.html)
- [`publish`](../commands/publish.html)
