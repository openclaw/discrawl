# Maintainer archive workflows

Use the local archive first. Reach for live Discord only when a question needs
new bot-visible data, current permissions, or messages that are absent locally.
This keeps routine maintainer research fast, repeatable, and within the archive's
privacy boundary.

## Know which archive mode you are using

Discrawl has four distinct data paths:

| Mode | Typical command | What it provides |
| --- | --- | --- |
| Local bot archive | `discrawl sync --source discord` | Bot-visible guild metadata and message history from the Discord API |
| Local desktop cache | `discrawl sync --source wiretap` | Classifiable cache evidence, including proven DMs, without a bot or user token |
| Git snapshot reader | `discrawl subscribe ...` / `discrawl update` | A shared non-DM archive imported into local SQLite |
| Cloud remote reader | `discrawl remote status` | Worker-fronted read-only archive metadata and queries without local SQLite |

Bot sync and wiretap can update the same local SQLite archive. Git snapshot and
cloud remote modes are reader paths for already-published data. See [sync
sources](sync-sources.html), [Git-backed snapshots](git-snapshots.html), and the
[`remote`](../commands/remote.html) command reference.

## Start with local health, freshness, and coverage

Run read-only checks before trusting an archive:

```bash
discrawl status --json
discrawl diagnostics --json
discrawl coverage --json
discrawl failures --json
```

- [`status`](../commands/status.html) reports archive counts, latest message
  time, Git snapshot freshness, and configured cloud remote metadata.
- [`diagnostics`](../commands/diagnostics.html) checks SQLite integrity, WAL
  state, freshness markers, and the authoritative Discrawl writer lock without
  authenticating to Discord or creating a missing database.
- [`coverage`](../commands/coverage.html) reports per-guild and per-channel
  message bounds, history-complete markers, named versus synthetic channels,
  wiretap skip counters, and unresolved known failures.
- [`failures`](../commands/failures.html) lists unresolved sync, import, media,
  embedding, and row-write failures. Use `--all` when resolved retry history is
  relevant.

Use [`doctor`](../commands/doctor.html) when bot configuration and live bot
reachability matter:

```bash
discrawl doctor
```

Unlike `diagnostics`, `doctor` resolves the configured bot token and checks
Discord auth. It does not print the token or run a sync.

If a configured Git snapshot is stale, run `discrawl update`. If cloud mode is
configured, use `discrawl status --json` or `discrawl remote status` without
opening local SQLite. Run a bot sync only when current bot-visible state is
needed.

## Query locally with stable channel ids

Discover channels once, then keep numeric ids in repeatable scripts and agent
prompts:

```bash
discrawl --json channels list
discrawl messages --channel 123456789012345678 --hours 24
discrawl search --channel 123456789012345678 "release checklist"
```

Names can collide across guilds and can change over time. Numeric ids make a
query's scope explicit and avoid ambiguity. See [`channels`](../commands/channels.html),
[`messages`](../commands/messages.html), and [`search`](../commands/search.html).

Use read-only SQL for exact counts or relationships that high-level commands do
not expose:

```bash
discrawl sql 'select count(*) as messages from messages'
discrawl sql 'select guild_id, count(*) from messages group by guild_id'
printf '%s\n' \
  'select channel_id, count(*) as messages' \
  'from messages group by channel_id' \
  'order by messages desc limit 20' |
  discrawl sql -
```

[`sql`](../commands/sql.html) uses a read-only connection. Inspect [data
layout](data-storage.html) before depending on raw column names, and prefer
`coverage` for ordinary archive-readiness checks.

## Use wiretap when bot access is unavailable

Import the Discord Desktop cache once through the normal sync workflow:

```bash
discrawl sync --source wiretap
```

For an investigation that needs manual browsing, open Discord Desktop, browse
or scroll the relevant channels, and run a watched importer:

```bash
discrawl wiretap --watch-every 2m --stats --json
```

`--stats` attaches a coverage snapshot to every pass and reports deltas after
the first watched sample. Stop the loop with Ctrl-C when the browsing pass is
done. Confirm the stopped state and archive health with:

```bash
discrawl diagnostics --json
discrawl coverage --json
discrawl failures --source wiretap --json
```

Wiretap reads cache files only. It does not extract credentials, use a Discord
user token, call the Discord API as the user, or run a selfbot. Cache evidence is
inherently incomplete: content appears only when Discord Desktop cached a
classifiable payload.

Proven direct messages are stored under the synthetic guild id `@me`. These
rows, their media, wiretap sync state, and DM embedding vectors stay local-only
and are excluded from Git snapshots. Snapshot imports preserve existing local
DM rows. See the [wiretap guide](wiretap.html) and [data layout](data-storage.html).

## Refresh bot metadata when classification matters

Run a Discord-source sync when a task depends on current guild/channel metadata,
permissions, threads, members, or bot-visible message history:

```bash
discrawl sync --source discord
discrawl coverage --json
discrawl failures --source discord --json
```

Desktop cache import cannot prove current Discord permissions. A bot sync is the
repair path when public/private publish classification lacks usable role or
permission metadata.

## Preflight privacy-sensitive publishing

Check scope without creating or modifying the snapshot repository:

```bash
discrawl sync --source discord
discrawl diagnostics --json
discrawl coverage --json
discrawl failures --json
discrawl publish --public-only --check --json
```

[`publish --check`](../commands/publish.html) uses the same permission and filter
logic as export. It reports candidate and allowed channel/message counts,
metadata readiness, source hints, and whether an empty result is intentional.
It fails closed when `--public-only` lacks usable `@everyone` role metadata and
does not initialize a share repo or contact its remote.

After the preflight is ready, publish the intended numeric scope explicitly:

```bash
discrawl publish --public-only \
  --include-channels 123456789012345678 \
  --no-media \
  --push
```

`--public-only`, `--include-channels`, and `--exclude-channels` narrow only the
Git snapshot; the richer local archive remains intact. DM `@me` rows are always
excluded. Use `--no-media` unless cached non-DM attachment bytes are intended to
be part of the snapshot.

## Handle active writers and archive errors

Discrawl serializes local writers with an operating-system file lock. Before a
deterministic publish, metadata repair, or test run, inspect it directly:

```bash
discrawl diagnostics --json
```

The report identifies active `sync`, `tail`, `wiretap`, import, and other
Discrawl writers from the same lock metadata used by the runtime. It distinguishes
a held lock from stale metadata, so an old PID or leftover lock file is not
mistaken for a running importer.

If coverage is incomplete, use its per-channel bounds and history markers to
choose a targeted or full sync. If `failures` has unresolved rows, use their
source and stable ids to retry the matching operation. Treat a non-`ok` SQLite
integrity result as a real archive-health problem; a large WAL or stale lock
metadata alone is diagnostic context, not proof of corruption.

## See also

- [Sync sources](sync-sources.html)
- [Desktop wiretap](wiretap.html)
- [Git-backed snapshots](git-snapshots.html)
- [Data layout](data-storage.html)
- [`status`](../commands/status.html)
- [`diagnostics`](../commands/diagnostics.html)
- [`coverage`](../commands/coverage.html)
- [`failures`](../commands/failures.html)
- [`doctor`](../commands/doctor.html)
- [`sql`](../commands/sql.html)
- [`publish`](../commands/publish.html)
