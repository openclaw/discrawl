# `sync`

Refreshes SQLite from one or both archive sources.

By default, `sync` runs both live/local sources and does **not** import the Git snapshot first:

- Discord bot-token sync for bot-visible guild data
- local Discord Desktop cache import for classifiable cached messages and proven DMs

Use [`update`](update.html) when you want to pull/import the shared Git snapshot. Snapshot imports normally use changed-shard deltas, but unsafe table changes fall back to a full import. If you intentionally want a sync run to import the snapshot before live deltas, pass `--update=auto` (only when stale) or `--update=force` (always). `--no-update` is accepted as an explicit no-op alias for the default.

Run one explicit `--full` pass when you want a complete historical guild archive. Use plain `sync` afterward for frequent latest-message and desktop-cache refreshes.

## Usage

```bash
discrawl sync
discrawl sync --update=auto
discrawl sync --update=force
discrawl sync --no-update
discrawl sync --full
discrawl sync --full --all
discrawl sync --guild 123456789012345678
discrawl sync --guilds 123,456 --concurrency 8
discrawl sync --source both      # default: bot API + desktop cache
discrawl sync --source discord   # bot API only; aliases: key, bot, api
discrawl sync --source wiretap   # desktop cache only; aliases: desktop, cache
discrawl sync --guild 123456789012345678 --all-channels
discrawl sync --channels 111,222 --since 2026-03-01T00:00:00Z
discrawl sync --with-embeddings
discrawl sync --with-media
```

## Sources

| Source | Reads from | Stores |
| --- | --- | --- |
| `both` | Discord bot API and local Discord Desktop cache | bot-visible guild data plus classifiable cached desktop messages |
| `discord` / `key` | Discord bot API | guilds, channels, threads, members, and messages the bot can access |
| `wiretap` | local Discord Desktop cache files | classifiable cached messages; proven DMs are stored under `@me` |

## Bot sync modes

| Command | Use when | Behavior |
| --- | --- | --- |
| `discrawl sync` | routine refresh | skips member refreshes, checks live top-level channels plus active threads, only fetches new messages for channels with a stored cursor |
| `discrawl sync --update=auto` | hybrid Git/live refresh | imports a stale Git snapshot first, then runs the routine live refresh |
| `discrawl sync --all-channels` | repair pass | broad incremental sweep across every stored channel/thread, including archived threads |
| `discrawl sync --full` | historical backfill | crawls older history until channels are complete |

## Flags

- `--source <both|discord|wiretap>` - which archive sources to read
- `--update <auto|force|none>` - whether to import the Git snapshot before live deltas
- `--full` - historical backfill (slow on large guilds)
- `--all-channels` - broader incremental sweep across every stored channel/thread
- `--latest-only` - explicit latest-only run (also the default for untargeted `sync`)
- `--all` - ignore `default_guild_id` and fan out across every discovered guild
- `--guild <id>` / `--guilds <id,id>` - target specific guilds
- `--channels <id,id>` - target specific channels (forum ids expand to threads)
- `--since <RFC3339>` - limit initial history and `--full` backfill to messages at or after this timestamp
- `--concurrency <n>` - override worker count (default auto-sized: floor 8, cap 32)
- `--skip-members` - refresh guild/channel/message data without crawling members
- `--with-embeddings` - also enqueue changed messages into `embedding_jobs`
- `--with-media` - after sync, download missing attachment media into `cache_dir/media`

## Notes

- `--latest-only` is the default for untargeted `sync`. Use `--all-channels` to opt out without doing a full historical crawl.
- `--since` does not mark older history as complete, so a later `sync --full` without `--since` can continue the backfill.
- Long runs emit periodic progress logs to stderr.
- Heartbeat logs (`message sync waiting`) name the oldest active channel and per-channel page activity if in-flight channels stop completing for a while.
- Every run ends with a `message sync finished` summary.
- Each channel crawl has a bounded runtime budget; pathological channels are deferred and retried next sync.
- Retryable failures and unavailable-channel markers are tracked per channel; stale unavailable markers are cleared after a later successful crawl.
- Marker cleanup is best-effort, so one missing local sync-state row cannot crash the run.
- Full sync member refresh is best-effort and gives up after five minutes without a caller-supplied deadline.
- When the archive is already complete, `sync --full` reuses backlog markers and limits steady-state refresh to live top-level channels plus active threads.

## See also

- [Sync sources](../guides/sync-sources.html)
- [`tail`](tail.html)
- [`update`](update.html)
