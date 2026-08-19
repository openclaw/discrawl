# Sync sources

Discrawl reads from two local archive sources. Either or both can run in a single `sync`.

## Sources

| Source | Reads from | Stores |
| --- | --- | --- |
| `both` | Discord bot API and local Discord Desktop cache | bot-visible guild data plus classifiable cached desktop messages |
| `discord` / `key` / `bot` / `api` | Discord bot API | guilds, channels, threads, members, and messages the bot can access |
| `wiretap` / `desktop` / `cache` | local Discord Desktop cache files | classifiable cached messages; proven DMs are stored under `@me` |

The default is `both`. Pick one with `--source` or by setting `[sync].source` in config.

## Bot sync modes

Sync modes control the Discord bot API side of a run. When `wiretap` is selected, the desktop cache import runs once alongside the chosen bot sync mode.

| Command | Use when | Behavior |
| --- | --- | --- |
| `discrawl sync` | routine refresh | skips member refreshes, checks live top-level channels plus active threads, fetches one newest page when no cursor exists, and otherwise fetches only new messages |
| `discrawl sync --update=auto` | hybrid Git/live refresh | imports a stale Git snapshot first, usually as a changed-shard delta, then runs the routine live refresh |
| `discrawl sync --all-channels` | repair pass | broad incremental sweep across every stored channel/thread, including archived threads |
| `discrawl sync --full` | historical backfill | crawls older history until channels are complete; can take a long time on large servers |

Run one explicit `--full` pass when you want a complete historical guild archive. Use plain `sync` afterward for frequent latest-message and desktop-cache refreshes.

## Concurrency

`sync` already uses parallel channel workers for bot API message crawling. The default is auto-sized from `GOMAXPROCS` with a floor of `8` and a cap of `32`. Override with `--concurrency`.

## Targeting

- `--guild <id>` runs only that guild
- `--guilds 123,456` runs an explicit set
- `--all` ignores `default_guild_id` and fans out across every discovered guild
- `--channels 111,222` targets specific channels (forum ids expand to their threads)
- `--since <RFC3339>` limits initial history and `--full` backfill to messages at or after the timestamp; older history is not marked complete, so a later `sync --full` without `--since` can continue the backfill

## Performance and resilience

- Long runs emit periodic progress logs to stderr.
- If in-flight channels stop completing for a while, `discrawl` emits `message sync waiting` heartbeat logs with the oldest active channel, per-channel page activity, and skip/defer counters.
- Every run ends with a `message sync finished` summary.
- Each channel crawl has a bounded runtime budget; pathological channels are deferred and retried on the next sync.
- Retryable failures and unavailable-channel markers are tracked per channel; stale unavailable markers are cleared after a later successful crawl.
- Marker cleanup is best-effort, so one missing local sync-state row cannot crash the run.
- Full sync member refresh is best-effort and gives up after five minutes without a caller-supplied deadline, so message sync completion is not held hostage by a slow guild member crawl.
- When the archive is already complete, `sync --full` reuses backlog markers and limits steady-state refresh to live top-level channels plus active threads instead of revisiting every stored archived thread.
- If a guild already has a local member snapshot, routine syncs reuse it and skip another full member crawl until that snapshot ages out.

## See also

- [`sync`](../commands/sync.html)
- [`tail`](../commands/tail.html)
- [Wiretap](wiretap.html)
- [Git snapshots](git-snapshots.html)
