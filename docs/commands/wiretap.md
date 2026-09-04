# `wiretap`

Imports classifiable Discord Desktop message payloads into the same local SQLite archive.

This is the path for searchable DMs because bot tokens cannot read personal direct messages.

`wiretap` is also available through `discrawl sync --source wiretap` and is included in the default `discrawl sync --source both` path.

## Usage

```bash
discrawl wiretap
discrawl wiretap --path "$HOME/Library/Application Support/discord"
discrawl wiretap --dry-run
discrawl wiretap --full-cache
discrawl wiretap --watch-every 2m
discrawl wiretap --watch-every 10s --stats --json
```

## Flags

- `--path <dir>` - override the desktop data directory (default: platform-specific Discord cache path)
- `--dry-run` - report what would be imported without creating an archive, importing data, or creating runtime directories
- `--full-cache` - exhaustive Chromium HTTP cache import for historical guild-cache archaeology (slower)
- `--watch-every <duration>` - keep importing on a periodic loop
- `--stats` - attach a full archive coverage snapshot; watched samples after the first include deltas
- `--max-file-bytes <n>` - skip unusually large files (default 64 MiB)

## Notes

- stores classifiable cache messages in the same `guilds`, `channels`, and `messages` tables used by bot sync
- stores proven DMs under the synthetic guild id `@me`
- `@me` rows stay local-only: never exported to `publish` / Git snapshot import / embedding snapshots
- preserves existing local `@me` rows when importing a Git snapshot
- drops message payloads whose channel cannot be classified from cached channel metadata or Discord route URLs; dropped rows are counted as `skipped_messages`
- retries unresolved payloads when channel metadata becomes available, including JSON inputs and `--full-cache`; older file checkpoints are rechecked once after upgrade
- in `--full-cache`, only files containing unresolved messages remain retryable; unchanged completed files keep their checkpoints
- preserves newer edited messages and deletion markers, and avoids duplicate events when the same cached payload is imported again
- imports what Discord Desktop has cached locally, not complete live DM history
- scans local `.ldb`, `.log`, `.json`, and `.txt` artifacts for Discord message JSON, plus route-bearing Chromium HTTP cache entries by default
- does not extract, store, or print Discord auth tokens
- persists only compact aggregate import counters for [`coverage`](coverage.html); raw cache paths and payloads are not added to coverage state
- with `--dry-run --stats`, reads coverage from an existing archive without migrating it; reports empty coverage if no archive exists, including in watch mode. Existing archives use normal SQLite read-only access, which may create WAL/SHM sidecar files.

## Default desktop paths

- macOS: `~/Library/Application Support/discord`
- Linux: `~/.config/discord`

## See also

- [Wiretap guide](../guides/wiretap.html)
- [`coverage`](coverage.html)
- [`dms`](dms.html)
- [`sync`](sync.html)
