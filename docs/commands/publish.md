# `publish`

Publishes the local SQLite archive as sharded, compressed NDJSON snapshots in a private Git repo.

## Usage

```bash
discrawl publish --remote https://github.com/example/discord-archive.git --push
discrawl publish --readme path/to/discord-backup/README.md --push
discrawl publish --message "sync: discord archive" --push
discrawl publish --with-embeddings --push
discrawl publish --no-media --push
discrawl publish --public-only --include-channels 1458141495701012561 --push
```

## Flags

- `--repo <path>` - local snapshot repo path (defaults to `[share].repo_path`)
- `--remote <url>` - target Git remote (defaults to `[share].remote`)
- `--branch <name>` - snapshot branch (defaults to `[share].branch`)
- `--message <text>` - commit message (default: `sync: discord archive`)
- `--no-commit` - write/export files without creating a Git commit
- `--push` - push the snapshot commit after writing it
- `--readme <path>` - update the activity block in this README file too
- `--public-only` - export only channels visible to the guild `@everyone` role
- `--include-channels <ids>` - comma-separated channel ids to export; forum parents include their allowed public threads
- `--exclude-channels <ids>` - comma-separated channel ids to omit; exclusions win over includes
- `--with-embeddings` - also export stored `message_embeddings` rows
- `--no-media` - omit cached attachment media files from the snapshot

Filters narrow only the published snapshot. The local SQLite archive can still
be synced from a richer bot-visible dataset. Git-only readers see the filtered
snapshot; the publisher keeps the complete local DB.

Filter rules:

- `--public-only` keeps channels visible to the guild `@everyone` role after
  category and channel permission overwrites; private threads are excluded
- `--include-channels` exports only the listed channel ids; including a forum
  parent also includes its allowed public threads
- `--exclude-channels` omits listed channel ids and wins over includes
- combined filters intersect: `--public-only --include-channels A,B` exports
  only included channels that are also public

The same defaults can be set in `[share.filter]`:

```toml
[share.filter]
public_only = true
include_channel_ids = ["1458141495701012561"]
exclude_channel_ids = []
```

`--readme` is disabled when filters are active because the activity report is
built from the full archive and would otherwise leak unfiltered totals. If the
share repo already has a generated Discrawl `README.md` from an earlier
unfiltered publish, filtered publish removes it before committing. Custom
README files without Discrawl report markers are left alone.

## What is published

- non-DM archive tables (DM `@me` rows are always excluded)
- cached non-DM attachment media files under `media/` unless `--no-media` is used
- when filters are enabled: only matching guilds, channels, messages, events,
  attachments, mentions, channel-scoped sync-state rows, member rows referenced
  by matching messages, and matching embedding rows
- when filters are disabled and `--readme` is used: README activity block
  (latest update, latest message, totals, day/week/month activity)
- with `--with-embeddings`: vectors for the configured `[search.embeddings]` provider/model/input version, plus identity manifest

## What is not published

- `@me` DM guilds, channels, messages, events, attachments, mentions, wiretap sync state
- `@me` DM media files
- when filters are enabled: share manifest state and guild-level member
  freshness markers, because they describe the full archive
- `embedding_jobs`
- raw bot tokens or any local secret

## See also

- [Git snapshots guide](../guides/git-snapshots.html)
- [`subscribe`](subscribe.html)
- [`update`](update.html)
- [`report`](report.html)
