# `update`

Pulls a Git snapshot and safely merges changed rows into the local cache.

Routine imports are delta-planned from crawlkit shard fingerprints, with a Git-object fallback for older manifests. Changed and new shards are upserted without deleting destination-only rows. Discrawl never falls back to an exact replacement unless you pass `--force`.

## Usage

```bash
discrawl update
discrawl update \
  --repo ~/.local/share/discrawl/share \
  --remote https://github.com/example/discord-archive.git
discrawl update --with-embeddings
discrawl update --no-media
discrawl update --force
discrawl update --force --ref backup-2026-06-19
```

## Flags

- `--repo <path>` - local snapshot repo path (defaults to `[share].repo_path`)
- `--remote <url>` - target Git remote (defaults to `[share].remote`)
- `--branch <name>` - snapshot branch (defaults to `[share].branch`)
- `--force` - replace the public snapshot tables and rebuild search indexes so the local database exactly matches the snapshot; local DM rows remain untouched
- `--ref <tag-or-commit>` - import a historical snapshot without changing the share checkout; requires `--force`
- `--with-embeddings` - also import vectors that match your local `[search.embeddings]` identity
- `--no-media` - skip restoring cached attachment media files into `cache_dir/media`

## When to use it

- you have `share.remote` configured and want a fresh shard-delta import before running a command that does not auto-update (`sync` does not auto-import unless `--update=auto` is passed)
- you set `--no-auto-update` when subscribing and want to refresh on demand
- a CI job already imported the latest snapshot but read commands still consider it stale
- you need an exact reconciliation after Discrawl reports removed shards or an incompatible table change (`discrawl update --force`)
- you need to restore a named tag or commit while leaving the checked-out share branch untouched (`discrawl update --force --ref <ref>`)

Normal updates preserve rows learned from live Discord or the desktop cache, even when those rows are absent from the Git snapshot. Generated event history and local sync cursors are not replayed during routine merges. If a safe merge is impossible, Discrawl keeps the current database, marks the snapshot as needing attention in `status --json`, and asks you to rerun with `--force`.

## How `sync` interacts

`discrawl sync` does **not** auto-import the share unless `--update=auto` (safe merge when stale) or `--update=force` (exact replacement before live deltas). Routine live refreshes stay fast; explicit imports happen via `update`.

## See also

- [Git snapshots guide](../guides/git-snapshots.html)
- [`subscribe`](subscribe.html)
- [`sync`](sync.html)
