# `update`

Pulls a Git snapshot and safely merges changed rows into the local cache.

Routine imports are delta-planned from crawlkit shard fingerprints, with a Git-object fallback for older manifests. Changed and new shards are upserted without deleting destination-only rows. Discrawl never falls back to an exact replacement unless you pass `--force` or explicitly configure `share.update_mode = "exact"`. Equivalent schemas with the same unique column names can merge even when their column order differs. Added, removed, renamed, or duplicate columns still require replacement.

## Usage

```bash
discrawl update
discrawl update \
  --repo ~/.local/share/discrawl/share \
  --remote https://github.com/example/discord-archive.git
discrawl update --with-embeddings
discrawl update --no-media
discrawl update --no-fetch
discrawl update --force
discrawl update --force --ref backup-2026-06-19
```

## Flags

- `--repo <path>` - local snapshot repo path (defaults to `[share].repo_path`)
- `--remote <url>` - target Git remote (defaults to `[share].remote`)
- `--branch <name>` - snapshot branch (defaults to `[share].branch`)
- `--force` - replace the public snapshot tables and rebuild search indexes so the local database exactly matches the snapshot; local DM rows remain untouched
- `--ref <tag-or-commit>` - import a historical snapshot without changing the share checkout; requires `--force`
- `--no-fetch` - import the current checkout without Git fetch or pull; cannot be combined with `--ref`. The configured merge/exact mode still applies.
- `--with-embeddings` - also import vectors that match your local `[search.embeddings]` identity
- `--no-media` - skip restoring cached attachment media files into `cache_dir/media`

## When to use it

- you have `share.remote` configured and want a fresh shard-delta import before running a command that does not auto-update (`sync` does not auto-import unless `--update=auto` is passed)
- you set `--no-auto-update` when subscribing and want to refresh on demand
- a CI job already imported the latest snapshot but read commands still consider it stale
- you need an exact reconciliation after Discrawl reports removed shards or an incompatible table change (`discrawl update --force`)
- this is a dedicated filtered-snapshot reader configured with `share.update_mode = "exact"`, so every update must remove rows no longer present upstream
- you need to restore a named tag or commit while leaving the checked-out share branch untouched (`discrawl update --force --ref <ref>`)

Normal updates preserve rows learned from live Discord or the desktop cache, even when those rows are absent from the Git snapshot. Generated event history and local sync cursors are not replayed during routine merges. If a safe merge is impossible, Discrawl keeps the current database, marks the snapshot as needing attention in `status --json`, and asks you to rerun with `--force`. Exact mode instead replaces public snapshot tables on every update; use it only for a dedicated cache because destination-only non-DM rows are intentionally removed.

## How `sync` interacts

`discrawl sync` does **not** auto-import the share unless `--update=auto` (configured update mode when stale) or `--update=force` (exact replacement before live deltas). Routine live refreshes stay fast; explicit imports happen via `update`.

## Externally managed snapshot caches

A scheduler may own a depth-1, current-only Git cache and replace it on each
refresh. Such a cache declares a nonempty local Git configuration value
`crawlkit.snapshot-cache-owner`. Discrawl refuses Git-mutating operations on
that checkout, including ordinary updates, automatic updates, publishing and
historical restores, so it cannot grow the owner's retained history.

Set `share.auto_update = false` for that subscription. Its owner must hold one
exclusive lock across snapshot refresh and `discrawl update --no-fetch`; Discrawl
then takes its normal database lock inside that scope. Keep the configured remote
and branch identity. Do not use `--force` unless exact replacement is intended.
Before retiring an older checkout's history, verify that the database has its
previous imported manifest and shard fingerprints; legacy timestamp-only imports
can still need the old Git manifest to plan a safe merge.

## See also

- [Git snapshots guide](../guides/git-snapshots.html)
- [`subscribe`](subscribe.html)
- [`sync`](sync.html)
