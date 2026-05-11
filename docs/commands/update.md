# `update`

Forces a Git snapshot pull and import.

Routine imports are delta-planned from crawlkit shard fingerprints, with a Git-object fallback for older manifests. The usual publish only imports changed tail shards; unsafe table changes fall back to a full import.

## Usage

```bash
discrawl update
discrawl update \
  --repo ~/.local/share/discrawl/share \
  --remote https://github.com/example/discord-archive.git
discrawl update --with-embeddings
```

## Flags

- `--repo <path>` - local snapshot repo path (defaults to `[share].repo_path`)
- `--remote <url>` - target Git remote (defaults to `[share].remote`)
- `--branch <name>` - snapshot branch (defaults to `[share].branch`)
- `--with-embeddings` - also import vectors that match your local `[search.embeddings]` identity

## When to use it

- you have `share.remote` configured and want a fresh shard-delta import before running a command that does not auto-update (`sync` does not auto-import unless `--update=auto` is passed)
- you set `--no-auto-update` when subscribing and want to refresh on demand
- a CI job already imported the latest snapshot but read commands still consider it stale

## How `sync` interacts

`discrawl sync` does **not** auto-import the share unless `--update=auto` (only when stale) or `--update=force` (always). Routine live refreshes stay fast; explicit imports happen via `update`.

## See also

- [Git snapshots guide](../guides/git-snapshots.html)
- [`subscribe`](subscribe.html)
- [`sync`](sync.html)
