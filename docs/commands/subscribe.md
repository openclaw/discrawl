# `subscribe`

Subscribes to a Git-backed snapshot repo. The Git-only setup path needs no
Discord bot token.

## Usage

```bash
discrawl subscribe https://github.com/example/discord-archive.git
discrawl subscribe \
  --repo ~/.local/share/discrawl/share \
  https://github.com/example/discord-archive.git
discrawl subscribe --branch main https://github.com/example/discord-archive.git
discrawl subscribe --stale-after 15m https://github.com/example/discord-archive.git
discrawl subscribe --no-auto-update https://github.com/example/discord-archive.git
discrawl subscribe --no-import https://github.com/example/discord-archive.git
discrawl subscribe --with-embeddings https://github.com/example/discord-archive.git
discrawl subscribe --no-media https://github.com/example/discord-archive.git
```

## What it does

- writes a config with `discord.token_source = "none"` (so no bot token is required)
- imports the latest snapshot into the local SQLite archive
- enables auto-refresh: read commands fetch and import when the local share import is older than `share.stale_after` (default `15m`)

## Flags

- `--repo <path>` - local snapshot repo path
- `--branch <name>` - snapshot branch (default: `main`)
- `--stale-after <duration>` - how stale the local import can get before read commands auto-refresh
- `--no-auto-update` - disable auto-refresh (use [`update`](update.html) manually)
- `--no-import` - write config only; skip the initial pull/import
- `--with-embeddings` - import vectors that match your local `[search.embeddings]` identity
- `--no-media` - skip restoring cached attachment media files into `cache_dir/media`

## Disabled in this mode

`sync` and `tail` are disabled when `discord.token_source = "none"` because they need live Discord access. Switch to a token-equipped config to re-enable them.

## After subscribing

```bash
discrawl search "launch checklist"
discrawl messages --channel general --hours 24
discrawl status
```

## See also

- [Git snapshots guide](../guides/git-snapshots.html)
- [`publish`](publish.html)
- [`update`](update.html)
