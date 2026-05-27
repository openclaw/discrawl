# `subscribe-cloud`

Writes a read-only Cloudflare remote archive config. This does not clone a Git
repo, import a snapshot, or create a local SQLite database.

## Usage

```bash
discrawl subscribe-cloud --endpoint https://crawl.example.workers.dev --archive openclaw/discord
discrawl subscribe-cloud --token-env DISCRAWL_REMOTE_TOKEN --endpoint https://crawl.example.workers.dev --archive openclaw/discord
```

## What it does

- writes `[remote]` with `mode = "cloud"`
- sets `discord.token_source = "none"` so no Discord bot token is required
- leaves existing `[share]` Git snapshot settings untouched
- skips local database creation and snapshot import

## After subscribing

```bash
export DISCRAWL_REMOTE_TOKEN="..."
discrawl status --json
discrawl remote status
discrawl remote archives
discrawl whoami
```

## See also

- [`remote`](remote.html)
- [`status`](status.html)
- [`subscribe`](subscribe.html)
