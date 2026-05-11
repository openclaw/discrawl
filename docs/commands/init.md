# `init`

Creates the local config and discovers accessible guilds.

## Usage

```bash
discrawl init
discrawl init --guild 123456789012345678
discrawl init --db ~/data/discrawl.db
discrawl init --with-embeddings
```

## What it does

- writes the default XDG config file, or whatever `--config` /
  `DISCRAWL_CONFIG` points to
- discovers guilds the configured bot can access
- if exactly one guild is available, sets it as `default_guild_id` automatically
- creates the SQLite database at `db_path`

## Flags

- `--guild <id>` - set a specific default guild instead of auto-picking
- `--db <path>` - override `db_path`
- `--with-embeddings` - enable `[search.embeddings]` in the generated config

## See also

- [Configuration](../configuration.html)
- [Bot setup](../bot-setup.html)
- [`doctor`](doctor.html)
