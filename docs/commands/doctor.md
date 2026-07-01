# `doctor`

Checks config, auth, DB, and FTS wiring. The fastest sanity check.

## Usage

```bash
discrawl doctor
```

## What it verifies

- config loads from the expected path
- where the bot token was resolved from (env var or OS keyring)
- bot auth succeeds against Discord
- how many guilds the bot can access
- local SQLite database exists and the schema version matches the binary
- FTS5 index is wired up

## What it does not do

- does not print the token contents
- does not run a sync; it only checks readiness

## Common outputs

- "token from env (DISCORD_BOT_TOKEN)" or "token from keyring (discrawl/discord_bot_token)"
- "0 guilds visible" - bot is not invited to any guild yet, or intents/permissions are missing
- "schema newer than binary" - update `discrawl` to a build that supports the local DB schema

## See also

- [Bot setup](../bot-setup.html)
- [Configuration](../configuration.html)
- [`status`](status.html)
- [`diagnostics`](diagnostics.html) - local archive integrity and writer state without Discord auth
