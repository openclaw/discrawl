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
- how many channels carry an unavailable marker, split into markers inside the seven-day retry window and markers already due for another attempt

## What it does not do

- does not print the token contents
- does not run a sync; it only checks readiness

## Common outputs

- "token from env (DISCORD_BOT_TOKEN)" or "token from keyring (discrawl/discord_bot_token)"
- "0 guilds visible" - bot is not invited to any guild yet, or intents/permissions are missing
- "schema newer than binary" - update `discrawl` to a build that supports the local DB schema
- `unavailable_markers_active` - channels a routine sync passes over until their marker ages out of the seven-day window; `sync --guild <id> --full` or `sync --channels <id>` attempts them now
- `unavailable_markers_expired` - channels whose marker has aged out, so the next routine sync attempts them again

## See also

- [Bot setup](../bot-setup.html)
- [Configuration](../configuration.html)
- [`status`](status.html)
- [`diagnostics`](diagnostics.html) - local archive integrity and writer state without Discord auth
