# `tail`

Runs the live Discord Gateway tail and a periodic repair loop.

## Usage

```bash
discrawl tail
discrawl tail --guild 123456789012345678
discrawl tail --repair-every 30m
discrawl tail --replay-failures-only
discrawl tail --repair-every 6h --repair-offset 15m
```

## What it does

- connects to the Discord Gateway with the configured bot token
- writes new messages, edits, and deletes into the local archive as they arrive
- periodically runs a repair pass to catch anything the live stream missed
- can replay a bounded set of unresolved exact-message failures without
  starting the Gateway tail
- ignores messages, edits, and deletes from channels excluded by
  `[sync].exclude_channel_ids` or `[sync].exclude_channel_kinds`

## Flags

- `--guild <id>` / `--guilds <id,id>` - tail a specific guild scope (default: `default_guild_id`, or all discovered guilds if unset)
- `--repair-every <duration>` - frequency of the repair sweep
- `--replay-failures-only` - replay unresolved exact-message tail failures and exit
- `--replay-limit <n>` - maximum failures to inspect in replay-only mode (default and maximum: `25`)
- `--repair-offset <duration>` - for positive values, align repairs to that wall-clock offset within each repair period

## Notes

- requires a working Discord bot token
- not available in Git-only mode (`discord.token_source = "none"`)
- terminates cleanly on SIGINT / SIGTERM and treats cancellation as normal exit
- replay-only mode uses the normal exclusive writer lock and does not create
  Gateway message events or update `tail:last_event`
- retains excluded channel metadata while omitting their message traffic
- positive-offset repairs skip missed aligned slots after a long repair rather than running catch-up sweeps
- separation from another scheduled job is only stable when that job also uses a fixed calendar schedule; a drifting `StartInterval` schedule cannot guarantee the offset

## See also

- [`sync`](sync.html)
- [Bot setup](../bot-setup.html)
