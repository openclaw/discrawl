# `tail`

Runs the live Discord Gateway tail and a periodic repair loop.

## Usage

```bash
discrawl tail
discrawl --verbose tail
discrawl tail --with-embeddings
discrawl tail --embed-live
discrawl tail --guild 123456789012345678
discrawl tail --repair-every 30m
discrawl tail --repair-on-start --repair-every 6h
discrawl tail --replay-failures-only
```

## What it does

- connects to the Discord Gateway with the configured bot token
- writes new messages, edits, and deletes in the configured guild, category,
  and channel scope into the local archive as they arrive
- periodically runs a repair pass to catch anything the live stream missed
- optionally queues live, replayed, and repair messages for background embedding
- can replay a bounded set of unresolved exact-message failures without
  starting the Gateway tail

## Flags

- `--guild <id>` / `--guilds <id,id>` - tail a specific guild scope (default: `default_guild_id`, or all discovered guilds if unset)
- `--repair-every <duration>` - frequency of the repair sweep
- `--repair-on-start` - run one catch-up repair after the Gateway connects, without waiting for the periodic timer (default: off; also works with `--repair-every 0`)
- `--embed-live` - continuously process queued embeddings while capture continues (opt-in; implies queueing; requires configured embeddings)
- `--with-embeddings` - queue live, replayed, and repair messages for embedding (default: off)
- `--replay-failures-only` - replay unresolved exact-message tail failures and exit
- `--replay-limit <n>` - maximum failures to inspect in replay-only mode (default and maximum: `25`)

## Notes

- requires a working Discord bot token
- startup repair uses the same writer owner and serialized repair lifecycle as periodic repair; capture remains connected while missed history is fetched, and shutdown cancels and joins the repair
- with `--repair-on-start`, REST repair owns history cursors for that tail session; live events still update messages and live freshness immediately, but cannot advance history progress past missing messages. Periodic and subsequent startup repairs may therefore re-fetch already captured messages. This also preserves catch-up progress if repair is interrupted.
- not available in Git-only mode (`discord.token_source = "none"`)
- `discrawl --verbose tail` traces Gateway receipt, worker handling, scope
  filtering, and successful archive writes with event and Discord IDs but no
  message content or author metadata
- terminates cleanly on SIGINT / SIGTERM and treats cancellation as normal exit
- replay-only mode uses the normal exclusive writer lock and does not create
  Gateway message events or update `tail:last_event`

## See also

- [`sync`](sync.html)
- [Bot setup](../bot-setup.html)
