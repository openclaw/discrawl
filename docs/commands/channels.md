# `channels`

Browse the offline channel directory.

## Usage

```bash
discrawl channels list
discrawl channels show 123456789012345678
discrawl channels resolve help --json
discrawl channels resolve help --guild 123456789012345678 --json
```

## Subcommands

- `list` - dump every channel and thread in the local archive
- `show <id>` - show metadata for one channel/thread
- `resolve <id|name|#name>` - resolve one stable channel id; optionally restrict with `--guild` or `--guilds`

## Notes

- threads are stored as channels because that matches the Discord model
- archived threads are part of the sync surface and appear here too
- resolution prefers an exact id, then an exact case-insensitive name, then a unique partial name
- ambiguous names fail and report every candidate guild/channel id; retry with the numeric id
- numeric channel ids are the safest stable input for repeated scripts and agent workflows

## See also

- [`members`](members.html)
- [Data layout](../guides/data-storage.html)
