# `dms`

Lists local wiretap DM conversations or reads one DM thread. Convenience layer over the synthetic `@me` guild id.

## Usage

```bash
discrawl dms
discrawl dms --with Molty --last 20
discrawl dms --with 1456464433768300635 --all
discrawl dms --search "launch checklist"
discrawl dms --with Molty --search "invoice"
```

## Default output

`discrawl dms` (no flags) shows one row per local DM channel with:

- message count
- author count
- first/last cached message times

## Flags

- `--with <name|id>` - switches to message output for that DM conversation (unless `--list` is also set)
- `--list` - keep the channel-summary listing even when `--with` is set
- `--search <query>` - search only local DM messages
- `--last <n>` / `--all` / `--limit <n>` - same slicing as [`messages`](messages.html)

## Notes

- only sees data imported by [`wiretap`](wiretap.html) - Discord Desktop cache, not live DM history
- skips Git snapshot auto-update because DMs are never imported from the shared mirror
- DMs are local-only and never published

## Empty results

`dms` reads the same tables as [`messages`](messages.html) and [`search`](search.html) and prints the same stderr notes when a run returns nothing: the window notes for a `--hours`/`--days`/`--since`/`--before` listing, and the multi-term note for `--search`. They go to stderr, `--json` suppresses them, and stdout and the exit code are unchanged.

The window notes count only messages in a conversation `dms` can list. A direct message whose channel has no `channels` row is returned by no `dms` listing at any window, so it is left out of the count rather than reported as a row that dropping the window would show.

A run that sets `--with` gets no note. `--with` names a person and the query matches it against channel id and channel name alike, which the counts behind a note cannot reproduce, so a `--with` run stays silent rather than reporting numbers taken from every conversation.

## See also

- [Wiretap guide](../guides/wiretap.html)
- [`messages --dm`](messages.html)
