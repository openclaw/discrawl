# `messages`

Lists exact message slices by channel, author, and time range. Unlike [`search`](search.html), this does not query the FTS index - it pulls a slice of rows.

## Usage

```bash
discrawl messages --channel maintainers --days 7 --all
discrawl messages --channel maintainers --hours 6 --all
discrawl messages --channel "#maintainers" --since 2026-03-01T00:00:00Z
discrawl messages --channel 1456744319972282449 --author steipete --limit 50
discrawl messages --channel maintainers --last 100 --sync
discrawl messages --dm --channel Molty --last 20
discrawl messages --channel maintainers --days 7 --all --include-empty
discrawl --json messages --channel maintainers --days 3
```

## Flags

- `--channel <id|name|#name>` - id, exact name, `#name`, or partial name match
- `--guild <id>` / `--guilds <id,id>` / `--dm` - restrict the guild scope (`--dm` is shorthand for `--guild @me`)
- `--author <name>` - restrict to one author
- `--hours <n>` - shorthand for "since now minus N hours"
- `--days <n>` - shorthand for "since now minus N days"
- `--since <RFC3339>` - explicit start timestamp
- `--last <n>` - return the newest `N` matching messages, then print oldest-to-newest
- `--limit <n>` - safety limit (default 200; `--all` removes it)
- `--all` - removes the safety limit
- `--sync` - blocking pre-query sync for the matching channel or guild scope; omit while `tail` is already maintaining live freshness
- `--include-empty` - include rows with no displayable/searchable content

## Notes

- at least one filter is required
- if `tail` is already running, plain `messages` reads the local archive without waiting; `messages --sync` fails fast instead of waiting behind the tail lock
- `--dm` skips Git snapshot auto-update because DMs are never imported from the shared mirror
- use either `--last` for the newest matching rows or `--all` for an uncapped oldest-to-newest slice

## See also

- [`search`](search.html)
- [`dms`](dms.html)
