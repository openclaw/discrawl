# `report`

Generates the Markdown activity block used by the shared backup repo README.

## Usage

```bash
discrawl report
discrawl report --published --readme path/to/discord-backup/README.md
discrawl report --published --field-notes --readme path/to/discord-backup/README.md
```

## Flags

- `--readme <path>` - update the activity block in the given README file in place
- `--published` - exclude local-only direct messages from all statistics and rankings
- `--field-notes` - also write deterministic aggregate notes; requires
  `--published` and `--readme`

Local reports include direct messages by default, including with `--readme`.
Use `--published` for a shared README. Private guild channels and threads remain
included: this is not a public-only report. This mode excludes direct messages but
does not implement public-only or channel filters; it refuses configured share
filters. `publish --readme` selects this mode automatically and continues to
reject active share filters.

## What gets rendered

Deterministic README stats:

- latest update time
- latest archived message
- archive totals
- day / week / month activity

Every scheduled snapshot publish updates this block.

## Field notes

The daily report workflow also generates `reports/latest-field-notes.md` and
`reports/latest-field-notes.json` beside the README. A separate
`discrawl-aggregate-field-notes:start/end` marker block displays the Markdown
notes. Both artifacts and the activity block use one report build.

These deterministic aggregates are distinct from historical AI-generated
narratives inside `discrawl-field-notes:start/end` markers. Ordinary report
writes preserve that legacy block byte for byte; they neither regenerate it nor
call a model. The aggregate block is updated independently.

Notes contain aggregate totals and the existing 24-hour, 7-day, and 30-day
windows, not names, IDs, message content, topic inference, or model output.
Intervals anchor to the latest archived message, not necessarily the current
time; the inclusive start and anchor timestamps are explicit. Attachment
counts mean messages containing attachments, not attachment files.

JSON version 1 records `generator: deterministic`, `scope: published-non-dm`,
`coverage: unknown`, timestamps, totals, and windows. Timestamp status is
`observed`, `empty`, `unavailable`, or `future`; unavailable/future timestamps
have no age value. Empty/unavailable timestamps retain the existing report's
generation-time fallback. Message timestamp age is not sync freshness, and
neither recent nor empty results establish complete history.

The two artifact paths are producer-owned. Keep maintainer documentation and
manual notes elsewhere. Filtered publishing removes these broader-scope
artifacts and generated README output, including legacy narrative blocks,
rather than leaking broader-scope content.
Ordinary snapshot publishes preserve the notes; only a successful daily report
generation refreshes them. A failed generation must not be committed.

## CI integration

The backup workflows restore and save `.discrawl-ci/discrawl.db` with `actions/cache`. On a warm runner cache, scheduled publishers skip the pre-sync snapshot import and go straight to the live latest-message delta before publishing. Cache misses still import the latest published snapshot first so `--latest-only` has channel cursors to resume from.

## See also

- [`publish`](publish.html)
- [Git snapshots](../guides/git-snapshots.html)
- [`status`](status.html)
