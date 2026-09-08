# `report`

Generates the Markdown activity block used by the shared backup repo README.

## Usage

```bash
discrawl report
discrawl report --published --readme path/to/discord-backup/README.md
```

## Flags

- `--readme <path>` - update the activity block in the given README file in place
- `--published` - exclude local-only direct messages from all statistics and rankings

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

## CI integration

The backup workflows restore and save `.discrawl-ci/discrawl.db` with `actions/cache`. On a warm runner cache, scheduled publishers skip the pre-sync snapshot import and go straight to the live latest-message delta before publishing. Cache misses still import the latest published snapshot first so `--latest-only` has channel cursors to resume from.

## See also

- [`publish`](publish.html)
- [Git snapshots](../guides/git-snapshots.html)
- [`status`](status.html)
