# `coverage`

Reports how much of the local archive is usable without authenticating to
Discord or running a sync.

## Usage

```bash
discrawl coverage
discrawl coverage --guild 123456789012345678
discrawl coverage --json
```

## Reports

- every guild by default, or one exact guild selected with `--guild`
- message, channel, and message-capable channel totals
- named versus synthetic channel counts
- per-channel message counts and earliest/latest timestamps
- history-complete markers when known
- last bot sync and wiretap import times
- skipped-message and skipped-channel counts from the latest successful wiretap pass
- unresolved known-failure counts per guild/channel, plus unscoped failures

The human output uses a table per guild. JSON returns the same guild/channel
rows plus aggregate totals for agent and script use. Soft-deleted messages do
not count toward coverage.

"Synthetic" means Discrawl only has an id-derived placeholder such as
`channel-123456` or `dm-123456`. "Named" means it has a useful channel or
conversation label. The two counts partition all channel rows, regardless of
whether the label came from bot sync or Discord Desktop cache metadata.

Coverage is local and read-only. It does not authenticate, start a sync, wait
for a writer lock, or update a configured share. Persisted wiretap coverage
state contains compact counters and timestamps only; `wiretap:*` state remains
excluded from Git snapshots.

Known failures mean Discrawl attempted work and retained the failure. Missing
rows without a known failure may still be unattempted. Use
[`failures`](failures.html) for row identifiers, retry counts, and errors.

## Watch progress

Use [`wiretap`](wiretap.html) with `--stats` to attach a coverage snapshot to
each import pass:

```bash
discrawl wiretap --watch-every 10s --stats --json
```

The first sample has no delta. Later samples include changes in messages,
channels, named channels, and synthetic channels since the previous pass.

## See also

- [`wiretap`](wiretap.html) - import local Discord Desktop cache data
- [`status`](status.html) - high-level archive and share status
- [`diagnostics`](diagnostics.html) - SQLite integrity, WAL, freshness, and writer-lock checks
- [`failures`](failures.html) - unresolved and recently resolved ingestion failures
