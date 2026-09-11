# Category opt-out, September 11, 2026

The native runtime uses upstream `88a0576` plus `5b9f8fb`, which applies category
and channel exclusions through the entire ancestry, including stored threads,
targeted/repair syncs, live capture, and parent metadata updates. No schema or
embedding-provider changes are included. The Mac coordinator is unchanged.

The six-category allowlist is replaced with an empty allowlist and explicit
category exclusions for Welcome (`1457818604988006672`), Archived
(`1465911398969381095`), and Archived 2 (`1525264656027877427`). All nine existing
individual exclusions remain, plus the previously omitted root channels Stage,
rules, Community Staff, and maintainer-lounge. Announcement exclusions remain.
The before/after comparison covered 24,857 known nodes and preserved the intended
included set of 24,779. Future categories are included by default.

Native syncer tests, race checks, and vet pass. The full Go suite passes except
the pre-existing `TestPublishProducerValidationBeforeMutation` failure
(`tracked remote branch origin/main is missing`), reproduced against the unchanged
source. The new regression tests failed against upstream before the fix.

The versioned executable is in
`openclaw-tools/releases/discrawl/88a0576-category-optout-5b9f8fb`.
Recovery configuration, the server catalog, scope comparison, previous release
links, and live deployment verification are kept in
`openclaw-tools/backups/discrawl-category-optout-20260911`.
