# `diagnostics`

Reports local archive health and active Discrawl writer state without running a
sync or authenticating to Discord.

## Usage

```bash
discrawl diagnostics
discrawl diagnostics --json
```

## Reports

- expanded SQLite database path, presence, and size
- SQLite journal mode, schema version, and read-only `PRAGMA quick_check` result
- WAL path, presence, and size
- last completed sync and last tail event when recorded
- sync-lock path and whether its operating-system file lock is held
- active writer PID, operation, phase, and timestamps from Discrawl lock metadata
- whether the archive passed integrity checks and is safe for read-only inspection
- orphaned message-to-channel references in the stored archive

An active writer can be `sync`, `tail`, `wiretap`, an import, or another
Discrawl operation. The operation and phase come from the same lock metadata
used by Discrawl's writer serialization. `diagnostics` does not scan unrelated
operating-system processes.

Lock files can remain after a writer exits. Discrawl verifies the file lock and
reports leftover owner data as `stale_metadata`, so an old PID or lock file is
not mistaken for an active writer. On platforms without a supported file-lock
probe, the JSON output uses `detection: "unsupported"` instead of guessing.

Missing or unreadable databases are returned as structured warning reports.
The command does not create a missing database or lock file.
SQLite integrity is checked independently of Discrawl's schema version, so a
healthy older or newer archive can still report `integrity: "ok"`; freshness
fields carry a separate compatibility error when this binary cannot read them.

`safe_for_read_only_inspection` reports SQLite-byte integrity only. It does not
prove every message has a channel record. `catalog.state: "consistent"` means
every stored message has a stored channel record; it does not prove that a sync
captured every source channel or message. A zero-row query therefore describes
the local snapshot, not authoritative absence from Discord.

## See also

- [`status`](status.html) - archive counts and high-level freshness
- [`doctor`](doctor.html) - config, Discord auth, DB schema, and FTS readiness
