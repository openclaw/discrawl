# `status`

Shows local archive status, or remote archive status when `[remote]` is in
cloud read-only mode.

## Usage

```bash
discrawl status
```

## Reports

- where the local database lives
- guild count and per-guild totals
- channel and thread counts
- message totals
- latest archived message time
- last captured Gateway event time (`last_tail_event_at` in local `--json` output,
  omitted until a tail event has been recorded), separately from the last completed sync
- whether the Git share is configured, whether its last check is stale, and whether an exact replacement is pending (`--json`)
- remote endpoint/archive metadata when `remote.mode = "cloud"`
- embeddings status if `[search.embeddings]` is enabled

## macOS scheduled reporter

A launchd `spawn failed` / `EX_CONFIG` result can occur before Discrawl runs.
Use current launchd diagnostic timestamps to establish the cause; an old stderr
file does not establish a new database failure. A `needs LWCR update` error means
launchd could not refresh its managed lightweight code requirement.

After an authorized signed-binary replacement, verify every job that invokes
that executable, including idle status reporters. Preserve its plist and signing
requirements, then use the job owner's supported unload/bootstrap registration
path when a stale managed requirement needs refreshing. Verify a new successful
JSON report and the original scheduling interval. Re-register only the affected
job; preserve the running collector and existing identity/security policy.

## See also

- [`coverage`](coverage.html) - per-guild/channel archive readiness and wiretap skip counts
- [`diagnostics`](diagnostics.html) - read-only SQLite, WAL, freshness, and writer-lock checks
- [`doctor`](doctor.html) - liveness check (config, auth, DB, FTS wiring)
- [`remote`](remote.html) - direct Cloudflare remote archive checks
- [`report`](report.html) - Markdown activity block for the shared backup README

When continuous embeddings have been used, `background_work` reports worker lifecycle, pending and in-flight work, queue age, last completion, and safe error codes. A missing heartbeat is reported as stale, rather than claiming a stopped worker is healthy.
