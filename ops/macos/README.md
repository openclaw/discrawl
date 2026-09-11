# Mac adapter for Discrawl continuous embedding

The application is built from unmodified upstream `8f0e93b808ae84ea390a2d28875f96654343de29`, using published
crawlkit v0.16.0. Native `tail --embed-live` runs capture and embedding workers
under one process owner. There are no local application patches.

The adapter retains Mac service lifecycle, existing Keychain credential loading,
resource guards (10 GiB free space and a 4 GiB WAL ceiling), startup catch-up,
and seven rotated 100 MB capture logs per stream. launchd owns restart/throttling.
The opt-out categories, channel exclusions and embedding provider are unchanged.

## Operation

- `discrawl-service run` starts repair followed by `tail --embed-live`.
- `discrawl-service health` returns native archive and `background_work` status,
  plus the local process/resource state.
- `discrawl-service embed` explains the continuous mode; it no longer schedules
  an hourly handoff. Pending work is picked up by native workers automatically.

The coordinator no longer stops capture to drain embeddings. Live worker batches
honor the configured batch size up to 64 inputs. Processing uses two workers,
250 ms batching and one-second fallback polling, with fresh input preferred over
catch-up. Provider timeouts are honored; outages pause embedding rather than
capture. Use native `background_work` for current indexing state. The old
`auto-index-latest.json` file, if retained, describes a historical hourly run.

The embedding key is read from the existing login Keychain item
`discrawl/openai-embeddings`, account `embedding-worker`, and passed only in the
child environment. A lookup failure permits capture to start with the worker
reporting missing credentials. Restart the service after resolving credentials.
Never print keys or run the launch job under an unrelated security session.

## Validation and rollback

Eight adapter tests verify native worker startup, credential failure/caching,
health reporting, graceful shutdown, and continuous capture beyond an hour.
Native tests cover schema upgrades, concurrent ingestion, edits/deletes, claim
recovery and provider failures. A 15 GB archive-copy migration preserved all
message, vector and job-state counts.

Schema 6 cannot be opened by the old schema-5 binary. To roll back the feature,
keep the new schema-compatible binary and restore the previous hourly adapter.
Do not restore an older database over newly captured data. Pre-migration archives,
old release links, manifests, and deployment verification are recorded under:

`/Volumes/Data/Projects/openclaw-tools/backups/discrawl-live-workers-20260911`.
