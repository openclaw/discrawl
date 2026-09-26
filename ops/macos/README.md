# Native Discrawl service on this Mac

launchd runs Discrawl directly. Discrawl loads both credentials, owns the archive
writer, captures Gateway events, repairs history, and processes embeddings.
There is no Python coordinator or credential-loading shell wrapper.

The installed application is `e6c7c2fff74572b23bd2f1d6872c5e197ba4c769`
(`0.15.1-21-ge6c7c2f`), with Crawlkit v0.16.3 and schema 7. It includes the
member-change index and the [ingestion integrity repair](../../docs/guides/ingestion-integrity.md):
persisted ancestry exclusions, attributed rich text, recovery snapshots,
attachment extraction receipts, and failure-aware status. The signed binary
retains the existing identity, message configuration, schedule and writer
ownership. Machine-specific service and signing files remain on this operations
branch.

Activated at `2026-09-26T01:33:54Z`; SHA-256:
`11662da58ad262e1e028c738d1fdb3509cb89dacfc3a3d308fe14d01fe77e46f`.
The bounded local text pass completed without resetting ingestion cursors or
fetching historical attachments. Its checkpoint survives restarts. Existing raw
evidence and unchanged vectors remain retained; changed text is processed by the
normal embedding worker. Metadata-only Desktop receipts remain unchanged and
are explicitly classified as unavailable for provider-payload reconstruction.

The separate metrics-only runtime and hourly job remain in place. Their current
targets come only from `~/.config/discrawl/metrics.json`, which contains the
OpenClaw `clawd` invite target. The message collector does not own that schedule
or replace the metrics executable.

## Installed configuration

- Executable: `/Volumes/Data/Projects/openclaw-tools/releases/discrawl/stable/discrawl`,
  a signed regular file at a stable path across rebuilds. CLI links point here.
- Config: `/Volumes/Data/AppData/discrawl/config.toml`.
- `com.hrudolph.discrawl-tail` runs `tail --guilds 1456350064065904867 --repair-every 6h --repair-on-start --embed-live` under launchd.
- `com.hrudolph.discrawl-status` records native JSON status hourly.
- `com.hrudolph.discrawl-logrotate` runs standard logrotate every 15 minutes.

The three plist files here mirror `~/Library/LaunchAgents`. The rotation policy
is installed at `~/.config/discrawl/logrotate.conf`; it uses Homebrew's logrotate
at `/opt/homebrew/opt/logrotate/sbin/logrotate` and stores rotation state in
`~/Library/Caches/discrawl/logrotate.status`.

Both keys are read natively from the existing Keychain items. Embeddings select
`api_key_source = "keyring"`, service `discrawl/openai-embeddings`, account
`embedding-worker`. No credential values are placed in LaunchAgents or exported
to the process environment. Provider, model, dimensions, batching, timeout, and
collection filters retain their existing settings.

Startup repair begins after Gateway connection, using the same writer owner.
In this mode REST alone advances history cursors, so a new live message cannot
skip offline history. Live messages, freshness, and embeddings still update
immediately. Repair may re-fetch already captured messages; interrupted repair
remains recoverable on restart.

## Operate and inspect

Native status is the source of truth:

```sh
discrawl --config /Volumes/Data/AppData/discrawl/config.toml --json status
launchctl print gui/$(id -u)/com.hrudolph.discrawl-tail
```

Restart gracefully; launchd starts the next process after shutdown:

```sh
launchctl kill SIGTERM gui/$(id -u)/com.hrudolph.discrawl-tail
```

For deliberate maintenance, stop and later start the service:

```sh
launchctl bootout gui/$(id -u)/com.hrudolph.discrawl-tail
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.hrudolph.discrawl-tail.plist
```

Do not schedule a separate `sync` or `embed` writer against the running tail.
The old `discrawl-service`, `discrawl-health`, and `discrawl-auto-index` aliases
and their custom health/state files are retired. Custom disk/WAL pause polling
and maintenance-inhibit policy are also retired; use OS storage monitoring,
native status, and launchd service controls. Native writer ownership remains.

## Logs and verification

Streams are in `~/Library/Logs/discrawl`: `tail-native.log`,
`tail-native.err.log`, `status-native.log`, and `status-native.err.log`.
Rotation checks for 100 MiB files every 15 minutes and keeps seven generations,
with compression. Copy/truncate preserves the running process's file descriptors.

The live rollout verified both keys from launchd with credential environment
values empty, advancing capture and embeddings, startup repair completion, and
a graceful native restart with exit code 0. The previous process exited in about
two seconds. Application behavior and dependencies now match upstream.

The temporary native-service cutover files and retired coordinator copies were
removed after verification. Do not retain obsolete cutover directories or
previous executables after a successful update. Never replace the live archive
with an older copy as part of an executable update.

## Stable signing for local rebuilds

Local deployment uses the existing **OCM Local Code Signing 2026** certificate,
also selected for Redcrawl and Youcrawl, with Discrawl's own fixed identifier:
`com.hannesrudolph.discrawl`. The explicitly selected public certificate
fingerprint is in `~/.config/discrawl/signing.env`; the private key remains in
Keychain. No certificate, trust, privacy, or Keychain ACL changes are performed.

`build-signed.sh` builds the selected commit in a disposable clean worktree,
signs a separate artifact, and verifies every architecture's signature and exact
certificate-bound designated requirement. It refuses an ad-hoc identity or an
open output file. It does not install the artifact or manage the service.

```sh
. "$HOME/.config/discrawl/signing.env"
umask 077
mkdir -p "$HOME/.local/share/discrawl-build"
ops/macos/build-signed.sh "$HOME/.local/share/discrawl-build/discrawl.signed" origin/main
ops/macos/build-signed.sh --verify "$HOME/.local/share/discrawl-build/discrawl.signed"
```

Fetch and select the reviewed source revision before building. The source stays
on Data; the build worktree is temporary and is removed on completion. The final
signed executable stays at the same physical `releases/discrawl/stable/discrawl` path.
The tail and status LaunchAgents use that exact path directly.
Use this signed build step for deployment; ordinary upstream `go build` retains
Go's build-specific ad-hoc signing behavior.

For installation, stop the tail gracefully, verify its process has exited, and
atomically replace the executable with the verified signed artifact. Restart the
same LaunchAgent and check native capture, startup repair, and embeddings. Remove
the staging artifact and previous executable once verification passes. The status
and log-rotation jobs continue to use the same executable/log paths.

Keep the certificate, application identifier, executable path, and launch method
stable across updates so macOS can retain its approval. The first launch under a
new identity may need normal macOS consent; certificate rotation or OS privacy
resets may require consent again. Do not edit TCC or grant a wrapper broader
access to avoid a prompt. The signing helper is never part of the running service.

Verified on September 11, 2026: two builds with different binary hashes retained
the same certificate-bound identity. Both ran through launchd and processed live
capture and embeddings. The second build retained Data access without another
permission prompt. Temporary builds and the superseded executable were removed.

Verified on September 14, 2026: the Crawlkit v0.16.3 build retained that signing
identity, resumed live capture and embeddings, and completed startup repair across
882 channels. The configuration, archive schema version, and existing
`tempplan.md` were unchanged. Local race/coverage checks passed at 85.9% with
isolated Git configuration; the default-branch-sensitive publication fixture also
fails with upstream's original dependency. Autoreview and all six platform/arch
snapshot builds passed. Deployment receipts are in
`~/.local/share/discrawl-upgrades/20260914-crawlkit-0163/`.

After PR #237 merged, the application was rebuilt from upstream `0d365ac`.
Its source tree is identical to the previously tested PR revision; the rebuilt
binary records the upstream commit. Post-merge deployment receipts are in
`~/.local/share/discrawl-upgrades/20260914-upstream-0d365ac/`.
