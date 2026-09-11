# Native Discrawl service on this Mac

launchd runs Discrawl directly. Discrawl loads both credentials, owns the archive
writer, captures Gateway events, repairs history, and processes embeddings.
There is no Python coordinator or credential-loading shell wrapper.

The application is clean upstream commit
`1907d9f70353eb331212d6ba584be055b8bd7eba`, version `0.14.1-10-g1907d9f`, with
Crawlkit v0.16.1. PR #220 supplies native embedding keyring selection; PR #219
supplies opt-in startup repair. Both passed CI and final ClawSweeper review.

## Installed configuration

- Executable: `/Volumes/Data/Projects/openclaw-tools/bin/discrawl`, through the
  `releases/discrawl/current` symlink.
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
two seconds. The application has no local patches.

Evidence and rollback files are under
`/Volumes/Data/Projects/openclaw-tools/backups/discrawl-native-service-20260911`.
See `native-live-verification.json`, `restart-verification.json`, and
`rollback.json`. The retired coordinator is preserved there solely for rollback.
Rollback restores the saved service/config and a schema-compatible executable;
it never restores an older database over newly captured messages.
