# Unmodified upstream rebuild, September 11, 2026

PR #216 merged as `34bb67ea09694f98a98b26288636b84ae4f4cff3`. The executable is
rebuilt directly from that clean `origin/main` commit, version
`0.14.1-6-g34bb67e`, with zero local application patches. Its source tree matches
the previously tested local exclusion fix exactly. All CI checks on the merged
commit passed. There are no schema or dependency changes from the prior runtime.

The retained Mac adapter is unchanged: hourly serialized indexing, Keychain
credential loading, disk/WAL guards, bounded logs, native freshness checks, and
the ten-second backlog probe with one-minute retries. The opt-out configuration
and both LaunchAgent definitions are preserved byte-for-byte.

The executable lives in `openclaw-tools/releases/discrawl/upstream-34bb67e`;
the matching adapter release is `openclaw-tools/discrawl-ops/releases/20260911-upstream-34bb67e`.
Only `ops/macos` differs from upstream in the operational source branch.

Previous release links, configuration, manifests, preserved-file hashes, and
live verification are recorded in
`openclaw-tools/backups/discrawl-upstream-34bb67e-20260911`.
