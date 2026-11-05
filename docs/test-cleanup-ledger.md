# Test cleanup ledger

## Metric-reading test oracle

- Move EntityMetrics and MetricValue unchanged into the existing record-efficiency
  test file. Their only consumer is the metric assertion helper; production has
  no callers. Keep the independent SQL read oracle and every test assertion.
- Keep metric persistence and the documented SeedEntityMetrics manual backfill
  unchanged. No public module API, providers, live archives or releases change.
- Validate the same eight real temporary-SQLite record-efficiency cases before
  and after with the race detector, an exact source-move proof, static checks,
  independent review and CI.

## Deterministic gateway queue overflow

- Keep the real WebSocket queue-overflow test, its four events, one-worker/
  one-slot configuration, and fatal-error assertion. Hold its worker with the
  existing dequeue hook until cancellation and its gateway until teardown.
- Remove the single-use sleeping handler and its six methods. Replace the
  50 ms gateway and 200 ms handler timing assumptions with synchronization;
  bound the Tail call with a five-second context.
- Delaying the same frames by 250 ms reproduces a global timeout before the
  change and passes afterward. Disabling production overflow signaling makes
  the fixed test fail its original error assertion within the context deadline.
  Validate normal and delayed delivery, the Discord race suite, static checks,
  independent review, and CI. All gateway traffic is local; no releases.

## Sync option error fixtures

- Consolidated the missing-scope and unknown-channel tests into parallel table
  cases. Both used identical config, temporary SQLite, and runtime scaffolding.
- Keep independent config/store paths per case, the actual service lifecycle and
  message sync option resolver, and both error assertions. Numeric channel IDs,
  guild inference, and Discord-service admission tests remain unchanged.

No sync behavior, provider calls, archive schema, or release changes.

## Publication transport Git shim

- Restore the original PATH when the transport-budget shim delegates to the
  resolved Git command. A PATH-dispatching Git wrapper otherwise rediscovers
  the test shim and recurses instead of running Git.
- Keep the full synthetic publication proof: bounded literal arguments,
  unrelated staged/unstaged content, deletion handling, output errors, and
  cancellation. Production Git selection and publication behavior are unchanged.

## CLI fixture scaffolding

- Moved the test-only `withServices` fixture wrapper into `cli_test.go` and
  removed its redundant `withServicesAuto` forwarding layer. Fixtures still
  call the active service lifecycle with the same arguments.
- Moved `writeSyncLockMetadataFiles` into the test file. Its callers construct
  legacy or malformed lock fixtures; production writes through an open record.
- Kept all CLI fixture callers and their lock, startup, query, and error checks.
  Production retains the active locked-service and lock-record implementations.

No CLI service behavior, lock format, or release artifacts changed.

## Test-only formatters

- Removed the unused `clipWhitespace` formatter and its two assertions from
  the report helper test. No report rendering path called it.
- Removed the test-only `normalizeMessage`/`normalizeMessageParts` chain and
  its obsolete attachment-adapter test. Production derives attributed message
  parts directly through `messagetext`.
- Updated the rich-message and Unicode tests to exercise `buildMessageMutation`,
  requiring the actual archived normalized content. Their attachment fixtures
  now carry IDs so the real extraction path includes them.
- Kept raw-message round-trip checks on the real `messagetext` API.
- Retained the SQLite-backed report rendering and Components V2 round-trip/search
  tests, plus rich-message and malformed-Unicode normalization checks.

No report output, message content, schema, or release artifacts changed.

## Gateway event delivery

- Reused the existing gateway handshake helper instead of maintaining a second
  copy of its HTTP discovery and WebSocket hello/identify/ready exchange.
- Replaced the 100ms cancellation sleep in `TestTailReceivesGatewayEvents`
  with explicit callback completion. The test waits for ready, message,
  channel/thread, and member callbacks across both worker queues.
- Removed the fake socket's 50ms lifetime. It now stays open until the test
  cancels after all expected callbacks, with bounded failure deadlines.
- Retained exact callback multiplicities, message observation order, and
  handler removal assertions. The fixture uses only a local HTTP/WebSocket
  server and synthetic events; it does not contact Discord.

This is a bounded gateway-test cleanup, not a complete repository audit.
No production behavior or release artifacts changed.

## Retired manifest timestamp predicates

- Removed `ManifestAlreadyImported` and `ManifestAlreadyMerged`. Production
  import/merge paths no longer call these timestamp-only predicates.
- Replaced their integration-test uses with exact persisted marker assertions
  after actual export, merge, and forced replacement operations.
- Removed malformed-marker fixtures and predicate-only assertions. Retained
  empty-manifest handling, pending-replacement clearing, import freshness, local
  row preservation, and exact replacement checks.

No import planning, stored state format, archive data, or release artifacts changed.

## Tail ancestor events

- Removed `trackChannelExclusion`, a retired in-memory updater called only by
  the ancestor-move test. Production persists channel events and refreshes scope.
- Reworked that test to send actual channel-upsert and message-create events
  against temporary SQLite storage. It retains child-before-parent ordering and
  moves a forum into and back out of an excluded category.
- Requires exactly the two admitted message IDs in the archive, proving both
  rejection while excluded and collection after readmission.
- Retained the separate concurrent-revocation and incomplete-ancestry tests.

No production event handling, archive format, or release artifacts changed.

## Retired tail test adapters

- Removed `SetTailFailureStage`, whose only callers were tests. Tail execution
  already installs failure metadata before invoking handlers.
- Changed the stage-context test to use that active metadata context and
  `UpdateTailFailureStage`. Keep virtual-time elapsed checks, canceled-context
  rejection, frozen stages, unknown-stage normalization, and nil safety.
- Removed `IsGatewayOpenError`, whose only callers were two error-path tests.
  Both now use `errors.As` assertions on the real `GatewayOpenError` type.
  Gateway startup sanitization, error chaining, and failure-report checks remain.

No gateway behavior, failure metadata, or release artifacts changed.

## Retired config test adapters

- Removed `SearchGuildDefaults`, a constant-nil method with no production
  callers, and its only nil assertion. Effective default-guild, normalization,
  search/filter, and runtime-directory tests remain.
- Removed the test-only `RemoteEnabled` forwarding method. The existing default
  and cloud-mode checks now call `cfg.Remote.Enabled()` on the embedded
  crawlkit config.
  The cloud read-only assertion and remote config fields remain unchanged.

Validation: config race tests, module consistency, formatting, vet, independent
P0–P3 review, and full PR CI. No config format, CLI behavior, live archive data,
or release artifacts changed.

## Retired standalone event writer

- Removed `AppendMessageEvent`, whose only callers were tests. Production
  message updates and deletions append events through their transactions.
- Removed the adapter's closed-store assertion and an unused event fixture
  from the mixed sync-state/helper test. Its real message deletion remains.
- Strengthened the existing batched-write test from an event count to exact
  persisted guild, channel, message, type, and payload rows. It still checks
  both messages and now rejects incorrectly wired event contents.

No production event behavior, archive format, or release artifacts changed.

## Retired incomplete-channel filter

- Removed `IncompleteMessageChannelIDs`, called only by tests. Full sync plans
  with `AllIncompleteMessageChannelIDs`; routine sync filters provider channels
  through `FreshUnavailableChannelIDs` in the syncer.
- Removed two tests of the retired composition and its assertions in the TTL
  tests. Keep production timestamp parsing, marker-window edges, refresh,
  clearing, suffix matching, and malformed/offset timestamp coverage.
- Preserved completed-thread coverage on the active full-sync listing and
  added an incomplete thread alongside guild, voice/stage, and marker cases.
  Closed-store error coverage now calls that same production query.
- Retained real syncer entrypoint and message-sync tests for routine skip/retry,
  explicit requests, full sync, and catalog progress using temporary archives.

No sync behavior, query/schema format, live archive data, or release changes.

## Test-only guild member count

- Removed `GuildMemberCount`, whose only callers were two store tests, and its
  dedicated `CountMembersByGuild` SQL definition. Regenerated sqlc output with
  the repository's pinned generator.
- Removed the closed-store error check and rebuild-test count assertion for
  that unused API. Keep the rebuild test's actual member-search assertion,
  channel count, and message-search assertion.
- Retained the active `Members` and `Status` paths, including existing member
  count assertions and the global `CountMembers` SQL query.

No member storage, search, status, archive schema, or CLI behavior changed.

## Publication fixture database cleanup

- Close the synthetic publication database after its setup transaction commits.
  The three real local-Git cases pass before the fix while leaving their fixture
  connections usable; afterward they pass with those connections closed.
- Remove the exact duplicate `save` helper import. Preserve every test body,
  fixture value and the custom collection hook that excludes imported base tests.
- Validate the publication cases, complete checkpoint Python suite, independent
  review and platform CI using temporary databases and local Git remotes only.
  No provider, live archive or release behavior changes.

## Test-only sync lock fixture

- Move the acquireSyncLock convenience wrapper out of the three platform
  production files into the existing CLI test fixture. Production uses the
  metadata-aware lock APIs; all twelve wrapper callers are tests.
- Keep every test body and PID metadata value unchanged. The shared fixture
  delegates to the same platform implementation, including the unsupported-OS
  no-op. Keep acquisition, cancellation, handoff, reader, and release behavior.
- Validate the same 17 real temporary-file lock/reader cases before and after
  with the race detector, plus static checks, independent review, and CI.
  Tests use temporary archive paths and test-owned locks; no releases.

## Test-only message failure forwarding method

- Remove RecordFailureWithMessageScope, an internal forwarding method used only
  by five test calls. Use the production RecordFailureWithMessageScopeTimed
  method directly and check the same returned errors.
- Preserve every scope value, error message, durable-ledger assertion, diagnostic
  case and temporary SQLite fixture. The persistence implementation is unchanged.
- Validate the same three real failure-persistence cases before and after under
  the race detector, plus static checks, independent review, and CI. No provider
  calls, live archives, release behavior, or public module API changes.

## Retired member batch API

- Remove MergeMembers, an internal batch API with five test callers and no
  production callers. Production refreshes use MergeObservedMembers.
- Seed search and tombstone fixtures through UpsertMember. Move the omitted-member
  preservation check onto the production refresh API; retain every fixture and
  behavioral assertion. Drop only the removed API's closed-store assertion.
- Preserve the production upsert and observed-refresh implementations. Validate
  the same seven temporary-SQLite cases before and after under the race detector,
  check omitted-member deletion sensitivity with a disposable fault overlay,
  and run static checks, independent review and CI. No releases or live archives.

## Embedding recovery poll read errors

- Use a fresh embedding count for each recovery poll and require a successful
  SQL read before reporting two embeddings. A failed read must not reuse the
  earlier count of two captured messages and falsely certify recovery.
- Preserve the real tail, mock-keyring and local HTTP provider path, temporary
  archive writes, normal retry timer, poll interval and deadlines.
- Disposable Go overlays reproduce the broken-read false green and verify the
  repaired poll keeps failing while reads fail. Keep the healthy integration
  cases at their original timing; run race, static, independent-review and PR
  CI checks. No production code, live archives, deployment or releases change.
