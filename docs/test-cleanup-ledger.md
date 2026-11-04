# Test cleanup ledger

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
