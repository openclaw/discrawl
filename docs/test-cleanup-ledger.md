# Test cleanup ledger

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
