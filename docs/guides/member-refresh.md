# Full member refresh

The native tail's existing repair cycle checks full member freshness. A completed
enumeration is reused for 24 hours; an older or missing completion receipt makes
the next repair refresh the list. Startup repair uses the same rule. Gateway
member add/update/remove events continue independently while pagination runs.
No additional scheduler or competing writer is needed.

The bot needs the Server Members privileged intent and guild access. Message
Content is a separate privileged intent. Use the configured native credential
resolver; macOS go-keyring entries may contain an encoded storage envelope, which
is decoded before native authentication. Stored bytes are not necessarily a
token ready to send. Neither a historical capability failure nor a probe of an
undecoded envelope justifies rotating a working token.

`GuildMemberEnumeration` follows the supported 1000-member `after` pagination
until a short or empty terminal page. Missing identities, a non-advancing cursor,
interruption and provider failures cannot certify completion. Native rate-limit
handling stays unchanged. Enumeration and merge share a 15-minute context budget (or an
earlier parent deadline), allowing large guilds to finish while respecting
provider bucket waits; the former five-minute budget could expire before their
terminal page. Per-request timeouts and provider limits are unchanged. Failed
attempts log completed-page count and cursor frontier, without advancing the
full-refresh gate. Bucket and 429 retry waits are context-aware and measured
separately from SDK-call elapsed time and database merge time. The same SDK
bucket/global state is consulted without mutation, and the SDK still enforces
it on every actual call. Per-call automatic 429 retry is replaced only in this
lane by a cancellable wait for the entire provider Retry-After; other endpoints
retain their existing behavior. SDK-call elapsed includes transport, body read,
decoding and any internal server-error retries; it is not CPU time and may include
an additional SDK wait if shared limiter state changes concurrently.

Progress every 25 received pages reports page/member count, last user ID,
measured provider wait, SDK-call elapsed and observed 429 count. No member names
or content are logged. Cancellation never authorizes another page or certifies
an incomplete listing.

After complete pagination, member rows merge in 256-row writer transactions.
Rows updated after enumeration began win, including newer Gateway additions,
role changes and removal tombstones. Unreturned rows remain retained: a missing
row in a changing paginated listing is not an invented provider deletion.
Partial merge progress may survive an interruption, but never advances the full
refresh gate. A subsequent ordinary repair retries the enumeration safely.

`guild:<guild_id>:members:last_success` advances only after complete enumeration
and merge. Existing cached authors or Gateway deltas cannot seed that gate.
The completion log records enumerated members/pages, applied/newer-skipped counts
and the last user ID frontier, without names or content. Enumeration is not a
Discord-wide atomic snapshot. The archive's retained member count can exceed
the returned membership count; its historical evidence is preserved.

Failures use the existing `failure_ledger` operation `guild_members`, source
`discord`, scoped to the guild. They keep health degraded until a completed
refresh resolves them. Intentional owner cancellation keeps the prior completion
receipt due instead of inventing success or flagging shutdown as a provider fault.

The explicit `sync --with-members` path still requires ordinary exclusive native
ownership. Do not run it as a competing writer against a live tail. Querying the
directory does not request provider refresh. See [Data layout](data-storage.html)
and [Sync](../commands/sync.html).
