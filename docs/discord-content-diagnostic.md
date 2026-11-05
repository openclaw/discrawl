# One-shot hosted Discord content diagnostic

This is a prepared operation, not a diagnosis and not permission to execute it.
Do not dispatch or merge until the existing task owner reviews the candidate and
explicitly authorizes the next action. The workflow CODEOWNER,
`@openclaw/openclaw-secops`, must actually approve the workflow change; an automated
or independent API review does not replace that source-owner approval.

## Existing operator admission

Only `vincentkoc` may manually dispatch, and both `actor` and
`triggering_actor` must remain `vincentkoc`. Use reviewed `main`, attempt 1,
and supply the exact checked-out main commit as `expected_sha`. Reruns refuse.
The existing shared `discord-backup-repo` concurrency group serializes this
operation without disabling jobs or changing schedules.

Before dispatch, the existing operator must confirm, using already available
trusted records or the ordinary Discord UI (not more diagnostic API calls):

- The existing collector application ID and bot ID; supply
  `expected_application_id` and `expected_bot_id`. No token input is accepted.
- The human author ID of **message 1564461144779784244**, supplied as
  `expected_author_id`. Do not paste the message body into GitHub.
- This exact message is ordinary human text, not a bot/webhook/interaction,
  reply/forward, app mention, or a message used with an app context-menu command.
  It must be in **#maintainers 1456744319972282449**, in guild
  **1456350064065904867**. Its snowflake timestamp is
  **2026-10-27 02:10:03.198 UTC**, within the October 26–27 UTC window.
  Set `ordinary_text_confirmed=true` only after confirming these facts.

If the fixed candidate is not suitable or cannot be confirmed, stop and return
to the existing task owner. Do not select another message or recollect.

## Fixed operation and output

The bot token stays in the same trusted hosted repository-secret context used
by the collector. Never retrieve, transfer, paste, or run it locally. The
separate manual workflow has no acquisition/publisher steps, private archive
credentials, cache/artifact upload, or checkpoint/database path. It cannot fall
through into normal acquisition. It makes only these requests, in order:

1. `GET https://discord.com/api/v10/applications/@me`
2. `GET https://discord.com/api/v10/channels/1456744319972282449/messages/1564461144779784244`

Application identity and flags must validate before the second GET. Each
connection has a 10-second socket timeout; a 30-second overall deadline bounds
both calls, and each success body is capped at 256 KiB. TLS uses the fixed origin
without environment proxies; redirects and automatic retries are not supported.
Permission errors, 429s, bad identity, malformed/oversized responses, and other
failures stop with a fixed error class. Error bodies are not read.

The only JSON output fields are the observed HTTP statuses, validated app/bot
IDs, two MessageContent flag booleans, a message-identity validation boolean,
content length (Unicode code points), and a fixed result/error. No message body,
author ID, owner/team metadata, response payload, private exception, or token is
printed or saved. Inputs are read from the hosted event file, not echoed as step
environment values. A message response need not include `guild_id` in REST:
fixed channel identity plus the operator's confirmed channel/guild mapping binds
it to the target guild; if a guild ID is returned it must match too.

Prefer the full decimal-string `flags_new`; use numeric legacy `flags` only
when the full field is absent. If both exist their low 31 bits must agree.
Missing/malformed flags are **unverified**, never interpreted as false. Report
bits 18 and 19 separately; do not confuse them with Gateway intent bit 15.
Neither flags nor an empty content length alone establish the historical cause.
A 200 message GET proves neither content access nor past ingestion behavior.
No grants, permissions, imports, backfills, checkpoints or publications change.

On any failure, stop and have the owner inspect only the sanitized result.
A fresh dispatch/retry needs another explicit decision; no automatic recollection.

## Official Discord references (checked October 27, 2026)

- [Current application endpoint](https://docs.discord.com/developers/resources/application#get-current-application)
- [Application flags and full-string encoding](https://docs.discord.com/developers/resources/application#application-object-application-flags)
- [HTTP restrictions independent of requested Gateway intents](https://docs.discord.com/developers/events/gateway#http-restrictions)
- [MessageContent restrictions and exceptions](https://docs.discord.com/developers/events/gateway#message-content-intent)
- [Single message GET and its VIEW_CHANNEL / READ_MESSAGE_HISTORY requirements](https://docs.discord.com/developers/resources/message#get-channel-message)
