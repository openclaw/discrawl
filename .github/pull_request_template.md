<!--
Optional linked context:
Add a visible `Closes #<issue-number>` or `Related: #<issue-number>` line
below this comment.

Write for someone searching or maintaining a local Discord archive who has not read the code.
The title and opening problem statement should make sense without the diff.

Required PR title:
type: <what changes for the person affected>
Types: feat, fix, improve, refactor, docs, chore.
Use a parenthesized scope only when it helps that reader.

Name the affected task and the symptom or outcome, not the implementation:
Good: fix: let readers find archived messages without a network connection
Bad: fix(search): change the query fallback
Good: docs: help archive operators choose between sync and tail
Bad: docs: update collector command notes

For internal-only work, name the actual developer or operator workflow affected.
Do not invent an end-user benefit.

**MUST:** Keep **Allow edits from maintainers** enabled for this PR so maintainers
can help update the branch when needed.
-->

## What Problem This Solves

<!--
Describe the concrete user, product, or operational problem.
In 1–3 short, plain-language sentences, explain what someone is trying to do,
what goes wrong or is missing, and why it matters. Include a known trigger for
bugs; describe the need without inventing a bug for other changes.

Good: "Readers searching a local archive can miss messages that were already saved when the network is unavailable, so they cannot recover the conversation they need."
Bad: "Fixes the search query fallback."

Describe the experienced problem, not just the code change.
-->

## User Impact

<!--
"User impact: <what users, operators, or developers can now do or expect>."
Lead with the concrete outcome in plain language, usually one sentence.
For internal-only changes, say there is no user-visible change; do not invent a benefit.
Keep important risks, breaking changes, migrations, and required user actions visible here.
-->

## Why This Change Was Made

<!--
Briefly explain how the change addresses the problem without repeating the impact.
Keep the body short. Leave file lists, internal acronyms, and root-cause walkthroughs
in the diff or optional <details>; include technical detail only when it explains
behavior or a material tradeoff. Do not hide risks or required actions in <details>.
-->

## Evidence

<!--
Show the most useful proof that this change works. Screenshots, screencasts,
terminal output, focused tests, CI results, live observations, redacted logs,
and artifact links are all useful. Include before/after evidence for visual
changes when it clarifies the result.

Start with the behavior checked and the observed result in plain language; note
meaningful gaps. Put exact commands, commit hashes, and long logs in links or
optional <details>. Do not make readers decode commands to learn what was verified.
-->
