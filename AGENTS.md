# AGENTS.md

## Independent review

Use a native OpenClaw isolated, read-only subagent for independent P0–P3 review
of the complete pinned candidate. Supply its exact base and candidate identity,
all task-owned changes, and the necessary unchanged context. Resolve findings
and review any resulting changes before delivery. Do not use scripted review
wrappers or automated review-dispatch workflows. Keep ordinary tests, static
analysis, and secret scanning enabled; they do not replace independent review.
