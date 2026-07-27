#!/usr/bin/env bash
set -euo pipefail

echo "local releases are disabled because this path cannot enforce the shared verifier and publication chain" >&2
echo "official releases must use: gh workflow run release-unified.yml --repo openclaw/discrawl -f version=X.Y.Z" >&2
exit 1
