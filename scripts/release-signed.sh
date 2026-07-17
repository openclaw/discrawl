#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VERSION=${1:-}
HELPER=${MAC_RELEASE_HELPER:-$HOME/Projects/agent-scripts/skills/release-mac-app/scripts/mac-release}

[[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] || {
  echo "usage: $0 vX.Y.Z" >&2
  exit 2
}
[[ "$(uname -s)" == Darwin ]] || {
  echo "signed releases must run on macOS" >&2
  exit 1
}
[[ -n "${NOTARYTOOL_KEYCHAIN_PROFILE:-}" ]] || {
  echo "NOTARYTOOL_KEYCHAIN_PROFILE is required for official macOS release notarization" >&2
  exit 1
}
[[ -x "$HELPER" ]] || {
  echo "managed-keychain release helper not found: $HELPER" >&2
  exit 1
}
command -v goreleaser >/dev/null || {
  echo "goreleaser is required" >&2
  exit 1
}

head_commit=$(git -C "$ROOT" rev-parse HEAD)
tag_commit=$(git -C "$ROOT" rev-parse "refs/tags/$VERSION^{commit}" 2>/dev/null) || {
  echo "release tag does not exist locally: $VERSION" >&2
  exit 1
}
[[ "$head_commit" == "$tag_commit" ]] || {
  echo "HEAD does not match release tag $VERSION" >&2
  exit 1
}
[[ -z "$(git -C "$ROOT" status --porcelain --untracked-files=normal)" ]] || {
  echo "release checkout is not clean" >&2
  exit 1
}
git -C "$ROOT" tag -v "$VERSION" >/dev/null 2>&1 || {
  echo "release tag is not signed by a trusted git signing key: $VERSION" >&2
  exit 1
}

release_auth=${GITHUB_TOKEN:-${GH_TOKEN:-}}
if [[ -z "$release_auth" ]]; then
  command -v gh >/dev/null || {
    echo "GITHUB_TOKEN, GH_TOKEN, or an authenticated gh CLI is required for GoReleaser publishing" >&2
    exit 1
  }
  release_auth=$(gh auth token)
fi
[[ -n "$release_auth" ]] || {
  echo "GitHub authentication is unavailable" >&2
  exit 1
}
export GITHUB_TOKEN=$release_auth
unset release_auth

cd "$ROOT"
exec "$HELPER" codesign-run -- \
  env DISCRAWL_CODESIGN_REQUIRED=1 \
  goreleaser release --clean --config "$ROOT/.goreleaser.yaml"
