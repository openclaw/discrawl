#!/bin/bash
# Local build-time tooling. launchd executes Discrawl itself, never this script.
set -euo pipefail
umask 077

die() { printf 'discrawl signing: %s\n' "$*" >&2; exit 1; }
[[ "$(/usr/bin/uname -s)" == Darwin ]] || die "macOS is required"
identity="${DISCRAWL_SIGNING_IDENTITY:-}"
[[ "$identity" =~ ^[0-9A-Fa-f]{40}$ ]] || die "set DISCRAWL_SIGNING_IDENTITY to the selected certificate fingerprint"
identity="$(printf '%s' "$identity" | tr '[:upper:]' '[:lower:]')"
app_id="com.hannesrudolph.discrawl"
requirement="identifier \"$app_id\" and certificate leaf = H\"$identity\""

verify() {
  local binary="$1" arch archs details designated
  [[ -f "$binary" && ! -L "$binary" && -x "$binary" ]] || die "expected a regular executable"
  /usr/bin/codesign --verify --strict --all-architectures -R "=$requirement" "$binary"
  archs="$(/usr/bin/lipo -archs "$binary")"
  [[ -n "$archs" ]] || die "missing Mach-O architecture"
  for arch in $archs; do
    case "$arch" in arm64|x86_64) ;; *) die "unsupported architecture" ;; esac
    details="$(/usr/bin/codesign -d --arch "$arch" -r- "$binary" 2>&1)"
    designated="$(printf '%s\n' "$details" | sed -n 's/^designated => //p')"
    [[ "$designated" == "$requirement" ]] || die "embedded identity is not the fixed certificate-bound requirement"
  done
}

not_running() {
  local result=0 owners
  [[ -e "$1" ]] || return 0
  owners="$(/usr/sbin/lsof -t "$1" 2>&1)" || result=$?
  [[ "$result" == 1 && -z "$owners" ]] || die "output is open or its ownership could not be checked"
}

if [[ "${1:-}" == --verify ]]; then
  [[ $# == 2 ]] || die "usage: $0 --verify BINARY"
  verify "$2"
  exit 0
fi
[[ $# -ge 1 && $# -le 2 ]] || die "usage: $0 OUTPUT [REF]"
output="$1"
[[ "$output" == /* ]] || output="$(pwd -P)/$output"
[[ ! -L "$output" && ! -d "$output" ]] || die "output must not be a symlink or directory"
parent="$(dirname "$output")"
[[ -d "$parent" ]] || die "output parent must exist"
not_running "$output"
repo="$(cd "$(dirname "$0")/../.." && pwd -P)"
commit="$(git -C "$repo" rev-parse --verify --end-of-options "${2:-origin/main}^{commit}")"
version="$(git -C "$repo" describe --tags --always "$commit")"
version="${version#v}"
stage="$(mktemp -d "$parent/.discrawl-sign.XXXXXX")"
cleanup() {
  git -C "$repo" worktree remove --force "$stage/source" >/dev/null 2>&1 || true
  rm -rf "$stage"
}
trap cleanup EXIT
git -C "$repo" worktree add --quiet --detach "$stage/source" "$commit"
(
  cd "$stage/source"
  GOWORK=off GOFLAGS= go build -mod=readonly -trimpath \
    -tags "${DISCRAWL_BUILD_TAGS:-}" \
    -ldflags "-X github.com/openclaw/discrawl/internal/cli.version=$version" \
    -o "$stage/discrawl" ./cmd/discrawl
  [[ -z "$(git status --porcelain)" ]] || die "source changed during the build"
)
/usr/bin/codesign --force --sign "$identity" --identifier "$app_id" \
  --timestamp=none --requirements "=designated => $requirement" "$stage/discrawl"
verify "$stage/discrawl"
chmod 0755 "$stage/discrawl"
not_running "$output"
mv -f "$stage/discrawl" "$output"
printf 'Signed %s from %s as %s\n' "$output" "$commit" "$app_id"
