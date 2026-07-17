#!/usr/bin/env bash
set -euo pipefail

VERSION=${1:-}
ARCHIVE=${2:-}
CHECKSUMS=${3:-}
IDENTIFIER=${DISCRAWL_CODESIGN_IDENTIFIER:-org.openclaw.discrawl}
EXPECTED_TEAM_ID=${DISCRAWL_CODESIGN_TEAM_ID:-FWJYW4S8P8}
REQUIREMENT="identifier \"$IDENTIFIER\" and anchor apple generic and certificate 1[field.1.2.840.113635.100.6.2.6] exists and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate leaf[subject.OU] = \"$EXPECTED_TEAM_ID\""

if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ || ! -f "$ARCHIVE" || ! -f "$CHECKSUMS" ]]; then
  echo "usage: $0 vX.Y.Z discrawl_X.Y.Z_darwin_ARCH.tar.gz checksums.txt" >&2
  exit 2
fi
[[ "$(uname -s)" == Darwin ]] || {
  echo "macOS signature verification must run on macOS" >&2
  exit 1
}

release_version=${VERSION#v}
archive=$(cd "$(dirname "$ARCHIVE")" && pwd)/$(basename "$ARCHIVE")
checksums=$(cd "$(dirname "$CHECKSUMS")" && pwd)/$(basename "$CHECKSUMS")
case "$(basename "$archive")" in
  "discrawl_${release_version}_darwin_arm64.tar.gz") expected_arch=arm64 ;;
  "discrawl_${release_version}_darwin_amd64.tar.gz") expected_arch=x86_64 ;;
  *)
    echo "unexpected macOS artifact name: $(basename "$archive")" >&2
    exit 1
    ;;
esac

expected=$(awk -v name="$(basename "$archive")" '$2 == name { print $1 }' "$checksums")
[[ "$expected" =~ ^[0-9a-fA-F]{64}$ ]] || {
  echo "missing checksum for $(basename "$archive")" >&2
  exit 1
}
actual=$(shasum -a 256 "$archive" | awk '{ print $1 }')
[[ "$actual" == "$expected" ]] || {
  echo "checksum mismatch for $(basename "$archive")" >&2
  exit 1
}

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/discrawl-verify.XXXXXX")
trap 'rm -rf "$WORK_DIR"' EXIT
entries=$(tar -tzf "$archive" | LC_ALL=C sort)
expected_entries=$(printf '%s\n' CHANGELOG.md LICENSE README.md discrawl | LC_ALL=C sort)
[[ "$entries" == "$expected_entries" ]] || {
  echo "archive must contain exactly the expected release files" >&2
  exit 1
}
tar -xzf "$archive" -C "$WORK_DIR"
binary="$WORK_DIR/discrawl"
[[ -f "$binary" && ! -L "$binary" && -x "$binary" ]] || {
  echo "archive does not contain executable discrawl" >&2
  exit 1
}

codesign --verify --strict -R="$REQUIREMENT" --verbose=2 "$binary"
codesign --verify --strict --check-notarization -R=notarized --verbose=2 "$binary"
signature=$(codesign -dvvv "$binary" 2>&1)
grep -Fx "Identifier=$IDENTIFIER" <<<"$signature" >/dev/null
grep -Fx "TeamIdentifier=$EXPECTED_TEAM_ID" <<<"$signature" >/dev/null
grep -F "Authority=Developer ID Application:" <<<"$signature" >/dev/null
grep -F '(runtime)' <<<"$signature" >/dev/null
lipo -archs "$binary" | tr ' ' '\n' | grep -Fx "$expected_arch" >/dev/null
[[ "$("$binary" --version)" == "$release_version" ]]
