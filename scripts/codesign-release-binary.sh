#!/usr/bin/env bash
set -euo pipefail

TARGET=${1:-}
BINARY=${2:-}
REQUIRED=${DISCRAWL_CODESIGN_REQUIRED:-0}
IDENTIFIER=${DISCRAWL_CODESIGN_IDENTIFIER:-org.openclaw.discrawl}
EXPECTED_TEAM_ID=${DISCRAWL_CODESIGN_TEAM_ID:-FWJYW4S8P8}
REQUIREMENT="identifier \"$IDENTIFIER\" and anchor apple generic and certificate 1[field.1.2.840.113635.100.6.2.6] exists and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate leaf[subject.OU] = \"$EXPECTED_TEAM_ID\""

case "$TARGET" in
  darwin_arm64*) EXPECTED_ARCH=arm64 ;;
  darwin_amd64*) EXPECTED_ARCH=x86_64 ;;
  darwin_*)
    echo "unsupported macOS release target: $TARGET" >&2
    exit 1
    ;;
  *) exit 0 ;;
esac

if [[ "$REQUIRED" != 1 && -z "${CODESIGN_IDENTITY:-}" ]]; then
  echo "skipping Developer ID signing for snapshot target $TARGET"
  exit 0
fi

[[ "$(uname -s)" == Darwin ]] || {
  echo "official macOS release signing must run on macOS" >&2
  exit 1
}
[[ -n "${CODESIGN_IDENTITY:-}" ]] || {
  echo "CODESIGN_IDENTITY is required; run the release through mac-release codesign-run" >&2
  exit 1
}
[[ -n "${NOTARYTOOL_KEYCHAIN_PROFILE:-}" ]] || {
  echo "NOTARYTOOL_KEYCHAIN_PROFILE is required for official macOS release notarization" >&2
  exit 1
}
[[ -f "$BINARY" ]] || {
  echo "release binary not found: $BINARY" >&2
  exit 1
}
for tool in codesign ditto lipo mktemp mv plutil xcrun; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "missing required release command: $tool" >&2
    exit 1
  }
done

binary_dir=$(cd "$(dirname "$BINARY")" && pwd)
binary_name=$(basename "$BINARY")
work_dir=$(mktemp -d "$binary_dir/.discrawl-notary.XXXXXX")
candidate="$work_dir/$binary_name"
submission="$work_dir/$binary_name.zip"
trap 'rm -rf "$work_dir"' EXIT

# Keep the GoReleaser output untouched unless signing, notarization, and every
# verification step succeeds.
cp -p "$BINARY" "$candidate"
codesign --force --options runtime --timestamp \
  --identifier "$IDENTIFIER" --sign "$CODESIGN_IDENTITY" "$candidate"

ditto -c -k --sequesterRsrc --keepParent "$candidate" "$submission"
notary_result=$(xcrun notarytool submit "$submission" \
  --keychain-profile "$NOTARYTOOL_KEYCHAIN_PROFILE" \
  --no-s3-acceleration \
  --wait \
  --output-format json)
notary_status=$(plutil -extract status raw -o - - <<<"$notary_result")
notary_id=$(plutil -extract id raw -o - - <<<"$notary_result")
[[ "$notary_status" == Accepted ]] || {
  echo "macOS notarization status is ${notary_status:-missing}, expected Accepted" >&2
  exit 1
}
[[ "$notary_id" =~ ^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$ ]] || {
  echo "macOS notarization response has an invalid submission id" >&2
  exit 1
}

codesign --verify --strict -R="$REQUIREMENT" --verbose=2 "$candidate"
codesign --verify --strict --check-notarization -R=notarized --verbose=2 "$candidate"

signature=$(codesign -dvvv "$candidate" 2>&1)
grep -Fx "Identifier=$IDENTIFIER" <<<"$signature" >/dev/null
grep -Fx "TeamIdentifier=$EXPECTED_TEAM_ID" <<<"$signature" >/dev/null
grep -F "Authority=Developer ID Application:" <<<"$signature" >/dev/null
grep -F '(runtime)' <<<"$signature" >/dev/null
[[ "$(lipo -archs "$candidate")" == "$EXPECTED_ARCH" ]] || {
  echo "release binary must contain exactly the $EXPECTED_ARCH architecture" >&2
  exit 1
}

mv -f "$candidate" "$BINARY"
