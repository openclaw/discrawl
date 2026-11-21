#!/usr/bin/env bash
set -euo pipefail

version=v0.24.1
case "$(uname -s)" in
  Darwin) platform=mac ;;
  Linux) platform=lnx ;;
  *) echo "unsupported Kiwi build platform: $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  arm64|aarch64)
    if [[ "$(uname -s)" == Linux ]]; then
      architecture=aarch64
    else
      architecture=arm64
    fi
    ;;
  x86_64|amd64) architecture=x86_64 ;;
  *) echo "unsupported Kiwi architecture: $(uname -m)" >&2; exit 1 ;;
esac

archive="kiwi_${platform}_${architecture}_${version}.tgz"
url="https://github.com/bab2min/Kiwi/releases/download/${version}/${archive}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

curl --fail --location "$url" --output "$work/kiwi.tgz"
case "$archive" in
  kiwi_lnx_aarch64_v0.24.1.tgz) expected_sha256=ff15838604039130d24e2803ed87c2edc36665f9ff24be8a23b257c2860bc25b ;;
  kiwi_lnx_x86_64_v0.24.1.tgz) expected_sha256=534f3b507784ea0f2c26884a13c22f2bfa573f86b6c20b9856feb9050a88f47a ;;
  kiwi_mac_arm64_v0.24.1.tgz) expected_sha256=510680643980c4af94bee57151020b6373a098bac809084e7cb26080fc5bd9b9 ;;
  kiwi_mac_x86_64_v0.24.1.tgz) expected_sha256=9042ea4ca99bbb57c8bd3574b2cb1542958734e1f1de8ff8fe6a6ad05e5f077a ;;
  *) echo "missing pinned Kiwi checksum for $archive" >&2; exit 1 ;;
esac
actual_sha256="$(shasum -a 256 "$work/kiwi.tgz" | awk '{print $1}')"
if [[ "$actual_sha256" != "$expected_sha256" ]]; then
  echo "Kiwi archive checksum mismatch for $archive" >&2
  exit 1
fi
tar -xzf "$work/kiwi.tgz" -C "$work"
sudo mkdir -p /usr/local/include /usr/local/lib
sudo cp -R "$work/include/kiwi" /usr/local/include/
sudo cp -P "$work"/lib/libkiwi* /usr/local/lib/
if [[ "$(uname -s)" == Linux ]]; then
  sudo ldconfig
fi
