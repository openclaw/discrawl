#!/usr/bin/env bash
set -euo pipefail

version=v0.24.0
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
  kiwi_lnx_aarch64_v0.24.0.tgz) expected_sha256=431fafce1bafc7bf5a4abcf7d306321df47cb4daabb9e8e16d40bb528a432fac ;;
  kiwi_lnx_x86_64_v0.24.0.tgz) expected_sha256=577768800258154da5fe6665081c73dafd6a0c39c8e091325d2b2bef8b5fb5d8 ;;
  kiwi_mac_arm64_v0.24.0.tgz) expected_sha256=87eda17f319c371824d5a2cc2e497eda6327ba3ddbf08a018db967f61ddbe48d ;;
  kiwi_mac_x86_64_v0.24.0.tgz) expected_sha256=98d64a1fd7acd409bb4b26889fa67355ed569e8be3568f731c3f3e27eb45a5f6 ;;
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
