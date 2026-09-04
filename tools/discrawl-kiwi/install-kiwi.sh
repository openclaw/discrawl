#!/usr/bin/env bash
set -euo pipefail

version=v0.23.2
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
  kiwi_lnx_aarch64_v0.23.2.tgz) expected_sha256=7e093121a367087d21e7c696bcc69a505935b07798d1e95c87f3b66a646c124e ;;
  kiwi_lnx_x86_64_v0.23.2.tgz) expected_sha256=0b6694a795891de22fb14ae46825403af02063450126282c18448d6562b97174 ;;
  kiwi_mac_arm64_v0.23.2.tgz) expected_sha256=ac124e32e013e2089cb4d842e2b735a1e6b4f3b126cdf692d78fda1130b8a382 ;;
  kiwi_mac_x86_64_v0.23.2.tgz) expected_sha256=422c4284cc73a7499a714090e4d2f1c039dbc565aa2b425e5a0c0656d7b483a5 ;;
  *) echo "missing pinned Kiwi checksum for $archive" >&2; exit 1 ;;
esac
actual_sha256="$(shasum -a 256 "$work/kiwi.tgz" | awk '{print $1}')"
if [[ "$actual_sha256" != "$expected_sha256" ]]; then
  echo "Kiwi archive checksum mismatch for $archive" >&2
  exit 1
fi
tar -xzf "$work/kiwi.tgz" -C "$work"
sudo cp -R "$work/include/kiwi" /usr/local/include/
sudo cp -P "$work"/lib/libkiwi* /usr/local/lib/
if [[ "$(uname -s)" == Linux ]]; then
  sudo ldconfig
fi
