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
    if [[ "$platform" == Linux ]]; then
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
tar -xzf "$work/kiwi.tgz" -C "$work"
sudo cp -R "$work/include/kiwi" /usr/local/include/
sudo cp -P "$work"/lib/libkiwi* /usr/local/lib/
if [[ "$(uname -s)" == Linux ]]; then
  sudo ldconfig
fi
