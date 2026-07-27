---
summary: "Official Discrawl releases through the shared GitHub Actions pipeline"
---

# Releasing `discrawl`

`.github/workflows/release-unified.yml` is the only official release path. It calls `openclaw/release-workflows@v1` from protected `main`, requires the existing SSH-signed version tag, builds the exact GoReleaser matrix, signs and notarizes both thin macOS binaries as `org.openclaw.discrawl`, verifies the complete asset inventory independently on arm64 and Intel macOS, publishes `checksums.txt`, and waits for the `openclaw/homebrew-tap` handoff to succeed.

The public compatibility contract remains:

- `discrawl_VERSION_{darwin,linux}_{amd64,arm64}.tar.gz`
- `discrawl_VERSION_windows_{amd64,arm64}.zip`
- `checksums.txt`
- `CHANGELOG.md`, `LICENSE`, `README.md`, and `discrawl` inside every platform archive
- OpenClaw Foundation Team ID `FWJYW4S8P8` and code identifier `org.openclaw.discrawl`

The shared pipeline also publishes verifier control assets (`ASSET-INVENTORY.json`, `SIGNING-MANIFEST.json`, and `RELEASE-NOTES.md`).

## Release

Prepare a dated changelog section and land it on protected `main`. The signing key configured by `user.signingkey` must be the SSH key listed for your principal in `.github/release-allowed-signers`. Create and push an annotated SSH-signed tag whose commit is reachable from `main`, verify it explicitly against the repository allowlist, then dispatch the workflow:

```sh
git -c gpg.format=ssh tag -s vX.Y.Z -m "Release X.Y.Z"
git -c gpg.format=ssh -c gpg.ssh.allowedSignersFile=.github/release-allowed-signers tag -v vX.Y.Z
git push origin vX.Y.Z
gh workflow run release-unified.yml --repo openclaw/discrawl -f version=X.Y.Z
```

Watch the exact run through publication and Homebrew handoff. The release is complete only when the GitHub Release contains the full asset set, both native macOS verification jobs pass, and the tap's `update-formula.yml` run is green.

## Local diagnostics

Local publishing is disabled. `make release` and the compatibility alias `make release-artifacts` refuse and print the official workflow command.

Use these credential-free or read-only diagnostics:

```sh
make check
make snapshot
make verify-release VERSION=vX.Y.Z ARTIFACT_DIR=/path/to/downloaded-assets
```

`make snapshot` builds without release credentials. `make verify-release` rechecks downloaded Darwin archives against `checksums.txt`, the stable Foundation designated requirement, hardened runtime, architecture, embedded version, and Apple's online notarization ticket.
