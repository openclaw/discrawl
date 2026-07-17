---
summary: "Release checklist for discrawl (GitHub release binaries via GoReleaser + Homebrew tap update)"
---

# Releasing `discrawl`

Always do all steps below. No partial releases.

Assumptions:
- Repo: `openclaw/discrawl`
- Binary: `discrawl`
- GoReleaser config: `.goreleaser.yaml`
- Homebrew tap repo: `~/Projects/homebrew-tap`
- Official releases run from macOS through the shared managed-keychain helper

## 0) Prereqs

- Clean working tree on `main`
- Go toolchain from `go.mod`
- GitHub CLI authenticated
- CI green on `main`
- OpenClaw Foundation Developer ID Application identity available through the managed release keychain
- A `NOTARYTOOL_KEYCHAIN_PROFILE` runtime value whose credentials are stored in the operator's login keychain

## 1) Verify build + tests

```sh
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 run
go test -count=1 ./... -coverprofile=coverage.out
go tool cover -func=coverage.out | tail -n 1
go test -count=1 -race ./...
go build -o /tmp/discrawl ./cmd/discrawl
gh run list -L 5 --branch main
```

Coverage floor: `85%+`

## 2) Finalize changelog and release notes

Replace the current `Unreleased` heading in `CHANGELOG.md` with the release
version and date. Use that exact section as the GitHub Release body and append a
link to the full changelog at the tagged commit.

Example:

- `## 0.2.0 - 2026-03-08`

## 3) Commit, tag, push

```sh
git checkout main
git pull --ff-only origin main
git commit -am "chore(release): vX.Y.Z"
git tag -s vX.Y.Z -m "Release X.Y.Z"
git tag -v vX.Y.Z
git push origin main
git push origin vX.Y.Z
```

## 4) Publish signed and notarized release artifacts

From the clean checkout whose `HEAD` exactly matches the signed tag, publish all
GoReleaser artifacts through the shared secret-safe keychain helper:

```sh
./scripts/release-signed.sh vX.Y.Z
```

The script uses an existing GitHub token environment variable or the
authenticated GitHub CLI without writing credentials to disk. Set
`NOTARYTOOL_KEYCHAIN_PROFILE` only in the private release environment; never
commit a real profile name or its credentials.

The GoReleaser hook signs and notarizes both thin macOS binaries. Each binary is
signed into a temporary candidate with hardened runtime and a trusted timestamp,
submitted to Apple as an ephemeral ZIP with `notarytool --wait`, and accepted
only after the response reports `Accepted` and an online ticket check passes.
Failed signing or notarization leaves the GoReleaser output untouched. Snapshot
builds and all non-macOS targets remain credential-free. Production builds fail
closed unless the notary profile is set and the macOS artifacts use identifier
`org.openclaw.discrawl` and OpenClaw Foundation Team ID `FWJYW4S8P8`.

Publishing the GitHub Release triggers `.github/workflows/release.yml`, which
downloads both macOS archives on native runners, verifies their checksums and
Developer ID signatures, hardened-runtime metadata, stable requirement, and
Apple notarization tickets, then dispatches the Homebrew update. Raw CLI
binaries cannot carry stapled tickets, so this verification requires network
access to Apple's ticket service.

## 5) Verify GitHub Release

```sh
gh run list -L 5 --workflow release.yml
gh release view vX.Y.Z --json url,body,assets
```

Before closeout, confirm the Release body contains the exact `X.Y.Z`
changelog section and a full-changelog link. If it is missing or stale, prepare
the corrected body in a reviewed file and run:

```sh
gh release edit vX.Y.Z --notes-file /tmp/discrawl-release-notes.md
```

Confirm checksums plus assets exist for:

- `darwin_amd64`
- `darwin_arm64`
- `linux_amd64`
- `linux_arm64`
- `windows_amd64`
- `windows_arm64`

## 6) Verify Homebrew tap update

`discrawl` ships a binary formula in `~/Projects/homebrew-tap/Formula/discrawl.rb` that points at the GitHub release archives.

The release workflow dispatches `openclaw/homebrew-tap`'s
`update-formula.yml` and waits for it to finish. Verify the formula version and
per-platform checksums landed, then test the installed binary. Do not manually
duplicate a successful automated update.

Useful commands:

```sh
curl -L -o /tmp/discrawl-darwin-arm64.tgz https://github.com/openclaw/discrawl/releases/download/vX.Y.Z/discrawl_X.Y.Z_darwin_arm64.tar.gz
shasum -a 256 /tmp/discrawl-darwin-arm64.tgz
brew uninstall discrawl || true
brew install openclaw/tap/discrawl
discrawl --version
brew info openclaw/tap/discrawl
```

## 7) Close out the release

Only after the GitHub Release, release notes, assets, and Homebrew formula are
verified, add the next patch section at the top of `CHANGELOG.md`. For example,
after `0.11.4`:

- `## 0.11.5 - Unreleased`

Commit that closeout with a conventional `chore:` message and push `main`.

## Notes

- Build-time version stamping comes from `-X github.com/openclaw/discrawl/internal/cli.version={{ .Version }}`
- If release workflow needs a rerun:

```sh
gh workflow run release.yml -f tag=vX.Y.Z
```
