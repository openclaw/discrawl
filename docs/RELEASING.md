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

## 0) Prereqs

- Clean working tree on `main`
- Go toolchain from `go.mod`
- GitHub CLI authenticated
- CI green on `main`

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
git tag -a vX.Y.Z -m "Release X.Y.Z"
git push origin main --tags
```

## 4) Verify GitHub Release

The tag push triggers `.github/workflows/release.yml`.

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

## 5) Verify Homebrew tap update

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

## 6) Close out the release

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
