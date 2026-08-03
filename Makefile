BINARY ?= bin/discrawl
VERSION ?= dev
ARTIFACT_DIR ?= dist

.DEFAULT_GOAL := help

.PHONY: help build test test-large test-race test-coverage fmt lint tidy-check smoke check snapshot release verify-release release-artifacts release-snapshot generate-sqlc clean

help:
	@printf '%s\n' \
		'Available targets:' \
		'  help              Print available targets (default).' \
		'  build             Build the CLI into $(BINARY).' \
		'  test              Run the full Go test suite.' \
		'  test-large        Run the opt-in catalog-integrity calibration fixture.' \
		'  fmt               Check Go formatting.' \
		'  lint              Run the static-analysis and vulnerability gates.' \
		'  check             Run every local gate enforced by CI.' \
		'  snapshot          Build credential-free release artifacts.' \
		'  release           Refuse local publishing and print the official CI command.' \
		'  verify-release    Verify downloaded release artifacts (VERSION=vX.Y.Z).' \
		'  release-artifacts Alias for release.' \
		'  release-snapshot  Alias for snapshot.' \
		'  generate-sqlc     Regenerate sqlc output.' \
		'  clean             Remove local build output.'

build:
	@binary="$(BINARY)"; mkdir -p "$$(dirname -- "$$binary")"; \
	GOWORK=off go build -ldflags "-X github.com/openclaw/discrawl/internal/cli.version=$(VERSION)" -o "$$binary" ./cmd/discrawl

test:
	GOWORK=off go test -count=1 ./...

test-large:
	@output_file="$$(mktemp)"; trap 'rm -f "$$output_file"' EXIT; \
	DISCRAWL_TEST_LARGE=1 GOWORK=off go test -json -count=1 -run '^TestCatalogIntegrityProbeLargeCompleteFixture$$' -timeout=30m ./internal/store >"$$output_file"; \
	cat "$$output_file"; \
	jq -e 'select(.Action == "pass" and .Test == "TestCatalogIntegrityProbeLargeCompleteFixture")' "$$output_file" >/dev/null

test-race:
	GOWORK=off go test -count=1 -race ./...

test-coverage:
	GOWORK=off go test -count=1 ./... -coverprofile=coverage.out
	@grep -v '^github.com/openclaw/discrawl/internal/store/storedb/' coverage.out > coverage.filtered.out
	@total="$$(go tool cover -func=coverage.filtered.out | awk '/^total:/ { sub(/%$$/, "", $$3); print $$3 }')"; \
	awk -v total="$$total" 'BEGIN { if (total == "" || total + 0 < 85.0) { printf("coverage %s%% is below 85%%\n", total == "" ? "missing" : total); exit 1 } printf("coverage %.1f%%\n", total + 0) }'

fmt:
	@changed="$$(GOWORK=off go run mvdan.cc/gofumpt@v0.10.0 -l .)"; \
	if [ -n "$$changed" ]; then printf 'gofumpt wants changes in:\n%s\n' "$$changed"; exit 1; fi

lint:
	GOWORK=off go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 run
	GOWORK=off go vet ./...
	GOWORK=off go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...
	GOWORK=off go run github.com/securego/gosec/v2/cmd/gosec@v2.27.1 -exclude=G101,G115,G202,G301,G304 ./...
	GOWORK=off go run golang.org/x/vuln/cmd/govulncheck@v1.5.0 ./...
	@output_file="$$(mktemp)"; trap 'rm -f "$$output_file"' EXIT; \
	GOWORK=off go run golang.org/x/tools/cmd/deadcode@v0.47.0 -test ./... >"$$output_file"; \
	if [ -s "$$output_file" ]; then cat "$$output_file"; exit 1; fi

tidy-check:
	GOWORK=off go mod verify
	GOWORK=off go mod tidy
	git diff --exit-code -- go.mod go.sum

smoke: build
	@test -n "$$($(BINARY) --version)"
	@$(BINARY) metadata --json | grep -q '"schema_version"'
	@$(BINARY) help tui | grep -q 'Usage: discrawl tui'

check: tidy-check fmt lint test-coverage test-race smoke snapshot

snapshot:
	GOWORK=off goreleaser release --snapshot --clean --skip=publish

release:
	@./scripts/release-signed.sh

verify-release:
	@test -n "$(VERSION)" && [ "$(VERSION)" != dev ] || (echo "usage: make verify-release VERSION=vX.Y.Z [ARTIFACT_DIR=dist]" >&2; exit 2)
	@set -e; version="$(VERSION)"; release_version="$${version#v}"; \
	for arch in amd64 arm64; do \
		./scripts/verify-macos-release.sh "$$version" \
			"$(ARTIFACT_DIR)/discrawl_$${release_version}_darwin_$${arch}.tar.gz" \
			"$(ARTIFACT_DIR)/checksums.txt"; \
	done

release-artifacts: release

release-snapshot: snapshot

generate-sqlc:
	./scripts/generate-sqlc.sh

clean:
	rm -rf -- bin dist coverage.out coverage.filtered.out
