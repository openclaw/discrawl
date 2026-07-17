package scripts_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCodesignReleaseBinarySkipsCredentialFreeBuilds(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("codesign-release-binary.sh is a POSIX shell script")
	}

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not available")
	}

	scriptDir := signingScriptDir(t)
	cmd := exec.CommandContext(t.Context(), bash, "./codesign-release-binary.sh", "darwin_arm64", "/does/not/exist")
	cmd.Dir = scriptDir
	cmd.Env = signingTestEnv("DISCRAWL_CODESIGN_REQUIRED=0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("credential-free signing hook failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "skipping Developer ID signing") {
		t.Fatalf("missing snapshot skip notice: %s", output)
	}
}

func TestCodesignReleaseBinaryFailsClosedForOfficialBuilds(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("codesign-release-binary.sh is a POSIX shell script")
	}

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not available")
	}

	scriptDir := signingScriptDir(t)
	cmd := exec.CommandContext(t.Context(), bash, "./codesign-release-binary.sh", "darwin_arm64", "/does/not/exist")
	cmd.Dir = scriptDir
	cmd.Env = signingTestEnv("DISCRAWL_CODESIGN_REQUIRED=1")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("official signing hook unexpectedly succeeded: %s", output)
	}
	want := "CODESIGN_IDENTITY is required"
	if runtime.GOOS != "darwin" {
		want = "official macOS release signing must run on macOS"
	}
	if !strings.Contains(string(output), want) {
		t.Fatalf("unexpected official signing failure: %s", output)
	}
}

func TestCodesignReleaseBinaryFailsClosedWithoutNotaryProfile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("codesign-release-binary.sh is a POSIX shell script")
	}

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not available")
	}

	tempDir := t.TempDir()
	stubDir := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(stubDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, filepath.Join(stubDir, "uname"), "#!/bin/sh\nprintf 'Darwin\\n'\n")
	binary := filepath.Join(tempDir, "discrawl")
	writeExecutable(t, binary, "#!/bin/sh\n")

	cmd := exec.CommandContext(t.Context(), bash, "./codesign-release-binary.sh", "darwin_arm64", binary)
	cmd.Dir = signingScriptDir(t)
	cmd.Env = signingTestEnv(
		"DISCRAWL_CODESIGN_REQUIRED=1",
		"CODESIGN_IDENTITY=Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)",
		"PATH="+stubDir+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("official signing hook accepted a missing notary profile: %s", output)
	}
	if !strings.Contains(string(output), "NOTARYTOOL_KEYCHAIN_PROFILE is required") {
		t.Fatalf("unexpected missing-profile failure: %s", output)
	}
}

func TestCodesignReleaseBinaryNotarizesBeforeReplacingArtifact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("codesign-release-binary.sh is a POSIX shell script")
	}

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not available")
	}

	for _, tc := range []struct {
		name   string
		target string
		arch   string
	}{
		{name: "arm64", target: "darwin_arm64_v8.0", arch: "arm64"},
		{name: "amd64", target: "darwin_amd64_v1", arch: "x86_64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			stubDir, logPath := writeSigningStubs(t, tempDir, tc.arch)
			binary := filepath.Join(tempDir, "discrawl")
			writeExecutable(t, binary, "#!/bin/sh\nprintf 'scratch\\n'\n")

			cmd := exec.CommandContext(t.Context(), bash, "./codesign-release-binary.sh", tc.target, binary)
			cmd.Dir = signingScriptDir(t)
			cmd.Env = signingTestEnv(
				"DISCRAWL_CODESIGN_REQUIRED=1",
				"CODESIGN_IDENTITY=Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)",
				"NOTARYTOOL_KEYCHAIN_PROFILE=test-profile",
				"SIGNING_TEST_LOG="+logPath,
				"PATH="+stubDir+string(os.PathListSeparator)+os.Getenv("PATH"),
			)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("sign and notarize scratch artifact: %v\n%s", err, output)
			}

			contents, err := os.ReadFile(binary)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(contents), "signed") {
				t.Fatal("successful notarization did not replace the original artifact")
			}
			logBytes, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			log := string(logBytes)
			for _, want := range []string{
				"xcrun notarytool submit",
				"--keychain-profile test-profile --no-s3-acceleration --wait --output-format json",
				"codesign --verify --strict --check-notarization -R=notarized --verbose=2",
			} {
				if !strings.Contains(log, want) {
					t.Fatalf("signing log missing %q:\n%s", want, log)
				}
			}
			leftovers, err := filepath.Glob(filepath.Join(tempDir, ".discrawl-notary.*"))
			if err != nil {
				t.Fatal(err)
			}
			if len(leftovers) != 0 {
				t.Fatalf("notarization scratch directories were not removed: %v", leftovers)
			}
		})
	}
}

func TestCodesignReleaseBinaryPreservesArtifactOnNotaryRejection(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("codesign-release-binary.sh is a POSIX shell script")
	}

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not available")
	}

	tempDir := t.TempDir()
	stubDir, logPath := writeSigningStubs(t, tempDir, "arm64")
	binary := filepath.Join(tempDir, "discrawl")
	original := []byte("#!/bin/sh\nprintf 'scratch\\n'\n")
	if err := os.WriteFile(binary, original, 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(t.Context(), bash, "./codesign-release-binary.sh", "darwin_arm64", binary)
	cmd.Dir = signingScriptDir(t)
	cmd.Env = signingTestEnv(
		"DISCRAWL_CODESIGN_REQUIRED=1",
		"CODESIGN_IDENTITY=Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)",
		"NOTARYTOOL_KEYCHAIN_PROFILE=test-profile",
		"MOCK_NOTARY_STATUS=Invalid",
		"SIGNING_TEST_LOG="+logPath,
		"PATH="+stubDir+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("signing hook accepted rejected notarization: %s", output)
	}
	if !strings.Contains(string(output), "notarization status is Invalid") {
		t.Fatalf("unexpected rejection failure: %s", output)
	}
	contents, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != string(original) {
		t.Fatal("failed notarization mutated the original artifact")
	}
}

func TestReleaseSignedFailsClosedWithoutNotaryProfile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("release-signed.sh is a POSIX shell script")
	}

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not available")
	}

	tempDir := t.TempDir()
	stubDir := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(stubDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, filepath.Join(stubDir, "uname"), "#!/bin/sh\nprintf 'Darwin\\n'\n")

	cmd := exec.CommandContext(t.Context(), bash, "./release-signed.sh", "v0.0.0")
	cmd.Dir = signingScriptDir(t)
	cmd.Env = signingTestEnv("PATH=" + stubDir + string(os.PathListSeparator) + os.Getenv("PATH"))
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("release entrypoint accepted a missing notary profile: %s", output)
	}
	if !strings.Contains(string(output), "NOTARYTOOL_KEYCHAIN_PROFILE is required") {
		t.Fatalf("unexpected release entrypoint failure: %s", output)
	}
}

func TestVerifyMacOSReleaseAcceptsGoReleaserArchive(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("verify-macos-release.sh is a POSIX shell script")
	}

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not available")
	}
	tar, err := exec.LookPath("tar")
	if err != nil {
		t.Skip("tar is not available")
	}
	if _, err := exec.LookPath("shasum"); err != nil {
		t.Skip("shasum is not available")
	}

	tempDir := t.TempDir()
	payloadDir := filepath.Join(tempDir, "payload")
	if err := os.Mkdir(payloadDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"CHANGELOG.md", "LICENSE", "README.md"} {
		if err := os.WriteFile(filepath.Join(payloadDir, name), []byte(name+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeExecutable(t, filepath.Join(payloadDir, "discrawl"), "#!/bin/sh\n[ \"$1\" = --version ]\nprintf '0.11.5\\n'\n")

	archive := filepath.Join(tempDir, "discrawl_0.11.5_darwin_arm64.tar.gz")
	tarCmd := exec.CommandContext(t.Context(), tar, "-czf", archive, "-C", payloadDir, "CHANGELOG.md", "LICENSE", "README.md", "discrawl")
	if output, err := tarCmd.CombinedOutput(); err != nil {
		t.Fatalf("create archive: %v\n%s", err, output)
	}
	archiveBytes, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	checksums := filepath.Join(tempDir, "checksums.txt")
	checksumLine := fmt.Sprintf("%x  %s\n", sha256.Sum256(archiveBytes), filepath.Base(archive))
	if err := os.WriteFile(checksums, []byte(checksumLine), 0o644); err != nil {
		t.Fatal(err)
	}

	stubDir := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(stubDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, filepath.Join(stubDir, "uname"), "#!/bin/sh\nprintf 'Darwin\\n'\n")
	logPath := filepath.Join(tempDir, "codesign.log")
	writeExecutable(t, filepath.Join(stubDir, "codesign"), "#!/bin/sh\nprintf 'codesign %s\\n' \"$*\" >> \"$SIGNING_TEST_LOG\"\nprintf 'Identifier=org.openclaw.discrawl\\nCodeDirectory v=20500 size=100 flags=0x10000(runtime) hashes=1+0 location=embedded\\nTeamIdentifier=FWJYW4S8P8\\nAuthority=Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)\\n' >&2\n")
	writeExecutable(t, filepath.Join(stubDir, "lipo"), "#!/bin/sh\nprintf 'arm64\\n'\n")

	scriptDir := signingScriptDir(t)
	cmd := exec.CommandContext(t.Context(), bash, "./verify-macos-release.sh", "v0.11.5", archive, checksums)
	cmd.Dir = scriptDir
	cmd.Env = append(
		os.Environ(),
		"SIGNING_TEST_LOG="+logPath,
		"PATH="+stubDir+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("verify GoReleaser archive: %v\n%s", err, output)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logBytes), "--verify --strict --check-notarization -R=notarized --verbose=2") {
		t.Fatalf("release verifier skipped notarization ticket check:\n%s", logBytes)
	}
}

func signingScriptDir(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate test file")
	}
	return filepath.Dir(testFile)
}

func signingTestEnv(extra ...string) []string {
	env := make([]string, 0, len(os.Environ())+len(extra))
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "CODESIGN_IDENTITY=") ||
			strings.HasPrefix(entry, "DISCRAWL_CODESIGN_REQUIRED=") ||
			strings.HasPrefix(entry, "NOTARYTOOL_KEYCHAIN_PROFILE=") ||
			strings.HasPrefix(entry, "SIGNING_TEST_LOG=") ||
			strings.HasPrefix(entry, "MOCK_NOTARY_STATUS=") {
			continue
		}
		env = append(env, entry)
	}
	return append(env, extra...)
}

func writeSigningStubs(t *testing.T, tempDir, arch string) (string, string) {
	t.Helper()
	stubDir := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(stubDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tempDir, "signing.log")
	writeExecutable(t, filepath.Join(stubDir, "uname"), "#!/bin/sh\nprintf 'Darwin\\n'\n")
	writeExecutable(t, filepath.Join(stubDir, "codesign"), `#!/usr/bin/env bash
set -euo pipefail
printf 'codesign %s\n' "$*" >> "$SIGNING_TEST_LOG"
if [[ " $* " == *' --sign '* ]]; then
  target=${!#}
  printf '\nsigned\n' >> "$target"
fi
if [[ "${1:-}" == -dvvv ]]; then
  printf 'Identifier=org.openclaw.discrawl\nCodeDirectory v=20500 size=100 flags=0x10000(runtime) hashes=1+0 location=embedded\nTeamIdentifier=FWJYW4S8P8\nAuthority=Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)\n' >&2
fi
`)
	writeExecutable(t, filepath.Join(stubDir, "ditto"), `#!/usr/bin/env bash
set -euo pipefail
args=("$@")
source=${args[${#args[@]}-2]}
destination=${args[${#args[@]}-1]}
cp "$source" "$destination"
printf 'ditto %s\n' "$*" >> "$SIGNING_TEST_LOG"
`)
	writeExecutable(t, filepath.Join(stubDir, "xcrun"), `#!/usr/bin/env bash
set -euo pipefail
printf 'xcrun %s\n' "$*" >> "$SIGNING_TEST_LOG"
printf '{"id":"12345678-1234-1234-1234-123456789abc","status":"%s"}\n' "${MOCK_NOTARY_STATUS:-Accepted}"
`)
	writeExecutable(t, filepath.Join(stubDir, "plutil"), `#!/usr/bin/env bash
set -euo pipefail
case "${2:-}" in
  status) printf '%s\n' "${MOCK_NOTARY_STATUS:-Accepted}" ;;
  id) printf '%s\n' '12345678-1234-1234-1234-123456789abc' ;;
  *) exit 1 ;;
esac
`)
	writeExecutable(t, filepath.Join(stubDir, "lipo"), fmt.Sprintf("#!/bin/sh\nprintf '%s\\n'\n", arch))
	return stubDir, logPath
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatal(err)
	}
}
