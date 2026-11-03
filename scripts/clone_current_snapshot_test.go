package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCloneCurrentSnapshot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("snapshot helper requires a POSIX shell")
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is unavailable")
	}
	script, err := filepath.Abs("clone-current-snapshot.sh")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	env := []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + root,
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_AUTHOR_NAME=Snapshot Test", "GIT_COMMITTER_NAME=Snapshot Test",
		"GIT_AUTHOR_EMAIL=snapshot@example.invalid", "GIT_COMMITTER_EMAIL=snapshot@example.invalid",
	}
	run := func(dir, command string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), command, args...)
		cmd.Dir, cmd.Env = dir, env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", command, args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	source := filepath.Join(root, "source with spaces")
	run(root, "git", "init", "--initial-branch=main", source)
	commit := func(repo, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(repo, "snapshot.txt"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		run(repo, "git", "add", "snapshot.txt")
		run(repo, "git", "-c", "commit.gpgsign=false", "commit", "-m", "snapshot")
	}
	commit(source, "old data")
	old := run(source, "git", "rev-parse", "HEAD")
	run(source, "git", "tag", "old-snapshot")
	commit(source, "current data")
	run(source, "git", "branch", "other", old)
	want := "current data"
	for round, change := range []bool{false, true, false, true, false} {
		if change {
			want = fmt.Sprintf("updated data %d", round)
			commit(source, want)
		}
		head := run(source, "git", "rev-parse", "HEAD")
		tree := run(source, "git", "rev-parse", "HEAD^{tree}")
		destination := filepath.Join(root, fmt.Sprintf("snapshot %d", round))
		run(root, bash, script, source, destination, "main")
		for _, check := range []struct{ revision, want string }{{"HEAD", head}, {"HEAD^{tree}", tree}} {
			if got := run(destination, "git", "rev-parse", check.revision); got != check.want {
				t.Fatalf("%s changed: %s != %s", check.revision, got, check.want)
			}
		}
		if got := run(destination, "git", "rev-list", "--count", "--all"); got != "1" {
			t.Fatalf("round %d retained %s commits", round, got)
		}
		if tags := run(destination, "git", "tag", "--list"); tags != "" {
			t.Fatalf("retained tags: %s", tags)
		}
		if _, err := os.Lstat(filepath.Join(destination, ".git", "objects", "info", "alternates")); !os.IsNotExist(err) {
			t.Fatalf("unexpected alternate object store: %v", err)
		}
		if err := os.RemoveAll(source); err != nil {
			t.Fatal(err)
		}
		run(destination, "git", "fsck", "--full", "--strict")
		data, err := os.ReadFile(filepath.Join(destination, "snapshot.txt"))
		if err != nil || string(data) != want {
			t.Fatalf("payload mismatch: %q, %v", data, err)
		}
		source = destination
	}
	for _, name := range []string{"empty-directory", "populated-directory", "dangling-symlink"} {
		t.Run(name, func(t *testing.T) {
			destination := filepath.Join(root, name)
			if name == "dangling-symlink" {
				err = os.Symlink(filepath.Join(root, "missing"), destination)
			} else {
				err = os.Mkdir(destination, 0o700)
				if err == nil && name == "populated-directory" {
					err = os.WriteFile(filepath.Join(destination, "keep"), []byte("keep"), 0o600)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), bash, script, source, destination, "main")
			cmd.Env = env
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "destination must not exist") {
				t.Fatalf("existing destination accepted: %v, %s", err, out)
			}
			if _, err := os.Lstat(destination); err != nil {
				t.Fatalf("existing destination removed: %v", err)
			}
		})
	}
}
