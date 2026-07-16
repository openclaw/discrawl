package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestGithubOwnerRepo(t *testing.T) {
	tests := []struct {
		name       string
		modulePath string
		owner      string
		repo       string
		ok         bool
	}{
		{
			name:       "github module",
			modulePath: "github.com/example/discrawl",
			owner:      "example",
			repo:       "discrawl",
			ok:         true,
		},
		{
			name:       "github module subpackage",
			modulePath: "github.com/example/discrawl/internal/cli",
			owner:      "example",
			repo:       "discrawl",
			ok:         true,
		},
		{
			name:       "non github module",
			modulePath: "code.example.com/example/discrawl",
		},
		{
			name: "empty module",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, repo, ok := githubOwnerRepo(tt.modulePath)
			if owner != tt.owner || repo != tt.repo || ok != tt.ok {
				t.Fatalf("githubOwnerRepo(%q) = %q, %q, %v; want %q, %q, %v", tt.modulePath, owner, repo, ok, tt.owner, tt.repo, tt.ok)
			}
		})
	}
}

func TestDiscrawlReleaseCheckOptionsUsesModulePath(t *testing.T) {
	oldReleaseModulePath := releaseModulePath
	t.Cleanup(func() { releaseModulePath = oldReleaseModulePath })
	releaseModulePath = func() string {
		return "github.com/example/discrawl"
	}

	opts := discrawlReleaseCheckOptions(true)
	if opts.Owner != "example" || opts.Repo != "discrawl" {
		t.Fatalf("owner/repo = %q/%q", opts.Owner, opts.Repo)
	}
	if !opts.Force {
		t.Fatal("force = false")
	}
	if opts.AppName != "discrawl" || opts.CurrentVersion == "" || opts.CacheDir == "" {
		t.Fatalf("incomplete options = %#v", opts)
	}
	if opts.CurrentVersion != currentVersion() {
		t.Fatalf("CurrentVersion = %q, want %q", opts.CurrentVersion, currentVersion())
	}
}

func TestRunCheckUpdateRejectsArgsBeforeNetwork(t *testing.T) {
	r := &runtime{ctx: context.Background(), stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}}
	err := r.runCheckUpdate([]string{"extra"})
	if err == nil || !strings.Contains(err.Error(), "takes flags only") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunCheckUpdateJSONSkipped(t *testing.T) {
	original := releaseModulePath
	releaseModulePath = func() string { return "" }
	t.Cleanup(func() { releaseModulePath = original })

	var out bytes.Buffer
	r := &runtime{ctx: context.Background(), stdout: &out, stderr: &bytes.Buffer{}}
	if err := r.runCheckUpdate([]string{"--json"}); err != nil {
		t.Fatalf("check update: %v", err)
	}
	if !strings.Contains(out.String(), `"skipped": true`) {
		t.Fatalf("output = %s", out.String())
	}
}
