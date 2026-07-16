package cli

import "testing"

func TestResolveVersion(t *testing.T) {
	for _, tc := range []struct {
		name          string
		linkerVersion string
		moduleVersion string
		want          string
	}{
		{name: "release linker override", linkerVersion: "0.12.0", moduleVersion: "v0.11.5", want: "0.12.0"},
		{name: "release linker override with prefix", linkerVersion: " v0.12.0 ", moduleVersion: "v0.11.5", want: "0.12.0"},
		{name: "installed tagged module", moduleVersion: "v0.11.5", want: "0.11.5"},
		{name: "installed pseudo-version", moduleVersion: "v0.11.6-0.20260716120000-deadbeefcafe", want: "0.11.6-0.20260716120000-deadbeefcafe"},
		{name: "local build", moduleVersion: "(devel)", want: "devel"},
		{name: "missing build info", want: "devel"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveVersion(tc.linkerVersion, tc.moduleVersion); got != tc.want {
				t.Fatalf("resolveVersion(%q, %q) = %q, want %q", tc.linkerVersion, tc.moduleVersion, got, tc.want)
			}
		})
	}
}

func TestDiscrawlUserAgentUsesCurrentVersion(t *testing.T) {
	want := "discrawl/" + currentVersion()
	if got := discrawlUserAgent(); got != want {
		t.Fatalf("discrawlUserAgent() = %q, want %q", got, want)
	}
}
