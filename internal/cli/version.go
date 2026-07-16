package cli

import (
	"runtime/debug"
	"strings"
)

var version string

func currentVersion() string {
	moduleVersion := ""
	if info, ok := debug.ReadBuildInfo(); ok {
		moduleVersion = info.Main.Version
	}
	return resolveVersion(version, moduleVersion)
}

func resolveVersion(linkerVersion, moduleVersion string) string {
	if linkerVersion = strings.TrimSpace(linkerVersion); linkerVersion != "" {
		return strings.TrimPrefix(linkerVersion, "v")
	}
	if moduleVersion = strings.TrimSpace(moduleVersion); moduleVersion != "" && moduleVersion != "(devel)" {
		return strings.TrimPrefix(moduleVersion, "v")
	}
	return "devel"
}

func discrawlUserAgent() string {
	return "discrawl/" + currentVersion()
}
