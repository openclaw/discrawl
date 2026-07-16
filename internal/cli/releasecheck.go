package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"runtime/debug"
	"strings"

	"github.com/openclaw/crawlkit/releasecheck"
	"github.com/openclaw/discrawl/internal/config"
)

var releaseModulePath = func() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Path == "" || info.Main.Path == "command-line-arguments" {
		return ""
	}
	return info.Main.Path
}

func discrawlReleaseCheckOptions(force bool) releasecheck.Options {
	cfg := config.Default()
	owner, repo, _ := githubOwnerRepo(releaseModulePath())
	return releasecheck.Options{
		AppName:        "discrawl",
		Owner:          owner,
		Repo:           repo,
		CurrentVersion: currentVersion(),
		CacheDir:       cfg.CacheDir,
		Force:          force,
	}
}

func githubOwnerRepo(modulePath string) (string, string, bool) {
	parts := strings.Split(strings.TrimSpace(modulePath), "/")
	if len(parts) < 3 || parts[0] != "github.com" || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func (r *runtime) maybeNotifyRelease(args []string) {
	_, _ = releasecheck.Notify(r.ctx, releasecheck.NotifyOptions{
		Options:    discrawlReleaseCheckOptions(false),
		Stderr:     r.stderr,
		Args:       args,
		JSONOutput: r.json,
		IsTerminal: releasecheck.StderrIsTerminal(),
	})
}

func (r *runtime) runCheckUpdate(args []string) error {
	fs := flag.NewFlagSet("check-update", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "")
	force := fs.Bool("force", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("check-update takes flags only"))
	}
	if *jsonOut {
		r.json = true
	}
	result, err := releasecheck.Check(r.ctx, discrawlReleaseCheckOptions(*force))
	if err != nil && !errors.Is(err, releasecheck.ErrSkipped) {
		return err
	}
	if r.json {
		return r.print(result)
	}
	_, err = fmt.Fprint(r.stdout, releasecheck.StatusText("discrawl", "", result))
	return err
}
