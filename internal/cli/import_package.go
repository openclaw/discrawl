package cli

import (
	"errors"
	"flag"
	"io"

	"github.com/openclaw/discrawl/internal/discordpackage"
)

func (r *runtime) runImportPackage(args []string) error {
	fs := flag.NewFlagSet("import-package", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("path", "", "")
	account := fs.String("account-id", "", "")
	dryRun := fs.Bool("dry-run", false, "")
	jsonOut := fs.Bool("json", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 || *path == "" {
		return usageErr(errors.New("import-package requires --path to a ZIP or extracted package directory"))
	}
	if *jsonOut {
		r.json = true
	}
	run := func() error {
		stats, importErr := discordpackage.Import(r.ctx, r.store, discordpackage.Options{Path: *path, AccountID: *account, DryRun: *dryRun})
		// A later channel may fail after earlier channels committed. Report the
		// actual progress as well as the failure; rerunning is idempotent.
		return errors.Join(importErr, r.print(stats))
	}
	if *dryRun {
		return run()
	}
	return r.withLocalStoreLocked(false, run)
}
