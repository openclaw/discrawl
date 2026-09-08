package cli

import (
	"errors"
	"flag"
	"io"

	"github.com/openclaw/discrawl/internal/report"
	"github.com/openclaw/discrawl/internal/share"
)

func (r *runtime) runReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	readmePath := fs.String("readme", "", "")
	published := fs.Bool("published", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("report takes no positional arguments"))
	}
	filter := share.FilterOptions{
		PublicOnly:        r.cfg.Share.Filter.PublicOnly,
		IncludeChannelIDs: r.cfg.Share.Filter.IncludeChannelIDs,
		ExcludeChannelIDs: r.cfg.Share.Filter.ExcludeChannelIDs,
	}
	if *published && filter.Active() {
		return usageErr(errors.New("report --published is not supported with share filters; filtered report stats would otherwise leak the full archive"))
	}
	activity, err := report.Build(r.ctx, r.store, report.Options{Published: *published})
	if err != nil {
		return err
	}
	section, err := report.RenderMarkdown(activity)
	if err != nil {
		return err
	}
	if *readmePath != "" {
		if err := report.WriteReadme(*readmePath, section); err != nil {
			return err
		}
		return r.print(map[string]any{
			"readme":            *readmePath,
			"generated_at":      activity.GeneratedAt,
			"latest_message_at": activity.LatestMessageAt,
		})
	}
	_, err = io.WriteString(r.stdout, section)
	return err
}
