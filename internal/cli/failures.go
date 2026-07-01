package cli

import (
	"errors"
	"flag"
	"io"
	"strings"

	"github.com/openclaw/discrawl/internal/store"
)

func (r *runtime) runFailures(args []string) error {
	fs := flag.NewFlagSet("failures", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	includeResolved := fs.Bool("all", false, "")
	source := fs.String("source", "", "")
	guildID := fs.String("guild", "", "")
	channelID := fs.String("channel", "", "")
	limit := fs.Int("limit", 100, "")
	jsonOut := fs.Bool("json", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("failures takes flags only"))
	}
	if *limit <= 0 {
		return usageErr(errors.New("failure limit must be positive"))
	}
	if *jsonOut {
		r.json = true
	}
	if r.store == nil {
		return dbErr(errors.New("failures requires a local SQLite archive"))
	}
	report, err := r.store.ListFailures(r.ctx, store.FailureListOptions{
		IncludeResolved: *includeResolved,
		Source:          strings.TrimSpace(*source),
		GuildID:         strings.TrimSpace(*guildID),
		ChannelID:       strings.TrimSpace(*channelID),
		Limit:           *limit,
	}, r.nowUTC())
	if err != nil {
		return err
	}
	return r.print(report)
}
