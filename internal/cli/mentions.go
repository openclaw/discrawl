package cli

import (
	"errors"
	"flag"
	"io"
	"strings"

	"github.com/openclaw/discrawl/internal/store"
)

func (r *runtime) runMentions(args []string) error {
	fs := flag.NewFlagSet("mentions", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	channel := fs.String("channel", "", "")
	author := fs.String("author", "", "")
	target := fs.String("target", "", "")
	targetType := fs.String("type", "", "")
	days := fs.Int("days", 0, "")
	since := fs.String("since", "", "")
	before := fs.String("before", "", "")
	limit := fs.Int("limit", defaultMessageLimit, "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("mentions takes flags only"))
	}
	if *days < 0 {
		return usageErr(errors.New("--days must be >= 0"))
	}
	if *days > 0 && strings.TrimSpace(*since) != "" {
		return usageErr(errors.New("use either --days or --since"))
	}
	if *limit < 0 {
		return usageErr(errors.New("--limit must be >= 0"))
	}
	if targetTypeValue := strings.TrimSpace(*targetType); targetTypeValue != "" && targetTypeValue != "user" && targetTypeValue != "role" {
		return usageErr(errors.New("--type must be user or role"))
	}

	sinceTime, beforeTime, err := r.parseMessageWindow(0, *days, *since, *before)
	if err != nil {
		return err
	}

	guildIDs := r.resolveSearchGuilds(*guildFlag, *guildsFlag)
	if strings.TrimSpace(*channel) == "" &&
		strings.TrimSpace(*author) == "" &&
		strings.TrimSpace(*target) == "" &&
		strings.TrimSpace(*targetType) == "" &&
		sinceTime.IsZero() &&
		beforeTime.IsZero() &&
		len(guildIDs) == 0 {
		return usageErr(errors.New("mentions needs at least one filter"))
	}

	rows, err := r.store.ListMentions(r.ctx, store.MentionListOptions{
		GuildIDs:   guildIDs,
		Channel:    *channel,
		Author:     *author,
		Target:     *target,
		TargetType: *targetType,
		Since:      sinceTime,
		Before:     beforeTime,
		Limit:      *limit,
	})
	if err != nil {
		return err
	}
	return r.print(rows)
}
