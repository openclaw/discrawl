package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

const defaultMessageLimit = 200

func (r *runtime) runMessages(args []string) error {
	fs := flag.NewFlagSet("messages", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	channel := fs.String("channel", "", "")
	author := fs.String("author", "", "")
	hours := fs.Int("hours", 0, "")
	days := fs.Int("days", 0, "")
	since := fs.String("since", "", "")
	before := fs.String("before", "", "")
	limit := fs.Int("limit", defaultMessageLimit, "")
	last := fs.Int("last", 0, "")
	all := fs.Bool("all", false, "")
	syncNow := fs.Bool("sync", false, "")
	includeEmpty := fs.Bool("include-empty", false, "")
	dm := fs.Bool("dm", false, "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("messages takes flags only"))
	}
	if *hours < 0 {
		return usageErr(errors.New("--hours must be >= 0"))
	}
	if *days < 0 {
		return usageErr(errors.New("--days must be >= 0"))
	}
	if countNonZero(*hours > 0, *days > 0, strings.TrimSpace(*since) != "") > 1 {
		return usageErr(errors.New("use only one of --hours, --days, or --since"))
	}
	if *limit < 0 {
		return usageErr(errors.New("--limit must be >= 0"))
	}
	if *last < 0 {
		return usageErr(errors.New("--last must be >= 0"))
	}
	limitSet := flagPassed(fs, "limit")
	if *all && *last > 0 {
		return usageErr(errors.New("use either --all or --last"))
	}
	if limitSet && *last > 0 {
		return usageErr(errors.New("use either --limit or --last"))
	}
	if *last > 0 {
		*limit = 0
	}

	var sinceTime time.Time
	var beforeTime time.Time
	var err error
	if *hours > 0 {
		now := time.Now().UTC()
		if r.now != nil {
			now = r.now().UTC()
		}
		sinceTime = now.Add(-time.Duration(*hours) * time.Hour)
	}
	if *days > 0 {
		now := time.Now().UTC()
		if r.now != nil {
			now = r.now().UTC()
		}
		sinceTime = now.Add(-time.Duration(*days) * 24 * time.Hour)
	}
	if strings.TrimSpace(*since) != "" {
		sinceTime, err = time.Parse(time.RFC3339, *since)
		if err != nil {
			return usageErr(fmt.Errorf("invalid --since: %w", err))
		}
	}
	if strings.TrimSpace(*before) != "" {
		beforeTime, err = time.Parse(time.RFC3339, *before)
		if err != nil {
			return usageErr(fmt.Errorf("invalid --before: %w", err))
		}
	}

	guildIDs, err := directMessageGuildScope(*dm, *guildFlag, *guildsFlag)
	if err != nil {
		return usageErr(err)
	}
	if *dm && *syncNow {
		return usageErr(errors.New("messages --sync is not supported with --dm; run wiretap or sync --source wiretap first"))
	}
	if strings.TrimSpace(*channel) == "" && strings.TrimSpace(*author) == "" && sinceTime.IsZero() && beforeTime.IsZero() && len(guildIDs) == 0 {
		return usageErr(errors.New("messages needs at least one filter"))
	}
	if *all {
		*limit = 0
	}
	if *syncNow {
		if err := r.syncMessagesQuery(*channel, *guildFlag, *guildsFlag); err != nil {
			return err
		}
	}

	opts := store.MessageListOptions{
		GuildIDs:     guildIDs,
		Channel:      *channel,
		Author:       *author,
		Since:        sinceTime,
		Before:       beforeTime,
		Limit:        *limit,
		Last:         *last,
		IncludeEmpty: *includeEmpty,
	}
	if r.cfg.RemoteCloudReadOnly() {
		rows, err := r.runRemoteMessages(opts, *dm)
		if err != nil {
			return err
		}
		return r.print(rows)
	}
	if strings.TrimSpace(opts.Channel) != "" {
		channelQuery := normalizeChannelQuery(opts.Channel)
		if isDiscordID(channelQuery) {
			opts.Channel = channelQuery
		} else {
			resolution, err := r.resolveLocalChannel(channelQuery, guildIDs)
			if err != nil {
				if resolution.Status != "not_found" || !*syncNow {
					return err
				}
			} else {
				opts.Channel = resolution.Selected.ChannelID
			}
		}
	}
	rows, err := r.store.ListMessages(r.ctx, opts)
	if err != nil {
		return err
	}
	return r.print(rows)
}

func countNonZero(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}
