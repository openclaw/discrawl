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

const defaultDMLast = 50

func (r *runtime) runDirectMessages(args []string) error {
	fs := flag.NewFlagSet("dms", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	with := fs.String("with", "", "")
	search := fs.String("search", "", "")
	hours := fs.Int("hours", 0, "")
	days := fs.Int("days", 0, "")
	since := fs.String("since", "", "")
	before := fs.String("before", "", "")
	limit := fs.Int("limit", defaultDMLast, "")
	last := fs.Int("last", defaultDMLast, "")
	all := fs.Bool("all", false, "")
	list := fs.Bool("list", false, "")
	includeEmpty := fs.Bool("include-empty", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("dms takes flags only"))
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
	if *all && *last > 0 && flagPassed(fs, "last") {
		return usageErr(errors.New("use either --all or --last"))
	}
	if flagPassed(fs, "limit") && flagPassed(fs, "last") {
		return usageErr(errors.New("use either --limit or --last"))
	}

	if *list || (strings.TrimSpace(*with) == "" && strings.TrimSpace(*search) == "" && noDMMessageTimeFilter(*hours, *days, *since, *before)) {
		rows, err := r.store.DirectMessageConversations(r.ctx, store.DirectMessageConversationOptions{With: *with})
		if err != nil {
			return err
		}
		return r.print(rows)
	}

	sinceTime, beforeTime, err := r.parseMessageWindow(*hours, *days, *since, *before)
	if err != nil {
		return err
	}
	if query := strings.TrimSpace(*search); query != "" {
		opts := store.SearchOptions{
			Query:        query,
			GuildIDs:     []string{store.DirectMessageGuildID},
			Channel:      *with,
			Limit:        *limit,
			IncludeEmpty: *includeEmpty,
		}
		results, err := r.store.SearchMessages(r.ctx, opts)
		if err != nil {
			return err
		}
		if len(results) == 0 {
			r.explainEmptyDirectMessageSearch(*with, opts)
		}
		return r.print(results)
	}

	messageLimit := *limit
	messageLast := *last
	switch {
	case *all:
		messageLimit = 0
		messageLast = 0
	case flagPassed(fs, "limit"):
		messageLast = 0
	default:
		messageLimit = 0
	}
	rows, err := r.store.ListMessages(r.ctx, store.MessageListOptions{
		GuildIDs:     []string{store.DirectMessageGuildID},
		Channel:      *with,
		Since:        sinceTime,
		Before:       beforeTime,
		Limit:        messageLimit,
		Last:         messageLast,
		IncludeEmpty: *includeEmpty,
	})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		r.explainEmptyDirectMessageList(*with, *includeEmpty, zeroResultWindow{
			hours: *hours, days: *days, sinceRaw: *since, beforeRaw: *before,
			since: sinceTime, before: beforeTime,
		})
	}
	return r.print(rows)
}

func (r *runtime) parseMessageWindow(hours, days int, since, before string) (time.Time, time.Time, error) {
	var sinceTime time.Time
	var beforeTime time.Time
	var err error
	if hours > 0 {
		now := time.Now().UTC()
		if r.now != nil {
			now = r.now().UTC()
		}
		sinceTime = now.Add(-time.Duration(hours) * time.Hour)
	}
	if days > 0 {
		now := time.Now().UTC()
		if r.now != nil {
			now = r.now().UTC()
		}
		sinceTime = now.Add(-time.Duration(days) * 24 * time.Hour)
	}
	if strings.TrimSpace(since) != "" {
		sinceTime, err = time.Parse(time.RFC3339, since)
		if err != nil {
			return time.Time{}, time.Time{}, usageErr(fmt.Errorf("invalid --since: %w", err))
		}
	}
	if strings.TrimSpace(before) != "" {
		beforeTime, err = time.Parse(time.RFC3339, before)
		if err != nil {
			return time.Time{}, time.Time{}, usageErr(fmt.Errorf("invalid --before: %w", err))
		}
	}
	return sinceTime, beforeTime, nil
}

func noDMMessageTimeFilter(hours, days int, since, before string) bool {
	return hours == 0 && days == 0 && strings.TrimSpace(since) == "" && strings.TrimSpace(before) == ""
}
