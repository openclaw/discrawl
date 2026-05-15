package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/media"
	"github.com/openclaw/discrawl/internal/store"
)

func (r *runtime) runAttachments(args []string) error {
	if len(args) > 0 && args[0] == "fetch" {
		return r.runAttachmentsFetch(args[1:])
	}
	opts, limit, err := r.parseAttachmentListFlags("attachments", args, defaultMessageLimit)
	if err != nil {
		return err
	}
	missingOnly := opts.MissingOnly
	if missingOnly {
		opts.MissingOnly = false
		opts.Limit = 0
	} else {
		opts.Limit = limit
	}
	rows, err := r.store.ListAttachments(r.ctx, opts)
	if err != nil {
		return err
	}
	if missingOnly {
		cacheDir, err := config.ExpandPath(r.cfg.CacheDir)
		if err != nil {
			return configErr(err)
		}
		rows = filterMissingAttachmentMedia(cacheDir, rows)
		if limit > 0 && len(rows) > limit {
			rows = rows[:limit]
		}
	}
	return r.print(rows)
}

func (r *runtime) runAttachmentsFetch(args []string) error {
	listArgs := stripFlags(args, map[string]struct{}{"force": {}, "max-bytes": {}})
	opts, limit, err := r.parseAttachmentListFlags("attachments fetch", listArgs, defaultMessageLimit)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("attachments fetch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	force := fs.Bool("force", false, "")
	maxBytes := fs.Int64("max-bytes", r.cfg.Sync.MaxAttachmentBytes, "")
	if err := parseKnown(fs, args, attachmentListFlagNames()); err != nil {
		return usageErr(err)
	}
	if *maxBytes <= 0 {
		return usageErr(errors.New("--max-bytes must be positive"))
	}
	cacheDir, err := config.ExpandPath(r.cfg.CacheDir)
	if err != nil {
		return configErr(err)
	}
	opts.Limit = limit
	stats, err := media.Fetch(r.ctx, r.store, media.FetchOptions{
		CacheDir:     cacheDir,
		List:         opts,
		MaxBytes:     *maxBytes,
		Force:        *force,
		StatusUpdate: true,
		Now:          r.now,
	})
	if err != nil {
		return err
	}
	return r.print(stats)
}

func filterMissingAttachmentMedia(cacheDir string, rows []store.AttachmentRow) []store.AttachmentRow {
	out := rows[:0]
	for _, row := range rows {
		if row.MediaPath == "" {
			out = append(out, row)
			continue
		}
		path, err := media.LocalPath(cacheDir, row.MediaPath)
		if err != nil {
			out = append(out, row)
			continue
		}
		if _, err := os.Stat(path); err != nil {
			out = append(out, row)
		}
	}
	return out
}

func (r *runtime) parseAttachmentListFlags(name string, args []string, defaultLimit int) (store.AttachmentListOptions, int, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	channel := fs.String("channel", "", "")
	author := fs.String("author", "", "")
	messageID := fs.String("message", "", "")
	filename := fs.String("filename", "", "")
	contentType := fs.String("type", "", "")
	hours := fs.Int("hours", 0, "")
	days := fs.Int("days", 0, "")
	since := fs.String("since", "", "")
	before := fs.String("before", "", "")
	limit := fs.Int("limit", defaultLimit, "")
	all := fs.Bool("all", false, "")
	missing := fs.Bool("missing", false, "")
	dm := fs.Bool("dm", false, "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return store.AttachmentListOptions{}, 0, usageErr(err)
	}
	if fs.NArg() != 0 {
		return store.AttachmentListOptions{}, 0, usageErr(errors.New(name + " takes flags only"))
	}
	if *hours < 0 || *days < 0 || *limit < 0 {
		return store.AttachmentListOptions{}, 0, usageErr(errors.New("--hours, --days, and --limit must be >= 0"))
	}
	if countNonZero(*hours > 0, *days > 0, strings.TrimSpace(*since) != "") > 1 {
		return store.AttachmentListOptions{}, 0, usageErr(errors.New("use only one of --hours, --days, or --since"))
	}
	var sinceTime time.Time
	var beforeTime time.Time
	var err error
	if *hours > 0 {
		sinceTime = r.nowUTC().Add(-time.Duration(*hours) * time.Hour)
	}
	if *days > 0 {
		sinceTime = r.nowUTC().Add(-time.Duration(*days) * 24 * time.Hour)
	}
	if strings.TrimSpace(*since) != "" {
		sinceTime, err = time.Parse(time.RFC3339, *since)
		if err != nil {
			return store.AttachmentListOptions{}, 0, usageErr(fmt.Errorf("invalid --since: %w", err))
		}
	}
	if strings.TrimSpace(*before) != "" {
		beforeTime, err = time.Parse(time.RFC3339, *before)
		if err != nil {
			return store.AttachmentListOptions{}, 0, usageErr(fmt.Errorf("invalid --before: %w", err))
		}
	}
	guildIDs, err := directMessageGuildScope(*dm, *guildFlag, *guildsFlag)
	if err != nil {
		return store.AttachmentListOptions{}, 0, usageErr(err)
	}
	if *all {
		*limit = 0
	}
	return store.AttachmentListOptions{
		GuildIDs:    guildIDs,
		Channel:     *channel,
		Author:      *author,
		MessageID:   *messageID,
		Filename:    *filename,
		ContentType: *contentType,
		Since:       sinceTime,
		Before:      beforeTime,
		MissingOnly: *missing,
	}, *limit, nil
}

func attachmentListFlagNames() map[string]struct{} {
	return map[string]struct{}{
		"channel": {}, "author": {}, "message": {}, "filename": {}, "type": {},
		"hours": {}, "days": {}, "since": {}, "before": {}, "limit": {},
		"all": {}, "missing": {}, "dm": {}, "guilds": {}, "guild": {},
	}
}

func parseKnown(fs *flag.FlagSet, args []string, known map[string]struct{}) error {
	filtered := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			filtered = append(filtered, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if key, _, ok := strings.Cut(name, "="); ok {
			name = key
		}
		if _, skip := known[name]; skip {
			if !strings.Contains(arg, "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
			}
			continue
		}
		filtered = append(filtered, arg)
	}
	return fs.Parse(filtered)
}

func stripFlags(args []string, strip map[string]struct{}) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			out = append(out, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if key, _, ok := strings.Cut(name, "="); ok {
			name = key
		}
		if _, ok := strip[name]; ok {
			if !strings.Contains(arg, "=") && name == "max-bytes" && i+1 < len(args) {
				i++
			}
			continue
		}
		out = append(out, arg)
	}
	return out
}
