package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

// explainEmptyChannel writes a stderr note when a --channel-scoped, empty
// result set is explained by the channel having no visible messages in the
// local mirror at all, rather than by the query legitimately matching
// nothing. It is a no-op (and returns zero stats) when --json is set, when
// no channel filter was resolved to a concrete channel id, or when the
// channel does have messages. It returns the channel's message stats so
// callers can reuse them (e.g. for the date-window note) without a second
// query, along with whether the "no messages" note was printed.
func (r *runtime) explainEmptyChannel(channelID string) (store.ChannelMessageStats, bool) {
	channelID = strings.TrimSpace(channelID)
	if r.json || channelID == "" || !isDiscordID(channelID) {
		return store.ChannelMessageStats{}, false
	}
	stats, err := r.store.ChannelMessageStats(r.ctx, channelID)
	if err != nil || stats.Count > 0 {
		return stats, false
	}
	name, kind := r.lookupChannelNameKind(channelID)
	_, _ = fmt.Fprintf(r.stderr, "note: channel %s (%s, kind=%s) has no messages in the local mirror\n", channelID, name, kind)
	if kind == "forum" {
		_, _ = fmt.Fprintf(r.stderr, "note: forum channels hold no messages directly — query its post threads instead (select id from channels where parent_id='%s')\n", channelID)
	}
	return stats, true
}

func (r *runtime) lookupChannelNameKind(channelID string) (string, string) {
	rows, err := r.store.Channels(r.ctx, "")
	if err != nil {
		return "", ""
	}
	for _, row := range rows {
		if row.ID == channelID {
			return row.Name, row.Kind
		}
	}
	return "", ""
}

// explainEmptyDateWindow writes a stderr note when a --days/--since window
// looks like the reason `messages` returned nothing. It only fires when the
// channel's newest (visible) message actually predates the window start --
// other active filters (--author, the default exclusion of empty-content
// messages) can also zero out a result set, and pointing at --days/--since
// in those cases would be a false diagnostic.
func (r *runtime) explainEmptyDateWindow(days int, sinceRaw string, sinceTime time.Time, stats store.ChannelMessageStats) {
	if r.json || stats.Count == 0 || stats.Newest.IsZero() || sinceTime.IsZero() {
		return
	}
	if !stats.Newest.Before(sinceTime) {
		return
	}
	switch {
	case days > 0:
		_, _ = fmt.Fprintf(r.stderr, "note: channel has %d messages but none within the last %d days (newest: %s) — try without --days\n", stats.Count, days, formatTime(stats.Newest))
	case strings.TrimSpace(sinceRaw) != "":
		_, _ = fmt.Fprintf(r.stderr, "note: channel has %d messages but none since %s (newest: %s) — try without --since\n", stats.Count, sinceRaw, formatTime(stats.Newest))
	}
}

// explainEmptySearchTerms writes a stderr note when a multi-word FTS query
// returned nothing but at least one of its individual terms would match on
// its own -- a hint that FTS ANDs terms together rather than searching them
// independently. It reuses the normal search path (SearchMessages) per term
// with limit 1, so the reported match count is only ever 0 or 1.
func (r *runtime) explainEmptySearchTerms(opts store.SearchOptions) {
	if r.json || strings.Contains(opts.Query, `"`) {
		return
	}
	terms := strings.Fields(opts.Query)
	if len(terms) < 2 {
		return
	}
	for _, term := range terms {
		termOpts := opts
		termOpts.Query = term
		termOpts.Limit = 1
		results, err := r.store.SearchMessages(r.ctx, termOpts)
		if err != nil || len(results) == 0 {
			continue
		}
		_, _ = fmt.Fprintf(r.stderr, "note: no message contains all %d terms together; %q alone matches %d message(s) — FTS terms are ANDed, try one term or an exact phrase\n", len(terms), term, len(results))
		return
	}
}
