package cli

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

// maxZeroResultTermProbes caps the extra single-term searches issued to work
// out which part of a multi-term query has matches, so a long query does not
// turn one empty search into one probe per word.
const maxZeroResultTermProbes = 4

// zeroResultScope carries the filters the query that returned nothing was
// run with, so every note below is derived from the same row set the query
// looked at rather than a re-modelled one.
type zeroResultScope struct {
	channelID      string
	guildIDs       []string
	includeEmpty   bool
	includeDeleted bool
}

// listMessagesScope models a store.ListMessages query. ListMessages carries no
// deleted_at predicate, so it returns soft-deleted rows and the stats behind
// its notes have to count them too. Without this a channel whose messages were
// all deleted over the gateway reads as "no messages in the local mirror" even
// though `messages --channel ID` lists them.
func (r *runtime) listMessagesScope(channel string, guildIDs []string, includeEmpty bool) (zeroResultScope, bool) {
	return r.newZeroResultScope(channel, guildIDs, includeEmpty, true)
}

// searchScope models a store.SearchMessages query, whose every leg filters
// `deleted_at is null`, so its notes must not count soft-deleted rows.
func (r *runtime) searchScope(channel string, guildIDs []string, includeEmpty bool) (zeroResultScope, bool) {
	return r.newZeroResultScope(channel, guildIDs, includeEmpty, false)
}

// newZeroResultScope reports the scope to explain, and false when the query
// carried a channel filter that never resolved to a concrete id. ListMessages
// and SearchMessages also match a channel by name fragment, and a stats query
// keyed on channel_id cannot reproduce that row set, so those runs get no
// note rather than one counted over the wrong rows.
func (r *runtime) newZeroResultScope(channel string, guildIDs []string, includeEmpty, includeDeleted bool) (zeroResultScope, bool) {
	scope := zeroResultScope{guildIDs: guildIDs, includeEmpty: includeEmpty, includeDeleted: includeDeleted}
	channel = strings.TrimSpace(channel)
	switch {
	case channel == "":
		return scope, true
	case isDiscordID(channel):
		scope.channelID = channel
		return scope, true
	default:
		return zeroResultScope{}, false
	}
}

func (s zeroResultScope) storeOptions() store.MessageScopeOptions {
	return store.MessageScopeOptions{
		ChannelID:      s.channelID,
		GuildIDs:       s.guildIDs,
		IncludeEmpty:   s.includeEmpty,
		IncludeDeleted: s.includeDeleted,
	}
}

// zeroResultWindow describes the time window as the user expressed it, so a
// note can name the flag that was actually passed rather than the resolved
// timestamp it turned into. `messages` and `dms` both take all four, and each
// entry point forwards every one of them, since the resolved `since` alone
// cannot say which flag produced it.
type zeroResultWindow struct {
	hours     int
	days      int
	sinceRaw  string
	beforeRaw string
	since     time.Time
	before    time.Time
}

// explainEmptyMessages writes stderr notes for a `messages` or `dms` run that
// returned nothing: a channel with no archived messages at all, a channel whose
// messages are all empty/attachment-only and therefore filtered out by
// default, or an --hours/--days/--since/--before window that sits outside the
// data.
func (r *runtime) explainEmptyMessages(scope zeroResultScope, window zeroResultWindow) {
	if r.json {
		return
	}
	stats, ok := r.zeroResultStats(scope)
	if !ok {
		return
	}
	if r.explainEmptyScopeContents(scope, stats, "discrawl messages") {
		return
	}
	r.explainEmptyDateWindow(scope, window, stats)
}

// explainEmptySearch writes stderr notes for a `search` run that returned
// nothing. The content notes apply to every mode; the AND-hint applies to
// the modes that run an FTS query, and the embedding-coverage note to the
// modes that run a vector query.
func (r *runtime) explainEmptySearch(opts store.SearchOptions, mode string) {
	if r.json {
		return
	}
	// Author matching differs across lexical, semantic, and message queries.
	// Only term probes preserve that predicate without reconstructing it.
	if strings.TrimSpace(opts.Author) != "" {
		if mode == "" || mode == "fts" || mode == "hybrid" {
			r.explainEmptySearchTerms(opts)
		}
		return
	}
	scope, ok := r.searchScope(opts.Channel, opts.GuildIDs, opts.IncludeEmpty)
	if !ok {
		return
	}
	stats, ok := r.zeroResultStats(scope)
	if !ok {
		return
	}
	// Search indexes normalized content only, so --include-empty cannot
	// surface attachment-only rows here; point at the command that can.
	if r.explainEmptyScopeContents(scope, stats, "discrawl messages") {
		return
	}
	switch mode {
	case "", "fts":
		r.explainEmptySearchTerms(opts)
	case "hybrid":
		// The lexical leg of a hybrid search is the same FTS query.
		r.explainEmptySearchTerms(opts)
		r.explainEmptyEmbeddings(scope, stats)
	case "semantic":
		r.explainEmptyEmbeddings(scope, stats)
	}
}

// `dms` reaches the same store.ListMessages and store.SearchMessages queries
// `messages` and `search` do, so an empty result there is just as silent and
// gets the notes that apply. Both entry points below decline when --with is
// set: --with names a person and the query matches it against channel id and
// channel name alike, which a channel_id-keyed stats query cannot reproduce,
// so such a run gets no note rather than one counted over every conversation.

// explainEmptyDirectMessageList explains an empty `dms` listing. Only the
// window notes can apply: `dms` has no --channel, so there is no concrete
// channel for the content notes to describe.
func (r *runtime) explainEmptyDirectMessageList(with string, includeEmpty bool, window zeroResultWindow) {
	if r.json || strings.TrimSpace(with) != "" {
		return
	}
	scope, ok := r.listMessagesScope("", []string{store.DirectMessageGuildID}, includeEmpty)
	if !ok {
		return
	}
	r.explainEmptyMessages(scope, window)
}

// explainEmptyDirectMessageSearch explains an empty `dms --search`. That path
// runs one FTS query with no mode flag, so the multi-term AND note is the only
// one that applies.
func (r *runtime) explainEmptyDirectMessageSearch(with string, opts store.SearchOptions) {
	if r.json || strings.TrimSpace(with) != "" {
		return
	}
	r.explainEmptySearchTerms(opts)
}

func (r *runtime) zeroResultStats(scope zeroResultScope) (store.MessageScopeStats, bool) {
	stats, err := r.store.MessageScopeStats(r.ctx, scope.storeOptions())
	if err != nil {
		return store.MessageScopeStats{}, false
	}
	return stats, true
}

// explainEmptyScopeContents reports that the scope holds nothing the query
// could have returned, and says which. It returns true when it printed a
// note, so callers stop rather than blaming a filter on top of it.
func (r *runtime) explainEmptyScopeContents(scope zeroResultScope, stats store.MessageScopeStats, listCmd string) bool {
	if scope.channelID == "" {
		return false
	}
	if stats.Total == 0 {
		row, found := r.lookupChannel(scope.channelID)
		// A channel outside the requested guild scope has no rows *here*, but
		// saying it has none at all contradicts what the same command prints
		// without the guild filter. Name the filter that emptied it instead.
		if found && len(scope.guildIDs) > 0 && !slices.Contains(scope.guildIDs, row.GuildID) {
			_, _ = fmt.Fprintf(r.stderr, "note: %s is in guild %s, which is outside the requested guild scope (%s); drop --guild/--guilds or use --guild %s\n",
				channelIdentity(scope.channelID, row), row.GuildID, strings.Join(scope.guildIDs, ","), row.GuildID)
			return true
		}
		_, _ = fmt.Fprintf(r.stderr, "note: %s has no messages in the local mirror\n", channelIdentity(scope.channelID, row))
		if row.Kind == "forum" {
			_, _ = fmt.Fprintf(r.stderr, "note: a forum holds its posts as separate thread channels; list them with `discrawl --json channels list` (thread_parent_id=%s) and query one with `discrawl messages --channel THREAD_ID`\n", scope.channelID)
		}
		return true
	}
	if stats.Count == 0 {
		_, _ = fmt.Fprintf(r.stderr, "note: all %d messages %s are empty or attachment-only, which the default filter drops; list them with `%s --channel %s --include-empty`\n", stats.Total, r.zeroResultScopeLabel(scope), listCmd, scope.channelID)
		return true
	}
	return false
}

// channelIdentity renders the "channel ID (name, kind=k)" label the
// channel-empty note prints. Every note that identifies a channel goes through
// it, so the whole set reads as one tool rather than each note inventing its
// own shape.
func channelIdentity(channelID string, row store.ChannelRow) string {
	if row.ID == "" {
		return "channel " + channelID
	}
	return fmt.Sprintf("channel %s (%s, kind=%s)", channelID, row.Name, row.Kind)
}

// zeroResultScopeLabel names the rows a count was taken over, for the notes
// that report one. A guild-wide query has no single channel to name and reads
// as "in scope"; an id that matches no channels row keeps the id and drops the
// parenthetical rather than printing an empty one.
func (r *runtime) zeroResultScopeLabel(scope zeroResultScope) string {
	if scope.channelID == "" {
		return "in scope"
	}
	row, found := r.lookupChannel(scope.channelID)
	if !found {
		return "in channel " + scope.channelID
	}
	return "in " + channelIdentity(scope.channelID, row)
}

func (r *runtime) lookupChannel(channelID string) (store.ChannelRow, bool) {
	row, found, err := r.store.ChannelByID(r.ctx, channelID)
	if err != nil || !found {
		return store.ChannelRow{}, false
	}
	return row, true
}

// explainEmptyDateWindow writes a stderr note when an
// --hours/--days/--since/--before window looks like the reason the query
// returned nothing. Each side fires only when every message in scope sits
// outside it, which stays true no matter what other filters (--author, and so
// on) were also applied, so the note never blames the window for someone
// else's exclusion.
func (r *runtime) explainEmptyDateWindow(scope zeroResultScope, window zeroResultWindow, stats store.MessageScopeStats) {
	if stats.Count == 0 {
		return
	}
	sinceExcludes := !window.since.IsZero() && !stats.Newest.IsZero() && stats.Newest.Before(window.since)
	beforeExcludes := !window.before.IsZero() && !stats.Oldest.IsZero() && !stats.Oldest.Before(window.before) && strings.TrimSpace(window.beforeRaw) != ""
	if !sinceExcludes && !beforeExcludes {
		return
	}
	// Resolved once: both halves can fire on the same run, and the label costs
	// a channel lookup.
	where := r.zeroResultScopeLabel(scope)
	if sinceExcludes && beforeExcludes {
		sinceFlag := "--since"
		if window.hours > 0 {
			sinceFlag = "--hours"
		} else if window.days > 0 {
			sinceFlag = "--days"
		}
		_, _ = fmt.Fprintf(r.stderr, "note: both %s and --before exclude all %d messages %s (oldest: %s; newest: %s); try without both %s and --before\n", sinceFlag, stats.Count, where, formatTime(stats.Oldest), formatTime(stats.Newest), sinceFlag)
		return
	}
	if sinceExcludes {
		switch {
		case window.hours > 0:
			_, _ = fmt.Fprintf(r.stderr, "note: %d messages %s but none within the last %d hours (newest: %s); try without --hours\n", stats.Count, where, window.hours, formatTime(stats.Newest))
		case window.days > 0:
			_, _ = fmt.Fprintf(r.stderr, "note: %d messages %s but none within the last %d days (newest: %s); try without --days\n", stats.Count, where, window.days, formatTime(stats.Newest))
		case strings.TrimSpace(window.sinceRaw) != "":
			_, _ = fmt.Fprintf(r.stderr, "note: %d messages %s but none since %s (newest: %s); try without --since\n", stats.Count, where, window.sinceRaw, formatTime(stats.Newest))
		}
	}
	if beforeExcludes {
		_, _ = fmt.Fprintf(r.stderr, "note: %d messages %s but none before %s (oldest: %s); try without --before\n", stats.Count, where, window.beforeRaw, formatTime(stats.Oldest))
	}
}

// explainEmptyEmbeddings writes a stderr note when a semantic or hybrid
// search returned nothing and no message in scope carries an embedding for
// the configured provider/model, which is the only content a vector query
// can match.
func (r *runtime) explainEmptyEmbeddings(scope zeroResultScope, stats store.MessageScopeStats) {
	if stats.Count == 0 || !r.cfg.Search.Embeddings.Enabled {
		return
	}
	embedded, err := r.store.MessageEmbeddingCoverage(
		r.ctx,
		scope.storeOptions(),
		r.cfg.Search.Embeddings.Provider,
		r.cfg.Search.Embeddings.Model,
		store.EmbeddingInputVersion,
	)
	if err != nil || embedded > 0 {
		return
	}
	lead := fmt.Sprintf("note: none of the %d messages %s have embeddings for provider=%s model=%s, and semantic search only matches embedded messages",
		stats.Count, r.zeroResultScopeLabel(scope), r.cfg.Search.Embeddings.Provider, r.cfg.Search.Embeddings.Model)
	// Neither the enqueue-on-write path nor store.RequeueAllEmbeddingJobs
	// creates a job for guild_id '@me', so no embed run of any kind can give
	// these messages embeddings. Recommending one would send the reader to a
	// command that cannot resolve what they are looking at.
	if stats.DirectMessages == stats.Count {
		_, _ = fmt.Fprintf(r.stderr, "%s; embedding jobs are never created for direct messages, so no `discrawl embed` run changes this. Use --mode fts\n", lead)
		return
	}
	if stats.Embeddable == 0 {
		return
	}
	pending, err := r.store.MessagePendingEmbeddingJobs(r.ctx, scope.storeOptions())
	if err != nil {
		return
	}
	// `discrawl embed` drains jobs that already exist and creates none, so it
	// resolves this only when the scope has pending jobs. With none, the
	// supported way to create them is `embed --rebuild`, whose scope is the
	// whole archive rather than this query's, and which drains at most --limit
	// messages per run.
	if pending == 0 {
		_, _ = fmt.Fprintf(r.stderr, "%s; none of them has a pending embedding job either, so `discrawl embed` drains nothing. `discrawl embed --rebuild` is what enqueues the missing jobs, and it requeues every non-deleted message outside DMs archive-wide, not just this scope, embedding up to --limit (default %d) per run; or use --mode fts\n", lead, store.DefaultEmbedLimit())
		return
	}
	_, _ = fmt.Fprintf(r.stderr, "%s; pending embedding jobs cover %d of them. `discrawl embed` processes the archive-wide queue up to --limit (default %d) per run; repeat until this scope is covered, or use --mode fts\n", lead, pending, store.DefaultEmbedLimit())
}

// explainEmptySearchTerms writes a stderr note when a multi-term query
// returned nothing but one of its terms matches on its own. Terms come from
// store.FTSQueryTerms, the same split the MATCH expression is built from, so
// the note counts the units the query actually ANDed.
func (r *runtime) explainEmptySearchTerms(opts store.SearchOptions) {
	terms := store.FTSQueryTerms(opts.Query)
	if len(terms) < 2 {
		return
	}
	probes := terms
	if len(probes) > maxZeroResultTermProbes {
		probes = probes[:maxZeroResultTermProbes]
	}
	for _, term := range probes {
		termOpts := opts
		termOpts.Query = term
		termOpts.Limit = 1
		results, err := r.store.SearchMessages(r.ctx, termOpts)
		if err != nil || len(results) == 0 {
			continue
		}
		_, _ = fmt.Fprintf(r.stderr, "note: no message contains all %d terms together; %q alone matches. Every term is required, so search one distinctive term with the same filters\n", len(terms), store.FTSTermText(term))
		return
	}
}
