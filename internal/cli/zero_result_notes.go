package cli

import (
	"fmt"
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
	channelID    string
	guildIDs     []string
	includeEmpty bool
}

// newZeroResultScope reports the scope to explain, and false when the query
// carried a channel filter that never resolved to a concrete id. ListMessages
// and SearchMessages also match a channel by name fragment, and a stats query
// keyed on channel_id cannot reproduce that row set, so those runs get no
// note rather than one counted over the wrong rows.
func (r *runtime) newZeroResultScope(channel string, guildIDs []string, includeEmpty bool) (zeroResultScope, bool) {
	scope := zeroResultScope{guildIDs: guildIDs, includeEmpty: includeEmpty}
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
	return store.MessageScopeOptions{ChannelID: s.channelID, GuildIDs: s.guildIDs, IncludeEmpty: s.includeEmpty}
}

// explainEmptyMessages writes stderr notes for a `messages` run that returned
// nothing: a channel with no archived messages at all, a channel whose
// messages are all empty/attachment-only and therefore filtered out by
// default, or a --days/--since/--before window that sits outside the data.
func (r *runtime) explainEmptyMessages(scope zeroResultScope, days int, sinceRaw, beforeRaw string, since, before time.Time) {
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
	r.explainEmptyDateWindow(days, sinceRaw, beforeRaw, since, before, stats)
}

// explainEmptySearch writes stderr notes for a `search` run that returned
// nothing. The content notes apply to every mode; the AND-hint applies to
// the modes that run an FTS query, and the embedding-coverage note to the
// modes that run a vector query.
func (r *runtime) explainEmptySearch(opts store.SearchOptions, mode string) {
	if r.json {
		return
	}
	scope, ok := r.newZeroResultScope(opts.Channel, opts.GuildIDs, opts.IncludeEmpty)
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
		name, kind := r.lookupChannelNameKind(scope.channelID)
		_, _ = fmt.Fprintf(r.stderr, "note: channel %s (%s, kind=%s) has no messages in the local mirror\n", scope.channelID, name, kind)
		if kind == "forum" {
			_, _ = fmt.Fprintf(r.stderr, "note: a forum holds its posts as separate thread channels; list them with `discrawl --json channels list` (thread_parent_id=%s) and query one with `discrawl messages --channel THREAD_ID`\n", scope.channelID)
		}
		return true
	}
	if stats.Count == 0 {
		_, _ = fmt.Fprintf(r.stderr, "note: all %d messages in channel %s are empty or attachment-only, which the default filter drops; list them with `%s --channel %s --include-empty`\n", stats.Total, scope.channelID, listCmd, scope.channelID)
		return true
	}
	return false
}

func (r *runtime) lookupChannelNameKind(channelID string) (string, string) {
	row, found, err := r.store.ChannelByID(r.ctx, channelID)
	if err != nil || !found {
		return "", ""
	}
	return row.Name, row.Kind
}

// explainEmptyDateWindow writes a stderr note when a --days/--since/--before
// window looks like the reason `messages` returned nothing. Each side fires
// only when every message in scope sits outside it, which stays true no
// matter what other filters (--author, and so on) were also applied, so the
// note never blames the window for someone else's exclusion.
func (r *runtime) explainEmptyDateWindow(days int, sinceRaw, beforeRaw string, since, before time.Time, stats store.MessageScopeStats) {
	if stats.Count == 0 {
		return
	}
	if !since.IsZero() && !stats.Newest.IsZero() && stats.Newest.Before(since) {
		switch {
		case days > 0:
			_, _ = fmt.Fprintf(r.stderr, "note: %d messages in scope but none within the last %d days (newest: %s); try without --days\n", stats.Count, days, formatTime(stats.Newest))
		case strings.TrimSpace(sinceRaw) != "":
			_, _ = fmt.Fprintf(r.stderr, "note: %d messages in scope but none since %s (newest: %s); try without --since\n", stats.Count, sinceRaw, formatTime(stats.Newest))
		}
	}
	if !before.IsZero() && !stats.Oldest.IsZero() && !stats.Oldest.Before(before) && strings.TrimSpace(beforeRaw) != "" {
		_, _ = fmt.Fprintf(r.stderr, "note: %d messages in scope but none before %s (oldest: %s); try without --before\n", stats.Count, beforeRaw, formatTime(stats.Oldest))
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
	_, _ = fmt.Fprintf(r.stderr, "note: none of the %d messages in scope have embeddings for provider=%s model=%s, and semantic search only matches embedded messages; run `discrawl embed`, or use --mode fts\n", stats.Count, r.cfg.Search.Embeddings.Provider, r.cfg.Search.Embeddings.Model)
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
		_, _ = fmt.Fprintf(r.stderr, "note: no message contains all %d terms together; %q alone matches. Every term is required, so search one distinctive term and narrow the result with --channel or --author\n", len(terms), store.FTSTermText(term))
		return
	}
}
