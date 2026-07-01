package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/openclaw/discrawl/internal/store"
)

func (r *runtime) runChannelsResolve(args []string) error {
	fs := flag.NewFlagSet("channels resolve", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	guild := fs.String("guild", "", "")
	guilds := fs.String("guilds", "", "")
	jsonOut := fs.Bool("json", false, "")
	if err := fs.Parse(permuteChannelResolveFlags(args)); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(errors.New("channels resolve requires one channel id or name"))
	}
	if *jsonOut {
		r.json = true
	}
	guildIDs := csvList(strings.Join([]string{*guild, *guilds}, ","))
	resolution, resolveErr := r.resolveLocalChannel(fs.Arg(0), guildIDs)
	if err := r.print(resolution); err != nil {
		return err
	}
	return resolveErr
}

func permuteChannelResolveFlags(args []string) []string {
	flags := make([]string, 0, len(args))
	positionals := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--json" || strings.HasPrefix(arg, "--json=") || strings.HasPrefix(arg, "--guild=") || strings.HasPrefix(arg, "--guilds=") {
			flags = append(flags, arg)
			continue
		}
		if (arg == "--guild" || arg == "--guilds") && i+1 < len(args) {
			flags = append(flags, arg, args[i+1])
			i++
			continue
		}
		positionals = append(positionals, arg)
	}
	return append(flags, positionals...)
}

type channelResolutionCandidate struct {
	ChannelID string `json:"channel_id"`
	GuildID   string `json:"guild_id"`
	Name      string `json:"name"`
	Kind      string `json:"kind,omitempty"`
}

type channelResolution struct {
	Query      string                       `json:"query"`
	Status     string                       `json:"status"`
	Match      string                       `json:"match,omitempty"`
	Selected   *channelResolutionCandidate  `json:"selected,omitempty"`
	Candidates []channelResolutionCandidate `json:"candidates,omitempty"`
}

type channelResolutionError struct {
	resolution channelResolution
}

func (e *channelResolutionError) Error() string {
	switch e.resolution.Status {
	case "ambiguous":
		candidates := make([]string, 0, len(e.resolution.Candidates))
		for _, candidate := range e.resolution.Candidates {
			candidates = append(candidates, fmt.Sprintf("guild=%s channel=%s name=#%s", candidate.GuildID, candidate.ChannelID, candidate.Name))
		}
		return fmt.Sprintf("channel %q is ambiguous; retry with a channel id; candidates: %s", e.resolution.Query, strings.Join(candidates, ", "))
	default:
		return fmt.Sprintf("cannot resolve channel %q; run discrawl channels resolve %q --json or pass a channel id", e.resolution.Query, e.resolution.Query)
	}
}

func (r *runtime) resolveLocalChannel(query string, guildIDs []string) (channelResolution, error) {
	rows, err := r.store.Channels(r.ctx, "")
	if err != nil {
		return channelResolution{}, err
	}
	return resolveChannelRows(rows, query, guildIDs)
}

func resolveChannelRows(rows []store.ChannelRow, query string, guildIDs []string) (channelResolution, error) {
	query = normalizeChannelQuery(query)
	resolution := channelResolution{Query: query, Status: "not_found"}
	allowedGuilds := map[string]struct{}{}
	for _, guildID := range guildIDs {
		allowedGuilds[guildID] = struct{}{}
	}
	filtered := make([]store.ChannelRow, 0, len(rows))
	for _, row := range rows {
		if len(allowedGuilds) > 0 {
			if _, ok := allowedGuilds[row.GuildID]; !ok {
				continue
			}
		}
		filtered = append(filtered, row)
	}

	match := func(kind string, predicate func(store.ChannelRow) bool) (channelResolution, bool) {
		candidates := make([]channelResolutionCandidate, 0)
		for _, row := range filtered {
			if predicate(row) {
				candidates = append(candidates, channelCandidate(row))
			}
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].GuildID != candidates[j].GuildID {
				return candidates[i].GuildID < candidates[j].GuildID
			}
			if candidates[i].Name != candidates[j].Name {
				return candidates[i].Name < candidates[j].Name
			}
			return candidates[i].ChannelID < candidates[j].ChannelID
		})
		switch len(candidates) {
		case 0:
			return channelResolution{}, false
		case 1:
			resolution.Status = "resolved"
			resolution.Match = kind
			resolution.Selected = &candidates[0]
			return resolution, true
		default:
			resolution.Status = "ambiguous"
			resolution.Match = kind
			resolution.Candidates = candidates
			return resolution, true
		}
	}

	if resolved, ok := match("id", func(row store.ChannelRow) bool { return row.ID == query }); ok {
		return finishChannelResolution(resolved)
	}
	if resolved, ok := match("exact_name", func(row store.ChannelRow) bool { return strings.EqualFold(row.Name, query) }); ok {
		return finishChannelResolution(resolved)
	}
	lowered := strings.ToLower(query)
	if query != "" {
		if resolved, ok := match("partial_name", func(row store.ChannelRow) bool {
			return strings.Contains(strings.ToLower(row.Name), lowered)
		}); ok {
			return finishChannelResolution(resolved)
		}
	}
	return resolution, &channelResolutionError{resolution: resolution}
}

func normalizeChannelQuery(query string) string {
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(query), "#"))
}

func finishChannelResolution(resolution channelResolution) (channelResolution, error) {
	if resolution.Status == "resolved" {
		return resolution, nil
	}
	return resolution, &channelResolutionError{resolution: resolution}
}

func channelCandidate(row store.ChannelRow) channelResolutionCandidate {
	return channelResolutionCandidate{ChannelID: row.ID, GuildID: row.GuildID, Name: row.Name, Kind: row.Kind}
}
