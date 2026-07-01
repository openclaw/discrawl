package cli

import (
	"errors"
	"strings"

	"github.com/openclaw/discrawl/internal/syncer"
)

func (r *runtime) syncMessagesQuery(channel, guild, guilds string) error {
	if r.syncer == nil {
		return usageErr(errors.New("messages --sync requires Discord access"))
	}
	opts, err := r.messageSyncOptions(channel, guild, guilds)
	if err != nil {
		return usageErr(err)
	}
	_, err = r.syncer.Sync(r.ctx, opts)
	return err
}

func (r *runtime) messageSyncOptions(channel, guild, guilds string) (syncer.SyncOptions, error) {
	requestedGuilds := r.resolveSyncGuilds(guild, guilds)
	opts := syncer.SyncOptions{
		GuildIDs:    requestedGuilds,
		Concurrency: r.cfg.Sync.Concurrency,
	}

	channelFilter := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(channel), "#"))
	if channelFilter == "" {
		if len(opts.GuildIDs) == 0 {
			return opts, errors.New("messages --sync needs --channel or --guild")
		}
		return opts, nil
	}
	if isDiscordID(channelFilter) {
		opts.ChannelIDs = []string{channelFilter}
		return opts, nil
	}

	rows, err := r.store.Channels(r.ctx, "")
	if err != nil {
		return opts, err
	}
	resolution, err := resolveChannelRows(rows, channelFilter, requestedGuilds)
	if err != nil {
		if resolution.Status == "not_found" && len(requestedGuilds) > 0 {
			return opts, nil
		}
		return opts, err
	}
	opts.ChannelIDs = []string{resolution.Selected.ChannelID}
	if len(requestedGuilds) == 0 {
		opts.GuildIDs = []string{resolution.Selected.GuildID}
	}
	return opts, nil
}

func isDiscordID(raw string) bool {
	if len(raw) < 16 {
		return false
	}
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func hasBoolFlag(args []string, name string) bool {
	for _, arg := range args {
		if arg == name || strings.HasPrefix(arg, name+"=") {
			return true
		}
	}
	return false
}

func boolFlagEnabled(args []string, name string) bool {
	enabled := false
	for _, arg := range args {
		if arg == name {
			enabled = true
			continue
		}
		if raw, ok := strings.CutPrefix(arg, name+"="); ok {
			switch strings.ToLower(strings.TrimSpace(raw)) {
			case "1", "t", "true", "y", "yes", "on":
				enabled = true
			default:
				enabled = false
			}
		}
	}
	return enabled
}

func hasHelpArg(args []string) bool {
	for _, arg := range args {
		if arg == "help" || arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}

func hasHelpFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}
