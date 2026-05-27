package cli

import (
	"errors"
	"fmt"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/store"
)

func (r *runtime) runRemoteSearch(opts store.SearchOptions, mode string, dm bool) ([]store.SearchResult, error) {
	if dm {
		return nil, usageErr(errors.New("cloud search does not support --dm"))
	}
	if mode != "" && mode != "fts" {
		return nil, usageErr(fmt.Errorf("cloud search supports fts mode only, got %q", mode))
	}
	if opts.Author != "" {
		return nil, usageErr(errors.New("cloud search does not support --author yet"))
	}
	guildID, err := singleRemoteGuild(opts.GuildIDs)
	if err != nil {
		return nil, err
	}
	client, err := r.remoteClient(true)
	if err != nil {
		return nil, err
	}
	result, err := client.Query(r.ctx, "discrawl", r.cfg.Remote.Archive, crawlremote.QueryRequest{
		Name: "discrawl.messages.search",
		Args: map[string]any{
			"query":      opts.Query,
			"channel_id": opts.Channel,
			"guild_id":   guildID,
		},
		Limit: opts.Limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]store.SearchResult, 0, len(result.Values))
	for _, value := range result.Values {
		out = append(out, store.SearchResult{
			MessageID:   remoteString(value, "message_id"),
			GuildID:     remoteString(value, "guild_id"),
			ChannelID:   remoteString(value, "channel_id"),
			ChannelName: remoteString(value, "channel_name"),
			AuthorID:    remoteString(value, "author_id"),
			AuthorName:  remoteString(value, "author_username"),
			Content:     remoteString(value, "content"),
			CreatedAt:   remoteTime(value, "created_at"),
		})
	}
	return out, nil
}

func (r *runtime) runRemoteMessages(opts store.MessageListOptions, dm bool) ([]store.MessageRow, error) {
	if dm {
		return nil, usageErr(errors.New("cloud messages does not support --dm"))
	}
	if opts.Author != "" {
		return nil, usageErr(errors.New("cloud messages does not support --author yet"))
	}
	if !opts.Since.IsZero() || !opts.Before.IsZero() || opts.Last > 0 {
		return nil, usageErr(errors.New("cloud messages currently supports --channel, --guild, --guilds, and --limit"))
	}
	guildID, err := singleRemoteGuild(opts.GuildIDs)
	if err != nil {
		return nil, err
	}
	client, err := r.remoteClient(true)
	if err != nil {
		return nil, err
	}
	limit := opts.Limit
	if limit == 0 {
		limit = 500
	}
	result, err := client.Query(r.ctx, "discrawl", r.cfg.Remote.Archive, crawlremote.QueryRequest{
		Name: "discrawl.messages.list",
		Args: map[string]any{
			"channel_id": opts.Channel,
			"guild_id":   guildID,
		},
		Limit: limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]store.MessageRow, 0, len(result.Values))
	for _, value := range result.Values {
		out = append(out, store.MessageRow{
			MessageID:   remoteString(value, "message_id"),
			GuildID:     remoteString(value, "guild_id"),
			ChannelID:   remoteString(value, "channel_id"),
			ChannelName: remoteString(value, "channel_name"),
			AuthorID:    remoteString(value, "author_id"),
			AuthorName:  remoteString(value, "author_username"),
			Content:     remoteString(value, "content"),
			CreatedAt:   remoteTime(value, "created_at"),
			Source:      "remote",
		})
	}
	return out, nil
}

func singleRemoteGuild(guildIDs []string) (string, error) {
	if len(guildIDs) > 1 {
		return "", usageErr(errors.New("cloud remote supports one guild filter at a time"))
	}
	if len(guildIDs) == 1 {
		return guildIDs[0], nil
	}
	return "", nil
}

func remoteString(value map[string]any, key string) string {
	if value == nil {
		return ""
	}
	if raw := value[key]; raw != nil {
		return fmt.Sprint(raw)
	}
	return ""
}

func remoteTime(value map[string]any, key string) time.Time {
	raw := remoteString(value, key)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t
		}
	}
	return time.Time{}
}
