package cli

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

func (r *runtime) resolveSyncGuilds(guild, guilds string) []string {
	requested := requestedGuilds(guild, guilds)
	if len(requested) > 0 {
		return requested
	}
	if defaultGuild := r.cfg.EffectiveDefaultGuildID(); defaultGuild != "" {
		return []string{defaultGuild}
	}
	return nil
}

func (r *runtime) resolveSyncGuildsAll(guild, guilds string, all bool) ([]string, error) {
	if !all {
		return r.resolveSyncGuilds(guild, guilds), nil
	}
	if len(csvList(guilds)) > 0 || strings.TrimSpace(guild) != "" {
		return nil, errors.New("use either --all or --guild/--guilds")
	}
	return nil, nil
}

func (r *runtime) resolveSearchGuilds(guild, guilds string) []string {
	return requestedGuilds(guild, guilds)
}

func requestedGuilds(guild, guilds string) []string {
	requested := append(csvList(guilds), strings.TrimSpace(guild))
	return csvList(strings.Join(requested, ","))
}

func directMessageGuildScope(dm bool, guild, guilds string) ([]string, error) {
	if !dm {
		return requestedGuilds(guild, guilds), nil
	}
	if len(csvList(guilds)) > 0 || strings.TrimSpace(guild) != "" {
		return nil, errors.New("use either --dm or --guild/--guilds")
	}
	return []string{store.DirectMessageGuildID}, nil
}

func csvList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func flagPassed(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func mustDuration(raw string) time.Duration {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 6 * time.Hour
	}
	return d
}

func usageErr(err error) error {
	return &cliError{code: 2, err: err}
}

func configErr(err error) error {
	return &cliError{code: 3, err: err}
}

func authErr(err error) error {
	return &cliError{code: 4, err: err}
}

func dbErr(err error) error {
	return &cliError{code: 5, err: err}
}

func firstNonEmpty(items ...string) string {
	for _, item := range items {
		if strings.TrimSpace(item) != "" {
			return item
		}
	}
	return ""
}
