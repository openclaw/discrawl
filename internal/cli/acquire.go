package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/acquisition"
	"github.com/openclaw/discrawl/internal/discord"
)

const acquireUsage = `Usage:
  discrawl acquire --api-url ORIGIN --source ID --guild ID --channel ID \
    --publisher-token-env NAME --discord-token-env NAME [--since UTC] [--horizon UTC] \
    [--max-pages 100] [--timeout 10m]

Collect one message-creation window through PostgreSQL-owned API progress.
The first run requires --since. Later runs use the channel's completed horizon.
An interrupted run resumes its original window; explicit bounds must match it.
UTC bounds must use millisecond precision or coarser. The default new horizon is
now. This command opens no SQLite store or local outbox and starts no schedule.
It does not collect old-message edits or imply complete channel history.
`

func runAcquire(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("acquire", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	origin := fs.String("api-url", "", "")
	source := fs.String("source", "", "")
	guild := fs.String("guild", "", "")
	channel := fs.String("channel", "", "")
	publisherEnv := fs.String("publisher-token-env", "", "")
	botEnv := fs.String("discord-token-env", "", "")
	since := fs.String("since", "", "")
	horizon := fs.String("horizon", "", "")
	maxPages := fs.Int("max-pages", 100, "")
	timeout := fs.Duration("timeout", 10*time.Minute, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(errors.New("invalid acquire options"))
	}
	if fs.NArg() != 0 || *timeout < time.Second || *timeout > time.Hour || *maxPages < 1 || *maxPages > 10000 || *publisherEnv == *botEnv {
		return usageErr(errors.New("acquire requires separate credentials and bounded options"))
	}
	token := func(name string) (string, error) {
		if name == "" || strings.IndexFunc(name, func(r rune) bool {
			return (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_'
		}) >= 0 {
			return "", usageErr(errors.New("explicit credential environment variable required"))
		}
		v := os.Getenv(name)
		if v == "" {
			return "", errors.New("required acquisition credential is unavailable")
		}
		return v, nil
	}
	publisher, err := token(*publisherEnv)
	if err != nil {
		return err
	}
	bot, err := token(*botEnv)
	if err != nil {
		return err
	}
	opts := acquisition.Options{Guild: *guild, Channel: *channel, MaxPages: *maxPages}
	for _, bound := range []struct {
		value  string
		target *time.Time
	}{{*since, &opts.Since}, {*horizon, &opts.Horizon}} {
		value, target := bound.value, bound.target
		if value != "" {
			*target, err = time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return usageErr(errors.New("acquire time must be UTC RFC3339"))
			}
		}
	}
	api, err := acquisition.NewClient(*origin, *source, publisher)
	if err != nil {
		return usageErr(err)
	}
	provider, err := discord.New(bot)
	if err != nil {
		return errors.New("discord bot client could not be initialized")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	result, err := acquisition.Collect(ctx, api, provider, opts)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(result)
}
