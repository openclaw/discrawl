package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/openclaw/crawlkit/control"
	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/config"
)

type remoteArchiveClient interface {
	Archives(context.Context) ([]crawlremote.Archive, error)
	Query(context.Context, string, string, crawlremote.QueryRequest) (crawlremote.QueryResult, error)
	Status(context.Context, string, string) (crawlremote.Status, error)
	Whoami(context.Context) (crawlremote.Identity, error)
}

func (r *runtime) runSubscribeCloud(args []string) error {
	fs := flag.NewFlagSet("subscribe-cloud", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	endpoint := fs.String("endpoint", "", "")
	archive := fs.String("archive", "", "")
	dbPath := fs.String("db", "", "")
	tokenEnv := fs.String("token-env", config.DefaultRemoteTokenEnv, "")
	staleAfter := fs.String("stale-after", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() > 1 {
		return usageErr(errors.New("subscribe-cloud accepts at most one endpoint"))
	}
	if fs.NArg() == 1 {
		if *endpoint != "" {
			return usageErr(errors.New("use either --endpoint or a positional endpoint"))
		}
		*endpoint = fs.Arg(0)
	}
	if strings.TrimSpace(*endpoint) == "" {
		return usageErr(errors.New("subscribe-cloud requires --endpoint"))
	}
	if strings.TrimSpace(*archive) == "" {
		return usageErr(errors.New("subscribe-cloud requires --archive"))
	}
	cfg, err := loadConfigOrDefault(r.configPath)
	if err != nil {
		return err
	}
	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}
	cfg.Remote.Mode = crawlremote.ModeCloud
	cfg.Remote.Endpoint = *endpoint
	cfg.Remote.Archive = *archive
	cfg.Remote.TokenEnv = *tokenEnv
	cfg.Remote.StaleAfter = *staleAfter
	cfg.Discord.TokenSource = "none"
	if err := config.Write(r.configPath, cfg); err != nil {
		return configErr(err)
	}
	return r.print(map[string]any{
		"config_path": r.configPath,
		"mode":        crawlremote.ModeCloud,
		"endpoint":    strings.TrimRight(strings.TrimSpace(*endpoint), "/"),
		"archive":     strings.TrimSpace(*archive),
		"token_env":   strings.TrimSpace(*tokenEnv),
		"db_path":     cfg.DBPath,
	})
}

func (r *runtime) runRemote(args []string) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		return printCommandUsage(r.stdout, []string{"remote"})
	}
	switch args[0] {
	case "status":
		return r.withConfig(func() error {
			if len(args) != 1 {
				return usageErr(errors.New("remote status takes no arguments"))
			}
			return r.runRemoteStatusOutput()
		})
	case "archives":
		return r.withConfig(func() error {
			if len(args) != 1 {
				return usageErr(errors.New("remote archives takes no arguments"))
			}
			return r.runRemoteArchives()
		})
	case "whoami":
		return r.withConfig(func() error {
			return r.runRemoteWhoami(args[1:])
		})
	default:
		return usageErr(fmt.Errorf("unknown remote subcommand %q", args[0]))
	}
}

func (r *runtime) runRemoteStatusOutput() error {
	client, err := r.remoteClient(true)
	if err != nil {
		return err
	}
	status, err := client.Status(r.ctx, "discrawl", r.cfg.Remote.Archive)
	if err != nil {
		return err
	}
	return r.print(remoteControlStatus(r.configPath, r.cfg, status))
}

func (r *runtime) runRemoteArchives() error {
	client, err := r.remoteClient(false)
	if err != nil {
		return err
	}
	archives, err := client.Archives(r.ctx)
	if err != nil {
		return err
	}
	return r.print(map[string]any{"archives": archives})
}

func (r *runtime) runRemoteWhoami(args []string) error {
	if len(args) != 0 {
		return usageErr(errors.New("whoami takes no arguments"))
	}
	client, err := r.remoteClient(false)
	if err != nil {
		return err
	}
	identity, err := client.Whoami(r.ctx)
	if err != nil {
		return err
	}
	return r.print(identity)
}

func (r *runtime) remoteClient(requireArchive bool) (remoteArchiveClient, error) {
	if strings.TrimSpace(r.cfg.Remote.Endpoint) == "" {
		return nil, configErr(errors.New("remote.endpoint is required"))
	}
	if requireArchive && strings.TrimSpace(r.cfg.Remote.Archive) == "" {
		return nil, configErr(errors.New("remote.archive is required"))
	}
	if r.newRemote != nil {
		return r.newRemote(r.cfg)
	}
	client, err := crawlremote.NewClientFromConfig(r.cfg.Remote, crawlremote.Options{
		UserAgent: "discrawl/" + version,
	})
	if err != nil {
		return nil, configErr(err)
	}
	return client, nil
}

func remoteControlStatus(configPath string, cfg config.Config, status crawlremote.Status) control.Status {
	counts := append([]control.Count(nil), status.Counts...)
	archive := firstNonEmpty(status.Archive, cfg.Remote.Archive)
	summary := fmt.Sprintf("remote archive %s", archive)
	if messages := countValue(counts, "messages"); messages > 0 {
		summary = fmt.Sprintf("%d messages in remote archive %s", messages, archive)
	}
	out := control.NewStatus("discrawl", summary)
	out.State = "current"
	out.ConfigPath = configPath
	out.Counts = counts
	out.LastSyncAt = firstNonEmpty(status.LastSyncAt, status.LastIngestAt)
	out.Warnings = append([]string(nil), status.Warnings...)
	out.Remote = &control.Remote{
		Enabled:      true,
		Mode:         firstNonEmpty(status.Mode, cfg.Remote.Mode),
		Endpoint:     strings.TrimRight(strings.TrimSpace(cfg.Remote.Endpoint), "/"),
		Archive:      archive,
		LastIngestAt: status.LastIngestAt,
		LastSyncAt:   status.LastSyncAt,
	}
	out.Share = &control.Share{
		Enabled:  cfg.ShareEnabled(),
		RepoPath: cfg.Share.RepoPath,
		Remote:   cfg.Share.Remote,
		Branch:   cfg.Share.Branch,
	}
	out.Databases = []control.Database{
		control.RemoteDatabase("remote", "Discord cloud archive", "archive", "cloudflare-d1", cfg.Remote.Endpoint, archive, true, counts),
	}
	return out
}

func countValue(counts []control.Count, id string) int64 {
	for _, count := range counts {
		if count.ID == id {
			return count.Value
		}
	}
	return 0
}
