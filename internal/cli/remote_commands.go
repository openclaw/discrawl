package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	goruntime "runtime"
	"strings"
	"time"

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
	case "login":
		return r.runRemoteLogin(args[1:])
	case "whoami":
		return r.withConfig(func() error {
			return r.runRemoteWhoami(args[1:])
		})
	default:
		return usageErr(fmt.Errorf("unknown remote subcommand %q", args[0]))
	}
}

func (r *runtime) runRemoteLogin(args []string) error {
	fs := flag.NewFlagSet("remote login", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	endpoint := fs.String("endpoint", "", "")
	githubTokenEnv := fs.String("github-token-env", "", "")
	noBrowser := fs.Bool("no-browser", false, "")
	timeoutRaw := fs.String("timeout", "5m", "")
	pollRaw := fs.String("poll-interval", "2s", "")
	jsonOut := fs.Bool("json", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if *jsonOut {
		r.json = true
	}
	if fs.NArg() > 1 {
		return usageErr(errors.New("remote login accepts at most one endpoint"))
	}
	if fs.NArg() == 1 {
		if *endpoint != "" {
			return usageErr(errors.New("use either --endpoint or a positional endpoint"))
		}
		*endpoint = fs.Arg(0)
	}
	timeout, err := time.ParseDuration(*timeoutRaw)
	if err != nil || timeout <= 0 {
		return usageErr(fmt.Errorf("invalid --timeout %q", *timeoutRaw))
	}
	pollInterval, err := time.ParseDuration(*pollRaw)
	if err != nil || pollInterval <= 0 {
		return usageErr(fmt.Errorf("invalid --poll-interval %q", *pollRaw))
	}
	cfg, err := loadConfigOrDefault(r.configPath)
	if err != nil {
		return err
	}
	if strings.TrimSpace(*endpoint) != "" {
		cfg.Remote.Endpoint = *endpoint
	}
	cfg.Remote.Normalize()
	if strings.TrimSpace(cfg.Remote.Endpoint) == "" {
		return usageErr(errors.New("remote login requires --endpoint or remote.endpoint"))
	}
	client, err := crawlremote.NewClientFromConfig(cfg.Remote, crawlremote.Options{UserAgent: discrawlUserAgent()})
	if err != nil {
		return configErr(err)
	}
	if tokenEnv := strings.TrimSpace(*githubTokenEnv); tokenEnv != "" {
		githubToken := strings.TrimSpace(os.Getenv(tokenEnv))
		if githubToken == "" {
			return fmt.Errorf("%s is empty", tokenEnv)
		}
		result, err := client.LoginWithGitHubToken(r.ctx, githubToken)
		if err != nil {
			return err
		}
		return r.finishRemoteLogin(cfg, "github-token", result)
	}
	pollSecret, err := crawlremote.NewLoginPollSecret()
	if err != nil {
		return err
	}
	start, err := client.StartGitHubLogin(r.ctx, crawlremote.LoginPollSecretHash(pollSecret))
	if err != nil {
		return err
	}
	if !*noBrowser {
		if err := openURL(r.ctx, start.URL); err != nil && !r.json {
			_, _ = fmt.Fprintf(r.stdout, "Open this URL to continue login:\n%s\n", start.URL)
		}
	} else if !r.json {
		_, _ = fmt.Fprintf(r.stdout, "Open this URL to continue login:\n%s\n", start.URL)
	}
	result, err := pollRemoteLogin(r.ctx, client, start.LoginID, pollSecret, timeout, pollInterval)
	if err != nil {
		return err
	}
	return r.finishRemoteLogin(cfg, "github-oauth", result)
}

func (r *runtime) finishRemoteLogin(cfg config.Config, method string, result crawlremote.LoginPollResult) error {
	if strings.ToLower(strings.TrimSpace(result.Status)) != "complete" {
		return fmt.Errorf("remote login returned status %q", result.Status)
	}
	if strings.TrimSpace(result.Token) == "" {
		return errors.New("remote login completed without token")
	}
	auth, err := config.StoreRemoteToken(cfg, result.Token)
	if err != nil {
		return configErr(fmt.Errorf("store remote token: %w", err))
	}
	if cfg.Remote.Mode == "" || cfg.Remote.Mode == crawlremote.ModeLocal {
		cfg.Remote.Mode = crawlremote.ModeCloud
	}
	cfg.Remote.Auth = auth
	if err := config.Write(r.configPath, cfg); err != nil {
		return configErr(err)
	}
	return r.print(map[string]any{
		"config_path":     r.configPath,
		"endpoint":        cfg.Remote.Endpoint,
		"archive":         cfg.Remote.Archive,
		"login":           result.Login,
		"org":             result.Org,
		"owner":           result.Owner,
		"login_method":    method,
		"auth_source":     cfg.Remote.Auth.TokenSource,
		"keyring_service": cfg.Remote.Auth.KeyringService,
		"keyring_account": cfg.Remote.Auth.KeyringAccount,
		"updated":         true,
	})
}

func pollRemoteLogin(ctx context.Context, client *crawlremote.Client, loginID, pollSecret string, timeout, interval time.Duration) (crawlremote.LoginPollResult, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result, err := client.PollGitHubLogin(ctx, loginID, pollSecret)
		if err != nil {
			return crawlremote.LoginPollResult{}, err
		}
		switch strings.ToLower(strings.TrimSpace(result.Status)) {
		case "complete":
			if strings.TrimSpace(result.Token) == "" {
				return crawlremote.LoginPollResult{}, errors.New("remote login completed without token")
			}
			return result, nil
		case "error":
			if result.Error != "" {
				return crawlremote.LoginPollResult{}, fmt.Errorf("remote login failed: %s", result.Error)
			}
			return crawlremote.LoginPollResult{}, errors.New("remote login failed")
		case "", "pending":
		default:
			return crawlremote.LoginPollResult{}, fmt.Errorf("remote login returned status %q", result.Status)
		}
		select {
		case <-ctx.Done():
			return crawlremote.LoginPollResult{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func openURL(ctx context.Context, rawURL string) error {
	var cmd *exec.Cmd
	switch goruntime.GOOS {
	case "darwin":
		// #nosec G204 -- fixed browser launcher executable; rawURL is passed as argv, not shell text.
		cmd = exec.CommandContext(ctx, "open", rawURL)
	case "windows":
		// #nosec G204 -- fixed browser launcher executable; rawURL is passed as argv, not shell text.
		cmd = exec.CommandContext(ctx, "rundll32", "url.dll,FileProtocolHandler", rawURL)
	default:
		// #nosec G204 -- fixed browser launcher executable; rawURL is passed as argv, not shell text.
		cmd = exec.CommandContext(ctx, "xdg-open", rawURL)
	}
	return cmd.Start()
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
	tokenProvider := crawlremote.TokenProvider(crawlremote.EnvTokenProvider{Name: r.cfg.Remote.TokenEnv})
	if token, err := config.ResolveRemoteToken(r.cfg); err != nil {
		return nil, configErr(err)
	} else if token.Token != "" {
		tokenProvider = crawlremote.StaticToken(token.Token)
	}
	client, err := crawlremote.NewClientFromConfig(r.cfg.Remote, crawlremote.Options{
		TokenProvider: tokenProvider,
		UserAgent:     discrawlUserAgent(),
	})
	if err != nil {
		return nil, configErr(err)
	}
	return client, nil
}

func remoteControlStatus(configPath string, cfg config.Config, status crawlremote.Status) control.Status {
	counts := append([]control.Count(nil), status.Counts...)
	archive := firstNonEmpty(status.Archive, cfg.Remote.Archive)
	summary := "remote archive " + archive
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
