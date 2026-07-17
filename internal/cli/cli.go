package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/crawlkit/embed"
	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/openclaw/discrawl/internal/syncer"
)

type cliError struct {
	code int
	err  error
}

func (e *cliError) Error() string {
	return e.err.Error()
}

func (e *cliError) Unwrap() error {
	return e.err
}

func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, context.Canceled) {
		return 1
	}
	var codeErr *cliError
	if errors.As(err, &codeErr) {
		return codeErr.code
	}
	return 1
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || rootHelpRequested(args, "config") {
		return printUsage(stdout)
	}
	var global discrawlRootArgs
	if err := parseKongArgs(&global, args, "discrawl", stdout, stderr); err != nil {
		return usageErr(err)
	}
	if global.Version {
		_, _ = io.WriteString(stdout, currentVersion()+"\n")
		return nil
	}
	rest := global.Args
	if len(rest) == 0 || rest[0] == "--help" || rest[0] == "-h" || (rest[0] == "help" && len(rest) == 1) {
		return printUsage(stdout)
	}
	if rest[0] == "help" {
		return printCommandUsage(stdout, rest[1:])
	}
	if topic, ok := earlyCommandHelpTopic(rest); ok {
		return printCommandUsage(stdout, topic)
	}
	if rest[0] == "version" {
		_, _ = io.WriteString(stdout, currentVersion()+"\n")
		return nil
	}
	level := slog.LevelInfo
	if global.Quiet {
		level = slog.LevelError
	}
	if global.Verbose {
		level = slog.LevelDebug
	}
	runtime := &runtime{
		ctx:        ctx,
		configPath: config.ResolvePath(global.Config),
		stdout:     stdout,
		stderr:     stderr,
		json:       global.JSON,
		plain:      global.Plain,
		logger:     slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level})),
	}
	runtime.maybeNotifyRelease(rest)
	return runtime.dispatch(rest)
}

type discrawlGlobalArgs struct {
	Config  string `help:"Config path."`
	JSON    bool   `name:"json" help:"Write JSON output."`
	Plain   bool   `help:"Write stable plain text output when available."`
	Quiet   bool   `short:"q" help:"Only log errors."`
	Verbose bool   `short:"v" help:"Enable debug logging."`
	Version bool   `help:"Print version and exit."`
	NoColor bool   `name:"no-color" help:"Disable color output."`
}

type discrawlRootArgs struct {
	discrawlGlobalArgs
	Args []string `arg:"" optional:"" passthrough:"partial" name:"command" help:"Command and arguments."`
}

type discrawlHelpArgs struct {
	discrawlGlobalArgs
}

type discrawlCommandSpec struct {
	name        string
	description string
}

var discrawlCommandSpecs = []discrawlCommandSpec{
	{name: "metadata", description: "Print archive snapshot metadata."},
	{name: "check-update", description: "Check GitHub Releases for a newer Discrawl build."},
	{name: "version", description: "Print the Discrawl version."},
	{name: "init", description: "Initialize Discrawl configuration."},
	{name: "sync", description: "Sync Discord data into the local archive."},
	{name: "tail", description: "Continuously archive new Discord messages."},
	{name: "tap", description: "Import Discord Desktop cache data (wiretap alias)."},
	{name: "cache-import", description: "Import Discord Desktop cache data (wiretap alias)."},
	{name: "wiretap", description: "Import Discord Desktop cache data."},
	{name: "search", description: "Search archived messages."},
	{name: "tui", description: "Explore the archive in an interactive terminal UI."},
	{name: "messages", description: "List archived messages."},
	{name: "digest", description: "Summarize recent archive activity."},
	{name: "analytics", description: "Analyze archive activity and trends."},
	{name: "dms", description: "List local Discord Desktop conversations."},
	{name: "mentions", description: "List archived mentions."},
	{name: "attachments", description: "List or fetch archived attachments."},
	{name: "embed", description: "Generate embeddings for archived messages."},
	{name: "sql", description: "Run SQL queries against the local archive."},
	{name: "members", description: "Inspect archived Discord members."},
	{name: "channels", description: "Inspect and resolve archived channels."},
	{name: "status", description: "Show archive status and freshness."},
	{name: "diagnostics", description: "Report SQLite and sync-lock diagnostics."},
	{name: "coverage", description: "Report archive coverage."},
	{name: "failures", description: "List retained sync and import failures."},
	{name: "remote", description: "Access a configured remote archive."},
	{name: "whoami", description: "Show the configured remote identity."},
	{name: "report", description: "Generate archive reports."},
	{name: "publish", description: "Publish the configured archive snapshot."},
	{name: "doctor", description: "Check Discrawl configuration and dependencies."},
	{name: "cloud", description: "Manage a Cloudflare-backed remote archive."},
	{name: "subscribe", description: "Configure a read-only snapshot subscription."},
	{name: "subscribe-cloud", description: "Configure a read-only cloud archive."},
	{name: "update", description: "Update the configured archive snapshot."},
}

func newDiscrawlHelpParser(stdout io.Writer) (*kong.Kong, error) {
	var root discrawlHelpArgs
	var command struct{}
	options := []kong.Option{
		kong.Name("discrawl"),
		kong.Description("discrawl archives Discord guild data into local SQLite."),
		kong.Writers(stdout, io.Discard),
		kong.Exit(func(int) {}),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true, NoExpandSubcommands: true}),
	}
	for _, spec := range discrawlCommandSpecs {
		options = append(options, kong.DynamicCommand(spec.name, spec.description, "", &command))
	}
	return kong.New(&root, options...)
}

func printKongUsage(stdout io.Writer) error {
	parser, err := newDiscrawlHelpParser(stdout)
	if err != nil {
		return err
	}
	_, _ = parser.Parse([]string{"--help"})
	return nil
}

func earlyCommandHelpTopic(rest []string) ([]string, bool) {
	if len(rest) < 2 || rest[0] == "tui" || !hasHelpFlag(rest[1:]) {
		return nil, false
	}
	if len(rest) >= 3 {
		topic := canonicalHelpTopic(strings.Join(rest[:2], " "))
		if _, ok := commandUsage[topic]; ok {
			return strings.Fields(topic), true
		}
	}
	topic := canonicalHelpTopic(rest[0])
	if _, ok := commandUsage[topic]; !ok {
		return nil, false
	}
	return []string{topic}, true
}

func canonicalHelpTopic(topic string) string {
	if topic == "tap" || topic == "cache-import" {
		return "wiretap"
	}
	if topic == "attachments fetch" {
		return "attachments"
	}
	return topic
}

func rootHelpRequested(args []string, valueFlags ...string) bool {
	valueFlagSet := make(map[string]struct{}, len(valueFlags))
	for _, flag := range valueFlags {
		valueFlagSet[flag] = struct{}{}
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--help" || arg == "-h" || (arg == "help" && i == len(args)-1) {
			return true
		}
		if !strings.HasPrefix(arg, "-") {
			return false
		}
		if name, ok := strings.CutPrefix(arg, "--"); ok {
			if strings.Contains(name, "=") {
				continue
			}
			if _, ok := valueFlagSet[name]; ok {
				i++
			}
		}
	}
	return false
}

func parseKongArgs(target any, args []string, name string, stdout, stderr io.Writer, options ...kong.Option) error {
	opts := []kong.Option{
		kong.Name(name),
		kong.NoDefaultHelp(),
		kong.Writers(stdout, stderr),
		kong.Exit(func(int) {}),
	}
	opts = append(opts, options...)
	parser, err := kong.New(target, opts...)
	if err != nil {
		return err
	}
	_, err = parser.Parse(args)
	return err
}

type runtime struct {
	ctx           context.Context
	configPath    string
	cfg           config.Config
	stdout        io.Writer
	stderr        io.Writer
	json          bool
	plain         bool
	logger        *slog.Logger
	store         *store.Store
	client        discordClient
	syncer        syncService
	dbLockHeld    bool
	lockStarted   time.Time
	lockOperation string
	lockToken     string
	lockTokenFree func() error
	openStore     func(context.Context, string) (*store.Store, error)
	newDiscord    func(config.Config) (discordClient, error)
	newRemote     func(config.Config) (remoteArchiveClient, error)
	newSyncer     func(syncer.Client, *store.Store, *slog.Logger) syncService
	newEmbed      func(config.EmbeddingsConfig) (embed.Provider, error)
	now           func() time.Time
}

func crawlkitEmbeddingConfig(cfg config.EmbeddingsConfig) embed.Config {
	return embed.Config{
		Provider:       cfg.Provider,
		Model:          cfg.Model,
		BaseURL:        cfg.BaseURL,
		APIKeyEnv:      cfg.APIKeyEnv,
		RequestTimeout: cfg.RequestTimeout,
		MaxInputChars:  cfg.MaxInputChars,
	}
}

type discordClient interface {
	syncer.Client
	Close() error
	Self(context.Context) (*discordgo.User, error)
	Guilds(context.Context) ([]*discordgo.UserGuild, error)
}

type syncService interface {
	DiscoverGuilds(context.Context) ([]*discordgo.UserGuild, error)
	Sync(context.Context, syncer.SyncOptions) (syncer.SyncStats, error)
	RunTail(context.Context, []string, time.Duration) error
}

type tailReadyConfigurer interface {
	SetTailReadyCallback(func(context.Context) error)
}

type tailMessageFailureReplayer interface {
	ReplayTailMessageFailures(context.Context, []string, int) (syncer.TailMessageReplayStats, error)
}

type attachmentTextConfigurer interface {
	SetAttachmentTextEnabled(bool)
}

func (r *runtime) dispatch(rest []string) error {
	switch rest[0] {
	case "metadata":
		return r.runMetadata(rest[1:])
	case "check-update":
		return r.runCheckUpdate(rest[1:])
	case "init":
		return r.runInit(rest[1:])
	case "sync":
		updateMode, err := syncShareUpdateMode(rest[1:])
		if err != nil {
			return usageErr(err)
		}
		return r.withLocalStoreUpdateLocked(updateMode, true, func() error { return r.runSync(rest[1:]) })
	case "tail":
		operation := "tail-starting"
		if tailReplayOnlyEnabled(rest[1:]) {
			operation = "tail-failure-replay"
		}
		return r.withServicesLockedOperation(true, operation, func() error { return r.runTail(rest[1:]) })
	case "wiretap":
		return r.withLocalStoreLocked(false, func() error { return r.runWiretap(rest[1:]) })
	case "tap", "cache-import":
		return r.withLocalStoreLocked(false, func() error { return r.runWiretap(rest[1:]) })
	case "search":
		if hasHelpFlag(rest[1:]) {
			return printCommandUsage(r.stdout, []string{"search"})
		}
		if r.configuredForCloudReadOnly() {
			return r.withConfig(func() error { return r.runSearch(rest[1:]) })
		}
		autoShareUpdate := !hasBoolFlag(rest[1:], "--dm")
		return r.withLocalStoreRead(autoShareUpdate, func() error { return r.runSearch(rest[1:]) })
	case "tui":
		if hasHelpArg(rest[1:]) {
			return r.runTUI(rest[1:])
		}
		return r.withLocalStoreReadOnly(func() error { return r.runTUI(rest[1:]) })
	case "messages":
		if hasHelpFlag(rest[1:]) {
			return printCommandUsage(r.stdout, []string{"messages"})
		}
		if r.configuredForCloudReadOnly() {
			return r.withConfig(func() error { return r.runMessages(rest[1:]) })
		}
		if boolFlagEnabled(rest[1:], "--sync") && !boolFlagEnabled(rest[1:], "--dm") {
			return r.withMessagesSyncServices(func() error { return r.runMessages(rest[1:]) })
		}
		autoShareUpdate := !boolFlagEnabled(rest[1:], "--dm")
		return r.withLocalStoreRead(autoShareUpdate, func() error { return r.runMessages(rest[1:]) })
	case "digest":
		return r.withLocalStoreRead(true, func() error { return r.runDigest(rest[1:]) })
	case "analytics":
		return r.runAnalytics(rest[1:])
	case "dms":
		return r.withLocalStoreRead(false, func() error { return r.runDirectMessages(rest[1:]) })
	case "mentions":
		return r.withLocalStoreRead(true, func() error { return r.runMentions(rest[1:]) })
	case "attachments":
		if hasHelpArg(rest[1:]) {
			return printCommandUsage(r.stdout, []string{"attachments"})
		}
		autoShareUpdate := !hasBoolFlag(rest[1:], "--dm")
		if len(rest) > 1 && rest[1] == "fetch" {
			return r.withLocalStoreLocked(autoShareUpdate, func() error { return r.runAttachments(rest[1:]) })
		}
		return r.withLocalStoreRead(autoShareUpdate, func() error { return r.runAttachments(rest[1:]) })
	case "embed":
		return r.withLocalStoreLocked(true, func() error { return r.runEmbed(rest[1:]) })
	case "sql":
		if hasHelpArg(rest[1:]) {
			return printCommandUsage(r.stdout, []string{"sql"})
		}
		if boolFlagEnabled(rest[1:], "--unsafe") {
			return r.withLocalStoreLocked(true, func() error { return r.runSQL(rest[1:]) })
		}
		return r.withLocalStoreRead(true, func() error { return r.runSQL(rest[1:]) })
	case "members":
		return r.withLocalStoreRead(true, func() error { return r.runMembers(rest[1:]) })
	case "channels":
		return r.withLocalStoreRead(true, func() error { return r.runChannels(rest[1:]) })
	case "status":
		if r.configuredForCloudReadOnly() {
			return r.withConfig(func() error { return r.runStatus(rest[1:]) })
		}
		return r.withLocalStoreReadOnly(func() error { return r.runStatus(rest[1:]) })
	case "diagnostics":
		return r.withConfig(func() error { return r.runDiagnostics(rest[1:]) })
	case "coverage":
		if hasHelpFlag(rest[1:]) {
			return printCommandUsage(r.stdout, []string{"coverage"})
		}
		return r.withLocalStoreReadOnly(func() error { return r.runCoverage(rest[1:]) })
	case "failures":
		if hasHelpFlag(rest[1:]) {
			return printCommandUsage(r.stdout, []string{"failures"})
		}
		return r.withLocalStoreReadOnly(func() error { return r.runFailures(rest[1:]) })
	case "report":
		return r.withLocalStoreRead(true, func() error { return r.runReport(rest[1:]) })
	case "publish":
		if boolFlagEnabled(rest[1:], "--check") || boolFlagEnabled(rest[1:], "-check") {
			return r.withLocalStoreReadOnly(func() error { return r.runPublish(rest[1:]) })
		}
		return r.withServicesAutoLocked(false, false, true, func() error { return r.runPublish(rest[1:]) })
	case "cloud":
		return r.runCloud(rest[1:])
	case "subscribe":
		return r.runSubscribe(rest[1:])
	case "subscribe-cloud":
		return r.runSubscribeCloud(rest[1:])
	case "remote":
		return r.runRemote(rest[1:])
	case "whoami":
		return r.withConfig(func() error { return r.runRemoteWhoami(rest[1:]) })
	case "update":
		return r.withServicesAutoLocked(false, false, true, func() error { return r.runUpdate(rest[1:]) })
	case "doctor":
		return r.runDoctor(rest[1:])
	default:
		return usageErr(fmt.Errorf("unknown command %q", rest[0]))
	}
}

func tailReplayOnlyEnabled(args []string) bool {
	enabled := false
	for _, arg := range args {
		for _, name := range []string{"--replay-failures-only", "-replay-failures-only"} {
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
	}
	return enabled
}

func (r *runtime) configuredForCloudReadOnly() bool {
	cfg, err := config.Load(r.configPath)
	return err == nil && cfg.RemoteCloudReadOnly()
}

func (r *runtime) withConfig(fn func() error) error {
	cfg, err := config.Load(r.configPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return configErr(err)
		}
		cfg = config.Default()
		if err := cfg.Normalize(); err != nil {
			return configErr(err)
		}
	}
	r.cfg = cfg
	return fn()
}

func (r *runtime) withServices(withDiscord bool, fn func() error) error {
	return r.withServicesAuto(withDiscord, !withDiscord, fn)
}

func (r *runtime) withServicesLockedOperation(withDiscord bool, operation string, fn func() error) error {
	return r.withServicesUpdateLockedOperation(withDiscord, boolShareUpdateMode(!withDiscord), true, operation, fn)
}

func (r *runtime) withLocalStoreLocked(autoShareUpdate bool, fn func() error) error {
	return r.withLocalStoreUpdateLocked(boolShareUpdateMode(autoShareUpdate), true, fn)
}

func (r *runtime) withLocalStoreRead(autoShareUpdate bool, fn func() error) error {
	return r.withLocalStoreReadUpdate(boolShareUpdateMode(autoShareUpdate), fn)
}

func (r *runtime) withLocalStoreReadUpdate(updateMode shareUpdateMode, fn func() error) error {
	cfg, err := config.Load(r.configPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return configErr(err)
		}
		cfg = config.Default()
		if err := cfg.Normalize(); err != nil {
			return configErr(err)
		}
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return configErr(err)
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	r.cfg = cfg
	if r.shouldAutoUpdateShare(updateMode) {
		if err := r.autoUpdateShareIfLockAvailable(dbPath, updateMode); err != nil {
			return err
		}
	}
	return r.openLocalStoreReadOnly(dbPath, func() error {
		if r.store == nil {
			return dbErr(errors.New("command requires a local SQLite archive"))
		}
		return fn()
	})
}

func (r *runtime) withLocalStoreUpdateLocked(updateMode shareUpdateMode, lockDB bool, fn func() error) error {
	cfg, err := config.Load(r.configPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return configErr(err)
		}
		cfg = config.Default()
		if err := cfg.Normalize(); err != nil {
			return configErr(err)
		}
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return configErr(err)
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	r.cfg = cfg
	if lockDB {
		return r.withSyncLock(func() error {
			return r.openLocalStore(dbPath, updateMode, fn)
		})
	}
	return r.openLocalStore(dbPath, updateMode, fn)
}

func (r *runtime) shouldAutoUpdateShare(mode shareUpdateMode) bool {
	return os.Getenv("DISCRAWL_NO_AUTO_UPDATE") != "1" &&
		r.cfg.ShareEnabled() &&
		(mode == shareUpdateForce || mode == shareUpdateAuto || (mode == shareUpdateConfigured && r.cfg.Share.AutoUpdate))
}

func (r *runtime) autoUpdateShareIfLockAvailable(dbPath string, updateMode shareUpdateMode) error {
	locked, err := r.tryWithSyncLock(func() error {
		storeFactory := r.openStore
		if storeFactory == nil {
			storeFactory = store.Open
		}
		var openErr error
		r.store, openErr = storeFactory(r.ctx, dbPath)
		if openErr != nil {
			return dbErr(openErr)
		}
		defer func() {
			_ = r.store.Close()
			r.store = nil
		}()
		return r.autoUpdateShare(updateMode)
	})
	if err != nil {
		return err
	}
	if !locked {
		r.logger.Info("share update skipped; sync lock is held")
	}
	return nil
}

func (r *runtime) openLocalStore(dbPath string, updateMode shareUpdateMode, fn func() error) error {
	storeFactory := r.openStore
	if storeFactory == nil {
		storeFactory = store.Open
	}
	var err error
	r.store, err = storeFactory(r.ctx, dbPath)
	if err != nil {
		return dbErr(err)
	}
	defer func() { _ = r.store.Close() }()
	if updateMode != shareUpdateNever && os.Getenv("DISCRAWL_NO_AUTO_UPDATE") != "1" {
		if err := r.autoUpdateShare(updateMode); err != nil {
			return err
		}
	}
	return fn()
}

func (r *runtime) withLocalStoreReadOnly(fn func() error) error {
	cfg, err := config.Load(r.configPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return configErr(err)
		}
		cfg = config.Default()
		if err := cfg.Normalize(); err != nil {
			return configErr(err)
		}
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	r.cfg = cfg
	return r.openLocalStoreReadOnly(dbPath, fn)
}

func (r *runtime) withExistingLocalStoreReadOnly(fn func() error) error {
	cfg, err := config.Load(r.configPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return configErr(err)
		}
		cfg = config.Default()
		if err := cfg.Normalize(); err != nil {
			return configErr(err)
		}
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	r.cfg = cfg
	return r.openExistingLocalStoreReadOnly(dbPath, fn)
}

func (r *runtime) openLocalStoreReadOnly(dbPath string, fn func() error) error {
	r.store = nil
	s, err := store.OpenReadOnly(r.ctx, dbPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fn()
		}
		return dbErr(err)
	}
	r.store = s
	defer func() {
		_ = r.store.Close()
		r.store = nil
	}()
	return fn()
}

func (r *runtime) openExistingLocalStoreReadOnly(dbPath string, fn func() error) error {
	r.store = nil
	s, err := store.OpenReadOnly(r.ctx, dbPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fn()
		}
		return dbErr(err)
	}
	r.store = s
	defer func() {
		_ = r.store.Close()
		r.store = nil
	}()
	return fn()
}

func (r *runtime) withServicesAuto(withDiscord, autoShareUpdate bool, fn func() error) error {
	return r.withServicesAutoLocked(withDiscord, autoShareUpdate, false, fn)
}

func (r *runtime) withServicesAutoLocked(withDiscord, autoShareUpdate, lockDB bool, fn func() error) error {
	return r.withServicesUpdateLockedOperation(withDiscord, boolShareUpdateMode(autoShareUpdate), lockDB, "writer", fn)
}

func (r *runtime) withMessagesSyncServices(fn func() error) error {
	return r.withServicesUpdateLockedOperation(true, shareUpdateConfigured, true, "messages-sync", fn)
}

func (r *runtime) withServicesUpdateLockedOperation(withDiscord bool, updateMode shareUpdateMode, lockDB bool, operation string, fn func() error) error {
	cfg, err := config.Load(r.configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return configErr(fmt.Errorf("config file not found at %s; run `discrawl init`, pass `--config <path>`, or set %s", r.configPath, config.DefaultConfigEnv))
		}
		return configErr(err)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return configErr(err)
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	r.cfg = cfg
	if lockDB {
		lockFn := r.withSyncLockOperation
		if operation == "messages-sync" {
			lockFn = func(_ string, fn func() error) error {
				return r.withMessagesSyncLock(fn)
			}
		}
		return lockFn(operation, func() error {
			return r.openServices(dbPath, withDiscord, updateMode, fn)
		})
	}
	return r.openServices(dbPath, withDiscord, updateMode, fn)
}

func (r *runtime) openServices(dbPath string, withDiscord bool, updateMode shareUpdateMode, fn func() error) error {
	storeFactory := r.openStore
	if storeFactory == nil {
		storeFactory = store.Open
	}
	var err error
	r.store, err = storeFactory(r.ctx, dbPath)
	if err != nil {
		return dbErr(err)
	}
	defer func() { _ = r.store.Close() }()
	if updateMode != shareUpdateNever && os.Getenv("DISCRAWL_NO_AUTO_UPDATE") != "1" {
		if err := r.autoUpdateShare(updateMode); err != nil {
			return err
		}
	}
	if withDiscord {
		if err := r.ensureDiscordServices(); err != nil {
			return err
		}
		if r.client != nil {
			defer func() { _ = r.client.Close() }()
		}
	}
	return fn()
}

func (r *runtime) ensureDiscordServices() error {
	discordFactory := r.newDiscord
	if discordFactory == nil {
		discordFactory = func(cfg config.Config) (discordClient, error) {
			token, err := config.ResolveDiscordToken(cfg)
			if err != nil {
				return nil, err
			}
			return discord.New(token.Token)
		}
	}
	client, err := discordFactory(r.cfg)
	if err != nil {
		return authErr(err)
	}
	r.client = client
	syncerFactory := r.newSyncer
	if syncerFactory == nil {
		syncerFactory = func(client syncer.Client, s *store.Store, logger *slog.Logger) syncService {
			return syncer.New(client, s, logger)
		}
	}
	r.syncer = syncerFactory(r.client, r.store, r.logger)
	if configurable, ok := r.syncer.(attachmentTextConfigurer); ok {
		configurable.SetAttachmentTextEnabled(r.cfg.AttachmentTextEnabled())
	}
	return nil
}

func (r *runtime) autoUpdateShare(mode shareUpdateMode) error {
	if !r.cfg.ShareEnabled() || (mode == shareUpdateConfigured && !r.cfg.Share.AutoUpdate) {
		return nil
	}
	staleAfter, err := time.ParseDuration(r.cfg.Share.StaleAfter)
	if err != nil {
		return configErr(fmt.Errorf("invalid share.stale_after: %w", err))
	}
	if mode != shareUpdateForce && !share.NeedsImport(r.ctx, r.store, staleAfter) {
		return nil
	}
	opts, err := r.shareOptions()
	if err != nil {
		return err
	}
	r.setSyncLockPhase("share pull")
	r.logger.Info("share update pulling", "repo_path", opts.RepoPath, "remote", opts.Remote)
	if err := share.Pull(r.ctx, opts); err != nil {
		return err
	}
	r.setSyncLockPhase("share import")
	if mode == shareUpdateForce {
		_, _, err = share.Replace(r.ctx, r.store, opts)
	} else {
		_, _, err = share.MergeIfChanged(r.ctx, r.store, opts)
	}
	if errors.Is(err, share.ErrNoManifest) {
		return nil
	}
	if errors.Is(err, share.ErrReplacementRequired) && mode != shareUpdateForce {
		r.logger.Warn("share update requires forced exact reconciliation", "error", err)
		return nil
	}
	return err
}

func (r *runtime) shareOptions() (share.Options, error) {
	repoPath, err := config.ExpandPath(r.cfg.Share.RepoPath)
	if err != nil {
		return share.Options{}, configErr(err)
	}
	cacheDir, err := config.ExpandPath(r.cfg.CacheDir)
	if err != nil {
		return share.Options{}, configErr(err)
	}
	return share.Options{
		RepoPath:     repoPath,
		CacheDir:     cacheDir,
		Remote:       r.cfg.Share.Remote,
		Branch:       r.cfg.Share.Branch,
		IncludeMedia: r.cfg.ShareMediaEnabled(),
		Progress:     r.shareProgress,
	}, nil
}
