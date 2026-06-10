package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/openclaw/crawlkit/embed"
	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/discorddesktop"
	"github.com/openclaw/discrawl/internal/media"
	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/openclaw/discrawl/internal/syncer"
)

type syncSources struct {
	name    string
	discord bool
	wiretap bool
}

type syncRunStats struct {
	Source  string                `json:"source"`
	Discord *syncer.SyncStats     `json:"discord,omitempty"`
	Wiretap *discorddesktop.Stats `json:"wiretap,omitempty"`
	Media   *media.FetchStats     `json:"media,omitempty"`
}

func (r *runtime) runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	guildID := fs.String("guild", "", "")
	dbPath := fs.String("db", "", "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	cfg := config.Default()
	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}
	cfg.Search.Embeddings.Enabled = *withEmbeddings
	if err := cfg.Normalize(); err != nil {
		return configErr(err)
	}
	token, err := config.ResolveDiscordToken(cfg)
	if err != nil {
		return authErr(err)
	}
	discordFactory := r.newDiscord
	if discordFactory == nil {
		discordFactory = func(cfg config.Config) (discordClient, error) {
			return discord.New(token.Token)
		}
	}
	client, err := discordFactory(cfg)
	if err != nil {
		return authErr(err)
	}
	defer func() { _ = client.Close() }()
	syncerFactory := r.newSyncer
	if syncerFactory == nil {
		syncerFactory = func(client syncer.Client, s *store.Store, logger *slog.Logger) syncService {
			return syncer.New(client, s, logger)
		}
	}
	syncerSvc := syncerFactory(client, nil, r.logger)
	guilds, err := syncerSvc.DiscoverGuilds(r.ctx)
	if err != nil {
		return authErr(err)
	}
	cfg.GuildIDs = make([]string, 0, len(guilds))
	discovered := make(map[string]struct{}, len(guilds))
	for _, guild := range guilds {
		cfg.GuildIDs = append(cfg.GuildIDs, guild.ID)
		discovered[guild.ID] = struct{}{}
	}
	if *guildID != "" {
		if _, ok := discovered[*guildID]; !ok {
			return usageErr(fmt.Errorf("guild %s is not accessible", *guildID))
		}
		cfg.DefaultGuildID = *guildID
	}
	if cfg.DefaultGuildID == "" && len(cfg.GuildIDs) == 1 {
		cfg.DefaultGuildID = cfg.GuildIDs[0]
	}
	if err := config.Write(r.configPath, cfg); err != nil {
		return configErr(err)
	}
	return r.print(map[string]any{
		"config_path":       r.configPath,
		"db_path":           cfg.DBPath,
		"token_source":      token.Source,
		"default_guild_id":  cfg.DefaultGuildID,
		"discovered_guilds": cfg.GuildIDs,
	})
}

func (r *runtime) runSync(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	full := fs.Bool("full", false, "")
	all := fs.Bool("all", false, "")
	allChannels := fs.Bool("all-channels", false, "")
	since := fs.String("since", "", "")
	channels := fs.String("channels", "", "")
	concurrency := fs.Int("concurrency", r.cfg.Sync.Concurrency, "")
	source := fs.String("source", r.cfg.Sync.Source, "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	withMedia := fs.Bool("with-media", r.cfg.AttachmentMediaEnabled(), "")
	skipMembers := fs.Bool("skip-members", false, "")
	latestOnly := fs.Bool("latest-only", false, "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	updateMode := fs.String("update", "", "")
	noUpdate := fs.Bool("no-update", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if *noUpdate && strings.TrimSpace(*updateMode) != "" && !strings.EqualFold(strings.TrimSpace(*updateMode), string(shareUpdateNever)) {
		return usageErr(errors.New("use either --no-update or --update, not both"))
	}
	if strings.TrimSpace(*updateMode) != "" {
		if _, err := parseShareUpdateMode(*updateMode); err != nil {
			return usageErr(err)
		}
	}
	sources, err := parseSyncSources(*source)
	if err != nil {
		return usageErr(err)
	}
	var sinceTime time.Time
	if *since != "" {
		parsed, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			return usageErr(fmt.Errorf("invalid --since: %w", err))
		}
		sinceTime = parsed
	}
	guildIDs, err := r.resolveSyncGuildsAll(*guildFlag, *guildsFlag, *all)
	if err != nil {
		return usageErr(err)
	}
	defaultLatest := defaultLatestSyncMode(*full, *allChannels, *since, *channels)
	opts := syncer.SyncOptions{
		Full:        *full,
		GuildIDs:    guildIDs,
		ChannelIDs:  csvList(*channels),
		Concurrency: *concurrency,
		Since:       sinceTime,
		Embeddings:  *withEmbeddings,
		SkipMembers: syncSkipsMembers(*skipMembers, defaultLatest),
		LatestOnly:  syncLatestOnly(*latestOnly, defaultLatest),
	}
	return r.withSyncLock(func() error {
		return r.runSyncLocked(sources, opts, *withMedia)
	})
}

func (r *runtime) runSyncLocked(sources syncSources, opts syncer.SyncOptions, withMedia bool) error {
	var apiStats *syncer.SyncStats
	if sources.discord {
		r.setSyncLockPhase("discord sync")
		shouldClose := r.client == nil
		if err := r.ensureDiscordServices(); err != nil {
			return err
		}
		if shouldClose && r.client != nil {
			defer func() { _ = r.client.Close() }()
		}
		stats, err := r.syncer.Sync(r.ctx, opts)
		if err != nil {
			return err
		}
		apiStats = &stats
	}
	var wiretapStats *discorddesktop.Stats
	if sources.wiretap {
		r.setSyncLockPhase("wiretap import")
		stats, err := discorddesktop.Import(r.ctx, r.store, discorddesktop.Options{
			Path:         r.cfg.Desktop.Path,
			MaxFileBytes: r.cfg.Desktop.MaxFileBytes,
			FullCache:    r.cfg.Desktop.FullCache,
			Now:          r.now,
		})
		if err != nil {
			return err
		}
		wiretapStats = &stats
	}
	var mediaStats *media.FetchStats
	if withMedia {
		r.setSyncLockPhase("attachment media fetch")
		cacheDir, err := config.ExpandPath(r.cfg.CacheDir)
		if err != nil {
			return configErr(err)
		}
		channelIDs := opts.ChannelIDs
		if len(channelIDs) > 0 {
			channelIDs, err = r.store.ExpandAttachmentChannelIDs(r.ctx, channelIDs)
			if err != nil {
				return err
			}
		}
		stats, err := r.fetchSyncMedia(sources, opts, cacheDir, channelIDs)
		if err != nil {
			return err
		}
		mediaStats = stats
	}
	if sources.discord && !sources.wiretap && mediaStats == nil {
		return r.print(*apiStats)
	}
	if sources.wiretap && !sources.discord && mediaStats == nil {
		return r.print(*wiretapStats)
	}
	return r.print(syncRunStats{Source: sources.name, Discord: apiStats, Wiretap: wiretapStats, Media: mediaStats})
}

func (r *runtime) fetchSyncMedia(sources syncSources, opts syncer.SyncOptions, cacheDir string, channelIDs []string) (*media.FetchStats, error) {
	total := media.FetchStats{}
	if sources.discord {
		stats, err := media.Fetch(r.ctx, r.store, media.FetchOptions{
			CacheDir: cacheDir,
			MaxBytes: r.cfg.Sync.MaxAttachmentBytes,
			List: store.AttachmentListOptions{
				GuildIDs:        opts.GuildIDs,
				ExcludeGuildIDs: []string{store.DirectMessageGuildID},
				ChannelIDs:      channelIDs,
				Since:           opts.Since,
			},
			StatusUpdate: true,
			Now:          r.now,
		})
		if err != nil {
			return nil, err
		}
		total = addFetchStats(total, stats)
	}
	if sources.wiretap {
		stats, err := media.Fetch(r.ctx, r.store, media.FetchOptions{
			CacheDir: cacheDir,
			MaxBytes: r.cfg.Sync.MaxAttachmentBytes,
			List: store.AttachmentListOptions{
				GuildIDs: []string{store.DirectMessageGuildID},
			},
			StatusUpdate: true,
			Now:          r.now,
		})
		if err != nil {
			return nil, err
		}
		total = addFetchStats(total, stats)
	}
	return &total, nil
}

func addFetchStats(a, b media.FetchStats) media.FetchStats {
	a.Attachments += b.Attachments
	a.Fetched += b.Fetched
	a.Reused += b.Reused
	a.Skipped += b.Skipped
	a.Failed += b.Failed
	a.Bytes += b.Bytes
	return a
}

func defaultLatestSyncMode(full bool, allChannels bool, since string, channels string) bool {
	return !full && !allChannels && since == "" && channels == ""
}

func syncLatestOnly(explicit bool, defaultLatest bool) bool {
	return explicit || defaultLatest
}

func syncSkipsMembers(explicit bool, defaultLatest bool) bool {
	return explicit || defaultLatest
}

func parseSyncSources(raw string) (syncSources, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		normalized = "both"
	}
	normalized = strings.ReplaceAll(normalized, "+", ",")
	parts := strings.Split(normalized, ",")
	out := syncSources{name: normalized}
	for _, part := range parts {
		switch strings.TrimSpace(part) {
		case "", "both", "all":
			out.discord = true
			out.wiretap = true
		case "discord", "api", "bot", "key":
			out.discord = true
		case "wiretap", "desktop", "cache":
			out.wiretap = true
		default:
			return syncSources{}, fmt.Errorf("invalid --source %q; use both, discord, or wiretap", raw)
		}
	}
	switch {
	case out.discord && out.wiretap:
		out.name = "both"
	case out.discord:
		out.name = "discord"
	case out.wiretap:
		out.name = "wiretap"
	default:
		return syncSources{}, fmt.Errorf("invalid --source %q; use both, discord, or wiretap", raw)
	}
	return out, nil
}

func (r *runtime) runTail(args []string) error {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repairEvery := fs.Duration("repair-every", mustDuration(r.cfg.Sync.RepairEvery), "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	ctx, stop := signal.NotifyContext(r.ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if configurable, ok := r.syncer.(tailReadyConfigurer); ok {
		configurable.SetTailReadyCallback(func(context.Context) error {
			return r.activateTailSyncLock()
		})
		defer configurable.SetTailReadyCallback(nil)
	}
	return r.syncer.RunTail(ctx, r.resolveSyncGuilds(*guildFlag, *guildsFlag), *repairEvery)
}

func (r *runtime) runWiretap(args []string) error {
	fs := flag.NewFlagSet("wiretap", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("path", r.cfg.Desktop.Path, "")
	maxFileBytes := fs.Int64("max-file-bytes", r.cfg.Desktop.MaxFileBytes, "")
	fullCache := fs.Bool("full-cache", r.cfg.Desktop.FullCache, "")
	dryRun := fs.Bool("dry-run", false, "")
	watchEvery := fs.Duration("watch-every", 0, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("wiretap takes flags only"))
	}
	if *maxFileBytes <= 0 {
		return usageErr(errors.New("--max-file-bytes must be positive"))
	}
	runOnce := func(ctx context.Context) error {
		stats, err := discorddesktop.Import(ctx, r.store, discorddesktop.Options{
			Path:         *path,
			MaxFileBytes: *maxFileBytes,
			FullCache:    *fullCache,
			DryRun:       *dryRun,
			Now:          r.now,
		})
		if err != nil {
			return err
		}
		return r.print(stats)
	}
	if *watchEvery <= 0 {
		return runOnce(r.ctx)
	}
	if *watchEvery < time.Second {
		return usageErr(errors.New("--watch-every must be at least 1s"))
	}
	ctx, stop := signal.NotifyContext(r.ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(*watchEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := runOnce(ctx); err != nil {
				return err
			}
		}
	}
}

func (r *runtime) runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("status takes no arguments"))
	}
	if *jsonOut {
		r.json = true
	}
	if r.cfg.RemoteCloudReadOnly() {
		return r.runRemoteStatusOutput()
	}
	dbPath, err := config.ExpandPath(r.cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	status := store.Status{DBPath: dbPath, DefaultGuildID: r.cfg.EffectiveDefaultGuildID()}
	if r.store != nil {
		status, err = r.store.Status(r.ctx, dbPath, r.cfg.EffectiveDefaultGuildID())
		if err != nil {
			return err
		}
	}
	if r.json {
		needsUpdate := false
		if r.store != nil && r.cfg.ShareEnabled() {
			if staleAfter, err := time.ParseDuration(r.cfg.Share.StaleAfter); err == nil {
				needsUpdate = share.NeedsImport(r.ctx, r.store, staleAfter)
			}
		}
		return r.print(controlStatus(r.configPath, r.cfg, status, needsUpdate))
	}
	return r.print(status)
}

func (r *runtime) runEmbed(args []string) error {
	fs := flag.NewFlagSet("embed", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	limit := fs.Int("limit", store.DefaultEmbedLimit(), "")
	batchSize := fs.Int("batch-size", r.cfg.Search.Embeddings.BatchSize, "")
	rebuild := fs.Bool("rebuild", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("embed takes no positional arguments"))
	}
	if *limit <= 0 {
		return usageErr(errors.New("--limit must be positive"))
	}
	if *batchSize <= 0 {
		return usageErr(errors.New("--batch-size must be positive"))
	}
	if !r.cfg.Search.Embeddings.Enabled {
		return usageErr(errors.New("embeddings are disabled in config"))
	}
	providerFactory := r.newEmbed
	if providerFactory == nil {
		providerFactory = func(cfg config.EmbeddingsConfig) (embed.Provider, error) {
			return embed.NewProvider(crawlkitEmbeddingConfig(cfg))
		}
	}
	provider, err := providerFactory(r.cfg.Search.Embeddings)
	if err != nil {
		return configErr(err)
	}
	opts := store.EmbeddingDrainOptions{
		Provider:      r.cfg.Search.Embeddings.Provider,
		Model:         r.cfg.Search.Embeddings.Model,
		InputVersion:  store.EmbeddingInputVersion,
		Limit:         *limit,
		BatchSize:     *batchSize,
		MaxInputChars: r.cfg.Search.Embeddings.MaxInputChars,
		Now:           r.now,
	}
	requeued := 0
	if *rebuild {
		requeued, err = r.store.RequeueAllEmbeddingJobs(r.ctx, opts)
		if err != nil {
			return err
		}
	}
	stats, err := r.store.DrainEmbeddingJobs(r.ctx, provider, opts)
	if err != nil {
		return err
	}
	stats.Requeued = requeued
	return r.print(stats)
}

func (r *runtime) runDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("doctor takes no arguments"))
	}
	if *jsonOut {
		r.json = true
	}
	report := map[string]any{
		"config_path": r.configPath,
	}
	cfg, err := config.Load(r.configPath)
	if err != nil {
		report["config"] = err.Error()
		return r.print(report)
	}
	report["config"] = "ok"
	report["default_guild_id"] = cfg.EffectiveDefaultGuildID()
	if cfg.ShareEnabled() {
		report["share_remote"] = cfg.Share.Remote
		report["share_repo_path"] = cfg.Share.RepoPath
		report["share_auto_update"] = cfg.Share.AutoUpdate
		report["share_stale_after"] = cfg.Share.StaleAfter
	}
	if cfg.Search.Embeddings.Enabled {
		check := embed.CheckProvider(r.ctx, crawlkitEmbeddingConfig(cfg.Search.Embeddings))
		report["embeddings"] = check.Status
		report["embeddings_provider"] = check.Provider
		report["embeddings_model"] = check.Model
		report["embeddings_base_url"] = check.BaseURL
		if check.Probed {
			report["embeddings_probe"] = "ok"
		}
		if check.Warning != "" {
			report["embeddings_warning"] = check.Warning
		}
	} else {
		report["embeddings"] = "disabled"
	}
	token, err := config.ResolveDiscordToken(cfg)
	if err != nil {
		if cfg.Discord.TokenSource == "none" && cfg.ShareEnabled() {
			report["discord_token"] = "disabled (git share mode)"
		} else {
			report["discord_token"] = err.Error()
		}
	} else {
		report["discord_token"] = token.Source
		discordFactory := r.newDiscord
		if discordFactory == nil {
			discordFactory = func(cfg config.Config) (discordClient, error) {
				return discord.New(token.Token)
			}
		}
		client, clientErr := discordFactory(cfg)
		if clientErr == nil {
			defer func() { _ = client.Close() }()
			self, authErr := client.Self(r.ctx)
			if authErr != nil {
				report["discord_auth"] = authErr.Error()
			} else {
				report["discord_auth"] = "ok"
				report["bot_user_id"] = self.ID
			}
			guilds, guildErr := client.Guilds(r.ctx)
			if guildErr != nil {
				report["guild_access"] = guildErr.Error()
			} else {
				report["guild_access"] = len(guilds)
			}
		}
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err == nil {
		db, dbErr := store.Open(r.ctx, dbPath)
		if dbErr != nil {
			report["database"] = dbErr.Error()
		} else {
			report["database"] = "ok"
			ftsErr := db.CheckMessageFTS(r.ctx)
			if ftsErr != nil {
				report["fts"] = ftsErr.Error()
			} else {
				report["fts"] = "ok"
			}
			report["vector"] = "not configured"
			_ = db.Close()
		}
	}
	return r.print(report)
}
