package cli

import (
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/report"
	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
)

func (r *runtime) runPublish(args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoPath := fs.String("repo", r.cfg.Share.RepoPath, "")
	remote := fs.String("remote", r.cfg.Share.Remote, "")
	branch := fs.String("branch", r.cfg.Share.Branch, "")
	message := fs.String("message", "", "")
	tag := fs.String("tag", "", "")
	readmePath := fs.String("readme", "", "")
	noCommit := fs.Bool("no-commit", false, "")
	push := fs.Bool("push", false, "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	noMedia := fs.Bool("no-media", !r.cfg.ShareMediaEnabled(), "")
	publicOnly := fs.Bool("public-only", r.cfg.Share.Filter.PublicOnly, "")
	includeChannels := fs.String("include-channels", strings.Join(r.cfg.Share.Filter.IncludeChannelIDs, ","), "")
	excludeChannels := fs.String("exclude-channels", strings.Join(r.cfg.Share.Filter.ExcludeChannelIDs, ","), "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("publish takes no positional arguments"))
	}
	if *noCommit && strings.TrimSpace(*tag) != "" {
		return usageErr(errors.New("publish --tag requires a commit"))
	}
	opts, err := shareOptionsFromFlags(*repoPath, *remote, *branch)
	if err != nil {
		return err
	}
	opts.Tag = strings.TrimSpace(*tag)
	if err := share.ValidateTag(r.ctx, opts); err != nil {
		return err
	}
	opts.Filter = share.FilterOptions{
		PublicOnly:        *publicOnly,
		IncludeChannelIDs: csvList(*includeChannels),
		ExcludeChannelIDs: csvList(*excludeChannels),
	}
	if *readmePath != "" && opts.Filter.Active() {
		return usageErr(errors.New("publish --readme is not supported with share filters; filtered report stats would otherwise leak the full archive"))
	}
	if *withEmbeddings {
		applyEmbeddingShareOptions(&opts, r.cfg)
	}
	if err := applyMediaShareOptions(&opts, r.cfg, !*noMedia); err != nil {
		return err
	}
	manifest, err := share.Export(r.ctx, r.store, opts)
	if err != nil {
		return err
	}
	if opts.Filter.Active() {
		if err := removeGeneratedReadmeForFilteredPublish(opts.RepoPath); err != nil {
			return err
		}
	}
	if *readmePath != "" {
		activity, err := report.Build(r.ctx, r.store, report.Options{})
		if err != nil {
			return err
		}
		section, err := report.RenderMarkdown(activity)
		if err != nil {
			return err
		}
		if err := report.WriteReadme(*readmePath, section); err != nil {
			return err
		}
	}
	committed := false
	if !*noCommit {
		msg := *message
		if msg == "" {
			msg = "sync: discord archive"
		}
		committed, err = share.Commit(r.ctx, opts, msg)
		if err != nil {
			return err
		}
	}
	createdTag, err := share.CreateImmutableTag(r.ctx, opts)
	if err != nil {
		return err
	}
	if *push {
		if err := share.Push(r.ctx, opts); err != nil {
			return err
		}
		if err := share.MarkImported(r.ctx, r.store, manifest); err != nil {
			return err
		}
	}
	return r.print(map[string]any{
		"repo_path":    opts.RepoPath,
		"remote":       opts.Remote,
		"generated_at": manifest.GeneratedAt,
		"tables":       manifest.Tables,
		"media":        manifest.Media,
		"embeddings":   manifest.Embeddings,
		"readme":       *readmePath,
		"committed":    committed,
		"tag":          createdTag,
		"pushed":       *push,
	})
}

func removeGeneratedReadmeForFilteredPublish(repoPath string) error {
	readmePath := filepath.Join(repoPath, "README.md")
	body, err := os.ReadFile(readmePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	text := string(body)
	if !strings.Contains(text, report.StartMarker) || !strings.Contains(text, report.EndMarker) {
		return nil
	}
	return os.Remove(readmePath)
}

func (r *runtime) runSubscribe(args []string) error {
	fs := flag.NewFlagSet("subscribe", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoPath := fs.String("repo", "", "")
	branch := fs.String("branch", "main", "")
	staleAfter := fs.String("stale-after", "15m", "")
	noAutoUpdate := fs.Bool("no-auto-update", false, "")
	noImport := fs.Bool("no-import", false, "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	noMedia := fs.Bool("no-media", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(errors.New("subscribe requires one remote"))
	}
	remote := fs.Arg(0)
	cfg, err := loadConfigOrDefault(r.configPath)
	if err != nil {
		return err
	}
	if *repoPath != "" {
		cfg.Share.RepoPath = *repoPath
	}
	cfg.Share.Remote = remote
	cfg.Share.Branch = *branch
	cfg.Share.AutoUpdate = !*noAutoUpdate
	cfg.Share.StaleAfter = *staleAfter
	if *noMedia {
		cfg.Share.Media = new(false)
	}
	cfg.Discord.TokenSource = "none"
	if err := config.Write(r.configPath, cfg); err != nil {
		return configErr(err)
	}
	if *noImport {
		return r.print(map[string]any{"config_path": r.configPath, "remote": remote, "repo_path": cfg.Share.RepoPath})
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return configErr(err)
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	r.cfg = cfg
	return r.withSyncLock(func() error {
		s, err := store.Open(r.ctx, dbPath)
		if err != nil {
			return dbErr(err)
		}
		defer func() { _ = s.Close() }()
		expandedRepo, err := config.ExpandPath(cfg.Share.RepoPath)
		if err != nil {
			return configErr(err)
		}
		opts := share.Options{RepoPath: expandedRepo, Remote: cfg.Share.Remote, Branch: cfg.Share.Branch, Progress: r.shareProgress}
		if err := applyMediaShareOptions(&opts, cfg, cfg.ShareMediaEnabled() && !*noMedia); err != nil {
			return err
		}
		if *withEmbeddings {
			applyEmbeddingShareOptions(&opts, cfg)
		}
		r.setSyncLockPhase("share pull")
		if err := share.Pull(r.ctx, opts); err != nil {
			return err
		}
		r.setSyncLockPhase("share import")
		manifest, imported, err := share.ImportIfChanged(r.ctx, s, opts)
		if err != nil {
			return err
		}
		return r.print(map[string]any{
			"config_path":  r.configPath,
			"repo_path":    opts.RepoPath,
			"remote":       opts.Remote,
			"generated_at": manifest.GeneratedAt,
			"tables":       manifest.Tables,
			"media":        manifest.Media,
			"embeddings":   manifest.Embeddings,
			"imported":     imported,
		})
	})
}

func (r *runtime) runUpdate(args []string) error {
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoPath := fs.String("repo", r.cfg.Share.RepoPath, "")
	remote := fs.String("remote", r.cfg.Share.Remote, "")
	branch := fs.String("branch", r.cfg.Share.Branch, "")
	ref := fs.String("ref", "", "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	noMedia := fs.Bool("no-media", !r.cfg.ShareMediaEnabled(), "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("update takes no positional arguments"))
	}
	opts, err := shareOptionsFromFlags(*repoPath, *remote, *branch)
	if err != nil {
		return err
	}
	opts.Progress = r.shareProgress
	if *withEmbeddings {
		applyEmbeddingShareOptions(&opts, r.cfg)
	}
	if err := applyMediaShareOptions(&opts, r.cfg, !*noMedia); err != nil {
		return err
	}
	var manifest share.Manifest
	var imported bool
	if strings.TrimSpace(*ref) == "" {
		r.setSyncLockPhase("share pull")
		if err := share.Pull(r.ctx, opts); err != nil {
			return err
		}
		r.setSyncLockPhase("share import")
		manifest, imported, err = share.ImportIfChanged(r.ctx, r.store, opts)
		if err != nil {
			return err
		}
	} else {
		r.setSyncLockPhase("share historical import")
		manifest, err = share.ImportAt(r.ctx, r.store, opts, *ref)
		if err != nil {
			return err
		}
		imported = true
	}
	return r.print(map[string]any{
		"repo_path":    opts.RepoPath,
		"remote":       opts.Remote,
		"generated_at": manifest.GeneratedAt,
		"tables":       manifest.Tables,
		"media":        manifest.Media,
		"embeddings":   manifest.Embeddings,
		"imported":     imported,
		"ref":          strings.TrimSpace(*ref),
	})
}

func shareOptionsFromFlags(repoPath, remote, branch string) (share.Options, error) {
	expandedRepo, err := config.ExpandPath(repoPath)
	if err != nil {
		return share.Options{}, configErr(err)
	}
	if remote == "" {
		return share.Options{}, configErr(errors.New("share remote is required"))
	}
	if branch == "" {
		branch = "main"
	}
	return share.Options{RepoPath: expandedRepo, Remote: remote, Branch: branch}, nil
}

func applyEmbeddingShareOptions(opts *share.Options, cfg config.Config) {
	opts.IncludeEmbeddings = true
	opts.EmbeddingProvider = cfg.Search.Embeddings.Provider
	opts.EmbeddingModel = cfg.Search.Embeddings.Model
	opts.EmbeddingInputVersion = store.EmbeddingInputVersion
}

func applyMediaShareOptions(opts *share.Options, cfg config.Config, enabled bool) error {
	cacheDir, err := config.ExpandPath(cfg.CacheDir)
	if err != nil {
		return configErr(err)
	}
	opts.CacheDir = cacheDir
	opts.IncludeMedia = enabled
	return nil
}

func loadConfigOrDefault(path string) (config.Config, error) {
	cfg, err := config.Load(path)
	if err == nil {
		return cfg, nil
	}
	if !os.IsNotExist(err) {
		return config.Config{}, configErr(err)
	}
	cfg = config.Default()
	if err := cfg.Normalize(); err != nil {
		return config.Config{}, configErr(err)
	}
	return cfg, nil
}
