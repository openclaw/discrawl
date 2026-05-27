package cli

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/openclaw/crawlkit/embed"
	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
)

func (r *runtime) runSearch(args []string) error {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	mode := fs.String("mode", r.cfg.Search.DefaultMode, "")
	channel := fs.String("channel", "", "")
	author := fs.String("author", "", "")
	limit := fs.Int("limit", 20, "")
	includeEmpty := fs.Bool("include-empty", false, "")
	dm := fs.Bool("dm", false, "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(permuteSearchFlags(args)); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(errors.New("search requires a query"))
	}
	guildIDs, err := directMessageGuildScope(*dm, *guildFlag, *guildsFlag)
	if err != nil {
		return usageErr(err)
	}
	opts := store.SearchOptions{
		Query:        fs.Arg(0),
		GuildIDs:     guildIDs,
		Channel:      *channel,
		Author:       *author,
		Limit:        *limit,
		IncludeEmpty: *includeEmpty,
	}
	normalizedMode := strings.ToLower(strings.TrimSpace(*mode))
	if r.cfg.RemoteCloudReadOnly() {
		results, err := r.runRemoteSearch(opts, normalizedMode, *dm)
		if err != nil {
			return err
		}
		return r.print(results)
	}
	switch normalizedMode {
	case "", "fts":
		results, err := r.store.SearchMessages(r.ctx, opts)
		if err != nil {
			return err
		}
		return r.print(results)
	case "semantic":
		results, err := r.searchMessagesSemantic(opts)
		if err != nil {
			return err
		}
		return r.print(results)
	case "hybrid":
		results, err := r.searchMessagesHybrid(opts)
		if err != nil {
			return err
		}
		return r.print(results)
	default:
		return usageErr(fmt.Errorf("unsupported search mode %q", *mode))
	}
}

func permuteSearchFlags(args []string) []string {
	valueFlags := map[string]struct{}{
		"--mode":    {},
		"--channel": {},
		"--author":  {},
		"--limit":   {},
		"--guilds":  {},
		"--guild":   {},
	}
	boolFlags := map[string]struct{}{
		"--include-empty": {},
		"--dm":            {},
	}
	flags := make([]string, 0, len(args))
	positionals := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if name, _, ok := strings.Cut(arg, "="); ok {
			if _, known := valueFlags[name]; known {
				flags = append(flags, arg)
				continue
			}
			if _, known := boolFlags[name]; known {
				flags = append(flags, arg)
				continue
			}
		}
		if _, known := boolFlags[arg]; known {
			flags = append(flags, arg)
			continue
		}
		if _, known := valueFlags[arg]; known && i+1 < len(args) {
			flags = append(flags, arg, args[i+1])
			i++
			continue
		}
		positionals = append(positionals, arg)
	}
	return append(flags, positionals...)
}

func (r *runtime) searchMessagesSemantic(opts store.SearchOptions) ([]store.SearchResult, error) {
	semanticOpts, err := r.semanticSearchOptions(opts)
	if err != nil {
		return nil, err
	}
	return r.store.SearchMessagesSemantic(r.ctx, semanticOpts)
}

func (r *runtime) searchMessagesHybrid(opts store.SearchOptions) ([]store.SearchResult, error) {
	if !r.cfg.Search.Embeddings.Enabled {
		return r.store.SearchMessages(r.ctx, opts)
	}
	hasEmbeddings, err := r.store.HasMessageEmbeddings(
		r.ctx,
		r.cfg.Search.Embeddings.Provider,
		r.cfg.Search.Embeddings.Model,
		store.EmbeddingInputVersion,
	)
	if err != nil {
		return nil, err
	}
	if !hasEmbeddings {
		return r.store.SearchMessages(r.ctx, opts)
	}
	semanticOpts, err := r.semanticSearchOptions(opts)
	if err != nil {
		return r.store.SearchMessages(r.ctx, opts)
	}
	results, err := r.store.SearchMessagesHybrid(r.ctx, opts, semanticOpts)
	if err != nil {
		if hybridSemanticUnavailable(err) {
			return r.store.SearchMessages(r.ctx, opts)
		}
		return nil, err
	}
	return results, nil
}

func (r *runtime) semanticSearchOptions(opts store.SearchOptions) (store.SemanticSearchOptions, error) {
	if !r.cfg.Search.Embeddings.Enabled {
		return store.SemanticSearchOptions{}, errors.New("embeddings are disabled; enable [search.embeddings] first")
	}
	providerFactory := r.newEmbed
	if providerFactory == nil {
		providerFactory = func(cfg config.EmbeddingsConfig) (embed.Provider, error) {
			return embed.NewProvider(crawlkitEmbeddingConfig(cfg))
		}
	}
	provider, err := providerFactory(r.cfg.Search.Embeddings)
	if err != nil {
		return store.SemanticSearchOptions{}, fmt.Errorf("create embedding provider: %w", err)
	}
	batch, err := provider.Embed(r.ctx, []string{opts.Query})
	if err != nil {
		return store.SemanticSearchOptions{}, fmt.Errorf("embedding query failed: %w", err)
	}
	if len(batch.Vectors) != 1 {
		return store.SemanticSearchOptions{}, fmt.Errorf("embedding query returned %d vectors for 1 input", len(batch.Vectors))
	}
	queryVector := batch.Vectors[0]
	dimensions := batch.Dimensions
	if dimensions == 0 {
		dimensions = len(queryVector)
	}
	return store.SemanticSearchOptions{
		QueryVector:  queryVector,
		Provider:     r.cfg.Search.Embeddings.Provider,
		Model:        r.cfg.Search.Embeddings.Model,
		InputVersion: store.EmbeddingInputVersion,
		Dimensions:   dimensions,
		GuildIDs:     opts.GuildIDs,
		Channel:      opts.Channel,
		Author:       opts.Author,
		Limit:        opts.Limit,
		IncludeEmpty: opts.IncludeEmpty,
	}, nil
}

func hybridSemanticUnavailable(err error) bool {
	return errors.Is(err, store.ErrNoCompatibleEmbeddings) || strings.HasPrefix(err.Error(), "semantic query embedding ")
}

func (r *runtime) runSQL(args []string) error {
	fs := flag.NewFlagSet("sql", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	unsafe := fs.Bool("unsafe", false, "")
	confirm := fs.Bool("confirm", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if *confirm && !*unsafe {
		return usageErr(errors.New("--confirm requires --unsafe"))
	}

	var query string
	rest := fs.Args()
	if len(rest) == 0 || rest[0] == "-" {
		body, err := io.ReadAll(bufio.NewReader(os.Stdin))
		if err != nil {
			return err
		}
		query = string(body)
	} else {
		query = strings.Join(rest, " ")
	}

	if !*unsafe {
		cols, rows, err := r.store.ReadOnlyQuery(r.ctx, query)
		if err != nil {
			return err
		}
		if r.json {
			return r.print(map[string]any{"columns": cols, "rows": rows})
		}
		return printRows(r.stdout, cols, rows)
	}
	if !*confirm {
		return usageErr(errors.New("--unsafe requires --confirm"))
	}

	if store.IsReadOnlySQL(query) {
		cols, rows, err := r.store.Query(r.ctx, query)
		if err != nil {
			return err
		}
		if r.json {
			return r.print(map[string]any{"columns": cols, "rows": rows})
		}
		return printRows(r.stdout, cols, rows)
	}

	affected, err := r.store.Exec(r.ctx, query)
	if err != nil {
		return err
	}
	return r.print(map[string]any{"rows_affected": affected})
}

func (r *runtime) runMembers(args []string) error {
	if len(args) == 0 {
		return usageErr(errors.New("members requires a subcommand"))
	}
	switch args[0] {
	case "list":
		rows, err := r.store.Members(r.ctx, r.cfg.EffectiveDefaultGuildID(), "", 500)
		if err != nil {
			return err
		}
		return r.print(rows)
	case "show":
		return r.runMembersShow(args[1:])
	case "search":
		if len(args) < 2 {
			return usageErr(errors.New("members search requires a query"))
		}
		rows, err := r.store.Members(r.ctx, "", strings.Join(args[1:], " "), 100)
		if err != nil {
			return err
		}
		return r.print(rows)
	default:
		return usageErr(fmt.Errorf("unknown members subcommand %q", args[0]))
	}
}

func (r *runtime) runMembersShow(args []string) error {
	fs := flag.NewFlagSet("members show", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	messageLimit := fs.Int("messages", 20, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() < 1 {
		return usageErr(errors.New("members show requires a user id or query"))
	}
	query := strings.Join(fs.Args(), " ")

	rows, err := r.store.MemberByID(r.ctx, query)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		rows, err = r.store.Members(r.ctx, "", query, 20)
		if err != nil {
			return err
		}
	}
	if len(rows) == 0 {
		return r.print([]store.MemberRow{})
	}
	if len(rows) > 1 {
		defaultGuild := r.cfg.EffectiveDefaultGuildID()
		if defaultGuild != "" {
			for _, row := range rows {
				if row.GuildID == defaultGuild && (row.UserID == query || row.Username == query || row.DisplayName == query || row.Nick == query || row.GlobalName == query) {
					profile, err := r.store.MemberProfile(r.ctx, row.GuildID, row.UserID, *messageLimit)
					if err != nil {
						return err
					}
					return r.print(profile)
				}
			}
		}
		return r.print(rows)
	}

	profile, err := r.store.MemberProfile(r.ctx, rows[0].GuildID, rows[0].UserID, *messageLimit)
	if err != nil {
		return err
	}
	return r.print(profile)
}

func (r *runtime) runChannels(args []string) error {
	if len(args) == 0 {
		return usageErr(errors.New("channels requires a subcommand"))
	}
	rows, err := r.store.Channels(r.ctx, "")
	if err != nil {
		return err
	}
	switch args[0] {
	case "list":
		return r.print(rows)
	case "show":
		if len(args) < 2 {
			return usageErr(errors.New("channels show requires a channel id"))
		}
		filtered := make([]store.ChannelRow, 0, 1)
		for _, row := range rows {
			if row.ID == args[1] {
				filtered = append(filtered, row)
			}
		}
		return r.print(filtered)
	default:
		return usageErr(fmt.Errorf("unknown channels subcommand %q", args[0]))
	}
}
