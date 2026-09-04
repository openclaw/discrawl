package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/openclaw/crawlkit/vector"
)

func (s *Store) searchMessagesMultilingual(
	ctx context.Context,
	opts SearchOptions,
) ([]SearchResult, error) {
	if strings.TrimSpace(opts.Query) == "" {
		return nil, nil
	}
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	candidateLimit := searchCandidateLimit(opts.Limit)
	rankings := make([][]SearchResult, 0, len(s.lexicalTokenizers)+1)

	defaultResults, err := s.searchMessagesFTSTable(
		ctx,
		"message_fts",
		normalizeFTSQuery(opts.Query),
		opts,
		candidateLimit,
	)
	if err != nil {
		if !shouldSearchFallback(err) {
			return nil, err
		}
		return s.searchFallback(ctx, opts)
	}
	rankings = append(rankings, defaultResults)

	for _, language := range s.lexicalLanguages() {
		query, err := s.lexicalTokenizers[language].Tokenize(ctx, opts.Query)
		if err != nil {
			return nil, fmt.Errorf("tokenize %s query: %w", language, err)
		}
		query = normalizeFTSQuery(query)
		if query == "" {
			continue
		}
		results, err := s.searchMessagesFTSTable(
			ctx,
			lexicalFTSTable(language),
			query,
			opts,
			candidateLimit,
		)
		if err != nil {
			return nil, err
		}
		rankings = append(rankings, results)
	}
	return fuseLexicalSearchResults(rankings, opts.Limit), nil
}

func (s *Store) searchMessagesFTSTable(
	ctx context.Context,
	table string,
	queryText string,
	opts SearchOptions,
	limit int,
) ([]SearchResult, error) {
	args := []any{queryText}
	clauses := []string{table + " match ?"}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, table+".guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if strings.TrimSpace(opts.Channel) != "" {
		clauses = append(clauses, "("+table+".channel_id = ? or "+table+".channel_name like ?)")
		args = append(args, opts.Channel, "%"+opts.Channel+"%")
	}
	if strings.TrimSpace(opts.Author) != "" {
		clauses = append(clauses, "("+table+".author_id = ? or "+table+".author_name like ?)")
		args = append(args, opts.Author, "%"+opts.Author+"%")
	}
	if !opts.IncludeEmpty {
		clauses = append(clauses, "trim(coalesce(m.normalized_content, '')) <> ''")
	}
	args = append(args, limit)
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(queryCtx, `
		select `+table+`.message_id
		from `+table+`
		join messages m on m.id = `+table+`.message_id
		where m.deleted_at is null
		  and `+strings.Join(clauses, " and ")+`
		order by bm25(`+table+`) asc, `+table+`.rowid desc
		limit ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	ids := make([]string, 0, limit)
	for rows.Next() {
		var messageID string
		if err := rows.Scan(&messageID); err != nil {
			return nil, err
		}
		ids = append(ids, messageID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	details, err := s.searchResultDetails(queryCtx, ids)
	if err != nil {
		return nil, err
	}
	results := make([]SearchResult, 0, len(ids))
	for _, messageID := range ids {
		if result, ok := details[messageID]; ok {
			results = append(results, result)
		}
	}
	return results, nil
}

func fuseLexicalSearchResults(rankings [][]SearchResult, limit int) []SearchResult {
	if limit <= 0 {
		limit = 20
	}
	ids := make([]func(SearchResult) string, len(rankings))
	weights := make([]float64, len(rankings))
	for i := range rankings {
		ids[i] = func(result SearchResult) string {
			return result.MessageID
		}
		weights[i] = 1
	}
	fused := vector.ReciprocalRankFusion(rankings, ids, weights, rrfK)
	if len(fused) > limit {
		fused = fused[:limit]
	}
	results := make([]SearchResult, 0, len(fused))
	for _, entry := range fused {
		results = append(results, entry.Item)
	}
	return results
}
