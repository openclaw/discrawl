package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/vector"
	"github.com/openclaw/discrawl/internal/store/storedb"
)

const (
	queryTimeout                         = 15 * time.Second
	semanticQueryTimeout                 = 2 * time.Minute
	queryRowLimit                        = 50000
	searchCandidateFloor                 = 200
	searchCandidateCap                   = 5000
	searchCandidateMultiple              = 20
	semanticTurboVecMaxBatchCandidates   = 2048
	semanticTurboVecRequestOverheadBytes = int64(256)
	semanticTurboVecJSONFloatBudgetBytes = int64(16)
	messageFTSHealthProbe                = "__discrawl_probe__"
	rrfK                                 = 60.0
	ftsRRFWeight                         = 1.0
	semanticRRFWeight                    = 1.0
)

var ErrNoCompatibleEmbeddings = errors.New("no compatible message embeddings for provider/model/input version; run discrawl embed --rebuild")

type SemanticSearchOptions struct {
	QueryVector   []float32
	Provider      string
	Model         string
	InputVersion  string
	Dimensions    int
	VectorBackend string
	GuildIDs      []string
	Channel       string
	Author        string
	Limit         int
	IncludeEmpty  bool
}

func (s *Store) GetSyncState(ctx context.Context, scope string) (string, error) {
	cursor, err := s.q.GetSyncState(ctx, scope)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return cursor, nil
}

func (s *Store) ChannelMessageBounds(ctx context.Context, channelID string) (string, string, error) {
	row, err := s.q.ChannelMessageBounds(ctx, channelID)
	if err != nil {
		return "", "", err
	}
	return row.OldestID, row.NewestID, nil
}

func (s *Store) SearchMessages(ctx context.Context, opts SearchOptions) ([]SearchResult, error) {
	if strings.TrimSpace(opts.Query) == "" {
		return nil, nil
	}
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	args := []any{normalizeFTSQuery(opts.Query)}
	clauses := []string{"message_fts match ?"}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, "message_fts.guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if strings.TrimSpace(opts.Channel) != "" {
		clauses = append(clauses, "(message_fts.channel_id = ? or message_fts.channel_name like ?)")
		args = append(args, opts.Channel, "%"+opts.Channel+"%")
	}
	if strings.TrimSpace(opts.Author) != "" {
		clauses = append(clauses, "(message_fts.author_id = ? or message_fts.author_name like ?)")
		args = append(args, opts.Author, "%"+opts.Author+"%")
	}
	args = append(args, searchCandidateLimit(opts.Limit), opts.Limit)
	query := `
		with recent_matches as (
			select
				rowid,
				message_id,
				guild_id,
				channel_id,
				author_id,
				coalesce(author_name, '') as author_name,
				coalesce(channel_name, '') as channel_name
			from message_fts
			where ` + strings.Join(clauses, " and ") + `
			order by rowid desc
			limit ?
		)
		select
			m.id, m.guild_id, m.channel_id, coalesce(c.name, recent_matches.channel_name),
			coalesce(m.author_id, ''), recent_matches.author_name,
			case
				when trim(coalesce(m.content, '')) <> '' then m.content
				else m.normalized_content
			end,
			m.created_at
		from recent_matches
		join messages m on m.id = recent_matches.message_id
		left join channels c on c.id = m.channel_id
		where (? or trim(coalesce(m.normalized_content, '')) <> '')
		order by recent_matches.rowid desc
		limit ?
	`
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(queryCtx, query, append(args[:len(args)-1], opts.IncludeEmpty, args[len(args)-1])...)
	if err != nil {
		if !shouldSearchFallback(err) {
			return nil, err
		}
		fallbackCtx, fallbackCancel := withQueryTimeout(ctx)
		defer fallbackCancel()
		return s.searchFallback(fallbackCtx, opts)
	}
	defer func() { _ = rows.Close() }()
	var out []SearchResult
	for rows.Next() {
		var row SearchResult
		var created string
		if err := rows.Scan(&row.MessageID, &row.GuildID, &row.ChannelID, &row.ChannelName, &row.AuthorID, &row.AuthorName, &row.Content, &created); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		out = append(out, row)
	}
	return out, rows.Err()
}

func shouldSearchFallback(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such table: message_fts") ||
		strings.Contains(message, "no such module: fts5") ||
		strings.Contains(message, "vtable constructor failed")
}

func (s *Store) SearchMessagesSemantic(ctx context.Context, opts SemanticSearchOptions) ([]SearchResult, error) {
	opts.Provider = strings.ToLower(strings.TrimSpace(opts.Provider))
	opts.Model = strings.TrimSpace(opts.Model)
	opts.InputVersion = strings.TrimSpace(opts.InputVersion)
	opts.VectorBackend = strings.ToLower(strings.TrimSpace(opts.VectorBackend))
	if opts.InputVersion == "" {
		opts.InputVersion = EmbeddingInputVersion
	}
	if opts.VectorBackend == "" {
		opts.VectorBackend = vector.BackendExact
	}
	if opts.VectorBackend != vector.BackendExact && opts.VectorBackend != vector.BackendTurboVec {
		return nil, fmt.Errorf("unsupported vector backend %q", opts.VectorBackend)
	}
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	if len(opts.QueryVector) == 0 {
		return nil, errors.New("semantic query embedding returned an empty vector")
	}
	if opts.Dimensions <= 0 {
		opts.Dimensions = len(opts.QueryVector)
	}
	if len(opts.QueryVector) != opts.Dimensions {
		return nil, fmt.Errorf("semantic query embedding dimensions mismatch: got %d want %d", len(opts.QueryVector), opts.Dimensions)
	}
	queryNorm := vector.Norm(opts.QueryVector)
	if queryNorm == 0 {
		return nil, errors.New("semantic query embedding returned a zero vector")
	}

	clauses := []string{
		"e.provider = ?",
		"e.model = ?",
		"e.input_version = ?",
		"e.dimensions = ?",
	}
	args := []any{opts.Provider, opts.Model, opts.InputVersion, opts.Dimensions}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, "m.guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if strings.TrimSpace(opts.Channel) != "" {
		clauses = append(clauses, "(m.channel_id = ? or c.name like ?)")
		args = append(args, opts.Channel, "%"+opts.Channel+"%")
	}
	authorExpr := `coalesce(
		json_extract(m.raw_json, '$.member.nick'),
		json_extract(m.raw_json, '$.author.global_name'),
		json_extract(m.raw_json, '$.author.username'),
		''
	)`
	if strings.TrimSpace(opts.Author) != "" {
		clauses = append(clauses, "(m.author_id = ? or "+authorExpr+" like ?)")
		args = append(args, opts.Author, "%"+opts.Author+"%")
	}
	if !opts.IncludeEmpty {
		clauses = append(clauses, "trim(coalesce(m.normalized_content, '')) <> ''")
	}
	queryCtx, cancel := context.WithTimeout(ctx, semanticQueryTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(queryCtx, `
		select
			e.message_id,
			m.created_at,
			e.dimensions,
			e.embedding_blob
		from message_embeddings e
		join messages m on m.id = e.message_id
		left join channels c on c.id = m.channel_id
		where `+strings.Join(clauses, " and ")+`
	`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	ranked := make([]semanticRankedResult, 0, opts.Limit)
	turboBatchSize := semanticTurboVecBatchSize(opts.Dimensions)
	turboBatch := make([]vector.SearchCandidate[semanticSearchCandidate], 0, turboBatchSize)
	flushTurboBatch := func() error {
		if len(turboBatch) == 0 {
			return nil
		}
		results, err := vector.Search(queryCtx, opts.QueryVector, turboBatch, vector.SearchOptions[semanticSearchCandidate]{
			Backend: opts.VectorBackend,
			Limit:   opts.Limit,
			TieLess: semanticCandidateLess,
		})
		if err != nil {
			return fmt.Errorf("score embeddings: %w", err)
		}
		for _, result := range results {
			ranked = insertSemanticRanked(ranked, semanticRankedResult{
				candidate: result.Item,
				score:     result.Score,
			}, opts.Limit)
		}
		turboBatch = turboBatch[:0]
		return nil
	}
	for rows.Next() {
		var (
			messageID  string
			created    string
			dimensions int
			blob       []byte
		)
		if err := rows.Scan(&messageID, &created, &dimensions, &blob); err != nil {
			return nil, err
		}
		if dimensions != opts.Dimensions {
			return nil, fmt.Errorf("stored embedding dimensions mismatch for message %s: got %d want %d", messageID, dimensions, opts.Dimensions)
		}
		storedVector, err := DecodeEmbeddingVector(blob)
		if err != nil {
			return nil, fmt.Errorf("decode embedding for message %s: %w", messageID, err)
		}
		if len(storedVector) != dimensions {
			return nil, fmt.Errorf("stored embedding vector length mismatch for message %s: got %d want %d", messageID, len(storedVector), dimensions)
		}
		if vector.Norm(storedVector) == 0 {
			return nil, fmt.Errorf("score embedding for message %s: stored embedding vector is zero", messageID)
		}
		candidate := semanticSearchCandidate{messageID: messageID, createdAt: parseTime(created)}
		switch opts.VectorBackend {
		case vector.BackendExact:
			score, err := vector.CosineSimilarity(opts.QueryVector, queryNorm, storedVector)
			if err != nil {
				return nil, fmt.Errorf("score embedding for message %s: %w", messageID, err)
			}
			ranked = insertSemanticRanked(ranked, semanticRankedResult{candidate: candidate, score: score}, opts.Limit)
		case vector.BackendTurboVec:
			turboBatch = append(turboBatch, vector.SearchCandidate[semanticSearchCandidate]{
				Item:   candidate,
				Vector: storedVector,
			})
			if len(turboBatch) >= turboBatchSize {
				if err := flushTurboBatch(); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if opts.VectorBackend == vector.BackendTurboVec {
		if err := flushTurboBatch(); err != nil {
			return nil, err
		}
	}
	if len(ranked) == 0 {
		compatible, err := s.hasCompatibleMessageEmbeddings(ctx, opts)
		if err != nil {
			return nil, err
		}
		if !compatible {
			return nil, ErrNoCompatibleEmbeddings
		}
		return []SearchResult{}, nil
	}
	ids := make([]string, 0, len(ranked))
	for _, item := range ranked {
		ids = append(ids, item.candidate.messageID)
	}
	details, err := s.searchResultDetails(queryCtx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]SearchResult, 0, len(ranked))
	for _, item := range ranked {
		row, ok := details[item.candidate.messageID]
		if !ok {
			return nil, fmt.Errorf("semantic search result %s disappeared before hydration", item.candidate.messageID)
		}
		row.CreatedAt = item.candidate.createdAt
		out = append(out, row)
	}
	return out, nil
}

func (s *Store) searchResultDetails(ctx context.Context, messageIDs []string) (map[string]SearchResult, error) {
	if len(messageIDs) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(messageIDs))
	for _, messageID := range messageIDs {
		args = append(args, messageID)
	}
	authorExpr := `coalesce(
		json_extract(m.raw_json, '$.member.nick'),
		json_extract(m.raw_json, '$.author.global_name'),
		json_extract(m.raw_json, '$.author.username'),
		''
	)`
	rows, err := s.db.QueryContext(ctx, `
		select
			m.id,
			m.guild_id,
			m.channel_id,
			coalesce(c.name, ''),
			coalesce(m.author_id, ''),
			`+authorExpr+`,
			case
				when trim(coalesce(m.content, '')) <> '' then m.content
				else m.normalized_content
			end,
			m.created_at
		from messages m
		left join channels c on c.id = m.channel_id
		where m.id in (`+placeholders(len(messageIDs))+`)
	`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]SearchResult, len(messageIDs))
	for rows.Next() {
		var row SearchResult
		var created string
		if err := rows.Scan(&row.MessageID, &row.GuildID, &row.ChannelID, &row.ChannelName, &row.AuthorID, &row.AuthorName, &row.Content, &created); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		out[row.MessageID] = row
	}
	return out, rows.Err()
}

type semanticSearchCandidate struct {
	messageID string
	createdAt time.Time
}

type semanticRankedResult struct {
	candidate semanticSearchCandidate
	score     float64
}

func insertSemanticRanked(ranked []semanticRankedResult, item semanticRankedResult, limit int) []semanticRankedResult {
	insertAt := sort.Search(len(ranked), func(i int) bool {
		return semanticRankedLess(item, ranked[i])
	})
	if insertAt >= limit {
		return ranked
	}
	ranked = append(ranked, semanticRankedResult{})
	copy(ranked[insertAt+1:], ranked[insertAt:])
	ranked[insertAt] = item
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}
	return ranked
}

func semanticRankedLess(left, right semanticRankedResult) bool {
	if left.score != right.score {
		return left.score > right.score
	}
	return semanticCandidateLess(left.candidate, right.candidate)
}

func semanticCandidateLess(left, right semanticSearchCandidate) bool {
	if !left.createdAt.Equal(right.createdAt) {
		return left.createdAt.After(right.createdAt)
	}
	return left.messageID > right.messageID
}

func semanticTurboVecBatchSize(dimensions int) int {
	if dimensions <= 0 {
		return 1
	}
	perCandidate := int64(dimensions)*semanticTurboVecJSONFloatBudgetBytes + 4
	budget := vector.DefaultTurboVecMaxInputSize - semanticTurboVecRequestOverheadBytes - int64(dimensions)*semanticTurboVecJSONFloatBudgetBytes
	if budget <= 0 || perCandidate <= 0 {
		return 1
	}
	maxByInput := int(budget / perCandidate)
	if maxByInput < 1 {
		return 1
	}
	if maxByInput > semanticTurboVecMaxBatchCandidates {
		return semanticTurboVecMaxBatchCandidates
	}
	return maxByInput
}

func (s *Store) SearchMessagesHybrid(ctx context.Context, opts SearchOptions, semanticOpts SemanticSearchOptions) ([]SearchResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}
	candidateLimit := searchCandidateLimit(limit)
	ftsOpts := opts
	ftsOpts.Limit = candidateLimit
	semanticOpts.Limit = candidateLimit

	ftsResults, err := s.SearchMessages(ctx, ftsOpts)
	if err != nil {
		return nil, err
	}
	semanticResults, err := s.SearchMessagesSemantic(ctx, semanticOpts)
	if err != nil {
		return nil, err
	}
	return fuseSearchResults(ftsResults, semanticResults, limit), nil
}

type hybridSearchEntry struct {
	result SearchResult
	score  float64
	hasFTS bool
}

func fuseSearchResults(ftsResults, semanticResults []SearchResult, limit int) []SearchResult {
	if limit <= 0 {
		limit = 20
	}
	id := func(result SearchResult) string {
		return result.MessageID
	}
	ftsIDs := make(map[string]struct{}, len(ftsResults))
	for _, result := range ftsResults {
		ftsIDs[result.MessageID] = struct{}{}
	}
	fused := vector.ReciprocalRankFusion(
		[][]SearchResult{ftsResults, semanticResults},
		[]func(SearchResult) string{id, id},
		[]float64{ftsRRFWeight, semanticRRFWeight},
		rrfK,
	)
	merged := make([]hybridSearchEntry, 0, len(fused))
	for _, entry := range fused {
		_, hasFTS := ftsIDs[entry.Item.MessageID]
		merged = append(merged, hybridSearchEntry{result: entry.Item, score: entry.Score, hasFTS: hasFTS})
	}
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].score != merged[j].score {
			return merged[i].score > merged[j].score
		}
		if merged[i].hasFTS != merged[j].hasFTS {
			return merged[i].hasFTS
		}
		if !merged[i].result.CreatedAt.Equal(merged[j].result.CreatedAt) {
			return merged[i].result.CreatedAt.After(merged[j].result.CreatedAt)
		}
		return merged[i].result.MessageID > merged[j].result.MessageID
	})
	if len(merged) > limit {
		merged = merged[:limit]
	}
	out := make([]SearchResult, 0, len(merged))
	for _, entry := range merged {
		out = append(out, entry.result)
	}
	return out
}

func (s *Store) hasCompatibleMessageEmbeddings(ctx context.Context, opts SemanticSearchOptions) (bool, error) {
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	var exists int
	err := s.db.QueryRowContext(queryCtx, `
		select exists(
			select 1
			from message_embeddings
			where provider = ?
			  and model = ?
			  and input_version = ?
			  and dimensions = ?
		)
	`, opts.Provider, opts.Model, opts.InputVersion, opts.Dimensions).Scan(&exists)
	return exists == 1, err
}

func (s *Store) HasMessageEmbeddings(ctx context.Context, provider, model, inputVersion string) (bool, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	model = strings.TrimSpace(model)
	inputVersion = strings.TrimSpace(inputVersion)
	if inputVersion == "" {
		inputVersion = EmbeddingInputVersion
	}
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	return s.q.HasMessageEmbeddings(queryCtx, storedb.HasMessageEmbeddingsParams{
		Provider:     provider,
		Model:        model,
		InputVersion: inputVersion,
	})
}

func (s *Store) CheckMessageFTS(ctx context.Context) error {
	db, cleanup, err := s.openReadOnlyDB()
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()

	var rowID sql.NullInt64
	err = db.QueryRowContext(
		queryCtx,
		`select rowid from message_fts where message_fts match ? limit 1`,
		messageFTSHealthProbe,
	).Scan(&rowID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

func (s *Store) searchFallback(ctx context.Context, opts SearchOptions) ([]SearchResult, error) {
	args := []any{"%" + opts.Query + "%"}
	clauses := []string{"m.normalized_content like ?"}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, "m.guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if strings.TrimSpace(opts.Channel) != "" {
		clauses = append(clauses, "(m.channel_id = ? or c.name like ?)")
		args = append(args, opts.Channel, "%"+opts.Channel+"%")
	}
	if strings.TrimSpace(opts.Author) != "" {
		clauses = append(clauses, "(m.author_id = ? or m.raw_json like ?)")
		args = append(args, opts.Author, "%"+opts.Author+"%")
	}
	if !opts.IncludeEmpty {
		clauses = append(clauses, "trim(coalesce(m.normalized_content, '')) <> ''")
	}
	args = append(args, opts.Limit)
	rows, err := s.db.QueryContext(ctx, `
		select
			m.id,
			m.guild_id,
			m.channel_id,
			coalesce(c.name, ''),
			coalesce(m.author_id, ''),
			'',
			case
				when trim(coalesce(m.content, '')) <> '' then m.content
				else m.normalized_content
			end,
			m.created_at
		from messages m
		left join channels c on c.id = m.channel_id
		where `+strings.Join(clauses, " and ")+`
		order by m.created_at desc
		limit ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []SearchResult
	for rows.Next() {
		var row SearchResult
		var created string
		if err := rows.Scan(&row.MessageID, &row.GuildID, &row.ChannelID, &row.ChannelName, &row.AuthorID, &row.AuthorName, &row.Content, &created); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) Members(ctx context.Context, guildID, query string, limit int) ([]MemberRow, error) {
	if strings.TrimSpace(query) != "" {
		return s.searchMembers(ctx, guildID, query, limit)
	}
	if limit <= 0 {
		limit = 100
	}
	var out []MemberRow
	if guildID != "" {
		rows, err := s.q.ListMembersByGuild(ctx, storedb.ListMembersByGuildParams{GuildID: guildID, Limit: int64(limit)})
		if err != nil {
			return nil, err
		}
		out = make([]MemberRow, 0, len(rows))
		for _, row := range rows {
			member := MemberRow{
				GuildID:       row.GuildID,
				UserID:        row.UserID,
				Username:      row.Username,
				GlobalName:    row.GlobalName,
				DisplayName:   row.DisplayName,
				Nick:          row.Nick,
				Discriminator: row.Discriminator,
				Avatar:        row.Avatar,
				RoleIDsJSON:   row.RoleIdsJson,
				Bot:           row.Bot == 1,
				JoinedAt:      parseTime(row.JoinedAt),
				RawJSON:       row.RawJson,
			}
			enrichMemberRow(&member)
			out = append(out, member)
		}
		return out, nil
	}
	rows, err := s.q.ListMembers(ctx, int64(limit))
	if err != nil {
		return nil, err
	}
	out = make([]MemberRow, 0, len(rows))
	for _, row := range rows {
		member := MemberRow{
			GuildID:       row.GuildID,
			UserID:        row.UserID,
			Username:      row.Username,
			GlobalName:    row.GlobalName,
			DisplayName:   row.DisplayName,
			Nick:          row.Nick,
			Discriminator: row.Discriminator,
			Avatar:        row.Avatar,
			RoleIDsJSON:   row.RoleIdsJson,
			Bot:           row.Bot == 1,
			JoinedAt:      parseTime(row.JoinedAt),
			RawJSON:       row.RawJson,
		}
		enrichMemberRow(&member)
		out = append(out, member)
	}
	return out, nil
}

func (s *Store) MemberByID(ctx context.Context, userID string) ([]MemberRow, error) {
	rows, err := s.q.ListMembersByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := make([]MemberRow, 0, len(rows))
	for _, row := range rows {
		member := MemberRow{
			GuildID:       row.GuildID,
			UserID:        row.UserID,
			Username:      row.Username,
			GlobalName:    row.GlobalName,
			DisplayName:   row.DisplayName,
			Nick:          row.Nick,
			Discriminator: row.Discriminator,
			Avatar:        row.Avatar,
			RoleIDsJSON:   row.RoleIdsJson,
			Bot:           row.Bot == 1,
			JoinedAt:      parseTime(row.JoinedAt),
			RawJSON:       row.RawJson,
		}
		enrichMemberRow(&member)
		out = append(out, member)
	}
	return out, nil
}

func (s *Store) Channels(ctx context.Context, guildID string) ([]ChannelRow, error) {
	var out []ChannelRow
	if guildID != "" {
		rows, err := s.q.ListChannelsByGuild(ctx, guildID)
		if err != nil {
			return nil, err
		}
		out = make([]ChannelRow, 0, len(rows))
		for _, row := range rows {
			out = append(out, ChannelRow{
				ID:               row.ID,
				GuildID:          row.GuildID,
				ParentID:         row.ParentID,
				Kind:             row.Kind,
				Name:             row.Name,
				Topic:            row.Topic,
				Position:         int(row.Position.Int64),
				IsNSFW:           row.IsNsfw == 1,
				IsArchived:       row.IsArchived == 1,
				IsLocked:         row.IsLocked == 1,
				IsPrivateThread:  row.IsPrivateThread == 1,
				ThreadParentID:   row.ThreadParentID,
				ArchiveTimestamp: parseTime(row.ArchiveTimestamp),
			})
		}
		return out, nil
	}
	rows, err := s.q.ListChannels(ctx)
	if err != nil {
		return nil, err
	}
	out = make([]ChannelRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, ChannelRow{
			ID:               row.ID,
			GuildID:          row.GuildID,
			ParentID:         row.ParentID,
			Kind:             row.Kind,
			Name:             row.Name,
			Topic:            row.Topic,
			Position:         int(row.Position.Int64),
			IsNSFW:           row.IsNsfw == 1,
			IsArchived:       row.IsArchived == 1,
			IsLocked:         row.IsLocked == 1,
			IsPrivateThread:  row.IsPrivateThread == 1,
			ThreadParentID:   row.ThreadParentID,
			ArchiveTimestamp: parseTime(row.ArchiveTimestamp),
		})
	}
	return out, nil
}

func (s *Store) GuildChannelCount(ctx context.Context, guildID string) (int, error) {
	count, err := s.q.CountChannelsByGuild(ctx, guildID)
	return int(count), err
}

func (s *Store) GuildMemberCount(ctx context.Context, guildID string) (int, error) {
	count, err := s.q.CountMembersByGuild(ctx, guildID)
	return int(count), err
}

func (s *Store) IncompleteMessageChannelIDs(ctx context.Context, guildID string) ([]string, error) {
	if guildID != "" {
		return s.q.ListIncompleteMessageChannelIDsByGuild(ctx, guildID)
	}
	return s.q.ListIncompleteMessageChannelIDs(ctx)
}

func (s *Store) Status(ctx context.Context, dbPath, defaultGuildID string) (Status, error) {
	status := Status{DBPath: dbPath, DefaultGuildID: defaultGuildID}
	guildCount, err := s.q.CountGuilds(ctx)
	if err != nil {
		return Status{}, err
	}
	channelCount, err := s.q.CountChannels(ctx)
	if err != nil {
		return Status{}, err
	}
	messageCount, err := s.q.CountMessages(ctx)
	if err != nil {
		return Status{}, err
	}
	memberCount, err := s.q.CountMembers(ctx)
	if err != nil {
		return Status{}, err
	}
	embeddingBacklog, err := s.q.CountEmbeddingBacklog(ctx)
	if err != nil {
		return Status{}, err
	}
	threadCount, err := s.q.CountThreads(ctx)
	if err != nil {
		return Status{}, err
	}
	status.GuildCount = int(guildCount)
	status.ChannelCount = int(channelCount)
	status.MessageCount = int(messageCount)
	status.MemberCount = int(memberCount)
	status.EmbeddingBacklog = int(embeddingBacklog)
	status.ThreadCount = int(threadCount)

	lastSync, err := s.q.GetSyncUpdatedAt(ctx, "sync:last_success")
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Status{}, err
	}
	status.LastSyncAt = parseTime(lastSync)
	lastTail, err := s.q.GetSyncUpdatedAt(ctx, "tail:last_event")
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Status{}, err
	}
	status.LastTailEventAt = parseTime(lastTail)
	if defaultGuildID != "" {
		name, err := s.q.GetGuildName(ctx, defaultGuildID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return Status{}, err
		}
		status.DefaultGuildName = name
	}
	guildIDs, err := s.q.ListGuildIDs(ctx)
	if err != nil {
		return Status{}, err
	}
	status.AccessibleGuildIDs = guildIDs
	return status, nil
}

func (s *Store) ReadOnlyQuery(ctx context.Context, query string) ([]string, [][]string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil, errors.New("empty query")
	}
	if !IsReadOnlySQL(query) {
		return nil, nil, errors.New("only read-only sql is allowed")
	}
	db, closeFn, err := s.openReadOnlyDB()
	if err != nil {
		return nil, nil, err
	}
	if closeFn != nil {
		defer closeFn()
	}
	return queryRows(ctx, db, query)
}

func (s *Store) Query(ctx context.Context, query string) ([]string, [][]string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil, errors.New("empty query")
	}
	return queryRows(ctx, s.db, query)
}

func (s *Store) Exec(ctx context.Context, query string) (int64, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return 0, errors.New("empty query")
	}
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()

	result, err := s.db.ExecContext(queryCtx, query)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func queryRows(ctx context.Context, db *sql.DB, query string) ([]string, [][]string, error) {
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()

	rows, err := db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	if len(cols) == 0 {
		return nil, nil, errors.New("query returned no columns")
	}

	var out [][]string
	for rows.Next() {
		if len(out) >= queryRowLimit {
			return nil, nil, fmt.Errorf("query returned more than %d rows", queryRowLimit)
		}
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		record := make([]string, len(cols))
		for i, value := range values {
			record[i] = stringify(value)
		}
		out = append(out, record)
	}
	return cols, out, rows.Err()
}

func (s *Store) openReadOnlyDB() (*sql.DB, func(), error) {
	if strings.TrimSpace(s.path) == "" {
		return s.db, nil, nil
	}
	if _, err := os.Stat(s.path); err != nil {
		return nil, nil, err
	}
	dsn := fmt.Sprintf(
		"file:%s?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)&_pragma=temp_store(MEMORY)&_pragma=mmap_size(268435456)",
		s.path,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, func() { _ = db.Close() }, nil
}

func withQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, queryTimeout)
}

func searchCandidateLimit(limit int) int {
	if limit <= 0 {
		limit = 20
	}
	candidates := limit * searchCandidateMultiple
	if candidates < searchCandidateFloor {
		return searchCandidateFloor
	}
	if candidates > searchCandidateCap {
		return searchCandidateCap
	}
	return candidates
}

func IsReadOnlySQL(query string) bool {
	switch leadingSQLKeyword(query) {
	case "select", "with", "explain", "pragma":
		return true
	default:
		return false
	}
}

func leadingSQLKeyword(query string) string {
	trimmed := strings.TrimSpace(query)
	for trimmed != "" {
		switch {
		case strings.HasPrefix(trimmed, "--"):
			if idx := strings.IndexByte(trimmed, '\n'); idx >= 0 {
				trimmed = strings.TrimSpace(trimmed[idx+1:])
				continue
			}
			return ""
		case strings.HasPrefix(trimmed, "/*"):
			end := strings.Index(trimmed, "*/")
			if end < 0 {
				return ""
			}
			trimmed = strings.TrimSpace(trimmed[end+2:])
		default:
			fields := strings.Fields(trimmed)
			if len(fields) == 0 {
				return ""
			}
			return strings.ToLower(fields[0])
		}
	}
	return ""
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
}

func stringify(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(v)
	case string:
		return v
	case time.Time:
		return v.Format(timeLayout)
	default:
		return fmt.Sprint(v)
	}
}

func normalizeFTSQuery(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	fields := strings.Fields(raw)
	for i, field := range fields {
		fields[i] = `"` + strings.ReplaceAll(field, `"`, " ") + `"`
	}
	return strings.Join(fields, " ")
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	t, err := time.Parse(timeLayout, value)
	if err == nil {
		return t
	}
	t, _ = time.Parse(time.RFC3339, value)
	return t
}
