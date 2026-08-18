package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

const lexicalFTSVersion = "1"

type LexicalTokenizer interface {
	Tokenize(context.Context, string) (string, error)
	Close() error
}

func openWithLexicalTokenizers(
	ctx context.Context,
	path string,
	tokenizers map[string]LexicalTokenizer,
) (*Store, error) {
	base, err := openBaseStore(ctx, path)
	if err != nil {
		closeLexicalTokenizers(tokenizers)
		return nil, err
	}
	store := &Store{
		db:                base.DB(),
		q:                 newStoreQueries(base.DB()),
		path:              path,
		baseClose:         base.Close,
		lexicalTokenizers: tokenizers,
	}
	if err := store.migrate(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := store.ensureLexicalFTS(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := store.invalidateDisabledLexicalVersions(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) lexicalLanguages() []string {
	languages := make([]string, 0, len(s.lexicalTokenizers))
	for language := range s.lexicalTokenizers {
		languages = append(languages, language)
	}
	sort.Strings(languages)
	return languages
}

func (s *Store) tokenizeLexical(ctx context.Context, text string) (map[string]string, error) {
	if len(s.lexicalTokenizers) == 0 || strings.TrimSpace(text) == "" {
		return nil, nil
	}
	tokenized := make(map[string]string, len(s.lexicalTokenizers))
	for _, language := range s.lexicalLanguages() {
		content, err := s.lexicalTokenizers[language].Tokenize(ctx, text)
		if err != nil {
			return nil, fmt.Errorf("tokenize %s text: %w", language, err)
		}
		tokenized[language] = content
	}
	return tokenized, nil
}

func (s *Store) ensureLexicalFTS(ctx context.Context) error {
	for _, language := range s.lexicalLanguages() {
		var version sql.NullString
		err := s.db.QueryRowContext(ctx, `
			select cursor from sync_state where scope = ?
		`, lexicalFTSScope(language)).Scan(&version)
		if err == nil && version.String == lexicalFTSVersion {
			continue
		}
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("check %s lexical index version: %w", language, err)
		}
		if err := s.rebuildLexicalFTS(ctx, language); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, `
			insert into sync_state(scope, cursor, updated_at)
			values(?, ?, ?)
			on conflict(scope) do update set
				cursor = excluded.cursor,
				updated_at = excluded.updated_at
		`, lexicalFTSScope(language), lexicalFTSVersion, time.Now().UTC().Format(timeLayout)); err != nil {
			return fmt.Errorf("stamp %s lexical index version: %w", language, err)
		}
	}
	return nil
}

func (s *Store) invalidateDisabledLexicalVersions(ctx context.Context) error {
	enabled := make(map[string]struct{}, len(s.lexicalTokenizers))
	for language := range s.lexicalTokenizers {
		enabled[language] = struct{}{}
	}
	knownScopes := map[string]string{
		lexicalFTSScope("ko"): "ko",
		lexicalFTSScope("ja"): "ja",
		lexicalFTSScope("zh"): "zh",
		lexicalFTSScope("ar"): "ar",
	}
	disabledScopes, err := func() ([]string, error) {
		rows, err := s.db.QueryContext(ctx, `
			select scope
			from sync_state
			where scope in (?, ?, ?, ?)
		`,
			lexicalFTSScope("ko"),
			lexicalFTSScope("ja"),
			lexicalFTSScope("zh"),
			lexicalFTSScope("ar"),
		)
		if err != nil {
			return nil, fmt.Errorf("query lexical index versions: %w", err)
		}
		defer func() { _ = rows.Close() }()
		var scopes []string
		for rows.Next() {
			var scope string
			if err := rows.Scan(&scope); err != nil {
				return nil, fmt.Errorf("scan lexical index version: %w", err)
			}
			language, ok := knownScopes[scope]
			if !ok {
				continue
			}
			if _, ok := enabled[language]; !ok {
				scopes = append(scopes, scope)
			}
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate lexical index versions: %w", err)
		}
		return scopes, nil
	}()
	if err != nil {
		return err
	}
	for _, scope := range disabledScopes {
		if _, err := s.db.ExecContext(
			ctx,
			`delete from sync_state where scope = ?`,
			scope,
		); err != nil {
			return fmt.Errorf("invalidate disabled lexical index %s: %w", scope, err)
		}
	}
	return nil
}

func (s *Store) rebuildLexicalIndexes(ctx context.Context) error {
	for _, language := range s.lexicalLanguages() {
		if err := s.rebuildLexicalFTS(ctx, language); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) rebuildLexicalFTS(ctx context.Context, language string) error {
	table := lexicalFTSTable(language)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, "drop table if exists "+table); err != nil {
		return fmt.Errorf("drop %s: %w", table, err)
	}
	if _, err := tx.ExecContext(ctx, createLexicalFTSSQL(table)); err != nil {
		return fmt.Errorf("create %s: %w", table, err)
	}
	if err := configureFTSBulkLoad(ctx, tx, table); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, lexicalRebuildRowsSQL)
	if err != nil {
		return fmt.Errorf("query %s rebuild rows: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var message MessageRecord
		if err := rows.Scan(
			&message.ID,
			&message.GuildID,
			&message.ChannelID,
			&message.AuthorID,
			&message.AuthorName,
			&message.ChannelName,
			&message.NormalizedContent,
		); err != nil {
			return fmt.Errorf("scan %s rebuild row: %w", table, err)
		}
		content, err := s.lexicalTokenizers[language].Tokenize(ctx, message.NormalizedContent)
		if err != nil {
			return fmt.Errorf("tokenize %s rebuild row %s: %w", language, message.ID, err)
		}
		if err := insertLexicalMessageTx(ctx, tx, table, message, content); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s rebuild rows: %w", table, err)
	}
	if err := optimizeFTS(ctx, tx, table); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) upsertLexicalMessageTx(
	ctx context.Context,
	tx *sql.Tx,
	message MessageRecord,
	tokenized map[string]string,
) error {
	for _, language := range s.lexicalLanguages() {
		table := lexicalFTSTable(language)
		rowID, ok := messageFTSRowID(message.ID)
		if !ok {
			continue
		}
		if _, err := tx.ExecContext(ctx, "delete from "+table+" where rowid = ?", rowID); err != nil {
			return err
		}
		if message.DeletedAt == "" {
			if err := insertLexicalMessageTx(ctx, tx, table, message, tokenized[language]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) deleteLexicalMessagesTx(ctx context.Context, tx *sql.Tx, column string, value any) error {
	if column != "rowid" && column != "guild_id" {
		return fmt.Errorf("unsupported lexical delete column %q", column)
	}
	for _, language := range s.lexicalLanguages() {
		if _, err := tx.ExecContext(ctx, "delete from "+lexicalFTSTable(language)+" where "+column+" = ?", value); err != nil {
			return err
		}
	}
	return nil
}

func insertLexicalMessageTx(ctx context.Context, tx *sql.Tx, table string, message MessageRecord, content string) error {
	rowID, ok := messageFTSRowID(message.ID)
	if !ok {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		insert into `+table+`(
			rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content
		) values(?, ?, ?, ?, ?, ?, ?, ?)
	`, rowID, message.ID, message.GuildID, message.ChannelID, nullable(message.AuthorID), message.AuthorName, message.ChannelName, content)
	return err
}

func lexicalFTSTable(language string) string {
	switch language {
	case "ko", "ja", "zh", "ar":
		return "message_fts_" + language
	default:
		panic("unsupported lexical language: " + language)
	}
}

func lexicalFTSScope(language string) string {
	return "schema:" + lexicalFTSTable(language) + "_version"
}

func isMessageFTSTable(table string) bool {
	return table == "message_fts" ||
		table == "message_fts_ko" ||
		table == "message_fts_ja" ||
		table == "message_fts_zh" ||
		table == "message_fts_ar"
}

func createLexicalFTSSQL(table string) string {
	return `create virtual table ` + table + ` using fts5(
		message_id unindexed,
		guild_id unindexed,
		channel_id unindexed,
		author_id unindexed,
		author_name,
		channel_name,
		content,
		tokenize = 'unicode61 remove_diacritics 0'
	)`
}

func closeLexicalTokenizers(tokenizers map[string]LexicalTokenizer) {
	for _, tokenizer := range tokenizers {
		_ = tokenizer.Close()
	}
}

const lexicalRebuildRowsSQL = `
	select
		m.id,
		m.guild_id,
		m.channel_id,
		coalesce(m.author_id, ''),
		coalesce(
			json_extract(m.raw_json, '$.member.nick'),
			json_extract(m.raw_json, '$.author.global_name'),
			json_extract(m.raw_json, '$.author.username'),
			''
		),
		coalesce(c.name, ''),
		m.normalized_content
	from messages m
	left join channels c on c.id = m.channel_id
	where m.deleted_at is null
	order by cast(m.id as integer)
`
