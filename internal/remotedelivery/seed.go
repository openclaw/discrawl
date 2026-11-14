package remotedelivery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/openclaw/crawlkit/snapshot"
	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/discrawl/internal/store"
)

const SeedPreparationFormat = "discrawl-native-seed/v1"

// SeedPreparation proves local projection/baseline agreement. Remote seed
// completion and permission to register a binding remain separate operations.
type SeedPreparation struct {
	Format             string               `json:"format"`
	Scope              Scope                `json:"scope"`
	Baseline           store.NativeBaseline `json:"baseline"`
	SeedManifestSHA256 string               `json:"seed_manifest_sha256"`
	OwnedKeysSHA256    string               `json:"owned_keys_sha256"`
	OwnedKeysCount     int64                `json:"owned_keys_count"`
}

// PrepareSeed exclusively creates a private directory containing snapshot/,
// ownership.jsonl, and preparation.json. The preparation receipt is written last;
// failures retain partial output for diagnosis and must not be used for admission.
func PrepareSeed(ctx context.Context, source, root string, scope Scope) (SeedPreparation, string, error) {
	var result SeedPreparation
	if _, ok := ctx.Deadline(); !ok {
		return result, "", errors.New("seed preparation requires a context deadline")
	}
	scope, err := canonicalScope(scope)
	if err != nil {
		return result, "", err
	}
	if err := literalOutboxPath(source); err != nil {
		return result, "", err
	}
	if err := literalOutboxPath(root); err != nil {
		return result, "", err
	}
	if err := ctx.Err(); err != nil {
		return result, "", err
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		return result, "", err
	}
	spoolPath := filepath.Join(root, ".ownership.sqlite")
	spool, err := crawlstore.Open(ctx, crawlstore.Options{
		Path: spoolPath, MaxOpenConns: 1, MaxIdleConns: 1,
		Schema: "PRAGMA cache_size=-4096; CREATE TABLE keys(key TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID",
	})
	if err != nil {
		return result, "", err
	}
	defer func() { _ = spool.Close() }()
	tx, err := spool.DB().BeginTx(ctx, nil)
	if err != nil {
		return result, "", err
	}
	defer func() { _ = tx.Rollback() }()
	insert, err := tx.PrepareContext(ctx, "INSERT INTO keys VALUES(?,?)")
	if err != nil {
		return result, "", err
	}
	defer func() { _ = insert.Close() }()
	checked := map[string]bool{}
	filter := func(ctx context.Context, read *sql.Tx, table string, row map[string]any) (bool, error) {
		if !checked[table] {
			if err := validateSeedStorage(ctx, read, table); err != nil {
				return false, err
			}
			checked[table] = true
		}
		change, err := seedChange(table, row)
		if err != nil {
			return false, err
		}
		values, err := projectRow(change)
		if err != nil {
			return false, err
		}
		coverage, channels := map[string]string{}, map[string]store.NativeChannelCoverage{}
		if table == "sync_state" && strings.HasPrefix(change.Key[0].Text, "channel:") {
			channel, suffix, valid := strings.Cut(strings.TrimPrefix(change.Key[0].Text, "channel:"), ":")
			if !valid || !validIdentity(channel) || suffix == "" {
				return false, ErrProjection
			}
			var guild string
			err := read.QueryRowContext(ctx, "SELECT guild_id FROM channels WHERE id=?", channel).Scan(&guild)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return false, err
			}
			coverage[change.Key[0].Text] = guild
			channels[channel] = store.NativeChannelCoverage{Present: err == nil, GuildID: guild}
		}
		included, err := rowInScope(change, values, coverage, channels, scope)
		if err != nil || !included {
			return false, err
		}
		entry := SeedKey{Table: table, Key: change.Key}
		key, err := OwnershipKey(entry.Table, entry.Key)
		if err != nil {
			return false, err
		}
		body, err := json.Marshal(entry)
		if err != nil {
			return false, err
		}
		_, err = insert.ExecContext(ctx, key, body)
		return err == nil, err
	}
	manifest, baseline, err := store.ExportNativeSnapshot(ctx, source, filepath.Join(root, "snapshot"), filter)
	if err != nil {
		return result, "", err
	}
	for _, table := range manifest.Tables {
		if !sameSeedColumns(table.Name, table.Columns) || (table.Name == "guilds" && table.Rows != len(scope.GuildIDs)) {
			return result, "", ErrProjection
		}
	}
	if err := tx.Commit(); err != nil {
		return result, "", err
	}
	count, digest, err := writeSeedOwnership(ctx, spool.DB(), filepath.Join(root, "ownership.jsonl"))
	if err != nil {
		return result, "", err
	}
	if err := spool.Close(); err != nil {
		return result, "", err
	}
	if err := os.Remove(spoolPath); err != nil {
		return result, "", err
	}
	body, err := os.ReadFile(filepath.Join(root, "snapshot", snapshot.ManifestName))
	if err != nil {
		return result, "", err
	}
	sum := sha256.Sum256(body)
	result = SeedPreparation{
		Format: SeedPreparationFormat, Scope: scope, Baseline: baseline,
		SeedManifestSHA256: hex.EncodeToString(sum[:]), OwnedKeysSHA256: digest, OwnedKeysCount: count,
	}
	body, err = json.Marshal(result)
	if err != nil {
		return SeedPreparation{}, "", err
	}
	body = append(body, '\n')
	if err := writeSeedFile(filepath.Join(root, "preparation.json"), body); err != nil {
		return SeedPreparation{}, "", err
	}
	sum = sha256.Sum256(body)
	return result, hex.EncodeToString(sum[:]), nil
}

func seedChange(table string, row map[string]any) (store.NativeChange, error) {
	change := store.NativeChange{Table: table, Operation: "upsert", Columns: columns[table]}
	if len(row) != len(change.Columns) || len(change.Columns) == 0 {
		return change, ErrProjection
	}
	for _, name := range change.Columns {
		var value store.NativeValue
		switch v := row[name].(type) {
		case nil:
			value.Kind = "null"
		case string:
			value.Kind, value.Text = "text", v
		case int64:
			value.Kind, value.Integer = "integer", v
		default:
			return change, ErrProjection
		}
		change.Row = append(change.Row, value)
		if slices.Contains(keys[table], name) {
			change.Key = append(change.Key, value)
		}
	}
	return change, nil
}

// Snapshot filters expose BLOB values as strings. Check the native storage
// classes in this same transaction before allowing that conversion to obscure
// a value the live report protocol cannot represent. This runs once per table.
func validateSeedStorage(ctx context.Context, tx *sql.Tx, table string) error {
	var invalid []string
	for _, name := range columns[table] {
		kind := "text"
		if slices.Contains(integers[table], name) {
			kind = "integer"
		}
		allowed := "'" + kind + "'"
		if slices.Contains(nullable[table], name) {
			allowed += ",'null'"
		}
		invalid = append(invalid, "typeof(\""+name+"\") NOT IN ("+allowed+")")
	}
	var found int
	err := tx.QueryRowContext(ctx, "SELECT 1 FROM \""+table+"\" WHERE "+strings.Join(invalid, " OR ")+" LIMIT 1").Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return ErrProjection
}

func sameSeedColumns(table string, names []string) bool {
	want, ok := columns[table]
	if !ok || len(names) != len(want) {
		return false
	}
	for _, name := range want {
		if !slices.Contains(names, name) {
			return false
		}
	}
	return true
}

func writeSeedOwnership(ctx context.Context, db *sql.DB, path string) (int64, string, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = file.Close() }()
	rows, err := db.QueryContext(ctx, "SELECT key,value FROM keys ORDER BY key COLLATE BINARY")
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = rows.Close() }()
	hash := sha256.New()
	var count int64
	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return 0, "", err
		}
		if _, err := file.Write(append(value, '\n')); err != nil {
			return 0, "", err
		}
		hashOwnershipKey(hash, key)
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, "", err
	}
	if err := file.Sync(); err != nil {
		return 0, "", err
	}
	return count, hex.EncodeToString(hash.Sum(nil)), file.Close()
}

func writeSeedFile(path string, body []byte) error {
	temporary := path + ".partial"
	f, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(body); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// Publish an already closed, synced receipt without replacing any file.
	// A crash before this link leaves only the explicitly incomplete name.
	if err := os.Link(temporary, path); err != nil {
		return err
	}
	_ = os.Remove(temporary) // A leftover link is harmless after publication.
	return nil
}
