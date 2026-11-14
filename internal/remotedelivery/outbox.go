package remotedelivery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/openclaw/crawlkit/remote"
	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/discrawl/internal/store"
)

var (
	ErrPendingCapture = errors.New("outbox has a pending capture; resume its persisted bytes")
	ErrOutboxBaseline = errors.New("outbox baseline differs; preserve state and reconcile")
	ErrOutboxLimit    = errors.New("outbox capture exceeds its row or byte limit")
)

// Registration is supplied only after external seed/scope qualification. These
// hashes pin that evidence; they do not create approval or verify the remote seed.
// Credentials and the source pathname never belong in this record.
type Registration struct {
	Origin, Archive, BindingID              string
	SeedManifestSHA256, QualificationSHA256 string
	OwnedKeysSHA256                         string
	OwnedKeysCount                          int64
	Scope                                   Scope
	Baseline                                store.NativeBaseline
	ServerCursor                            string
}

type SeedKey struct {
	Table string
	Key   []store.NativeValue
}

// SeedKeys streams qualified ownership in ascending OwnershipKey byte order.
// Its digest is SHA256 over uint64 big-endian byte length followed by each key.
type SeedKeys func(yield func(SeedKey) error) error

type OutboxState struct {
	Registration Registration
	Baseline     store.NativeBaseline
	ServerCursor string
	PendingID    string
}

type (
	Outbox  struct{ base *crawlstore.Store }
	PartRef struct {
		CaptureID string
		Index     int
	}
)

type PendingPart struct {
	Ref    PartRef
	Body   []byte
	SHA256 string
}

const outboxApplicationID = 0x4443444f // DCDO

const outboxSchema = `
pragma application_id=1145259087;
pragma user_version=1;
create table binding(singleton integer primary key check(singleton=1), registration blob not null, baseline blob not null, server_cursor text not null);
create table captures(id text primary key, native_from blob not null, native_through blob not null, initial_cursor text not null, part_count integer not null check(part_count between 1 and 64), ownership blob not null, completed integer not null default 0 check(completed in (0,1)));
create unique index one_pending_capture on captures((1)) where completed=0;
create table parts(capture_id text not null references captures(id), part_index integer not null check(part_index between 0 and 63), body blob not null, sha256 text not null, receipt blob, primary key(capture_id,part_index));
create table owned_keys(key text primary key);
`

func validateRegistration(r Registration) error {
	u, err := url.Parse(r.Origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("outbox requires a service origin without credentials, path, query or fragment")
	}
	if !validIdentity(r.Archive) || !validIdentity(r.BindingID) || len(r.Archive) > 128 || len(r.BindingID) > 128 || r.OwnedKeysCount < 0 {
		return ErrOutboxBaseline
	}
	for _, digest := range []string{r.SeedManifestSHA256, r.QualificationSHA256, r.OwnedKeysSHA256} {
		if !lowerHex(digest, 32) {
			return ErrOutboxBaseline
		}
	}
	if _, err := canonicalScope(r.Scope); err != nil {
		return err
	}
	if err := validateBaseline(r.Baseline); err != nil {
		return err
	}
	anchor, err := NativeAnchor(r.Baseline.Cursor)
	if err != nil {
		return err
	}
	_, err = remote.EncodeChangeBatch(remote.ChangeBatch{Contract: remote.ChangeContract, App: "discrawl", Archive: r.Archive, BindingID: r.BindingID, CaptureID: strings.Repeat("0", 32), PartCount: 1, NativeFrom: anchor, NativeThrough: anchor, ExpectedCursor: r.ServerCursor, Changes: []remote.Change{}})
	return err
}

func validateBaseline(b store.NativeBaseline) error {
	if !validIdentity(b.SourceID) || b.Cursor.Sequence < 0 || len(b.Cursor.Generations) != len(columns) {
		return ErrOutboxBaseline
	}
	seen := map[string]bool{}
	for _, g := range b.Cursor.Generations {
		if _, ok := columns[g.Table]; !ok || seen[g.Table] || !lowerHex(g.GenerationID, 16) || !lowerHex(g.SchemaSHA256, 32) {
			return ErrOutboxBaseline
		}
		seen[g.Table] = true
	}
	return nil
}

func lowerHex(value string, size int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == size && strings.ToLower(value) == value
}

func literalOutboxPath(path string) error {
	if !filepath.IsAbs(path) || strings.HasPrefix(path, "file:") || strings.ContainsRune(path, 0) {
		return errors.New("outbox requires a literal absolute file path")
	}
	return nil
}

// InitOutbox creates a new private file exclusively. A failed initialization
// retains its file for diagnosis; it never replaces an existing database.
func InitOutbox(ctx context.Context, path string, r Registration, seed SeedKeys) (*Outbox, error) {
	if err := validateRegistration(r); err != nil {
		return nil, err
	}
	if err := literalOutboxPath(path); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if seed == nil {
		return nil, ErrOwnership
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	base, err := crawlstore.Open(ctx, crawlstore.Options{Path: path})
	if err != nil {
		return nil, err
	}
	o := &Outbox{base: base}
	r.Scope, _ = canonicalScope(r.Scope)
	registration, err := json.Marshal(r)
	if err == nil {
		err = o.write(ctx, func(conn *sql.Conn) error {
			if _, err := conn.ExecContext(ctx, outboxSchema); err != nil {
				return err
			}
			baseline, err := json.Marshal(r.Baseline)
			if err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `insert into binding values(1,?,?,?)`, registration, baseline, r.ServerCursor); err != nil {
				return err
			}
			digest := sha256.New()
			var count int64
			previous := ""
			if err := seed(func(value SeedKey) error {
				key, err := OwnershipKey(value.Table, value.Key)
				if err != nil {
					return err
				}
				if key <= previous {
					return errors.New("seed ownership must be strictly ordered without duplicates")
				}
				if _, err := conn.ExecContext(ctx, `insert into owned_keys values(?)`, key); err != nil {
					return err
				}
				hashOwnershipKey(digest, key)
				previous, count = key, count+1
				return nil
			}); err != nil {
				return err
			}
			if count != r.OwnedKeysCount || hex.EncodeToString(digest.Sum(nil)) != r.OwnedKeysSHA256 {
				return ErrOwnership
			}
			return nil
		})
	}
	if err != nil {
		_ = base.Close()
		return nil, err
	}
	return o, nil
}

func hashOwnershipKey(h hash.Hash, key string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(key)))
	_, _ = h.Write(size[:])
	_, _ = h.Write([]byte(key))
}

// OpenOutbox recognizes existing files read-only before the writable helper
// can change their mode or permissions. There is no automatic schema upgrade.
func OpenOutbox(ctx context.Context, path string) (*Outbox, error) {
	if err := literalOutboxPath(path); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, ErrOutboxBaseline
	}
	reader, err := crawlstore.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, err
	}
	err = reader.WithTx(ctx, func(tx *sql.Tx) error {
		var application, version, tables int
		if err := tx.QueryRowContext(ctx, `pragma application_id`).Scan(&application); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `select count(*) from sqlite_schema where type='table' and name not like 'sqlite_%'`).Scan(&tables); err != nil {
			return err
		}
		if application != outboxApplicationID || version != 1 || tables != 4 {
			return ErrOutboxBaseline
		}
		for _, query := range []string{
			`select key from owned_keys limit 0`,
			`select id,native_from,native_through,initial_cursor,part_count,ownership,completed from captures limit 0`,
			`select capture_id,part_index,body,sha256,receipt from parts limit 0`,
		} {
			rows, err := tx.QueryContext(ctx, query)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			if err := rows.Close(); err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return err
			}
		}
		_, err := readOutboxState(ctx, tx)
		return err
	})
	closeErr := reader.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	after, err := os.Lstat(path)
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(info, after) {
		return nil, ErrOutboxBaseline
	}
	base, err := crawlstore.Open(ctx, crawlstore.Options{Path: path})
	if err != nil {
		return nil, err
	}
	return &Outbox{base: base}, nil
}

func (o *Outbox) Close() error { return o.base.Close() }

type outboxQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readOutboxState(ctx context.Context, q outboxQuery) (OutboxState, error) {
	var state OutboxState
	var registration, baseline []byte
	if err := q.QueryRowContext(ctx, `select registration,baseline,server_cursor from binding where singleton=1`).Scan(&registration, &baseline, &state.ServerCursor); err != nil {
		return state, err
	}
	if json.Unmarshal(registration, &state.Registration) != nil || json.Unmarshal(baseline, &state.Baseline) != nil {
		return OutboxState{}, ErrOutboxBaseline
	}
	if err := validateRegistration(state.Registration); err != nil {
		return OutboxState{}, err
	}
	if err := validateBaseline(state.Baseline); err != nil {
		return OutboxState{}, err
	}
	initial := state.Registration.Baseline
	if initial.SourceID != state.Baseline.SourceID || !slices.Equal(initial.Cursor.Generations, state.Baseline.Cursor.Generations) || initial.Cursor.Sequence > state.Baseline.Cursor.Sequence {
		return OutboxState{}, ErrOutboxBaseline
	}
	err := q.QueryRowContext(ctx, `select id from captures where completed=0`).Scan(&state.PendingID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return OutboxState{}, err
	}
	return state, nil
}

func (o *Outbox) State(ctx context.Context) (OutboxState, error) {
	var state OutboxState
	err := o.base.WithTx(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = readOutboxState(ctx, tx)
		return err
	})
	return state, err
}

func (o *Outbox) write(ctx context.Context, fn func(*sql.Conn) error) error {
	conn, err := o.base.DB().Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, `pragma synchronous=FULL`); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `begin immediate`); err != nil {
		return err
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), `rollback`) }()
	if err := fn(conn); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `commit`)
	return err
}

func sameBaseline(a, b store.NativeBaseline) bool {
	return a.SourceID == b.SourceID && a.Cursor.Sequence == b.Cursor.Sequence && a.Cursor.Entry == b.Cursor.Entry && slices.Equal(a.Cursor.Generations, b.Cursor.Generations)
}

// Stage materializes every request before reserving a pending capture. It
// reads changed-key and channel-checkpoint ownership, then rechecks the baseline
// at the write. No source reopen or state advancement occurs during projection.
func (o *Outbox) Stage(ctx context.Context, capture store.NativeCapture, maxEncodedBytes int64) (string, error) {
	if maxEncodedBytes <= 0 || maxEncodedBytes > int64(remote.MaxChangeBytes)*remote.MaxChangeParts {
		return "", ErrOutboxLimit
	}
	var state OutboxState
	var ownership OwnershipSnapshot
	err := o.base.WithTx(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = readOutboxState(ctx, tx)
		if err != nil {
			return err
		}
		if state.PendingID != "" {
			return ErrPendingCapture
		}
		if !sameBaseline(capture.From, state.Baseline) || capture.Through.SourceID != capture.From.SourceID || capture.Through.Cursor.Sequence <= capture.From.Cursor.Sequence || !slices.Equal(capture.Through.Cursor.Generations, capture.From.Cursor.Generations) {
			return ErrOutboxBaseline
		}
		ownership = OwnershipSnapshot{Baseline: state.Baseline, Keys: map[string]bool{}}
		ownership.ScopeSHA256, err = ScopeSHA256(state.Registration.Scope)
		if err != nil {
			return err
		}
		lookup := func(change store.NativeChange) error {
			key, err := OwnershipKey(change.Table, change.Key)
			if err != nil {
				return err
			}
			if _, known := ownership.Keys[key]; known {
				return nil
			}
			var owned bool
			if err := tx.QueryRowContext(ctx, `select exists(select 1 from owned_keys where key=?)`, key).Scan(&owned); err != nil {
				return err
			}
			ownership.Keys[key] = owned
			return nil
		}
		for _, change := range capture.Changes {
			if err := lookup(change); err != nil {
				return err
			}
		}
		for _, entry := range capture.ChannelCoverage {
			for _, change := range entry.Checkpoints {
				if err := lookup(change); err != nil {
					return err
				}
			}
		}
		return readOwnedCheckpoints(ctx, tx, capture.ChannelCoverage, &ownership, maxEncodedBytes)
	})
	if err != nil {
		return "", err
	}
	projection, err := ProjectCapture(capture, state.Registration.Scope, ownership)
	if err != nil {
		return "", err
	}
	from, err := NativeAnchor(capture.From.Cursor)
	if err != nil {
		return "", err
	}
	through, err := NativeAnchor(capture.Through.Cursor)
	if err != nil {
		return "", err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(random[:])
	parts, err := encodeOutboxParts(state, id, from, through, projection.Changes, maxEncodedBytes)
	if err != nil {
		return "", err
	}
	fromJSON, err := json.Marshal(capture.From)
	if err != nil {
		return "", err
	}
	throughJSON, err := json.Marshal(capture.Through)
	if err != nil {
		return "", err
	}
	deltas, err := json.Marshal(projection.Ownership)
	if err != nil {
		return "", err
	}
	err = o.write(ctx, func(conn *sql.Conn) error {
		current, err := readOutboxState(ctx, conn)
		if err != nil {
			return err
		}
		if current.PendingID != "" {
			return ErrPendingCapture
		}
		if !sameBaseline(current.Baseline, state.Baseline) || current.ServerCursor != state.ServerCursor {
			return ErrOutboxBaseline
		}
		if _, err := conn.ExecContext(ctx, `insert into captures(id,native_from,native_through,initial_cursor,part_count,ownership) values(?,?,?,?,?,?)`, id, fromJSON, throughJSON, state.ServerCursor, len(parts), deltas); err != nil {
			return err
		}
		for i, body := range parts {
			if _, err := conn.ExecContext(ctx, `insert into parts(capture_id,part_index,body,sha256) values(?,?,?,?)`, id, i, body, bodySHA256(body)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (o *Outbox) NextPart(ctx context.Context) (*PendingPart, error) {
	var part *PendingPart
	err := o.base.WithTx(ctx, func(tx *sql.Tx) error {
		state, err := readOutboxState(ctx, tx)
		if err != nil || state.PendingID == "" {
			return err
		}
		part = &PendingPart{Ref: PartRef{CaptureID: state.PendingID}}
		if err := tx.QueryRowContext(ctx, `select part_index,body,sha256 from parts where capture_id=? and receipt is null order by part_index limit 1`, state.PendingID).Scan(&part.Ref.Index, &part.Body, &part.SHA256); err != nil {
			return err
		}
		if bodySHA256(part.Body) != part.SHA256 {
			return errors.New("persisted outbox part hash differs")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return part, nil
}

// RecordReceipt atomically applies the final receipt, ownership delta, capture
// completion and new baseline. Replaying an already saved receipt is harmless.
func (o *Outbox) RecordReceipt(ctx context.Context, ref PartRef, receipt remote.ChangeReceipt) error {
	return o.write(ctx, func(conn *sql.Conn) error {
		state, err := readOutboxState(ctx, conn)
		if err != nil {
			return err
		}
		var digest, initial string
		var recorded, from, through, ownership []byte
		var count int
		if err := conn.QueryRowContext(ctx, `select p.sha256,p.receipt,c.native_from,c.native_through,c.initial_cursor,c.part_count,c.ownership from parts p join captures c on c.id=p.capture_id where p.capture_id=? and p.part_index=?`, ref.CaptureID, ref.Index).Scan(&digest, &recorded, &from, &through, &initial, &count, &ownership); err != nil {
			return err
		}
		batchID := "chg1:" + ref.CaptureID + ":" + strconv.Itoa(ref.Index)
		sequence, err := strconv.ParseInt(receipt.Sequence, 10, 64)
		if err != nil || sequence <= 0 || strconv.FormatInt(sequence, 10) != receipt.Sequence || receipt.App != "discrawl" || receipt.Archive != state.Registration.Archive || receipt.BindingID != state.Registration.BindingID || receipt.BatchID != batchID || receipt.Cursor != batchID || receipt.PayloadSHA256 != digest {
			return errors.New("receipt does not match the persisted outbox part")
		}
		if recorded != nil {
			var prior remote.ChangeReceipt
			if json.Unmarshal(recorded, &prior) != nil {
				return ErrOutboxBaseline
			}
			prior.Replayed = receipt.Replayed
			if prior != receipt {
				return errors.New("receipt differs from recorded receipt")
			}
			return nil
		}
		var baseline store.NativeBaseline
		if json.Unmarshal(from, &baseline) != nil || !sameBaseline(baseline, state.Baseline) || initial != state.ServerCursor || state.PendingID != ref.CaptureID {
			return ErrOutboxBaseline
		}
		var next int
		if err := conn.QueryRowContext(ctx, `select min(part_index) from parts where capture_id=? and receipt is null`, ref.CaptureID).Scan(&next); err != nil {
			return err
		}
		if next != ref.Index {
			return errors.New("outbox requires receipts in part order")
		}
		body, err := json.Marshal(receipt)
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `update parts set receipt=? where capture_id=? and part_index=?`, body, ref.CaptureID, ref.Index); err != nil {
			return err
		}
		if ref.Index != count-1 {
			return nil
		}
		var changes []OwnershipChange
		if json.Unmarshal(ownership, &changes) != nil {
			return ErrOwnership
		}
		for _, change := range changes {
			query := `delete from owned_keys where key=?`
			if change.Owned {
				query = `insert into owned_keys values(?) on conflict do nothing`
			}
			if _, err := conn.ExecContext(ctx, query, change.Key); err != nil {
				return err
			}
		}
		if _, err := conn.ExecContext(ctx, `update binding set baseline=?,server_cursor=? where singleton=1`, through, receipt.Cursor); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `update captures set completed=1 where id=?`, ref.CaptureID)
		return err
	})
}

func bodySHA256(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}
