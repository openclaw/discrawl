// Command postgres_receiver is a test-only bridge built in a selected
// crawl-remote checkout. It never becomes part of the Discrawl executable.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/openclaw/crawl-remote/archive"
	"github.com/openclaw/crawl-remote/ingest"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 5 {
		return fmt.Errorf("expected fixture database, seed directory, native anchor and qualification hash")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", os.Args[1])
	if err != nil {
		return err
	}
	defer db.Close()
	for db.PingContext(ctx) != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("fixture database readiness: %w", ctx.Err())
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, name := range []string{"0001_discord_messages.sql", "0004_discord_binary_text.sql", "0005_discord_batch_lookup.sql", "0006_discord_report_inputs.sql", "0015_discord_changes.sql", "0016_discord_live_reads.sql"} {
		data, err := os.ReadFile("postgres/" + name)
		if err != nil {
			return err
		}
		if _, err = db.ExecContext(ctx, string(data)); err != nil {
			return err
		}
	}
	manifest, err := os.ReadFile(os.Args[2] + "/manifest.json")
	if err != nil {
		return err
	}
	sum := sha256.Sum256(manifest)
	sha := hex.EncodeToString(sum[:])
	if _, err = db.ExecContext(ctx, `insert into crawl_archive.sources(id,provider,scope) values('fixture','discord','fixture')`); err != nil {
		return err
	}
	seed, err := ingest.SeedDiscordReportInputs(ctx, db, os.Args[2], "fixture", sha)
	if err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, `insert into crawl_ingest.discord_change_bindings(id,source_id,profile,guild_ids,baseline_native,baseline_qualification_sha256,seed_manifest_sha256,seed_completion_sequence)
		values('fixture-binding','fixture',$1,array['g1'],$2,decode($3,'hex'),$4,$5)`, ingest.DiscordReportChangeProfile, os.Args[3], os.Args[4], sha, seed.Sequence); err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, `create role fixture_reader login;
		grant usage on schema crawl_archive,crawl_ingest to fixture_reader;
		grant select on all tables in schema crawl_archive,crawl_ingest to fixture_reader`); err != nil {
		return err
	}
	readerURL, err := url.Parse(os.Args[1])
	if err != nil {
		return err
	}
	readerURL.User = url.User("fixture_reader")
	q := readerURL.Query()
	q.Set("default_transaction_read_only", "on")
	readerURL.RawQuery = q.Encode()
	readerDB, err := sql.Open("pgx", readerURL.String())
	if err != nil {
		return err
	}
	defer readerDB.Close()
	publisher, err := ingest.NewDiscordChangesHandler(db, "fixture", "fixture-binding", "fixture-publisher")
	if err != nil {
		return err
	}
	reader, err := archive.NewDiscordLiveHandler(readerDB, "fixture", "fixture-binding", "fixture-reader")
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("POST /v1/apps/discrawl/archives/fixture/changes", publisher)
	mux.Handle("/", reader)
	mux.HandleFunc("GET /fixture/sequence", func(w http.ResponseWriter, r *http.Request) {
		var sequence int64
		if err := readerDB.QueryRowContext(r.Context(), `select sequence from crawl_archive.sources where id='fixture'`).Scan(&sequence); err != nil {
			http.Error(w, "fixture state unavailable", 500)
			return
		}
		_ = json.NewEncoder(w).Encode(sequence)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	defer server.Close()
	go server.Serve(listener)
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"origin": "http://" + listener.Addr().String(), "cursor": seed.Cursor, "sequence": seed.Sequence, "manifest_sha256": sha}); err != nil {
		return err
	}
	// The parent owns the helper and closes its input before stopping PostgreSQL.
	_, err = io.Copy(io.Discard, os.Stdin)
	return err
}
