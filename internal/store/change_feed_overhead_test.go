package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestChangeFeedWriterOverhead measures trigger cost on a fixture shaped like
// the Studio archive (13% of messages with mentions, 1.5% with attachments,
// ~1.5 KB raw payloads). Opt in with DISCRAWL_CHANGE_FEED_OVERHEAD=1.
func TestChangeFeedWriterOverhead(t *testing.T) {
	if os.Getenv("DISCRAWL_CHANGE_FEED_OVERHEAD") != "1" {
		t.Skip("set DISCRAWL_CHANGE_FEED_OVERHEAD=1 to measure change feed writer overhead")
	}
	messages := 20000
	type result struct {
		ingest, resync, edit time.Duration
		bytes, entries       int64
	}
	run := func(feed bool) result {
		ctx := t.Context()
		dir := t.TempDir()
		path := filepath.Join(dir, "discrawl.db")
		s, err := Open(ctx, path)
		require.NoError(t, err)
		defer func() { _ = s.Close() }()
		if !feed {
			for _, table := range changeFeedTables {
				for _, suffix := range []string{"ai", "au", "ad"} {
					_, err := s.DB().ExecContext(ctx, `drop trigger `+quoteIdent("change_feed_"+table+"_"+suffix))
					require.NoError(t, err)
				}
			}
		}
		seedChangeFeedGuild(t, s)
		raw := `{"payload":"` + strings.Repeat("x", 1500) + `"}`
		batch := func(content string, from, to int) []MessageMutation {
			out := make([]MessageMutation, 0, to-from)
			for i := from; i < to; i++ {
				var attachments []string
				mentions := 0
				if i%100 < 13 {
					mentions = 1
				}
				if i%200 < 3 {
					attachments = []string{fmt.Sprintf("a%d", i)}
				}
				m := changeFeedMutation(strconv.Itoa(1_000_000+i), "c1", fmt.Sprintf("%s %d", content, i), attachments, mentions)
				m.Record.RawJSON = raw
				out = append(out, m)
			}
			return out
		}
		write := func(content string) time.Duration {
			started := time.Now()
			for from := 0; from < messages; from += 50 {
				require.NoError(t, s.UpsertMessages(ctx, batch(content, from, min(from+50, messages))))
			}
			return time.Since(started)
		}
		var r result
		r.ingest = write("original")
		_, err = s.DB().ExecContext(ctx, `insert into message_embeddings(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at)
			select id,'openai','text-embedding-3-small','v3',512,randomblob(2048),'2026-10-28T00:00:00Z' from messages where cast(id as integer)%10=0`)
		require.NoError(t, err)
		r.resync = write("original")
		r.edit = write("edited")
		_, err = s.DB().ExecContext(ctx, `pragma wal_checkpoint(truncate)`)
		require.NoError(t, err)
		require.NoError(t, s.DB().QueryRowContext(ctx, `select page_count*page_size from pragma_page_count, pragma_page_size`).Scan(&r.bytes))
		require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from change_log`).Scan(&r.entries))
		return r
	}
	var base, feed []result
	for range 3 {
		base = append(base, run(false))
		feed = append(feed, run(true))
	}
	best := func(rs []result, pick func(result) time.Duration) time.Duration {
		out := pick(rs[0])
		for _, r := range rs[1:] {
			out = min(out, pick(r))
		}
		return out
	}
	for _, phase := range []struct {
		name string
		pick func(result) time.Duration
	}{{"ingest", func(r result) time.Duration { return r.ingest }}, {"resync", func(r result) time.Duration { return r.resync }}, {"edit", func(r result) time.Duration { return r.edit }}} {
		b, f := best(base, phase.pick), best(feed, phase.pick)
		t.Logf("%s: baseline=%s feed=%s overhead=%.1f%% per_message=%s", phase.name, b, f, 100*(f.Seconds()-b.Seconds())/b.Seconds(), (f-b)/time.Duration(messages))
	}
	t.Logf("database bytes: baseline=%d feed=%d overhead=%d (%.2f%%) entries=%d bytes_per_entry=%.0f",
		base[0].bytes, feed[0].bytes, feed[0].bytes-base[0].bytes, 100*float64(feed[0].bytes-base[0].bytes)/float64(base[0].bytes),
		feed[0].entries, float64(feed[0].bytes-base[0].bytes)/float64(feed[0].entries))
}
