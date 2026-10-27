package syncer

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/messagetext"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	issue321ID   = "100000000000000001"
	issue321Text = "quartz preservation sentinel"
	issue321Edit = "2026-10-01T00:02:00Z"
)

// Only the transport is replaced: no sockets, secrets, or external state.
// The SDK request/decoder and native admission/SQLite/search are real.
type issue321Transport struct {
	t              *testing.T
	body           string
	beforeResponse func()
	calls          int
}

func (f *issue321Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.t.Helper()
	require.Equal(f.t, "GET", r.Method)
	require.Equal(f.t, "discord.com", r.URL.Host)
	require.True(f.t, r.URL.Path == "/api/v9/channels/c1/messages" || r.URL.Path == "/api/v9/channels/c1/messages/"+issue321ID)
	require.Equal(f.t, "Bot synthetic-local-fixture", r.Header.Get("Authorization"))
	f.calls++
	if f.beforeResponse != nil {
		f.beforeResponse()
	}
	body := f.body
	if strings.HasSuffix(r.URL.Path, "/messages") {
		body = "[" + body + "]"
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

type issue321Client struct {
	fakeClient
	session *discordgo.Session
}

func (c *issue321Client) ChannelMessage(ctx context.Context, channel, id string) (*discordgo.Message, error) {
	return c.session.ChannelMessage(channel, id, discordgo.WithContext(ctx))
}

func issue321Payload(t *testing.T, content, edited string) map[string]any {
	t.Helper()
	fields := map[string]any{"id": issue321ID, "guild_id": "g1", "channel_id": "c1", "author": map[string]any{"id": "u1", "username": "synthetic", "bot": false}, "content": content, "type": 0, "timestamp": "2026-10-01T00:00:00Z", "attachments": []any{}, "embeds": []any{}, "components": []any{}, "mentions": []any{}, "flags": 0, "pinned": false}
	if edited != "" {
		fields["edited_timestamp"] = edited
	}
	return fields
}

func issue321JSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return string(raw)
}

func issue321Decode(t *testing.T, raw string) *discordgo.Message {
	t.Helper()
	var msg discordgo.Message
	require.NoError(t, json.Unmarshal([]byte(raw), &msg))
	return &msg
}

func issue321Store(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "private-fixture.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.UpsertChannel(t.Context(), store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", RawJSON: "{}"}))
	_, err = s.DB().ExecContext(t.Context(), "update channels set collection_scope='allowed',scope_policy='fixture' where id='c1'")
	require.NoError(t, err)
	return s
}

// Capture full canonical and related rows so retention cannot silently stitch
// old authored text to replacement receipts, extraction state, or job leases.
type issue321State struct {
	CanonicalRows, AttachmentRows, MentionRows, JobRows           string
	Content, Normalized, Raw, Parts, Edited, Deleted, Updated     string
	Version, SearchHits, EmbeddingRevision, Attachments, Mentions int
}

func issue321Rows(t *testing.T, s *store.Store, query string) string {
	t.Helper()
	rows, err := s.DB().QueryContext(t.Context(), query, issue321ID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	columns, err := rows.Columns()
	require.NoError(t, err)
	var snapshot [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for i := range values {
			destinations[i] = &values[i]
		}
		require.NoError(t, rows.Scan(destinations...))
		snapshot = append(snapshot, values)
	}
	require.NoError(t, rows.Err())
	return issue321JSON(t, snapshot)
}

func issue321Read(t *testing.T, s *store.Store) issue321State {
	t.Helper()
	var v issue321State
	v.CanonicalRows = issue321Rows(t, s, "select * from messages where id=?")
	v.AttachmentRows = issue321Rows(t, s, "select * from message_attachments where message_id=? order by attachment_id")
	v.MentionRows = issue321Rows(t, s, "select * from mention_events where message_id=? order by event_id")
	v.JobRows = issue321Rows(t, s, "select * from embedding_jobs where message_id=?")
	require.NoError(t, s.DB().QueryRowContext(t.Context(), "select content,normalized_content,raw_json,text_parts_json,text_version,coalesce(edited_at,''),coalesce(deleted_at,''),updated_at,coalesce((select revision from embedding_jobs where message_id=messages.id),0),(select count(*) from message_attachments where message_id=messages.id),(select count(*) from mention_events where message_id=messages.id) from messages where id=?", issue321ID).Scan(&v.Content, &v.Normalized, &v.Raw, &v.Parts, &v.Version, &v.Edited, &v.Deleted, &v.Updated, &v.EmbeddingRevision, &v.Attachments, &v.Mentions))
	hits, err := s.SearchMessages(t.Context(), store.SearchOptions{Query: "quartz", GuildIDs: []string{"g1"}, Limit: 10})
	require.NoError(t, err)
	v.SearchHits = len(hits)
	require.Equal(t, v.Content, issue321Decode(t, v.Raw).Content, "canonical/raw agreement")
	var parts []messagetext.Part
	require.NoError(t, json.Unmarshal([]byte(v.Parts), &parts))
	if v.Version == messagetext.Version {
		require.Equal(t, v.Normalized, messagetext.Normalize(parts))
	}
	return v
}

func issue321Seed(t *testing.T, s *store.Store, edited string) {
	t.Helper()
	h := &tailHandler{store: s, preserveHistoryCursor: true, enqueueEmbeddings: true}
	require.NoError(t, h.OnMessageCreate(t.Context(), issue321Decode(t, issue321JSON(t, issue321Payload(t, issue321Text, edited)))))
	initial := issue321Read(t, s)
	require.Equal(t, issue321Text, initial.Content)
	require.Equal(t, 1, initial.SearchHits)
	require.NotEqual(t, "[]", initial.Parts)
	require.Positive(t, initial.EmbeddingRevision)
}

func issue321Reread(t *testing.T, s *store.Store, lane, raw string, before func()) {
	t.Helper()
	transport := &issue321Transport{t: t, body: raw, beforeResponse: before}
	session, err := discordgo.New("Bot synthetic-local-fixture")
	require.NoError(t, err)
	session.Client = &http.Client{Transport: transport}
	session.LogLevel = -1
	if lane == "REST" {
		started := time.Now()
		messages, err := session.ChannelMessages("c1", 100, "", "", "", discordgo.WithContext(t.Context()))
		require.NoError(t, err)
		svc := New(nil, s, nil)
		svc.SetAttachmentTextEnabled(false)
		_, err = svc.persistMessagePage(t.Context(), messages, "", "g1", true, started)
		require.NoError(t, err)
	} else {
		require.Equal(t, "Gateway-refetch", lane)
		h := &tailHandler{store: s, client: &issue321Client{session: session}, preserveHistoryCursor: true, enqueueEmbeddings: true}
		partial := issue321Decode(t, `{"id":"100000000000000001","guild_id":"g1","channel_id":"c1","pinned":false}`)
		require.NoError(t, h.OnMessageUpdate(t.Context(), partial))
	}
	require.Equal(t, 1, transport.calls)
}

func TestIssue321UnchangedEmptySnapshot(t *testing.T) {
	for _, lane := range []string{"REST", "Gateway-refetch"} {
		for _, revision := range []string{"equal", "absent"} {
			t.Run(lane+"/"+revision, func(t *testing.T) {
				edited := issue321Edit
				if revision == "absent" {
					edited = ""
				}
				s := issue321Store(t)
				issue321Seed(t, s, edited)
				before := issue321Read(t, s)
				issue321Reread(t, s, lane, issue321JSON(t, issue321Payload(t, "", edited)), nil)
				after := issue321Read(t, s)
				t.Logf("canonical_len=%d->%d normalized_len=%d->%d search_hits=%d->%d raw_equal=%v parts_equal=%v version=%d->%d", len(before.Content), len(after.Content), len(before.Normalized), len(after.Normalized), before.SearchHits, after.SearchHits, before.Raw == after.Raw, before.Parts == after.Parts, before.Version, after.Version)
				assert.Equal(t, before, after, "an otherwise unchanged, unordered empty snapshot must not replace canonical state or related projections")
			})
		}
	}
}

func TestIssue321EmptySnapshotReceiptCompatibility(t *testing.T) {
	for _, lane := range []string{"REST", "Gateway-refetch"} {
		for _, scenario := range []string{"legacy-v0", "metadata-only", "same-rich-envelope"} {
			t.Run(lane+"/"+scenario, func(t *testing.T) {
				s := issue321Store(t)
				issue321Seed(t, s, "")
				incoming := issue321Payload(t, "", "")
				if scenario == "legacy-v0" {
					var receipt map[string]any
					require.NoError(t, json.Unmarshal([]byte(issue321Read(t, s).Raw), &receipt))
					// Old native receipts omitted these fields; null/[]/omitted
					// empty collections and unrelated SDK additions are not edits.
					for _, key := range []string{"guild_id", "components", "poll", "sticker_items", "message_snapshots", "interaction_metadata", "reactions", "member"} {
						delete(receipt, key)
					}
					_, err := s.DB().ExecContext(t.Context(), "update messages set raw_json=?,text_version=0,text_parts_json='[]' where id=?", issue321JSON(t, receipt), issue321ID)
					require.NoError(t, err)
				} else if scenario == "same-rich-envelope" {
					incoming["attachments"] = []any{map[string]any{"id": "a1", "filename": "quartz.txt"}}
					incoming["embeds"] = []any{map[string]any{"description": "quartz embed"}}
					incoming["flags"] = 1 << 15
					incoming["components"] = []any{map[string]any{"type": 10, "content": "quartz component"}}
					incoming["mentions"] = []any{map[string]any{"id": "u2", "username": "mentioned"}}
					incoming["content"] = issue321Text
					h := &tailHandler{store: s, preserveHistoryCursor: true, enqueueEmbeddings: true}
					require.NoError(t, h.OnMessageCreate(t.Context(), issue321Decode(t, issue321JSON(t, incoming))))
					incoming["content"] = ""
				} else {
					incoming["author"] = map[string]any{"id": "u1", "username": "renamed", "global_name": "new display", "bot": false}
					incoming["pinned"] = true
					incoming["reactions"] = []any{map[string]any{"count": 1, "emoji": map[string]any{"name": "thumbsup"}}}
				}
				before := issue321Read(t, s)
				if scenario == "same-rich-envelope" {
					require.Equal(t, 1, before.Attachments)
					require.Equal(t, 1, before.Mentions)
					require.Contains(t, before.Normalized, "quartz component")
					require.Contains(t, before.Normalized, "quartz embed")
				}
				issue321Reread(t, s, lane, issue321JSON(t, incoming), nil)
				assert.Equal(t, before, issue321Read(t, s), "metadata freshness/schema changes are not permission to clear text")
			})
		}
	}
}

func TestIssue321EmptySnapshotControls(t *testing.T) {
	for _, lane := range []string{"REST", "Gateway-refetch"} {
		for _, scenario := range []string{"newer-clear", "newer-text", "older-empty", "known-edit-missing", "tombstone", "concurrent-write", "attachment-change", "component-change", "embed-change", "nonempty-refresh"} {
			t.Run(lane+"/"+scenario, func(t *testing.T) {
				s := issue321Store(t)
				issue321Seed(t, s, issue321Edit)
				incoming := issue321Payload(t, "", issue321Edit)
				want, normalized := issue321Text, issue321Text
				var concurrent func()
				switch scenario {
				case "newer-clear":
					incoming["edited_timestamp"] = "2026-10-01T00:03:00Z"
					want, normalized = "", ""
				case "newer-text":
					incoming["edited_timestamp"] = "2026-10-01T00:03:00Z"
					incoming["content"] = "quartz newer text"
					want, normalized = "quartz newer text", "quartz newer text"
				case "older-empty":
					incoming["edited_timestamp"] = "2026-10-01T00:01:00Z"
				case "known-edit-missing":
					delete(incoming, "edited_timestamp")
				case "tombstone":
					require.NoError(t, s.MarkMessageDeleted(t.Context(), "g1", "c1", issue321ID, map[string]string{"id": issue321ID}))
					incoming["edited_timestamp"] = "2026-10-01T00:03:00Z"
				case "concurrent-write":
					want, normalized = "quartz concurrent text", "quartz concurrent text"
					concurrent = func() {
						h := &tailHandler{store: s, preserveHistoryCursor: true, enqueueEmbeddings: true}
						require.NoError(t, h.OnMessageCreate(t.Context(), issue321Decode(t, issue321JSON(t, issue321Payload(t, want, issue321Edit)))))
					}
				case "attachment-change":
					incoming["attachments"] = []any{map[string]any{"id": "a1", "filename": "quartz.txt"}}
					want, normalized = "", "quartz.txt"
				case "component-change":
					prior := issue321Payload(t, issue321Text, issue321Edit)
					prior["flags"] = 1 << 15
					prior["components"] = []any{map[string]any{"type": 10, "content": "quartz old component"}}
					h := &tailHandler{store: s, preserveHistoryCursor: true, enqueueEmbeddings: true}
					require.NoError(t, h.OnMessageCreate(t.Context(), issue321Decode(t, issue321JSON(t, prior))))
					incoming["flags"] = 1 << 15
					incoming["components"] = []any{map[string]any{"type": 10, "content": "quartz component"}}
					want, normalized = "", "quartz component"
				case "embed-change":
					incoming["embeds"] = []any{map[string]any{"description": "quartz embed"}}
					want, normalized = "", "quartz embed"
				case "nonempty-refresh":
					incoming["content"] = "quartz same revision refresh"
					want, normalized = "quartz same revision refresh", "quartz same revision refresh"
				}
				before := issue321Read(t, s)
				issue321Reread(t, s, lane, issue321JSON(t, incoming), concurrent)
				after := issue321Read(t, s)
				require.Equal(t, want, after.Content)
				if scenario == "tombstone" {
					require.Equal(t, before, after)
					require.NotEmpty(t, after.Deleted)
					require.Zero(t, after.SearchHits)
				} else {
					require.Equal(t, normalized, after.Normalized)
					if normalized == "" {
						require.Zero(t, after.SearchHits)
						require.Equal(t, "[]", after.Parts)
					} else {
						require.Equal(t, 1, after.SearchHits)
					}
					if scenario == "older-empty" || scenario == "known-edit-missing" {
						require.Equal(t, before, after)
					}
					if scenario == "attachment-change" {
						require.Equal(t, 1, after.Attachments)
					}
				}
				t.Logf("CONTROL %s canonical_len=%d search_hits=%d", scenario, len(after.Content), after.SearchHits)
			})
		}
	}
}
