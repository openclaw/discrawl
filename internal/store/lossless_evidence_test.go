package store

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLosslessEvidenceReconstructsEachObservation(t *testing.T) {
	s := openEfficiencyStore(t)
	ctx := t.Context()
	a := " {\n\"id\":\"g\",\"features\":[\"B\",\"A\"],\"member_count\":2,\"unknown\":null,\"signed_url\":\"https://x/a?expires=123\"} "
	b := `{"id":"g","features":["A","B"],"member_count":0,"unknown":null,"signed_url":"https://x/a?expires=123"}`
	for _, raw := range []string{a, a, b, a} {
		require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "G", RawJSON: raw}))
	}
	require.Equal(t, 1, feedCount(t, s, "guilds"))
	var blobs, checks int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from entity_evidence`).Scan(&blobs))
	require.Equal(t, 2, blobs)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from entity_check_history`).Scan(&checks))
	require.Equal(t, 4, checks)
	rows, err := s.DB().QueryContext(ctx, `select payload_json from entity_observations join entity_evidence using(evidence_hash) order by observation_id`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var raws []string
	for rows.Next() {
		var body string
		require.NoError(t, rows.Scan(&body))
		var rec GuildRecord
		require.NoError(t, json.Unmarshal([]byte(body), &rec))
		raws = append(raws, rec.RawJSON)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, []string{a, a, b, a}, raws)
	require.Equal(t, []string{"2", "0", "2"}, metricRows(t, s, "guild", "g", "member_count"))
	// Unknown null/absent and array shape differences remain real content.
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "G", RawJSON: `{"id":"g","features":["A","B"],"signed_url":"https://x/a?expires=123"}`}))
	require.Equal(t, 2, feedCount(t, s, "guilds"))
}

func TestLegacyMentionIDsRemainResolvable(t *testing.T) {
	s := openEfficiencyStore(t)
	ctx := t.Context()
	seedEfficiencyChannel(t, s)
	m := messageMutation(restMessage, "hello", "t", nil)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{m}))
	_, err := s.DB().ExecContext(ctx, `insert into mention_events(message_id,guild_id,channel_id,target_type,target_id,target_name,event_at) select message_id,guild_id,channel_id,target_type,target_id,'legacy',event_at from mention_events`)
	require.NoError(t, err)
	var old int64
	require.NoError(t, s.DB().QueryRowContext(ctx, `select max(event_id) from mention_events`).Scan(&old))
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{m}))
	var body string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select payload_json from mention_event_history where event_id=?`, old).Scan(&body))
	require.Contains(t, body, `"target_name":"legacy"`)
	m.Mentions = nil
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{m}))
	var n int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from mention_event_history`).Scan(&n))
	require.Equal(t, 2, n)
}

func TestLegacyRawSurvivesFirstCanonicalReplacement(t *testing.T) {
	s := openEfficiencyStore(t)
	ctx := t.Context()
	legacy := " {\n\"id\":\"g\",\"features\":[\"B\",\"A\"],\"member_count\":123,\"unknown\":1.2300} "
	_, err := s.DB().ExecContext(ctx, `insert into guilds(id,name,raw_json,updated_at) values('g','old',?,'2026-01-01T00:00:00Z')`, legacy)
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "old", RawJSON: `{"id":"g","features":["A","B"],"member_count":123,"unknown":1.2300}`}))
	}
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "new", RawJSON: `{"id":"g","features":["A","B"],"unknown":2}`}))
	var body string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select payload_json from entity_observations join entity_evidence using(evidence_hash) where observed_at='legacy'`).Scan(&body))
	var rec struct {
		RawJSON   string
		UpdatedAt string
	}
	require.NoError(t, json.Unmarshal([]byte(body), &rec))
	require.Equal(t, legacy, rec.RawJSON)
	require.Equal(t, "2026-01-01T00:00:00Z", rec.UpdatedAt)
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "newer", RawJSON: `{"id":"g","unknown":3}`}))
	var n int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from entity_observations where observed_at='legacy'`).Scan(&n))
	require.Equal(t, 1, n)
}

func TestLosslessMigrationIsIdempotentAndRollbackKeepsEvidence(t *testing.T) {
	s := openEfficiencyStore(t)
	ctx := t.Context()
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "a", RawJSON: `{"id":"g","member_count":1}`}))
	var schemaBefore, schemaAfter int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma schema_version`).Scan(&schemaBefore))
	require.NoError(t, s.applyEntityStateMigration(ctx))
	require.NoError(t, s.ensureChangeFeed(ctx))
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma schema_version`).Scan(&schemaAfter))
	require.Equal(t, schemaBefore, schemaAfter)
	var evidenceBefore, evidenceAfter int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from entity_observations`).Scan(&evidenceBefore))
	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, recordEvidenceTx(ctx, tx, "guilds", GuildRecord{ID: "g", Name: "b"}, "g"))
	require.NoError(t, tx.Rollback())
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from entity_observations`).Scan(&evidenceAfter))
	require.Equal(t, evidenceBefore, evidenceAfter)
	var version int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, 7, version)
}

func TestLegacyAttachmentAndRefetchEvidenceSurvive(t *testing.T) {
	s := openEfficiencyStore(t)
	ctx := t.Context()
	seedEfficiencyChannel(t, s)
	a := &AttachmentRecord{AttachmentID: "a", MessageID: "m", GuildID: "g", ChannelID: "c", Filename: "old.txt", URL: "https://x/?signed=old", TextStatus: "failed", TextError: "old", TextAttemptedAt: "2026-01-01T01:00:00.123456Z"}
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{messageMutation(restMessage, "hello", "t", a)}))
	require.NoError(t, s.RecordAttachmentRefetchFailure(ctx, MessageRecord{ID: "m"}, "new-error"))
	a.Filename = "new.txt"
	a.URL = "https://x/?signed=new"
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{messageMutation(restMessage, "hello", "t", a)}))
	var body string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select payload_json from entity_evidence join entity_observations using(evidence_hash) where table_name='message_attachments' and observed_at='legacy'`).Scan(&body))
	var legacy struct{ RawJSON string }
	require.NoError(t, json.Unmarshal([]byte(body), &legacy))
	require.Contains(t, legacy.RawJSON, `"filename":"old.txt"`)
	require.Contains(t, legacy.RawJSON, `"text_error":"old"`)
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from entity_evidence where json_extract(payload_json,'$.Code')='new-error'`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestRepairStartClockDoesNotDuplicateMessageEvidence(t *testing.T) {
	s := openEfficiencyStore(t)
	ctx := t.Context()
	seedEfficiencyChannel(t, s)
	m := messageMutation(restMessage, "hello", "t", nil)
	for _, at := range []time.Time{time.Now(), time.Now().Add(time.Minute)} {
		m.Options.ObservationStartedAt = at
		require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{m}))
	}
	var blobs, checks int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(distinct evidence_hash) from entity_observations where table_name='messages'`).Scan(&blobs))
	require.Equal(t, 1, blobs)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from entity_check_history where table_name='messages' and check_kind='observation_started'`).Scan(&checks))
	require.Equal(t, 2, checks)
}

func TestImportedMentionIdentityFenceRetainsFirstBinding(t *testing.T) {
	s := openEfficiencyStore(t)
	ctx := t.Context()
	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	defer rollback(tx)
	row := map[string]any{"event_id": int64(42), "message_id": "m", "target_type": "user", "target_id": "u", "target_name": "original"}
	require.NoError(t, MapImportedMentionIdentityTx(ctx, tx, row))
	require.NoError(t, MapImportedMentionIdentityTx(ctx, tx, row))
	row["message_id"] = "other"
	require.NoError(t, MapImportedMentionIdentityTx(ctx, tx, row))
	require.Equal(t, int64(43), row["event_id"])
	require.NoError(t, tx.Commit())
	var body string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select payload_json from mention_event_history where event_id=42`).Scan(&body))
	require.Contains(t, body, `"message_id":"m"`)
	require.Contains(t, body, `"target_name":"original"`)
}

func TestReplyTransportMergePreservesAmbiguousEmptyReread(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "batch"}[batch], func(t *testing.T) {
			s := openEfficiencyStore(t)
			ctx := t.Context()
			gateway := `{"id":"100","channel_id":"c","author":{"id":"u"},"timestamp":"2026-10-01T00:00:00Z","content":"quartz","referenced_message":{"id":"r","channel_id":"c","guild_id":"g","content":"quoted","member":{"roles":["role"]},"author":{"id":"ru","public_flags":0}}}`
			rest := `{"id":"100","channel_id":"c","author":{"id":"u"},"timestamp":"2026-10-01T00:00:00Z","content":"quartz","referenced_message":{"id":"r","channel_id":"c","content":"quoted","author":{"id":"ru","public_flags":64}}}`
			m := MessageRecord{ID: "100", GuildID: "g", ChannelID: "c", AuthorID: "u", CreatedAt: "2026-10-01T00:00:00Z", Content: "quartz", NormalizedContent: "quartz"}
			apply := func(raw, content string) {
				m.RawJSON = raw
				m.Content = content
				m.NormalizedContent = content
				opts := WriteOptions{PreserveNewer: true, ObservationStartedAt: time.Now()}
				if batch {
					require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{{Record: m, Options: opts}}))
				} else {
					require.NoError(t, s.UpsertMessageWithOptions(ctx, m, opts))
				}
			}
			apply(gateway, "quartz")
			apply(rest, "quartz")
			switched, _ := canonicalizeRaw(entityMessage, strings.Replace(rest, `"id":"r"`, `"id":"new-reference"`, 1), gateway, "")
			require.Nil(t, rawField(t, switched, "referenced_message.member"))
			require.Nil(t, rawField(t, switched, "referenced_message.guild_id"))
			empty := strings.Replace(rest, `"content":"quartz"`, `"content":""`, 1)
			apply(empty, "")
			var content string
			require.NoError(t, s.DB().QueryRowContext(ctx, `select content from messages where id='100'`).Scan(&content))
			require.Equal(t, "quartz", content)
			// A real quoted rich-content change still passes admission.
			apply(strings.Replace(empty, `"content":"quoted"`, `"content":"changed quote"`, 1), "")
			require.NoError(t, s.DB().QueryRowContext(ctx, `select content from messages where id='100'`).Scan(&content))
			require.Empty(t, content)
		})
	}
}

func TestUnchangedMessageRenameRefreshesLexicalMetadata(t *testing.T) {
	ctx := t.Context()
	s, err := openWithLexicalTokenizers(ctx, filepath.Join(t.TempDir(), "archive.db"), map[string]LexicalTokenizer{"ar": newArabicLexicalTokenizer()})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	c := ChannelRecord{ID: "c", GuildID: "g", Kind: "text", Name: "general", RawJSON: `{}`}
	require.NoError(t, s.UpsertChannel(ctx, c))
	m := MessageRecord{ID: "100", GuildID: "g", ChannelID: "c", ChannelName: "general", AuthorID: "u", AuthorName: "old", Content: "والكتاب", NormalizedContent: "والكتاب", RawJSON: `{}`}
	require.NoError(t, s.UpsertMessage(ctx, m))
	var before, tokens string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from messages where id='100'`).Scan(&before))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content from message_fts_ar where message_id='100'`).Scan(&tokens))
	c.Name = "maktaba"
	require.NoError(t, s.UpsertChannel(ctx, c))
	m.ChannelName = "maktaba"
	m.AuthorName = "new"
	require.NoError(t, s.UpsertMessage(ctx, m))
	results, err := s.SearchMessages(ctx, SearchOptions{Query: "كتاب", Channel: "maktaba", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	var after, nowTokens, author string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from messages where id='100'`).Scan(&after))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content,author_name from message_fts_ar where message_id='100'`).Scan(&nowTokens, &author))
	require.Equal(t, before, after)
	require.Equal(t, tokens, nowTokens)
	require.Equal(t, "new", author)
}
