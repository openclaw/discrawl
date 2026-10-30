package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Record efficiency: unchanged observations write no content row, real
// changes write exactly one, live counters write one metric row per change.

func openEfficiencyStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

type rowState struct {
	UpdatedAt string
	RowID     int64
	Raw       string
}

func readRowState(t *testing.T, s *Store, table, where string, args ...any) rowState {
	t.Helper()
	var st rowState
	query := `select updated_at,rowid,raw_json from ` + table + ` where ` + where
	if table == "message_attachments" {
		query = `select updated_at,rowid,'' from ` + table + ` where ` + where
	}
	require.NoError(t, s.DB().QueryRowContext(t.Context(), query, args...).Scan(&st.UpdatedAt, &st.RowID, &st.Raw))
	return st
}

func feedCount(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from change_log where table_name=?`, table).Scan(&n))
	return n
}

func metricRows(t *testing.T, s *Store, entityType, id, metric string) []string {
	t.Helper()
	metrics, err := s.EntityMetrics(t.Context(), entityType, id)
	require.NoError(t, err)
	var out []string
	for _, m := range metrics {
		if m.Metric == metric {
			out = append(out, m.Value)
		}
	}
	return out
}

func rawField(t *testing.T, raw, path string) any {
	t.Helper()
	obj, ok := decodeRawObject(raw)
	require.True(t, ok, raw)
	var cur any = obj
	for _, key := range splitPath(path) {
		m, _ := cur.(map[string]any)
		cur = m[key]
	}
	if n, ok := cur.(json.Number); ok {
		return n.String()
	}
	return cur
}

func splitPath(path string) []string {
	var out []string
	start := 0
	for i := range len(path) {
		if path[i] == '.' {
			out = append(out, path[start:i])
			start = i + 1
		}
	}
	return append(out, path[start:])
}

const (
	gatewayGuild = `{"id":"g","name":"G","region":"india","joined_at":"2026-01-01T00:00:00.123Z","large":true,"member_count":120000,"premium_subscription_count":49,"features":["B","A","C"],"members":[{"user":{"id":"u"}}],"channels":[{"id":"c"}],"description":"one","roles":[{"id":"g"}]}`
	restGuild    = `{"id":"g","name":"G","region":"deprecated","joined_at":"0001-01-01T00:00:00Z","large":false,"member_count":0,"premium_subscription_count":49,"features":["C","A","B"],"members":null,"channels":null,"description":"one","roles":[{"id":"g"}],"approximate_member_count":0}`
)

func TestGuildRepairWithUnchangedContentWritesNoContentRow(t *testing.T) {
	t.Parallel()
	s := openEfficiencyStore(t)
	ctx := t.Context()
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "G", RawJSON: gatewayGuild}))
	first := readRowState(t, s, "guilds", "id='g'")
	entries := feedCount(t, s, "guilds")
	require.NotNil(t, rawField(t, first.Raw, "members"))
	require.Nil(t, rawField(t, first.Raw, "member_count"))
	require.Equal(t, []any{"A", "B", "C"}, rawField(t, first.Raw, "features"))

	// The 6-hourly REST repair and a reordered Gateway GUILD_CREATE are the
	// same content.
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, s.UpsertObservedGuild(ctx, GuildRecord{ID: "g", Name: "G", RawJSON: restGuild}, time.Now()))
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "G", RawJSON: gatewayGuild}))
	require.Equal(t, first, readRowState(t, s, "guilds", "id='g'"))
	require.Equal(t, entries, feedCount(t, s, "guilds"))
	require.Equal(t, []string{"120000", "0", "120000"}, metricRows(t, s, "guild", "g", "member_count"))
	require.Equal(t, []string{"49"}, metricRows(t, s, "guild", "g", "premium_subscription_count"))

	// A counter change writes one metric row and no content row.
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "G", RawJSON: `{"id":"g","name":"G","region":"india","member_count":120001,"premium_subscription_count":49,"features":["A","B","C"],"description":"one","roles":[{"id":"g"}]}`}))
	require.Equal(t, first.UpdatedAt, readRowState(t, s, "guilds", "id='g'").UpdatedAt)
	require.Equal(t, []string{"120000", "0", "120000", "120001"}, metricRows(t, s, "guild", "g", "member_count"))

	// A real change writes exactly one version.
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "G", RawJSON: `{"id":"g","name":"G","description":"two","features":["A","B","C"],"roles":[{"id":"g"}]}`}))
	changed := readRowState(t, s, "guilds", "id='g'")
	require.NotEqual(t, first.UpdatedAt, changed.UpdatedAt)
	require.Equal(t, entries+1, feedCount(t, s, "guilds"))
	require.Equal(t, "two", rawField(t, changed.Raw, "description"))
	require.Equal(t, "india", rawField(t, changed.Raw, "region"))
	require.Equal(t, "2026-01-01T00:00:00.123Z", rawField(t, changed.Raw, "joined_at"))

	var checks int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from entity_checks where table_name='guilds' and row_key='["g"]' and check_kind='observed'`).Scan(&checks))
	require.Equal(t, 1, checks)
}

func TestChannelTransportShapeAndCountersDoNotVersionContent(t *testing.T) {
	t.Parallel()
	s := openEfficiencyStore(t)
	ctx := t.Context()
	thread := func(bitrate int, invitable bool, archived string, overwrites string, last string, count int) ChannelRecord {
		raw := map[string]any{
			"id": "t", "guild_id": "g", "type": 11, "name": "thread", "bitrate": bitrate, "last_message_id": last, "message_count": count, "member_count": 2,
			"permission_overwrites": json.RawMessage(overwrites),
			"thread_metadata":       map[string]any{"archived": false, "archive_timestamp": archived, "invitable": invitable},
		}
		body, err := json.Marshal(raw)
		require.NoError(t, err)
		return ChannelRecord{ID: "t", GuildID: "g", ParentID: "p", Kind: "thread_public", Name: "thread", ArchiveTimestamp: archived, ThreadParentID: "p", RawJSON: string(body)}
	}
	ow1 := `[{"id":"2","type":0,"allow":"0","deny":"1"},{"id":"1","type":1,"allow":"8","deny":"0"}]`
	ow2 := `[{"id":"1","type":1,"allow":"8","deny":"0"},{"id":"2","type":0,"allow":"0","deny":"1"}]`
	require.NoError(t, s.UpsertChannel(ctx, thread(64000, true, "2026-10-30T13:11:29.580123Z", ow1, "100", 5)))
	first := readRowState(t, s, "channels", "id='t'")
	entries := feedCount(t, s, "channels")
	require.Equal(t, "100", rawField(t, first.Raw, "last_message_id"))

	time.Sleep(2 * time.Millisecond)
	require.NoError(t, s.UpsertObservedChannel(ctx, thread(0, false, "2026-10-30T13:11:29.58Z", ow2, "100", 5), time.Now()))
	require.Equal(t, first, readRowState(t, s, "channels", "id='t'"))
	require.Equal(t, entries, feedCount(t, s, "channels"))

	require.NoError(t, s.UpsertChannel(ctx, thread(0, false, "2026-10-30T13:11:29.58Z", ow2, "100", 6)))
	require.Equal(t, first, readRowState(t, s, "channels", "id='t'"))
	require.Empty(t, metricRows(t, s, "channel", "t", "last_message_id"))
	require.Equal(t, []string{"5", "6"}, metricRows(t, s, "channel", "t", "message_count"))
	require.Equal(t, []string{"2"}, metricRows(t, s, "channel", "t", "member_count"))

	archived := thread(0, false, "2026-10-31T00:00:00Z", ow2, "101", 6)
	archived.IsArchived = true
	require.NoError(t, s.UpsertChannel(ctx, archived))
	require.NotEqual(t, first.UpdatedAt, readRowState(t, s, "channels", "id='t'").UpdatedAt)
	require.Equal(t, entries+1, feedCount(t, s, "channels"))
}

const (
	restMember    = `{"guild_id":"","joined_at":"2026-06-09T02:34:30.363Z","nick":"","avatar":"","user":{"id":"u","username":"rn","avatar":"a1","public_flags":64,"flags":64},"roles":[],"premium_since":null,"flags":42,"communication_disabled_until":null}`
	gatewayMember = `{"guild_id":"g","joined_at":"2026-06-09T02:34:30.363456Z","nick":"","avatar":"","user":{"id":"u","username":"rn","avatar":"a1","public_flags":0,"flags":0},"roles":[],"premium_since":null,"flags":0,"communication_disabled_until":null}`
)

func member(raw, joined, avatar string) MemberRecord {
	return MemberRecord{GuildID: "g", UserID: "u", Username: "rn", Avatar: avatar, JoinedAt: joined, RoleIDsJSON: "[]", RawJSON: raw}
}

func TestMemberRefreshKeepsRESTFlagsAndSkipsUnchangedMembers(t *testing.T) {
	t.Parallel()
	s := openEfficiencyStore(t)
	ctx := t.Context()
	require.NoError(t, s.UpsertMember(ctx, member(restMember, "2026-06-09T02:34:30.363Z", "")))
	require.NoError(t, s.UpsertMember(ctx, member(gatewayMember, "2026-06-09T02:34:30.363456Z", "")))
	// The Gateway shape fills guild_id once; it never zeroes REST flags.
	filled := readRowState(t, s, "members", "user_id='u'")
	require.Equal(t, "g", rawField(t, filled.Raw, "guild_id"))
	require.Equal(t, "42", rawField(t, filled.Raw, "flags"))
	require.Equal(t, "64", rawField(t, filled.Raw, "user.public_flags"))
	entries := feedCount(t, s, "members")

	time.Sleep(2 * time.Millisecond)
	for range 3 {
		require.NoError(t, s.UpsertMember(ctx, member(restMember, "2026-06-09T02:34:30.363Z", "")))
		require.NoError(t, s.UpsertMember(ctx, member(gatewayMember, "2026-06-09T02:34:30.363456Z", "")))
	}
	merge, err := s.MergeObservedMembers(ctx, "g", []MemberRecord{member(restMember, "2026-06-09T02:34:30.363Z", "")}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, merge.Applied)
	require.Equal(t, filled, readRowState(t, s, "members", "user_id='u'"))
	require.Equal(t, entries, feedCount(t, s, "members"))

	// A stale refresh that started before the last check does not win.
	started := time.Now().Add(-time.Hour)
	merge, err = s.MergeObservedMembers(ctx, "g", []MemberRecord{member(restMember, "2026-06-09T02:34:30.363Z", "changed")}, started)
	require.NoError(t, err)
	require.Equal(t, 1, merge.NewerSkipped)

	// A real avatar change writes one version.
	require.NoError(t, s.UpsertMember(ctx, member(restMember, "2026-06-09T02:34:30.363Z", "new-avatar")))
	require.NotEqual(t, filled.UpdatedAt, readRowState(t, s, "members", "user_id='u'").UpdatedAt)
	require.Equal(t, entries+1, feedCount(t, s, "members"))
}

const (
	gatewayMessage = `{"id":"m","channel_id":"c","guild_id":"g","content":"hello","timestamp":"2026-10-27T03:23:43.912Z","author":{"id":"u","username":"x","public_flags":0,"flags":0},"mentions":[{"id":"t","username":"t","member":{"nick":"tee"}}],"member":{"roles":["r"],"joined_at":"2026-04-30T02:58:35.356Z"},"reactions":null,"flags":4}`
	restMessage    = `{"id":"m","channel_id":"c","content":"hello","timestamp":"2026-10-27T03:23:43.912Z","author":{"id":"u","username":"x","public_flags":64,"flags":64},"mentions":[{"id":"t","username":"t"}],"member":null,"reactions":null,"flags":4}`
)

func messageMutation(raw, content, targetName string, attachment *AttachmentRecord) MessageMutation {
	m := MessageMutation{
		Record: MessageRecord{
			ID: "m", GuildID: "g", ChannelID: "c", AuthorID: "u", CreatedAt: "2026-10-27T03:23:43.912Z",
			Content: content, NormalizedContent: content, RawJSON: raw,
		},
		Mentions: []MentionEventRecord{{MessageID: "m", GuildID: "g", ChannelID: "c", AuthorID: "u", TargetType: "user", TargetID: "t", TargetName: targetName, EventAt: "2026-10-27T03:23:43.912Z"}},
		Options:  WriteOptions{PreserveNewer: true},
	}
	if attachment != nil {
		m.Record.HasAttachments = true
		m.Attachments = []AttachmentRecord{*attachment}
	}
	return m
}

func seedEfficiencyChannel(t *testing.T, s *Store) {
	t.Helper()
	require.NoError(t, s.UpsertGuild(t.Context(), GuildRecord{ID: "g", Name: "G", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(t.Context(), ChannelRecord{ID: "c", GuildID: "g", Kind: "text", Name: "c", RawJSON: `{}`}))
}

func TestMessageRepairMergesTransportsAndKeepsStableRelationships(t *testing.T) {
	t.Parallel()
	s := openEfficiencyStore(t)
	ctx := t.Context()
	seedEfficiencyChannel(t, s)
	att := &AttachmentRecord{AttachmentID: "a", MessageID: "m", GuildID: "g", ChannelID: "c", AuthorID: "u", Filename: "a.txt", Size: 3, URL: "https://cdn.example/a", TextStatus: "skipped", TextError: "not_eligible"}
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{messageMutation(gatewayMessage, "hello", "tee", att)}))
	// The REST reread fills REST-only public flags once and keeps Gateway
	// guild_id and member.
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{messageMutation(restMessage, "hello", "tee", att)}))
	merged := readRowState(t, s, "messages", "id='m'")
	require.Equal(t, "g", rawField(t, merged.Raw, "guild_id"))
	require.NotNil(t, rawField(t, merged.Raw, "member"))
	require.Equal(t, "64", rawField(t, merged.Raw, "author.public_flags"))
	mentions := rawField(t, merged.Raw, "mentions").([]any)
	require.NotNil(t, mentions[0].(map[string]any)["member"])
	var eventID int64
	require.NoError(t, s.DB().QueryRowContext(ctx, `select event_id from mention_events where message_id='m'`).Scan(&eventID))
	attachment := readRowState(t, s, "message_attachments", "attachment_id='a'")
	feed := map[string]int{}
	for _, table := range []string{"messages", "message_attachments", "mention_events"} {
		feed[table] = feedCount(t, s, table)
	}
	var maxEventID int64
	require.NoError(t, s.DB().QueryRowContext(ctx, `select seq from sqlite_sequence where name='mention_events'`).Scan(&maxEventID))

	// Gateway -> REST -> Gateway repeats are one content version.
	time.Sleep(2 * time.Millisecond)
	for range 3 {
		require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{messageMutation(gatewayMessage, "hello", "tee", att)}))
		require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{messageMutation(restMessage, "hello", "tee", att)}))
	}
	require.Equal(t, merged, readRowState(t, s, "messages", "id='m'"))
	require.Equal(t, attachment, readRowState(t, s, "message_attachments", "attachment_id='a'"))
	for table, n := range feed {
		require.Equal(t, n, feedCount(t, s, table), table)
	}
	var sameEventID, sameMax int64
	require.NoError(t, s.DB().QueryRowContext(ctx, `select event_id from mention_events where message_id='m'`).Scan(&sameEventID))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select seq from sqlite_sequence where name='mention_events'`).Scan(&sameMax))
	require.Equal(t, eventID, sameEventID)
	require.Equal(t, maxEventID, sameMax)

	// A target rename updates the mention in place and advances the owner.
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{messageMutation(restMessage, "hello", "renamed", att)}))
	var name string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select event_id,target_name from mention_events where message_id='m'`).Scan(&sameEventID, &name))
	require.Equal(t, eventID, sameEventID)
	require.Equal(t, "renamed", name)
	require.Equal(t, feed["mention_events"]+1, feedCount(t, s, "mention_events"))
	require.NotEqual(t, merged.UpdatedAt, readRowState(t, s, "messages", "id='m'").UpdatedAt)

	// A real edit writes one message version; the attachment row is
	// updated in place without a delete.
	edited := messageMutation(`{"id":"m","channel_id":"c","content":"edited","edited_timestamp":"2026-10-28T00:00:00Z"}`, "edited", "renamed", att)
	edited.Record.EditedAt = "2026-10-28T00:00:00Z"
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{edited}))
	require.Equal(t, attachment.RowID, readRowState(t, s, "message_attachments", "attachment_id='a'").RowID)
	require.Equal(t, feed["messages"]+2, feedCount(t, s, "messages"))
	var deletes int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from change_log where table_name='message_attachments' and op='d'`).Scan(&deletes))
	require.Zero(t, deletes)

	// A disappeared attachment and mention are the only deletions.
	gone := messageMutation(`{"id":"m","channel_id":"c","content":"edited","edited_timestamp":"2026-10-28T00:00:00Z"}`, "edited", "renamed", nil)
	gone.Record.EditedAt = "2026-10-28T00:00:00Z"
	gone.Mentions = nil
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{gone}))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from change_log where table_name='message_attachments' and op='d'`).Scan(&deletes))
	require.Equal(t, 1, deletes)
	var mentionRows int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from mention_events where message_id='m'`).Scan(&mentionRows))
	require.Zero(t, mentionRows)
}

func TestAttachmentTextRetryWithSameOutcomeIsACheck(t *testing.T) {
	t.Parallel()
	s := openEfficiencyStore(t)
	ctx := t.Context()
	seedEfficiencyChannel(t, s)
	_, err := s.DB().ExecContext(ctx, `update channels set collection_scope='allowed'`)
	require.NoError(t, err)
	failed := func(at string) *AttachmentRecord {
		return &AttachmentRecord{
			AttachmentID: "a", MessageID: "m", GuildID: "g", ChannelID: "c", Filename: "a.txt", Size: 3,
			TextStatus: "failed", TextError: "http_503", TextAttemptedAt: at,
		}
	}
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{messageMutation(restMessage, "hello", "t", failed("2026-10-30T00:00:00Z"))}))
	before := readRowState(t, s, "message_attachments", "attachment_id='a'")
	entries := feedCount(t, s, "message_attachments")
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{messageMutation(restMessage, "hello", "t", failed("2026-10-30T01:00:00Z"))}))
	require.Equal(t, before, readRowState(t, s, "message_attachments", "attachment_id='a'"))
	require.Equal(t, entries, feedCount(t, s, "message_attachments"))
	var attempted, checked string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select text_attempted_at from message_attachments where attachment_id='a'`).Scan(&attempted))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select checked_at from entity_checks where table_name='message_attachments' and row_key='["a"]' and check_kind='attachment_text'`).Scan(&checked))
	require.Equal(t, "2026-10-30T00:00:00Z", attempted)
	require.Equal(t, "2026-10-30T01:00:00.000000000Z", checked)

	// A successful extraction is content.
	ok := failed("2026-10-30T02:00:00Z")
	ok.TextStatus, ok.TextError, ok.TextContent, ok.TextSucceededAt = "succeeded", "", "abc", "2026-10-30T02:00:00Z"
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{messageMutation(restMessage, "hello", "t", ok)}))
	require.Equal(t, entries+1, feedCount(t, s, "message_attachments"))
}

func TestChangeFeedServesContractTwoNextToContractOne(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(ctx, path)
	require.NoError(t, err)
	status, err := s.ChangeFeedStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, status.ContractVersion)
	require.Equal(t, []int{1, 2}, status.ContractVersions)
	contracts := map[string]int{}
	for _, table := range status.Tables {
		contracts[table.Table] = table.Contract
	}
	for _, table := range changeFeedV1Tables {
		require.Equal(t, 1, contracts[table], table)
	}
	for _, table := range changeFeedV2Tables {
		require.Equal(t, 2, contracts[table], table)
	}
	var tables string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select tables_json from change_feed_contracts where contract_version=2`).Scan(&tables))
	require.JSONEq(t, `["message_attachments","mention_events","message_embedding_history","attachment_text_history","guilds","channels","members","messages","entity_metrics","entity_evidence","entity_observations","entity_check_history","mention_event_history","mention_event_id_map"]`, tables)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select tables_json from change_feed_contracts where contract_version=1`).Scan(&tables))
	require.JSONEq(t, `["message_attachments","mention_events","message_embedding_history","attachment_text_history"]`, tables)

	// Upgrading a contract-1 archive (no contracts table, no contract-2
	// triggers) adds contract 2 without a new generation for contract-1
	// tables, so contract-1 cursors stay valid.
	v1Gen := map[string]string{}
	for _, table := range changeFeedV1Tables {
		_, id, _ := feedGeneration(t, s, table)
		v1Gen[table] = id
	}
	_, err = s.DB().ExecContext(ctx, `drop table change_feed_contracts`)
	require.NoError(t, err)
	for _, table := range changeFeedV2Tables {
		require.NoError(t, func() error {
			tx, err := s.DB().BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer rollback(tx)
			if err := SuspendChangeFeedTx(ctx, tx, table); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `delete from change_feed_tables where table_name=?`, table); err != nil {
				return err
			}
			return tx.Commit()
		}())
	}
	require.NoError(t, s.Close())
	reopened, err := Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	for _, table := range changeFeedV1Tables {
		_, id, _ := feedGeneration(t, reopened, table)
		require.Equal(t, v1Gen[table], id, table)
	}
	for _, table := range changeFeedV2Tables {
		_, _, reason := feedGeneration(t, reopened, table)
		require.Equal(t, "enabled", reason, table)
	}
}

func TestSeedEntityMetricsBackfillsWithoutContentWrites(t *testing.T) {
	t.Parallel()
	s := openEfficiencyStore(t)
	ctx := t.Context()
	// Legacy rows written by an older binary still embed counters.
	_, err := s.DB().ExecContext(ctx, `insert into guilds(id,name,raw_json,updated_at) values('g','G',?,'2026-01-01T00:00:00.000000000Z')`, gatewayGuild)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `insert into channels(id,guild_id,kind,name,raw_json,updated_at) values('c','g','text','c','{"id":"c","last_message_id":"77"}','2026-01-01T00:00:00.000000000Z')`)
	require.NoError(t, err)
	guildFeed, channelFeed := feedCount(t, s, "guilds"), feedCount(t, s, "channels")
	n, err := s.SeedEntityMetrics(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	n, err = s.SeedEntityMetrics(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, metricRows(t, s, "channel", "c", "last_message_id"))
	require.Equal(t, guildFeed, feedCount(t, s, "guilds"))
	require.Equal(t, channelFeed, feedCount(t, s, "channels"))

	// The legacy guild body (with counters and collections) already holds
	// the canonical content: the next REST repair writes no content row.
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "G", RawJSON: restGuild}))
	require.Equal(t, "2026-01-01T00:00:00.000000000Z", readRowState(t, s, "guilds", "id='g'").UpdatedAt)
}

func TestCanonicalRawKeepsIncomingBytesWithoutEdits(t *testing.T) {
	t.Parallel()
	raw := `{"id":"1","channel_id":"c","content":"x"}`
	out, metrics := canonicalizeRaw(entityMessage, raw, "", "")
	require.Equal(t, raw, out)
	require.Empty(t, metrics)
	out, _ = canonicalizeRaw(entityMessage, "not json", "", "")
	require.Equal(t, "not json", out)
	require.Equal(t, "2026-10-30T13:11:29.58Z", normalizeProviderTime("2026-10-30T13:11:29.580999+00:00"))
	require.Equal(t, "bogus", normalizeProviderTime("bogus"))
	_ = context.Background()
}
