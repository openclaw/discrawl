package store

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRecordEfficiencyReplay is an opt-in measurement on a fixture extracted
// from a real archive (never the live store):
//
//	DISCRAWL_EFFICIENCY_FIXTURE=/path/fixture.db go test -run TestRecordEfficiencyReplay -v ./internal/store
//
// The fixture holds copies of guilds, channels, members, messages, their
// attachments and mention events, plus a gateway(message_id, payload_json)
// table with each message's Gateway create payload. The test loads the rows
// into a fresh archive with the Gateway shapes, then replays a REST repair
// (stored raw_json), a Gateway replay and a second REST repair, and reports
// content writes (change-feed entries) per pass. Before 0.20.0 every pass
// rewrote every row.
func TestRecordEfficiencyReplay(t *testing.T) {
	path := os.Getenv("DISCRAWL_EFFICIENCY_FIXTURE")
	if path == "" {
		t.Skip("set DISCRAWL_EFFICIENCY_FIXTURE to a fixture database")
	}
	ctx := t.Context()
	src, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = src.Close() }()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	var guilds []GuildRecord
	rows, err := src.QueryContext(ctx, `select id,name,coalesce(icon,''),raw_json from guilds`)
	require.NoError(t, err)
	defer func(r *sql.Rows) { _ = r.Close() }(rows)
	for rows.Next() {
		var g GuildRecord
		require.NoError(t, rows.Scan(&g.ID, &g.Name, &g.Icon, &g.RawJSON))
		guilds = append(guilds, g)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close()) //nolint:sqlclosecheck // Each cursor is also closed by the captured-pointer defer before rows is reassigned.
	var channels []ChannelRecord
	rows, err = src.QueryContext(ctx, `select id,guild_id,coalesce(parent_id,''),kind,name,coalesce(topic,''),coalesce(position,0),is_nsfw,is_archived,is_locked,is_private_thread,coalesce(thread_parent_id,''),coalesce(archive_timestamp,''),raw_json from channels where deleted_at is null`)
	require.NoError(t, err)
	defer func(r *sql.Rows) { _ = r.Close() }(rows)
	for rows.Next() {
		var c ChannelRecord
		require.NoError(t, rows.Scan(&c.ID, &c.GuildID, &c.ParentID, &c.Kind, &c.Name, &c.Topic, &c.Position, &c.IsNSFW, &c.IsArchived, &c.IsLocked, &c.IsPrivateThread, &c.ThreadParentID, &c.ArchiveTimestamp, &c.RawJSON))
		channels = append(channels, c)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close()) //nolint:sqlclosecheck // Each cursor is also closed by the captured-pointer defer before rows is reassigned.
	var members []MemberRecord
	rows, err = src.QueryContext(ctx, `select guild_id,user_id,username,coalesce(global_name,''),coalesce(display_name,''),coalesce(nick,''),coalesce(discriminator,''),coalesce(avatar,''),bot,coalesce(joined_at,''),role_ids_json,raw_json from members where deleted_at is null`)
	require.NoError(t, err)
	defer func(r *sql.Rows) { _ = r.Close() }(rows)
	for rows.Next() {
		var m MemberRecord
		require.NoError(t, rows.Scan(&m.GuildID, &m.UserID, &m.Username, &m.GlobalName, &m.DisplayName, &m.Nick, &m.Discriminator, &m.Avatar, &m.Bot, &m.JoinedAt, &m.RoleIDsJSON, &m.RawJSON))
		members = append(members, m)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close()) //nolint:sqlclosecheck // Each cursor is also closed by the captured-pointer defer before rows is reassigned.

	type message struct {
		rest, gateway, shape MessageMutation
	}
	var messages []message
	rows, err = src.QueryContext(ctx, `select m.id,m.guild_id,m.channel_id,coalesce(m.author_id,''),m.message_type,m.created_at,coalesce(m.edited_at,''),m.content,m.normalized_content,coalesce(m.reply_to_message_id,''),m.pinned,m.has_attachments,m.raw_json,g.payload_json from messages m join gateway g on g.message_id=m.id`)
	require.NoError(t, err)
	defer func(r *sql.Rows) { _ = r.Close() }(rows)
	for rows.Next() {
		var r MessageRecord
		var gateway string
		require.NoError(t, rows.Scan(&r.ID, &r.GuildID, &r.ChannelID, &r.AuthorID, &r.MessageType, &r.CreatedAt, &r.EditedAt, &r.Content, &r.NormalizedContent, &r.ReplyToMessageID, &r.Pinned, &r.HasAttachments, &r.RawJSON, &gateway))
		rest := MessageMutation{Record: r, Options: WriteOptions{PreserveNewer: true}}
		gw := rest
		gw.Record.RawJSON = gateway
		shape := rest
		shape.Record.RawJSON = gatewayShape(t, r.RawJSON, gateway)
		messages = append(messages, message{rest: rest, gateway: gw, shape: shape})
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close()) //nolint:sqlclosecheck // Each cursor is also closed by the captured-pointer defer before rows is reassigned.
	for i := range messages {
		id := messages[i].rest.Record.ID
		arows, err := src.QueryContext(ctx, `select attachment_id,message_id,guild_id,channel_id,coalesce(author_id,''),filename,coalesce(content_type,''),size,coalesce(url,''),coalesce(proxy_url,''),text_content,text_status,text_error,coalesce(text_attempted_at,''),coalesce(text_succeeded_at,'') from message_attachments where message_id=?`, id)
		require.NoError(t, err)
		var atts []AttachmentRecord
		defer func(r *sql.Rows) { _ = r.Close() }(arows)
		for arows.Next() {
			var a AttachmentRecord
			require.NoError(t, arows.Scan(&a.AttachmentID, &a.MessageID, &a.GuildID, &a.ChannelID, &a.AuthorID, &a.Filename, &a.ContentType, &a.Size, &a.URL, &a.ProxyURL, &a.TextContent, &a.TextStatus, &a.TextError, &a.TextAttemptedAt, &a.TextSucceededAt))
			atts = append(atts, a)
		}
		require.NoError(t, arows.Err())
		require.NoError(t, arows.Close()) //nolint:sqlclosecheck // Captured-pointer defer also closes this cursor.
		mrows, err := src.QueryContext(ctx, `select message_id,guild_id,channel_id,coalesce(author_id,''),target_type,target_id,target_name,event_at from mention_events where message_id=? order by event_id`, id)
		require.NoError(t, err)
		var mentions []MentionEventRecord
		defer func(r *sql.Rows) { _ = r.Close() }(mrows)
		for mrows.Next() {
			var m MentionEventRecord
			require.NoError(t, mrows.Scan(&m.MessageID, &m.GuildID, &m.ChannelID, &m.AuthorID, &m.TargetType, &m.TargetID, &m.TargetName, &m.EventAt))
			mentions = append(mentions, m)
		}
		require.NoError(t, mrows.Err())
		require.NoError(t, mrows.Close()) //nolint:sqlclosecheck // Captured-pointer defer also closes this cursor.
		for _, mm := range []*MessageMutation{&messages[i].rest, &messages[i].gateway, &messages[i].shape} {
			mm.Attachments, mm.Mentions = atts, mentions
		}
	}

	tables := []string{"guilds", "channels", "members", "messages", "message_attachments", "mention_events", "entity_metrics"}
	counts := func() map[string]int {
		out := map[string]int{}
		for _, table := range tables {
			out[table] = feedCount(t, s, table)
		}
		var checks int
		require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from entity_checks`).Scan(&checks))
		out["entity_checks(rows)"] = checks
		return out
	}
	apply := func(variant int) {
		for _, g := range guilds {
			require.NoError(t, s.UpsertGuild(ctx, g))
		}
		require.NoError(t, s.UpsertChannels(ctx, channels))
		for _, m := range members {
			require.NoError(t, s.UpsertMember(ctx, m))
		}
		for _, m := range messages {
			mm := m.rest
			switch variant {
			case 1:
				mm = m.gateway
			case 2:
				mm = m.shape
			}
			require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{mm}))
		}
	}
	report := func(label string, before map[string]int) map[string]int {
		after := counts()
		for _, table := range append(tables, "entity_checks(rows)") {
			t.Logf("%-22s %-20s +%d", label, table, after[table]-before[table])
		}
		return after
	}
	t.Logf("fixture: %d guilds, %d channels, %d members, %d messages", len(guilds), len(channels), len(members), len(messages))
	apply(1)
	base := report("load (gateway create)", map[string]int{})
	base = report("rest repair", func() map[string]int { apply(0); return base }())
	// Same content in the Gateway transport shape (guild_id, member,
	// mention members, zero user flags): transport differences only.
	base = report("gateway-shape replay", func() map[string]int { apply(2); return base }())
	base = report("rest repair again", func() map[string]int { apply(0); return base }())
	// Replaying the original create payloads also replays content that
	// changed after creation (reactions, embeds, flags, components).
	_ = report("stale create replay", func() map[string]int { apply(1); return base }())
}

// gatewayShape returns the REST content in the Gateway transport shape.
func gatewayShape(t *testing.T, rest, create string) string {
	t.Helper()
	out, ok := decodeRawObject(rest)
	require.True(t, ok)
	gw, ok := decodeRawObject(create)
	require.True(t, ok)
	for _, key := range []string{"guild_id", "member"} {
		if v, ok := gw[key]; ok {
			out[key] = v
		}
	}
	zeroFlags := func(obj map[string]any, key string) {
		if user, ok := obj[key].(map[string]any); ok {
			user["flags"], user["public_flags"] = json.Number("0"), json.Number("0")
		}
	}
	zeroFlags(out, "author")
	if ref, ok := out["referenced_message"].(map[string]any); ok {
		zeroFlags(ref, "author")
	}
	members := map[any]any{}
	if list, ok := gw["mentions"].([]any); ok {
		for _, m := range list {
			if obj, ok := m.(map[string]any); ok {
				members[obj["id"]] = obj["member"]
			}
		}
	}
	if list, ok := out["mentions"].([]any); ok {
		for _, m := range list {
			if obj, ok := m.(map[string]any); ok {
				obj["member"] = members[obj["id"]]
				obj["flags"], obj["public_flags"] = json.Number("0"), json.Number("0")
			}
		}
	}
	return encodeRawObject(out)
}
