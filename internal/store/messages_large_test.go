package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageHydrationBeyondSQLiteVariableLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	// SQLite permits 32766 bound parameters; uncapped archive slices can exceed it.
	const count = 32767
	_, err = s.db.ExecContext(ctx, `
		with recursive sequence(n) as (select 1 union all select n + 1 from sequence where n < ?)
		insert into messages (id, guild_id, channel_id, message_type, created_at, content, normalized_content, raw_json, updated_at)
		select printf('m%05d', n), 'g1', 'archive', 0, '2026-09-01T00:00:00.000000000Z',
		       'ping <@&role>', 'ping <@&role>', '{}', '2026-09-01T00:00:00.000000000Z' from sequence`, count)
	require.NoError(t, err)
	_, err = s.db.ExecContext(ctx, `
		insert into mention_events (message_id, guild_id, channel_id, target_type, target_id, target_name, event_at)
		select id, guild_id, channel_id, 'role', 'role', 'Readers', created_at from messages`)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{GuildID: "g1", UserID: "u32767", Username: "last-user", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c32767", GuildID: "g1", Kind: "text", Name: "last-channel", RawJSON: `{}`}))

	t.Run("uncapped listing", func(t *testing.T) {
		rows, err := s.ListMessages(ctx, MessageListOptions{Channel: "archive"})
		require.NoError(t, err)
		require.Len(t, rows, count)
		require.Equal(t, "m00001", rows[0].MessageID)
		require.Equal(t, "m32767", rows[count-1].MessageID)
		for _, row := range rows {
			require.Equal(t, "ping @Readers", row.DisplayContent)
		}
	})

	t.Run("reply roots", func(t *testing.T) {
		replies := make([]MessageRow, count)
		for i := range replies {
			replies[i] = MessageRow{MessageID: fmt.Sprintf("reply%d", i), GuildID: "g1", ReplyToMessage: fmt.Sprintf("m%05d", i+1)}
		}
		rows, err := s.hydrateMessageThreadContext(ctx, replies, count)
		require.NoError(t, err)
		require.Len(t, rows, 2*count)
		require.Equal(t, replies, rows[:count])
		require.Equal(t, "m00001", rows[count].MessageID)
		require.Equal(t, "m32767", rows[2*count-1].MessageID)
		require.Equal(t, "ping @Readers", rows[2*count-1].DisplayContent)
	})

	for _, kind := range []string{"user", "channel"} {
		t.Run(kind+" names", func(t *testing.T) {
			rows := make([]MessageRow, count)
			for i := range rows {
				content := fmt.Sprintf("<@u%d>", i+1)
				if kind == "channel" {
					content = fmt.Sprintf("<#c%d>", i+1)
				}
				rows[i] = MessageRow{GuildID: "g1", DisplayContent: content}
			}
			require.NoError(t, s.resolveInlineDiscordMentions(ctx, rows))
			if kind == "user" {
				require.Equal(t, "<@u1>", rows[0].DisplayContent)
				require.Equal(t, "@last-user", rows[count-1].DisplayContent)
			} else {
				require.Equal(t, "<#c1>", rows[0].DisplayContent)
				require.Equal(t, "#last-channel", rows[count-1].DisplayContent)
			}
		})
	}
}
