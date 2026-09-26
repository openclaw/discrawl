package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

type metadataRaceClient struct {
	*fakeClient
	onChannel, onGuild, onMember func()
}

func (c *metadataRaceClient) Channel(context.Context, string) (*discordgo.Channel, error) {
	c.onChannel()
	return &discordgo.Channel{ID: "c", GuildID: "g", Type: discordgo.ChannelTypeGuildText}, nil
}

func (c *metadataRaceClient) Guild(context.Context, string) (*discordgo.Guild, error) {
	c.onGuild()
	return &discordgo.Guild{ID: "g", Roles: []*discordgo.Role{{ID: "g", Permissions: 1024}}}, nil
}

func (c *metadataRaceClient) GuildMember(context.Context, string, string) (*discordgo.Member, error) {
	c.onMember()
	return &discordgo.Member{GuildID: "g", User: &discordgo.User{ID: "u", Username: "stale"}}, nil
}

func TestMetadataRecoveryCannotUndoNewerGatewayState(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	h := &tailHandler{store: s}
	require.NoError(t, h.OnGuildUpsert(ctx, &discordgo.Guild{ID: "g", Roles: []*discordgo.Role{{ID: "g", Permissions: 1024}}}))
	require.NoError(t, h.OnChannelUpsert(ctx, &discordgo.Channel{ID: "c", GuildID: "g", Type: discordgo.ChannelTypeGuildText}))
	require.NoError(t, h.OnMemberUpsert(ctx, "g", &discordgo.Member{User: &discordgo.User{ID: "u"}}))
	c := &metadataRaceClient{
		fakeClient: &fakeClient{guildThreads: map[string][]*discordgo.Channel{"g": {{ID: "thread", GuildID: "g", ParentID: "c", Type: discordgo.ChannelTypeGuildPublicThread}}}},
		onChannel:  func() { require.NoError(t, h.OnChannelDelete(ctx, &discordgo.Channel{ID: "c", GuildID: "g"})) },
		onGuild: func() {
			require.NoError(t, h.OnGuildRoleUpsert(ctx, &discordgo.GuildRole{GuildID: "g", Role: &discordgo.Role{ID: "g", Permissions: 0}}))
		},
		onMember: func() { require.NoError(t, s.MarkMemberDeleted(ctx, "g", "u", "discord-gateway", "actual-remove")) },
	}
	for _, f := range []store.FailureRef{{ChannelID: "c", RelatedID: "CHANNEL_UPDATE:"}, {RelatedID: "GUILD_ROLE_UPDATE:"}, {RelatedID: "GUILD_MEMBER_UPDATE:u"}, {RelatedID: "THREAD_LIST_SYNC:"}} {
		f.Operation, f.Source, f.GuildID, f.RelatedKind = "tail_metadata", "discord", "g", "gateway_event"
		require.NoError(t, s.RecordFailure(ctx, f, errors.New("fixture")))
	}
	svc := New(c, s, nil)
	require.NoError(t, svc.replayMetadataFailures(ctx, []string{"g"}))
	var channelDeleted, memberDeleted, permission string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select deleted_at from channels where id='c'`).Scan(&channelDeleted))
	require.NotEmpty(t, channelDeleted)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select deleted_at from members where user_id='u'`).Scan(&memberDeleted))
	require.NotEmpty(t, memberDeleted)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select json_extract(raw_json,'$.roles[0].permissions') from guilds where id='g'`).Scan(&permission))
	require.Equal(t, "0", permission)
	require.Equal(t, 1, c.guildThreadCalls, "thread recovery requires the authoritative thread endpoint")
	var n int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from channels where id='thread'`).Scan(&n))
	require.Equal(t, 1, n)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from failure_ledger where resolved_at is null`).Scan(&n))
	require.Zero(t, n)
	// A failed active-thread request stays visible instead of being resolved by
	// unrelated, successful guild metadata.
	require.NoError(t, s.RecordFailure(ctx, store.FailureRef{Operation: "tail_metadata", Source: "discord", GuildID: "g", RelatedKind: "gateway_event", RelatedID: "THREAD_LIST_SYNC:"}, errors.New("fixture")))
	c.guildThreadErrs = map[string]error{"g": errors.New("unavailable")}
	require.NoError(t, svc.replayMetadataFailures(ctx, []string{"g"}))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from failure_ledger where resolved_at is null`).Scan(&n))
	require.Equal(t, 1, n)
}

func TestAttachmentRecoveryCannotOverwriteConcurrentEditOrDeletion(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		t.Run(map[bool]string{false: "edit", true: "delete"}[deletion], func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "db"))
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			h := &tailHandler{store: s}
			require.NoError(t, h.OnChannelUpsert(ctx, &discordgo.Channel{ID: "c", GuildID: "g", Type: discordgo.ChannelTypeGuildText}))
			m := &discordgo.Message{ID: "10", GuildID: "g", ChannelID: "c", Timestamp: time.Now(), Content: "old", Attachments: []*discordgo.MessageAttachment{{ID: "a", Filename: "file.png"}}}
			require.NoError(t, h.OnMessageCreate(ctx, m))
			_, err = s.DB().ExecContext(ctx, `update message_attachments set text_status='failed' where attachment_id='a'`)
			require.NoError(t, err)
			c := &exactReplayClient{messages: map[string]*discordgo.Message{"c/10": m}, onFetch: func(context.Context) {
				if deletion {
					require.NoError(t, h.OnMessageDelete(ctx, &discordgo.MessageDelete{Message: m}))
				} else {
					newer := *m
					newer.Content, newer.Attachments = "newer embed observation with same edit timestamp", nil
					require.NoError(t, h.OnMessageCreate(ctx, &newer))
				}
			}}
			require.NoError(t, New(c, s, nil).retryAttachmentText(ctx, []string{"g"}))
			var content, tombstone string
			require.NoError(t, s.DB().QueryRowContext(ctx, `select content,coalesce(deleted_at,'') from messages where id='10'`).Scan(&content, &tombstone))
			if deletion {
				require.NotEmpty(t, tombstone)
			} else {
				require.Equal(t, "newer embed observation with same edit timestamp", content)
				var n int
				require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_attachments where message_id='10'`).Scan(&n))
				require.Zero(t, n)
			}
		})
	}
}
