package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func packageMutation(id, channel string) MessageMutation {
	return MessageMutation{
		Record:    MessageRecord{ID: id, GuildID: DirectMessageGuildID, ChannelID: channel, AuthorID: "sender", Content: "package text", NormalizedContent: "package text", RawJSON: `{"content":"package text"}`, CreatedAt: "2026-01-01T00:00:00Z"},
		EventType: "data_package", PayloadJSON: `{"source":"discord_data_package","coverage":"sent_only"}`,
	}
}

func TestPersonalPackageTransactionAndEvidence(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	channel := ChannelRecord{ID: "dm", GuildID: DirectMessageGuildID, Kind: "dm", Name: "package name", RawJSON: `{}`}
	mutation := packageMutation("sent", channel.ID)
	inserted, err := s.ImportPersonalPackageChannel(ctx, channel, []MessageMutation{mutation})
	require.NoError(t, err)
	require.Equal(t, 1, inserted)
	inserted, err = s.ImportPersonalPackageChannel(ctx, channel, []MessageMutation{mutation})
	require.NoError(t, err)
	require.Zero(t, inserted)
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events`).Scan(&count))
	require.Equal(t, 1, count)
	mutation.Record.Content = "conflicting package version"
	mutation.PayloadJSON = `{"source":"discord_data_package","message":"conflicting package version"}`
	_, err = s.ImportPersonalPackageChannel(ctx, channel, []MessageMutation{mutation})
	require.NoError(t, err)
	var content string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content from messages where id='sent'`).Scan(&content))
	require.Equal(t, "package text", content)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events`).Scan(&count))
	require.Equal(t, 2, count, "different evidence is retained without replacing canonical content")
	mutation.Record.AuthorID = "different sender"
	_, err = s.ImportPersonalPackageChannel(ctx, channel, []MessageMutation{packageMutation("rollback", channel.ID), mutation})
	require.ErrorContains(t, err, "conflicts with stored identity")
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages where id='rollback'`).Scan(&count))
	require.Zero(t, count, "failure must roll back earlier writes in this channel")
}

func TestPersonalPackageRejectsScopeChanges(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	channel := ChannelRecord{ID: "dm", GuildID: DirectMessageGuildID, Kind: "dm", RawJSON: `{}`}
	_, err = s.ImportPersonalPackageChannel(ctx, ChannelRecord{ID: "invalid", GuildID: "guild", Kind: "dm"}, nil)
	require.ErrorContains(t, err, "direct-message channel")
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: channel.ID, GuildID: "guild", Kind: "text", RawJSON: `{}`}))
	_, err = s.ImportPersonalPackageChannel(ctx, channel, nil)
	require.ErrorContains(t, err, "conflicts with stored guild")
	channel.ID = "new-dm"
	bad := packageMutation("message", channel.ID)
	bad.Record.ChannelID = "wrong-channel"
	_, err = s.ImportPersonalPackageChannel(ctx, channel, []MessageMutation{bad})
	require.ErrorContains(t, err, "invalid personal package message identity")
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from channels where id='new-dm'`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, s.UpsertChannel(ctx, channel))
	_, err = s.DB().ExecContext(ctx, `update channels set deleted_at='2026-02-01T00:00:00Z' where id='new-dm'`)
	require.NoError(t, err)
	_, err = s.ImportPersonalPackageChannel(ctx, channel, []MessageMutation{packageMutation("message", channel.ID)})
	require.ErrorIs(t, err, ErrCollectionScope)
	var deleted string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select deleted_at from channels where id='new-dm'`).Scan(&deleted))
	require.NotEmpty(t, deleted)
}

func TestPersonalPackageRollsBackWhenEvidenceWriteFails(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, err = s.DB().ExecContext(ctx, `create trigger reject_package_event before insert on message_events begin select raise(abort, 'synthetic event failure'); end`)
	require.NoError(t, err)
	channel := ChannelRecord{ID: "dm", GuildID: DirectMessageGuildID, Kind: "dm", RawJSON: `{}`}
	_, err = s.ImportPersonalPackageChannel(ctx, channel, []MessageMutation{packageMutation("sent", channel.ID)})
	require.ErrorContains(t, err, "synthetic event failure")
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from channels`).Scan(&count))
	require.Zero(t, count)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = s.ImportPersonalPackageChannel(canceled, channel, nil)
	require.ErrorIs(t, err, context.Canceled)
}
