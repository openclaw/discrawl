package syncer

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

type exactMemberFixture struct {
	*fakeClient
	member *discordgo.Member
}

func (c *exactMemberFixture) GuildMember(context.Context, string, string) (*discordgo.Member, error) {
	return c.member, nil
}

func TestMetadataFailuresReconcileExactLanesWithoutMemberCrawl(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g", RawJSON: `{"roles":[{"id":"g","permissions":"1024"}]}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g", UserID: "gone", Username: "retained", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	c := &exactMemberFixture{fakeClient: &fakeClient{guildByID: map[string]*discordgo.Guild{"g": {ID: "g", Roles: []*discordgo.Role{{ID: "g", Permissions: 0}}}}, channelByID: map[string]*discordgo.Channel{"c": {ID: "c", GuildID: "g", Type: discordgo.ChannelTypeGuildText}}}, member: &discordgo.Member{GuildID: "g", User: &discordgo.User{ID: "u", Username: "fixture"}, JoinedAt: time.Now()}}
	refs := []store.FailureRef{
		{ChannelID: "c", RelatedID: "CHANNEL_UPDATE:"},
		{RelatedID: "GUILD_ROLE_UPDATE:"},
		{RelatedID: "GUILD_MEMBER_REMOVE:gone"},
		{RelatedID: "GUILD_MEMBER_UPDATE:u"},
	}
	for _, r := range refs {
		r.Operation = "tail_metadata"
		r.Source = "discord"
		r.GuildID = "g"
		r.RelatedKind = "gateway_event"
		require.NoError(t, s.RecordFailure(ctx, r, errors.New("fixture failure")))
	}
	svc := New(c, s, nil)
	require.NoError(t, svc.replayMetadataFailures(ctx, []string{"g"}))
	var n int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from failure_ledger where resolved_at is null`).Scan(&n))
	require.Zero(t, n)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from members where user_id='gone' and deleted_at is not null`).Scan(&n))
	require.Equal(t, 1, n)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from members where user_id='u' and deleted_at is null`).Scan(&n))
	require.Equal(t, 1, n)
	require.Zero(t, c.memberCalls, "ordinary metadata recovery must not enumerate guild membership")
	// A missing provider member is not converted into a successful observation.
	r := store.FailureRef{Operation: "tail_metadata", Source: "discord", GuildID: "g", RelatedKind: "gateway_event", RelatedID: "GUILD_MEMBER_UPDATE:missing"}
	require.NoError(t, s.RecordFailure(ctx, r, errors.New("fixture")))
	c.member = nil
	require.NoError(t, svc.replayMetadataFailures(ctx, []string{"g"}))
	var retries int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select retry_count from failure_ledger where resolved_at is null`).Scan(&retries))
	require.Equal(t, 1, retries)
}

func TestOrdinaryAttachmentRetryPersistsResultAndParentClock(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("recovered attachment"))
	}))
	defer server.Close()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	h := &tailHandler{store: s}
	require.NoError(t, h.OnChannelUpsert(ctx, &discordgo.Channel{ID: "c", GuildID: "g", Type: discordgo.ChannelTypeGuildText}))
	m := &discordgo.Message{ID: "10", GuildID: "g", ChannelID: "c", Timestamp: time.Now(), Content: "owner", Attachments: []*discordgo.MessageAttachment{{ID: "a", Filename: "a.txt", ContentType: "text/plain", URL: server.URL, Size: 20}}}
	require.NoError(t, h.OnMessageCreate(ctx, m))
	_, err = s.DB().ExecContext(ctx, `update message_attachments set text_status='failed',text_error='http_503' where attachment_id='a'`)
	require.NoError(t, err)
	c := &exactReplayClient{messages: map[string]*discordgo.Message{"c/10": m}}
	svc := New(c, s, nil)
	require.NoError(t, svc.retryAttachmentText(ctx, []string{"g"}))
	var text, status, clock string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select text_content,text_status from message_attachments where attachment_id='a'`).Scan(&text, &status))
	require.Equal(t, "recovered attachment", text)
	require.Equal(t, "succeeded", status)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from messages where id='10'`).Scan(&clock))
	require.NotEmpty(t, clock)
	var snapshots int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events where message_id='10' and event_type='snapshot'`).Scan(&snapshots))
	require.Equal(t, 1, snapshots)
	_, err = s.DB().ExecContext(ctx, `update message_attachments set text_status='failed' where attachment_id='a'`)
	require.NoError(t, err)
	require.NoError(t, svc.retryAttachmentText(ctx, []string{"g"}))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events where message_id='10' and event_type='snapshot'`).Scan(&snapshots))
	require.Equal(t, 1, snapshots, "exact repeated provider snapshots deduplicate")
	_, err = s.DB().ExecContext(ctx, `update message_attachments set text_status='failed' where attachment_id='a'`)
	require.NoError(t, err)
	c.errors = map[string]error{"c/10": errors.New("must not persist untrusted provider body")}
	require.NoError(t, svc.retryAttachmentText(ctx, []string{"g"}))
	var after, reason string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select text_content,text_error from message_attachments where attachment_id='a'`).Scan(&text, &reason))
	require.Equal(t, "recovered attachment", text)
	require.Equal(t, "message_refetch_failed", reason)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from messages where id='10'`).Scan(&after))
	require.Greater(t, after, clock)
	_, err = s.AttachmentTextRetryCandidates(ctx, []string{"g"}, 0)
	require.Error(t, err)
}
