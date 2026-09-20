package syncer

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

type reconcileFixture struct {
	denyHistory bool
	scopeErr    error
	member      func(string, string) (*discordgo.Member, error)
	message     func(string, string) (*discordgo.Message, error)
}

func (f reconcileFixture) CanReadHistory(context.Context, *discordgo.Channel) (bool, error) {
	return !f.denyHistory, nil
}

func (f reconcileFixture) Guild(_ context.Context, id string) (*discordgo.Guild, error) {
	if f.scopeErr != nil {
		return nil, f.scopeErr
	}
	return &discordgo.Guild{ID: id}, nil
}

func (f reconcileFixture) Channel(_ context.Context, id string) (*discordgo.Channel, error) {
	return &discordgo.Channel{ID: id, GuildID: "1"}, nil
}

func (f reconcileFixture) GuildMember(_ context.Context, g, u string) (*discordgo.Member, error) {
	return f.member(g, u)
}

func (f reconcileFixture) ChannelMessage(_ context.Context, c, m string) (*discordgo.Message, error) {
	return f.message(c, m)
}

func reconcileStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.UpsertGuild(context.Background(), store.GuildRecord{ID: "1", Name: "fixture", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(context.Background(), store.ChannelRecord{ID: "2", GuildID: "1", Name: "channel", Kind: "text", RawJSON: `{}`}))
	return s
}

func missingResponse(status, code int) error {
	return &discordgo.RESTError{Response: &http.Response{StatusCode: status}, Message: &discordgo.APIErrorMessage{Code: code, Message: "private provider detail"}}
}

func TestReconcileRestoresLiveRecordsAndRecordsOnlyConfirmedDeletions(t *testing.T) {
	ctx := context.Background()
	s := reconcileStore(t)
	timestamp := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	plan := ReconcilePlan{Archive: "fixture", Members: []ReconcileMember{{GuildID: "1", UserID: "3"}, {GuildID: "1", UserID: "4"}}, Messages: []ReconcileMessage{{GuildID: "1", ChannelID: "2", MessageID: "5", CreatedAt: timestamp.Format(time.RFC3339Nano)}, {GuildID: "1", ChannelID: "2", MessageID: "6", CreatedAt: timestamp.Format(time.RFC3339Nano)}}}
	calls := 0
	client := reconcileFixture{
		member: func(g, u string) (*discordgo.Member, error) {
			calls++
			if u == "4" {
				return nil, missingResponse(404, 10007)
			}
			return &discordgo.Member{GuildID: g, User: &discordgo.User{ID: u, Username: "restored", Bot: true}, Roles: []string{"7"}, JoinedAt: timestamp}, nil
		},
		message: func(c, m string) (*discordgo.Message, error) {
			calls++
			if m == "6" {
				return nil, missingResponse(404, 10008)
			}
			return &discordgo.Message{ID: m, ChannelID: c, GuildID: "1", Content: "restored message", Author: &discordgo.User{ID: "3", Username: "restored"}, Timestamp: timestamp, Attachments: []*discordgo.MessageAttachment{{ID: "8", Filename: "fixture.txt", URL: "https://example.test/file"}}}, nil
		},
	}
	stats, err := ReconcileMissing(ctx, s, client, plan)
	require.NoError(t, err)
	require.Equal(t, ReconcileStats{Restored: 2, ConfirmedDeleted: 2}, stats)
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, "select count(*) from members where deleted_at is not null").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, s.DB().QueryRowContext(ctx, "select count(*) from messages where deleted_at is not null").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, s.DB().QueryRowContext(ctx, "select count(*) from message_attachments where message_id='5'").Scan(&count))
	require.Equal(t, 1, count)
	var bot int
	var roles string
	require.NoError(t, s.DB().QueryRowContext(ctx, "select bot,role_ids_json from members where user_id='3'").Scan(&bot, &roles))
	require.Equal(t, 1, bot)
	require.JSONEq(t, `["7"]`, roles)
	stats, err = ReconcileMissing(ctx, s, client, plan)
	require.NoError(t, err)
	require.Equal(t, 4, stats.AlreadyPresent)
	require.Equal(t, 4, calls)
}

func TestReconcileRefusesPermissionAndAmbiguousErrorsBeforeApplyingAnyObservation(t *testing.T) {
	for _, failure := range []struct{ status, code int }{{403, 50001}, {404, 10004}, {500, 10007}, {404, 10008}} {
		s := reconcileStore(t)
		ctx := context.Background()
		client := reconcileFixture{member: func(g, u string) (*discordgo.Member, error) {
			if u == "3" {
				return &discordgo.Member{User: &discordgo.User{ID: u, Username: "live"}}, nil
			}
			return nil, missingResponse(failure.status, failure.code)
		}}
		_, err := ReconcileMissing(ctx, s, client, ReconcilePlan{Archive: "fixture", Members: []ReconcileMember{{GuildID: "1", UserID: "3"}, {GuildID: "1", UserID: "4"}}})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private provider detail")
		var count int
		require.NoError(t, s.DB().QueryRowContext(ctx, "select count(*) from members").Scan(&count))
		require.Zero(t, count)
	}
}

func TestReconcileRejectsUnscopedAndMismatchedIdentities(t *testing.T) {
	ctx := context.Background()
	s := reconcileStore(t)
	client := reconcileFixture{member: func(g, u string) (*discordgo.Member, error) {
		return &discordgo.Member{GuildID: g, User: &discordgo.User{ID: "99"}}, nil
	}}
	_, err := ReconcileMissing(ctx, s, client, ReconcilePlan{Archive: "fixture", Members: []ReconcileMember{{GuildID: "1", UserID: "3"}}})
	require.ErrorContains(t, err, "identity mismatch")
	_, err = ReconcileMissing(ctx, s, client, ReconcilePlan{Archive: "fixture", Members: []ReconcileMember{{GuildID: "9", UserID: "3"}}})
	require.ErrorContains(t, err, "outside the live source scope")
	_, err = ReconcileMissing(ctx, s, client, ReconcilePlan{Archive: "fixture", Members: []ReconcileMember{{GuildID: "@me", UserID: "3"}}})
	require.ErrorContains(t, err, "invalid")
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, "select count(*) from members").Scan(&count))
	require.Zero(t, count)
}

func TestReconcileMessagePermissionFailureDoesNotApplyBufferedMembers(t *testing.T) {
	ctx := context.Background()
	s := reconcileStore(t)
	client := reconcileFixture{
		member: func(g, u string) (*discordgo.Member, error) {
			return &discordgo.Member{GuildID: g, User: &discordgo.User{ID: u, Username: "live"}}, nil
		},
		message: func(c, m string) (*discordgo.Message, error) { return nil, missingResponse(403, 50001) },
	}
	_, err := ReconcileMissing(ctx, s, client, ReconcilePlan{Archive: "fixture", Members: []ReconcileMember{{GuildID: "1", UserID: "3"}}, Messages: []ReconcileMessage{{GuildID: "1", ChannelID: "2", MessageID: "5", CreatedAt: "2026-09-19T00:00:00Z"}}})
	require.Error(t, err)
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, "select count(*) from members").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, s.DB().QueryRowContext(ctx, "select count(*) from messages").Scan(&count))
	require.Zero(t, count)
}

func TestReconcileParentPermissionFailurePreventsChildLookups(t *testing.T) {
	s := reconcileStore(t)
	client := reconcileFixture{scopeErr: missingResponse(403, 50001), member: func(g, u string) (*discordgo.Member, error) {
		t.Error("child lookup without parent access")
		return nil, nil
	}}
	_, err := ReconcileMissing(context.Background(), s, client, ReconcilePlan{Archive: "fixture", Members: []ReconcileMember{{GuildID: "1", UserID: "3"}}})
	require.Error(t, err)
	var count int
	require.NoError(t, s.DB().QueryRowContext(context.Background(), "select count(*) from members").Scan(&count))
	require.Zero(t, count)
}

func TestReconcileVisibleChannelWithoutHistoryPermissionIsNotDeletion(t *testing.T) {
	s := reconcileStore(t)
	client := reconcileFixture{denyHistory: true, message: func(c, m string) (*discordgo.Message, error) {
		t.Error("message lookup without history permission")
		return nil, missingResponse(404, 10008)
	}}
	_, err := ReconcileMissing(context.Background(), s, client, ReconcilePlan{Archive: "fixture", Messages: []ReconcileMessage{{GuildID: "1", ChannelID: "2", MessageID: "5", CreatedAt: "2026-09-19T00:00:00Z"}}})
	require.ErrorContains(t, err, "not readable")
	var count int
	require.NoError(t, s.DB().QueryRowContext(context.Background(), "select count(*) from messages").Scan(&count))
	require.Zero(t, count)
}
