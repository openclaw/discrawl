package syncer

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

type memberEnumerationFixture struct {
	*fakeClient
	enumerate func(context.Context) (discordclient.MemberEnumeration, error)
}

func (f *memberEnumerationFixture) GuildMemberEnumeration(ctx context.Context, _ string) (discordclient.MemberEnumeration, error) {
	return f.enumerate(ctx)
}

func fixtureMember(id, role string) *discordgo.Member {
	return &discordgo.Member{GuildID: "g", User: &discordgo.User{ID: id, Username: id}, Roles: []string{role}}
}

func TestMemberRefreshPreservesInterleavedGatewayAddUpdateRemove(t *testing.T) {
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	for _, id := range []string{"update", "remove", "stable", "absent"} {
		require.NoError(t, s.UpsertMember(ctx, toMemberRecord("g", fixtureMember(id, "before"))))
	}
	started, release := make(chan struct{}), make(chan struct{})
	client := &memberEnumerationFixture{fakeClient: &fakeClient{}, enumerate: func(ctx context.Context) (discordclient.MemberEnumeration, error) {
		close(started)
		select {
		case <-ctx.Done():
			return discordclient.MemberEnumeration{}, ctx.Err()
		case <-release:
		}
		return discordclient.MemberEnumeration{Pages: 2, Complete: true, LastUserID: "stable", Members: []*discordgo.Member{fixtureMember("add", "stale"), fixtureMember("update", "stale"), fixtureMember("remove", "stale"), fixtureMember("stable", "refreshed")}}, nil
	}}
	svc := New(client, s, nil)
	done := make(chan error, 1)
	go func() { _, err := svc.refreshGuildMembers(ctx, "g", false); done <- err }()
	<-started
	handler := &tailHandler{store: s, guilds: makeGuildSet([]string{"g"})}
	require.NoError(t, handler.OnMemberUpsert(ctx, "g", fixtureMember("add", "live-add")))
	require.NoError(t, handler.OnMemberUpsert(ctx, "g", fixtureMember("update", "live-update")))
	require.NoError(t, handler.OnMemberDelete(ctx, "g", "remove"))
	var beforeWatermark string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from members where guild_id='g' and user_id='update'`).Scan(&beforeWatermark))
	close(release)
	require.NoError(t, <-done)
	for id, role := range map[string]string{"add": `["live-add"]`, "update": `["live-update"]`, "stable": `["refreshed"]`, "absent": `["before"]`} {
		var actual string
		require.NoError(t, s.DB().QueryRowContext(ctx, `select role_ids_json from members where guild_id='g' and user_id=?`, id).Scan(&actual))
		require.Equal(t, role, actual)
	}
	var removed, watermark string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select deleted_at from members where guild_id='g' and user_id='remove'`).Scan(&removed))
	require.NotEmpty(t, removed)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from members where guild_id='g' and user_id='update'`).Scan(&watermark))
	require.Equal(t, beforeWatermark, watermark)
	gate, err := s.GetSyncState(ctx, guildMemberSyncSuccessScope("g"))
	require.NoError(t, err)
	_, err = time.Parse(time.RFC3339Nano, gate)
	require.NoError(t, err)
	require.False(t, svc.shouldRefreshMembers(ctx, "g"))
}

func TestMemberRefreshOnlyCompletedEnumerationAdvancesGate(t *testing.T) {
	for _, kind := range []string{"incomplete", "429", "interrupted", "merge-interrupted"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			old := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339Nano)
			require.NoError(t, s.SetSyncState(ctx, guildMemberSyncSuccessScope("g"), old))
			client := &memberEnumerationFixture{fakeClient: &fakeClient{}, enumerate: func(context.Context) (discordclient.MemberEnumeration, error) {
				result := discordclient.MemberEnumeration{Pages: 1, Members: []*discordgo.Member{fixtureMember("partial", "role")}}
				switch kind {
				case "429":
					return result, &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusTooManyRequests}, ResponseBody: []byte(`{"code":0,"message":"fixture rate limit"}`)}
				case "interrupted":
					cancel()
					return result, context.Canceled
				case "merge-interrupted":
					result.Complete = true
					result.Members = nil
					for i := range 257 {
						result.Members = append(result.Members, fixtureMember(fmt.Sprintf("row%d", i), "role"))
					}
				}
				return result, nil
			}}
			if kind == "merge-interrupted" {
				_, err := s.DB().ExecContext(ctx, `create trigger fixture_stop before insert on members when new.user_id='row256' begin select raise(abort,'fixture interruption'); end`)
				require.NoError(t, err)
			}
			svc := New(client, s, nil)
			_, err = svc.refreshGuildMembers(ctx, "g", false)
			require.Error(t, err)
			gate, err := s.GetSyncState(t.Context(), guildMemberSyncSuccessScope("g"))
			require.NoError(t, err)
			require.Equal(t, old, gate)
			var count int
			require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from members`).Scan(&count))
			if kind == "merge-interrupted" {
				require.Equal(t, 256, count) // Committed batches survive; no false full receipt.
				_, err = s.DB().ExecContext(t.Context(), `drop trigger fixture_stop`)
				require.NoError(t, err)
			} else {
				require.Zero(t, count)
			}
			if kind == "interrupted" {
				return // Shutdown is not a failed provider lane.
			}
			var unresolved int
			require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from failure_ledger where operation='guild_members' and resolved_at is null`).Scan(&unresolved))
			require.Equal(t, 1, unresolved)
			client.enumerate = func(context.Context) (discordclient.MemberEnumeration, error) {
				return discordclient.MemberEnumeration{Pages: 2, Complete: true, Members: []*discordgo.Member{fixtureMember("complete", "role")}}, nil
			}
			_, err = svc.refreshGuildMembers(t.Context(), "g", false)
			require.NoError(t, err)
			gate, err = s.GetSyncState(t.Context(), guildMemberSyncSuccessScope("g"))
			require.NoError(t, err)
			require.NotEqual(t, old, gate)
			require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from failure_ledger where operation='guild_members' and resolved_at is null`).Scan(&unresolved))
			require.Zero(t, unresolved)
		})
	}
}

func TestMemberRefreshRejectsInvalidCompleteRows(t *testing.T) {
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	client := &memberEnumerationFixture{fakeClient: &fakeClient{}, enumerate: func(context.Context) (discordclient.MemberEnumeration, error) {
		return discordclient.MemberEnumeration{Complete: true, Members: []*discordgo.Member{nil}}, nil
	}}
	_, err = New(client, s, nil).refreshGuildMembers(t.Context(), "g", false)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled)
	gate, err := s.GetSyncState(t.Context(), guildMemberSyncSuccessScope("g"))
	require.NoError(t, err)
	require.Empty(t, gate)
}

func TestMemberRefreshBudgetCapsLongerParentDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	client := &memberEnumerationFixture{fakeClient: &fakeClient{}, enumerate: func(call context.Context) (discordclient.MemberEnumeration, error) {
		deadline, ok := call.Deadline()
		require.True(t, ok)
		require.Less(t, time.Until(deadline), time.Second)
		<-call.Done()
		return discordclient.MemberEnumeration{Pages: 7, LastUserID: "123"}, call.Err()
	}}
	svc := New(client, s, nil)
	svc.memberRefreshTimeout = 10 * time.Millisecond
	_, err = svc.refreshGuildMembers(ctx, "g", false)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	gate, err := s.GetSyncState(ctx, guildMemberSyncSuccessScope("g"))
	require.NoError(t, err)
	require.Empty(t, gate)
}
