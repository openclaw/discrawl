package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func revisionMessage(t *testing.T, content, edited string) *discordgo.Message {
	t.Helper()
	var message discordgo.Message
	payload, err := json.Marshal(map[string]any{
		"id": "100000000000000001", "guild_id": "g1", "channel_id": "c1",
		"type": 0, "timestamp": "2026-10-01T00:00:00Z", "content": content,
		"author": map[string]any{"id": "u1", "username": "synthetic", "bot": false},
	})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(payload, &message))
	if edited != "" {
		stamp, err := time.Parse(time.RFC3339, edited)
		require.NoError(t, err)
		message.EditedTimestamp = &stamp
	}
	return &message
}

func TestNativeMessageRevisionOrdering(t *testing.T) {
	for _, scenario := range []string{
		"delayed REST page", "older refetch", "newer clear", "equal revision",
		"missing incoming revision", "both revisions missing", "deleted message", "refetch error",
	} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			current := revisionMessage(t, "newest human text", "2026-10-01T00:02:00Z")
			if scenario == "both revisions missing" {
				current.EditedTimestamp = nil
			}
			client := &exactReplayClient{messages: map[string]*discordgo.Message{
				"c1/100000000000000001": current,
			}, errors: map[string]error{}}
			handler := &tailHandler{store: s, client: client, preserveHistoryCursor: true, enqueueEmbeddings: true}
			var partial discordgo.Message
			require.NoError(t, json.Unmarshal([]byte(`{"id":"100000000000000001","guild_id":"g1","channel_id":"c1","pinned":true}`), &partial))
			want := current.Content
			deleted := false
			if scenario != "delayed REST page" {
				require.NoError(t, handler.OnMessageCreate(ctx, current))
			}
			switch scenario {
			case "delayed REST page":
				older := revisionMessage(t, "", "2026-10-01T00:01:00Z")
				older.Attachments = []*discordgo.MessageAttachment{{ID: "a1", Filename: "old.txt"}}
				older.Mentions = []*discordgo.User{{ID: "u2", Username: "old"}}
				require.NoError(t, handler.OnMessageCreate(ctx, older))
				var revisionBefore int
				pageClient := &revisionDelayedPageClient{page: older, beforeReturn: func() {
					require.NoError(t, handler.OnMessageUpdate(ctx, &partial))
					require.NoError(t, s.DB().QueryRowContext(ctx, `select revision from embedding_jobs where message_id=?`, current.ID).Scan(&revisionBefore))
				}}
				svc := New(pageClient, s, nil)
				svc.SetAttachmentTextEnabled(false)
				count, err := svc.syncChannelMessages(ctx, "g1", &discordgo.Channel{
					ID: "c1", GuildID: "g1", Name: "synthetic", Type: discordgo.ChannelTypeGuildText, LastMessageID: older.ID,
				}, true, true, time.Time{}, false, nil)
				require.NoError(t, err)
				require.Equal(t, 1, count)
				var attachments, mentions int
				require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_attachments where message_id=?`, current.ID).Scan(&attachments))
				require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from mention_events where message_id=?`, current.ID).Scan(&mentions))
				require.Zero(t, attachments, "stale page must not restore old attachment references")
				require.Zero(t, mentions, "stale page must not restore old mentions")
				var revisionAfter int
				require.NoError(t, s.DB().QueryRowContext(ctx, `select revision from embedding_jobs where message_id=?`, current.ID).Scan(&revisionAfter))
				require.Equal(t, revisionBefore, revisionAfter, "stale page must not supersede current embedding work")
			case "deleted message":
				require.NoError(t, handler.OnMessageDelete(ctx, &discordgo.MessageDelete{Message: current}))
				deleted = true
				client.messages["c1/100000000000000001"] = revisionMessage(t, "late replacement", "2026-10-01T00:03:00Z")
				require.NoError(t, handler.OnMessageUpdate(ctx, &partial))
			case "refetch error":
				client.errors["c1/100000000000000001"] = errors.New("synthetic REST failure")
				require.Error(t, handler.OnMessageUpdate(ctx, &partial))
			default:
				incoming := revisionMessage(t, "", "2026-10-01T00:01:00Z")
				incoming.Attachments = []*discordgo.MessageAttachment{{ID: "a1", Filename: "file.txt"}}
				switch scenario {
				case "newer clear":
					incoming = revisionMessage(t, "", "2026-10-01T00:03:00Z")
					incoming.Attachments = []*discordgo.MessageAttachment{{ID: "a1", Filename: "file.txt"}}
					want = ""
				case "equal revision":
					incoming = revisionMessage(t, "equal revision refresh", "2026-10-01T00:02:00Z")
					want = incoming.Content
				case "missing incoming revision", "both revisions missing":
					incoming.EditedTimestamp = nil
					if scenario == "both revisions missing" {
						want = ""
					}
				}
				client.messages["c1/100000000000000001"] = incoming
				require.NoError(t, handler.OnMessageUpdate(ctx, &partial))
			}
			var body, edited, tombstone string
			require.NoError(t, s.DB().QueryRowContext(ctx, `select content,coalesce(edited_at,''),coalesce(deleted_at,'') from messages where id=?`, current.ID).Scan(&body, &edited, &tombstone))
			require.Equal(t, want, body)
			require.Equal(t, deleted, tombstone != "")
			if scenario == "delayed REST page" || scenario == "older refetch" || scenario == "missing incoming revision" {
				require.Equal(t, "2026-10-01T00:02:00.000000000Z", edited)
			}
			hits, err := s.SearchMessages(ctx, store.SearchOptions{Query: "newest", GuildIDs: []string{"g1"}, Limit: 10})
			require.NoError(t, err)
			if want == current.Content && !deleted {
				require.Len(t, hits, 1)
			} else {
				require.Empty(t, hits)
			}
		})
	}
}

type revisionDelayedPageClient struct {
	fakeClient
	page         *discordgo.Message
	beforeReturn func()
	sent         bool
}

func (c *revisionDelayedPageClient) ChannelMessages(context.Context, string, int, string, string) ([]*discordgo.Message, error) {
	if c.sent {
		return nil, nil
	}
	c.sent = true
	c.beforeReturn()
	return []*discordgo.Message{c.page}, nil
}
