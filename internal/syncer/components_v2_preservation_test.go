package syncer

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/messagetext"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func componentMessage(t *testing.T) *discordgo.Message {
	t.Helper()
	var message discordgo.Message
	require.NoError(t, json.Unmarshal([]byte(`{
		"id":"100000000000000001","guild_id":"g1","channel_id":"c1",
		"timestamp":"2026-10-01T00:00:00Z","flags":32768,"content":"",
		"author":{"id":"u1","username":"synthetic","bot":true},
		"components":[{"type":17,"components":[
			{"type":10,"content":"Synthetic card"},
			{"type":14},
			{"type":9,"components":[{"type":10,"content":"Synthetic details"}],
			 "accessory":{"type":11,"media":{"url":"https://example.invalid/thumb"},"description":"Thumbnail description"}},
			{"type":12,"items":[{"media":{"url":"https://example.invalid/image"},"description":"Gallery description"}]},
			{"type":1,"components":[{"type":2,"style":5,"label":"Docs","url":"https://example.invalid/card"}]}
		]}]
	}`), &message))
	return &message
}

func TestComponentsV2NativePreservationAndReuse(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	handler := &tailHandler{store: s, enqueueEmbeddings: true}
	message := componentMessage(t)
	require.NoError(t, handler.OnMessageCreate(ctx, message))
	var content, normalized, raw, partsJSON, updated string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content,normalized_content,raw_json,text_parts_json,updated_at from messages where id=?`, message.ID).Scan(&content, &normalized, &raw, &partsJSON, &updated))
	require.Empty(t, content, "component text is not authored content")
	for _, text := range []string{"Synthetic card", "Synthetic details", "Thumbnail description", "Gallery description", "Docs", "https://example.invalid/card"} {
		require.Contains(t, normalized, text)
	}
	var decoded discordgo.Message
	require.NoError(t, json.Unmarshal([]byte(raw), &decoded))
	require.Len(t, decoded.Components, 1)
	require.Equal(t, normalized, messagetext.Normalize(messagetext.Parts(&decoded, nil)))
	var parts []messagetext.Part
	require.NoError(t, json.Unmarshal([]byte(partsJSON), &parts))
	require.Contains(t, parts, messagetext.Part{Kind: "component_description", Path: "/components/0/components/2/accessory/description", Text: "Thumbnail description"})
	hits, err := s.SearchMessages(ctx, store.SearchOptions{Query: "Synthetic", GuildIDs: []string{"g1"}, Limit: 10})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	var revision int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select revision from embedding_jobs where message_id=?`, message.ID).Scan(&revision))
	_, err = s.DB().ExecContext(ctx, `insert into message_embeddings(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at)values(?,'fixture','fixture','fixture',1,?,?)`, message.ID, []byte{0, 0, 128, 63}, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	require.NoError(t, handler.OnMessageCreate(ctx, message))
	var nextRevision, vectors int
	var observed string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select revision from embedding_jobs where message_id=?`, message.ID).Scan(&nextRevision))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_embeddings where message_id=?`, message.ID).Scan(&vectors))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from messages where id=?`, message.ID).Scan(&observed))
	require.Equal(t, revision, nextRevision)
	require.Equal(t, 1, vectors)
	require.Greater(t, observed, updated, "reobservation clock remains independent of text identity")
	message.Components[0].(*discordgo.Container).Components[0].(*discordgo.TextDisplay).Content = "Changed card"
	require.NoError(t, handler.OnMessageCreate(ctx, message))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select revision from embedding_jobs where message_id=?`, message.ID).Scan(&nextRevision))
	require.Greater(t, nextRevision, revision)
	var historicalVectors int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_embedding_history where message_id=?`, message.ID).Scan(&historicalVectors))
	require.Equal(t, 1, historicalVectors)
}

func TestComponentSerializationClassicControl(t *testing.T) {
	message := &discordgo.Message{ID: "m", Content: "ordinary text"}
	require.Equal(t, marshalJSONString(message, "{}"), marshalMessageJSON(message))
	message.Components = []discordgo.MessageComponent{&discordgo.ActionsRow{Components: []discordgo.MessageComponent{&discordgo.Button{Label: "Button", URL: "https://example.invalid"}}}}
	raw := marshalMessageJSON(message)
	require.Contains(t, raw, `"components"`)
	require.Equal(t, "ordinary text", messagetext.Normalize(messagetext.Parts(message, nil)))
}

func TestRecoveryPagePreservesConcurrentGatewayState(t *testing.T) {
	for _, operation := range []string{"edit", "delete"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			old := componentMessage(t)
			newer := componentMessage(t)
			newer.Components[0].(*discordgo.Container).Components[0].(*discordgo.TextDisplay).Content = "Concurrent correction"
			client := &exactReplayClient{messages: map[string]*discordgo.Message{"c1/" + old.ID: newer}}
			handler := &tailHandler{store: s, client: client, preserveHistoryCursor: true}
			require.NoError(t, handler.OnMessageCreate(ctx, old))
			// Match the supported recovery path: keep the archived row, rewind
			// only its channel cursor, then run normal latest-only repair.
			require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "100000000000000000"))
			pageClient := &componentsPageClient{page: old, beforeReturn: func() {
				if operation == "delete" {
					require.NoError(t, handler.OnMessageDelete(ctx, &discordgo.MessageDelete{Message: old}))
				} else {
					require.NoError(t, handler.OnMessageUpdate(ctx, &discordgo.Message{ID: old.ID, GuildID: "g1", ChannelID: "c1"}))
				}
			}}
			svc := New(pageClient, s, nil)
			_, err = svc.syncChannelMessages(ctx, "g1", &discordgo.Channel{ID: "c1", GuildID: "g1", Name: "synthetic", Type: discordgo.ChannelTypeGuildText, LastMessageID: old.ID}, false, false, time.Time{}, true, nil)
			require.NoError(t, err)
			require.Equal(t, "100000000000000000", pageClient.requestedAfter)
			cursor, err := s.GetSyncState(ctx, channelLatestScope("c1"))
			require.NoError(t, err)
			require.Equal(t, old.ID, cursor)
			var text, deleted string
			require.NoError(t, s.DB().QueryRowContext(ctx, `select normalized_content,coalesce(deleted_at,'') from messages where id=?`, old.ID).Scan(&text, &deleted))
			if operation == "delete" {
				require.NotEmpty(t, deleted)
			} else {
				require.Contains(t, text, "Concurrent correction", "both snapshots have absent edit times; request-start fence must win")
			}
		})
	}
}

type componentsPageClient struct {
	fakeClient
	page           *discordgo.Message
	beforeReturn   func()
	sent           bool
	requestedAfter string
}

func (c *componentsPageClient) ChannelMessages(_ context.Context, _ string, _ int, _ string, after string) ([]*discordgo.Message, error) {
	if c.sent {
		return nil, nil
	}
	c.sent = true
	c.requestedAfter = after
	c.beforeReturn()
	return []*discordgo.Message{c.page}, nil
}
