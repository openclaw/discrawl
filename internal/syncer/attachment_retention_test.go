package syncer

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestAttachmentRetryPreservesTextAndAdvancesParentClock(t *testing.T) {
	var failed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if failed.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("attachment evidence"))
	}))
	defer server.Close()
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	h := &tailHandler{store: s, attachmentTextEnabled: true, enqueueEmbeddings: true}
	m := &discordgo.Message{ID: "100", GuildID: "g", ChannelID: "c", Content: "author", Timestamp: time.Now(), Author: &discordgo.User{ID: "u"}, Attachments: []*discordgo.MessageAttachment{{ID: "a", Filename: "trace.txt", ContentType: "text/plain", URL: server.URL, Size: 19}}}
	require.NoError(t, h.OnMessageCreate(t.Context(), m))
	var oldText, oldSuccess, oldClock string
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select text_content,text_succeeded_at from message_attachments where attachment_id='a'`).Scan(&oldText, &oldSuccess))
	require.Equal(t, "attachment evidence", oldText)
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select updated_at from messages where id='100'`).Scan(&oldClock))
	failed.Store(true)
	require.NoError(t, h.OnMessageUpdate(t.Context(), m))
	var text, status, reason, success, clock, parts string
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select text_content,text_status,text_error,text_succeeded_at from message_attachments where attachment_id='a'`).Scan(&text, &status, &reason, &success))
	require.Equal(t, oldText, text)
	require.Equal(t, oldSuccess, success)
	require.Equal(t, "failed", status)
	require.Equal(t, "http_503", reason)
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select updated_at,text_parts_json from messages where id='100'`).Scan(&clock, &parts))
	require.Greater(t, clock, oldClock)
	require.Contains(t, parts, `"kind":"attachment_text"`)
	require.Contains(t, parts, oldText)
	var revision int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select revision from embedding_jobs where message_id='100'`).Scan(&revision))
	require.Equal(t, 2, revision, "failed retry does not change text or requeue its embedding")
	_, err = s.DB().ExecContext(t.Context(), `insert into message_embeddings(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at)values('100','fixture','model','v1',1,x'0000803f','2026-01-01T00:00:00Z')`)
	require.NoError(t, err)
	require.NoError(t, h.OnMessageDelete(t.Context(), &discordgo.MessageDelete{Message: &discordgo.Message{ID: "100", GuildID: "g", ChannelID: "c"}}))
	var retained, live int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from message_embedding_history where message_id='100'`).Scan(&retained))
	require.Equal(t, 1, retained)
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from message_embeddings where message_id='100'`).Scan(&live))
	require.Zero(t, live, "deleted message cannot remain in the live vector index")
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from attachment_text_history where message_id='100'`).Scan(&retained))
	require.Equal(t, 1, retained)
}

func TestCanonicalAndEventRollbackTogether(t *testing.T) {
	t.Parallel()
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	_, err = s.DB().ExecContext(t.Context(), `create trigger reject_fixture_event before insert on message_events begin select raise(abort,'fixture append failure'); end`)
	require.NoError(t, err)
	h := &tailHandler{store: s, enqueueEmbeddings: true}
	m := &discordgo.Message{ID: "100", GuildID: "g", ChannelID: "c", Content: "transactional", Timestamp: time.Now()}
	require.Error(t, h.OnMessageCreate(t.Context(), m))
	for _, table := range []string{"messages", "message_events", "embedding_jobs", "message_fts"} {
		var n int
		require.NoError(t, s.DB().QueryRowContext(t.Context(), "select count(*) from "+table).Scan(&n))
		require.Zero(t, n)
	}
}
