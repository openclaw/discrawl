package store

import (
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/messagetext"
	"github.com/stretchr/testify/require"
)

func TestChannelCollectionAdmittedHonorsTombstonesWithoutPolicy(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "cat", GuildID: "g", Kind: "category", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c", GuildID: "g", ParentID: "cat", Kind: "text", RawJSON: `{}`}))

	for _, id := range []string{"c", "never-seen"} {
		admitted, err := s.ChannelCollectionAdmitted(ctx, id, "")
		require.NoError(t, err)
		require.True(t, admitted, "unconfigured collection admits %s without a tombstone", id)
	}
	admitted, err := s.ChannelCollectionAdmitted(ctx, "c", "policy")
	require.NoError(t, err)
	require.False(t, admitted, "a configured policy still requires a persisted allowance")

	require.NoError(t, s.MarkChannelDeleted(ctx, "g", "cat", "gateway"))
	admitted, err = s.ChannelCollectionAdmitted(ctx, "c", "")
	require.NoError(t, err)
	require.False(t, admitted, "an ancestor tombstone denies content I/O")
}

func TestStatusCountsOnlyActionableAttachmentFailures(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	for _, id := range []string{"c1", "c2", "c3"} {
		require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: id, GuildID: "g", Kind: "text", RawJSON: `{}`}))
	}
	rows, err := s.ScopeChannels(ctx)
	require.NoError(t, err)
	decisions := map[string]ScopeDecision{}
	for _, r := range rows {
		decisions[r.ID] = ScopeDecision{State: "allowed", Revision: r.Revision}
	}
	require.NoError(t, s.SetChannelScopes(ctx, decisions, "policy"))
	var mutations []MessageMutation
	for _, ch := range []string{"c1", "c2", "c3"} {
		id := "m-" + ch
		mutations = append(mutations, MessageMutation{
			Record:      MessageRecord{ID: id, GuildID: "g", ChannelID: ch, Content: "x", RawJSON: `{"id":"` + id + `","attachments":[{"id":"a-` + ch + `"}]}`, TextVersion: messagetext.Version},
			Attachments: []AttachmentRecord{{AttachmentID: "a-" + ch, MessageID: id, GuildID: "g", ChannelID: ch, Filename: "f.txt", TextStatus: "failed", TextError: "http_503", TextAttemptedAt: "2026-01-01T00:00:00Z"}},
			Options:     WriteOptions{ScopePolicy: "policy"},
		})
	}
	require.NoError(t, s.UpsertMessages(ctx, mutations))
	status, err := s.Status(ctx, path, "")
	require.NoError(t, err)
	require.Equal(t, 3, status.AttachmentTextFailures)

	// A deleted message and an out-of-scope channel keep their receipts but
	// are no longer actionable health failures.
	_, err = s.DB().ExecContext(ctx, `update messages set deleted_at='2026-01-02T00:00:00Z' where id='m-c1'`)
	require.NoError(t, err)
	require.NoError(t, s.MarkChannelDeleted(ctx, "g", "c2", "gateway"))
	status, err = s.Status(ctx, path, "")
	require.NoError(t, err)
	require.Equal(t, 1, status.AttachmentTextFailures)
	var retained int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_attachments where text_status='failed'`).Scan(&retained))
	require.Equal(t, 3, retained, "historical failure receipts are retained")
}
