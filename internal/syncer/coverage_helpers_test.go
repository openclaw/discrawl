package syncer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestAttachmentFailureCodeClassifiesCauses(t *testing.T) {
	t.Parallel()
	require.Equal(t, "http_503", attachmentFailureCode(fmt.Errorf("fetch: %w", attachmentTextError{code: "http_503"})))
	require.Equal(t, "http_503", attachmentTextError{code: "http_503"}.Error())
	require.Equal(t, "canceled", attachmentFailureCode(fmt.Errorf("fetch: %w", context.Canceled)))
	require.Equal(t, "timeout", attachmentFailureCode(context.DeadlineExceeded))
	require.Equal(t, "request_failed", attachmentFailureCode(errors.New("boom")))
}

func TestNormalizeMessagePartsPlacesAttachmentTextAfterAuthoredContent(t *testing.T) {
	t.Parallel()
	m := &discordgo.Message{ID: "m", Content: "authored", Attachments: []*discordgo.MessageAttachment{{ID: "a", Filename: "trace.txt"}}}
	// Extracted text replaces raw attachment parts and follows authored text.
	require.Equal(t, "authored\nextracted one\nextracted two", normalizeMessageParts(m, []string{"extracted one", "extracted two"}))
	require.Equal(t, "authored\ntrace.txt", normalizeMessageParts(m, nil))
	require.Equal(t, "only", normalizeMessageParts(&discordgo.Message{ID: "n"}, []string{"only"}))
}

func TestThreadParentIDsSelectsThreadCapableChannels(t *testing.T) {
	t.Parallel()
	require.Equal(t, []string{"text", "forum"}, threadParentIDs([]*discordgo.Channel{
		{ID: "text", Type: discordgo.ChannelTypeGuildText},
		{ID: "voice", Type: discordgo.ChannelTypeGuildVoice},
		nil,
		{ID: "forum", Type: discordgo.ChannelTypeGuildForum},
	}))
}

func TestOnThreadListStoresThreadsAndRejectsGuildMismatch(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := repairFrontierStore(t)
	h := &tailHandler{store: s, guilds: makeGuildSet([]string{"g"})}
	require.NoError(t, h.OnThreadList(ctx, "other", []*discordgo.Channel{{ID: "ignored"}}))
	require.NoError(t, h.OnThreadList(ctx, "g", []*discordgo.Channel{nil, {ID: ""}, {ID: "t1", ParentID: "p", Type: discordgo.ChannelTypeGuildPublicThread, Name: "thread"}}))
	rows, err := s.ScopeChannels(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "t1", rows[0].ID)
	require.Equal(t, "g", rows[0].GuildID)
	require.EqualError(t, h.OnThreadList(ctx, "g", []*discordgo.Channel{{ID: "t2", GuildID: "x"}}), "thread list guild mismatch")
}
