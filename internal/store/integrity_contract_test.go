package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/messagetext"
	"github.com/stretchr/testify/require"
)

func TestScopeStorageRejectsStaleDecisionsAndRetainsDeletion(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(ctx, path)
	require.NoError(t, err)
	for _, c := range []ChannelRecord{{ID: "p", GuildID: "g", Kind: "category", RawJSON: `{}`}, {ID: "c", GuildID: "g", Kind: "text", ParentID: "p", RawJSON: `{}`}} {
		require.NoError(t, s.UpsertChannel(ctx, c))
	}
	rows, err := s.ScopeChannels(ctx)
	require.NoError(t, err)
	decisions := map[string]ScopeDecision{}
	for _, c := range rows {
		decisions[c.ID] = ScopeDecision{State: "allowed", Revision: c.Revision}
	}
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "p", GuildID: "g", Kind: "category", ParentID: "excluded", RawJSON: `{}`}))
	require.NoError(t, s.SetChannelScopes(ctx, decisions, "policy"))
	m := MessageMutation{Record: MessageRecord{ID: "1", GuildID: "g", ChannelID: "c", Content: "blocked", RawJSON: `{}`}, Options: WriteOptions{ScopePolicy: "policy"}}
	require.ErrorIs(t, s.UpsertMessages(ctx, []MessageMutation{m}), ErrCollectionScope)
	rows, err = s.ScopeChannels(ctx)
	require.NoError(t, err)
	for _, c := range rows {
		require.Equal(t, "unknown", c.CollectionScope, "stale descendant decisions cannot reopen admission")
	}
	require.Error(t, s.SetChannelScopes(ctx, map[string]ScopeDecision{"c": {State: "public"}}, "p"))
	require.Error(t, s.MarkChannelDeleted(ctx, "other", "c", "discord-gateway"))
	require.Error(t, s.MarkChannelDeleted(ctx, "", "c", "discord-gateway"))
	require.NoError(t, s.MarkChannelDeleted(ctx, "g", "c", "discord-gateway"))
	require.NoError(t, s.MarkChannelDeleted(ctx, "g", "missing", "discord-gateway"))
	require.NoError(t, s.Close())
	s, err = Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	rows, err = s.ScopeChannels(ctx)
	require.NoError(t, err)
	for _, c := range rows {
		if c.ID != "p" {
			require.Equal(t, "excluded", c.CollectionScope)
			require.NotEmpty(t, c.DeletedAt)
		}
	}
	ref := FailureRef{Operation: "tail_message", Source: "discord", GuildID: "g", ChannelID: "c", MessageID: "1"}
	require.NoError(t, s.RecordFailure(ctx, ref, errors.New("fixture")))
	require.Error(t, s.ResolveFailureWithReason(ctx, ref, ""))
	require.NoError(t, s.ResolveFailureWithReason(ctx, ref, "excluded_by_collection_policy"))
	var reason string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select resolution_reason from failure_ledger where message_id='1'`).Scan(&reason))
	require.Equal(t, "excluded_by_collection_policy", reason)
}

func TestExtractionStorageRetainsSuccessAndScopesRetries(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c", GuildID: "g", Kind: "text", RawJSON: `{}`}))
	rows, err := s.ScopeChannels(ctx)
	require.NoError(t, err)
	require.NoError(t, s.SetChannelScopes(ctx, map[string]ScopeDecision{"c": {State: "allowed", Revision: rows[0].Revision}}, "policy"))
	m := MessageMutation{Record: MessageRecord{ID: "1", GuildID: "g", ChannelID: "c", Content: "own", RawJSON: `{"id":"1","guild_id":"g","channel_id":"c","content":"own","attachments":[{"id":"a","filename":"f.txt"}]}`, TextVersion: messagetext.Version}, Attachments: []AttachmentRecord{{AttachmentID: "a", MessageID: "1", GuildID: "g", ChannelID: "c", Filename: "f.txt", TextContent: "retained extraction", TextStatus: "succeeded", TextAttemptedAt: "2026-01-01T00:00:00Z", TextSucceededAt: "2026-01-01T00:00:00Z"}}, Options: WriteOptions{ScopePolicy: "policy"}}
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{m}))
	m.Attachments[0].TextContent = ""
	m.Attachments[0].TextStatus = "failed"
	m.Attachments[0].TextError = "http_503"
	m.Attachments[0].TextAttemptedAt = "2026-01-02T00:00:00Z"
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{m}))
	var text, status, succeeded, before, after string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select text_content,text_status,text_succeeded_at from message_attachments where attachment_id='a'`).Scan(&text, &status, &succeeded))
	require.Equal(t, "retained extraction", text)
	require.Equal(t, "failed", status)
	require.Equal(t, "2026-01-01T00:00:00Z", succeeded)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from messages where id='1'`).Scan(&before))
	for _, guilds := range [][]string{nil, {"g"}} {
		candidates, err := s.AttachmentTextRetryCandidates(ctx, guilds, 5)
		require.NoError(t, err)
		require.Len(t, candidates, 1)
		require.Equal(t, "1", candidates[0].ID)
	}
	other, err := s.AttachmentTextRetryCandidates(ctx, []string{"other"}, 5)
	require.NoError(t, err)
	require.Empty(t, other)
	_, err = s.AttachmentTextRetryCandidates(ctx, nil, 0)
	require.Error(t, err)
	require.NoError(t, s.RecordAttachmentRefetchFailure(ctx, m.Record, "http_503"))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select updated_at from messages where id='1'`).Scan(&after))
	require.Greater(t, after, before)
	// Disabled extraction must preserve the latest successful text and receipt.
	m.Attachments[0].TextStatus = "skipped"
	m.Attachments[0].TextAttemptedAt = ""
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{m}))
	var normalized string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select normalized_content from messages where id='1'`).Scan(&normalized))
	require.Contains(t, normalized, text)
	require.NoError(t, s.MarkChannelDeleted(ctx, "g", "c", "discord-gateway"))
	other, err = s.AttachmentTextRetryCandidates(ctx, nil, 5)
	require.NoError(t, err)
	require.Empty(t, other, "excluded content must never be refetched for extraction")
}

func TestRoleMutationPreservesOtherGuildEvidenceAndRejectsInvalidIdentity(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", RawJSON: `{"id":"g","fixture":"retained","roles":[{"id":"g","permissions":"1024"},{"id":"other","permissions":"8"}]}`}))
	started := time.Now()
	require.NoError(t, s.ApplyGuildRole(ctx, "g", "g", &discordgo.Role{ID: "g", Permissions: 0}))
	require.NoError(t, s.ApplyGuildRole(ctx, "g", "new", &discordgo.Role{ID: "new", Permissions: 1024}))
	require.NoError(t, s.ApplyGuildRole(ctx, "g", "new", nil))
	require.NoError(t, s.UpsertObservedGuild(ctx, GuildRecord{ID: "g", RawJSON: `{}`}, started))
	records, err := s.GuildScopePayloads(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(records[0].RawJSON), &fields))
	require.JSONEq(t, `"retained"`, string(fields["fixture"]))
	var roles []*discordgo.Role
	require.NoError(t, json.Unmarshal(fields["roles"], &roles))
	require.Len(t, roles, 2)
	require.Equal(t, "other", roles[0].ID)
	require.Equal(t, int64(8), roles[0].Permissions)
	require.Zero(t, roles[1].Permissions)
	require.Error(t, s.ApplyGuildRole(ctx, "g", "g", &discordgo.Role{ID: "different"}))
	require.Error(t, s.ApplyGuildRole(ctx, "missing", "r", nil))
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "invalid", RawJSON: `{"roles":"invalid"}`}))
	require.Error(t, s.ApplyGuildRole(ctx, "invalid", "r", nil))
}
