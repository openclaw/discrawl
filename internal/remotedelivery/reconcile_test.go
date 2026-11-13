package remotedelivery

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func finishCapture(t *testing.T, outbox *Outbox) []remote.Change {
	t.Helper()
	var changes []remote.Change
	for {
		part, err := outbox.NextPart(t.Context())
		require.NoError(t, err)
		if part == nil {
			return changes
		}
		batch, err := remote.DecodeChangeBatch(part.Body)
		require.NoError(t, err)
		changes = append(changes, batch.Changes...)
		require.NoError(t, outbox.RecordReceipt(t.Context(), part.Ref, receiptFor(t, part)))
	}
}

func TestReconcileChannelEntryAndExit(t *testing.T) {
	for _, exit := range []string{"scope", "delete"} {
		t.Run(exit, func(t *testing.T) {
			ctx, source, path, outbox, _, baseline := outboxFixture(t)
			checkpoint := "channel:cg2:history_complete"
			require.NoError(t, source.SetSyncState(ctx, checkpoint, "unchanged"))
			_, err := outbox.Stage(ctx, capture(t, ctx, path, baseline), 1<<20)
			require.NoError(t, err)
			require.Empty(t, finishCapture(t, outbox), "excluded checkpoint must not be published")
			state, err := outbox.State(ctx)
			require.NoError(t, err)
			_, err = source.DB().ExecContext(ctx, `update channels set guild_id='g1' where id='cg2'`)
			require.NoError(t, err)
			entry := capture(t, ctx, path, state.Baseline)
			require.Len(t, entry.Changes, 1, "checkpoint did not change")
			_, err = outbox.Stage(ctx, entry, 1<<20)
			require.NoError(t, err)
			part, err := outbox.NextPart(ctx)
			require.NoError(t, err)
			persisted := append([]byte(nil), part.Body...)
			require.NoError(t, source.SetSyncState(ctx, checkpoint, "newer source value"))
			part, err = outbox.NextPart(ctx)
			require.NoError(t, err)
			require.Equal(t, persisted, part.Body)
			key, err := checkpointKey(checkpoint)
			require.NoError(t, err)
			var owned bool
			require.NoError(t, outbox.base.DB().QueryRowContext(ctx, `select exists(select 1 from owned_keys where key=?)`, key).Scan(&owned))
			require.False(t, owned, "staging cannot advance ownership")
			changes := finishCapture(t, outbox)
			require.Len(t, changes, 2)
			for _, change := range changes {
				require.Equal(t, "upsert", change.Operation)
				if change.Table == "sync_state" {
					require.Equal(t, checkpoint, *change.Key[0].Value)
					require.Equal(t, "unchanged", *change.Row[1].Value)
				}
			}
			require.NoError(t, outbox.base.DB().QueryRowContext(ctx, `select exists(select 1 from owned_keys where key=?)`, key).Scan(&owned))
			require.True(t, owned)
			state, err = outbox.State(ctx)
			require.NoError(t, err)
			query := `update channels set guild_id='g2' where id='cg2'`
			if exit == "delete" {
				query = `delete from channels where id='cg2'`
			}
			_, err = source.DB().ExecContext(ctx, query)
			require.NoError(t, err)
			_, err = outbox.Stage(ctx, capture(t, ctx, path, state.Baseline), 1<<20)
			require.NoError(t, err)
			changes = finishCapture(t, outbox)
			require.Len(t, changes, 2, "journal/dependency overlap must produce one checkpoint removal")
			for _, change := range changes {
				require.Equal(t, "remove", change.Operation)
			}
			require.NoError(t, outbox.base.DB().QueryRowContext(ctx, `select exists(select 1 from owned_keys where key=?)`, key).Scan(&owned))
			require.False(t, owned)
		})
	}
}

func TestReconcileAbsentOwnedCheckpointAndEvidence(t *testing.T) {
	ctx, source, path, baseline := projectionFixture(t)
	_, err := source.DB().ExecContext(ctx, `update channels set guild_id='g2' where id='cg1'`)
	require.NoError(t, err)
	captured := capture(t, ctx, path, baseline)
	channel, err := OwnershipKey("channels", []store.NativeValue{{Kind: "text", Text: "cg1"}})
	require.NoError(t, err)
	missing, err := checkpointKey("channel:cg1:no_longer_present")
	require.NoError(t, err)
	ownership := ownershipFor(t, captured, map[string]bool{channel: true, missing: true})
	projected, err := ProjectCapture(captured, Scope{GuildIDs: []string{"g1"}}, ownership)
	require.NoError(t, err)
	require.Len(t, projected.Changes, 2)
	require.Equal(t, "remove", projected.Changes[1].Operation)
	require.Equal(t, "channel:cg1:no_longer_present", *projected.Changes[1].Key[0].Value)
	// Completeness must not contradict the captured channel identity.
	wrong := captured
	wrong.ChannelCoverage = map[string]store.NativeChannelCoverage{"cg1": {Present: true, GuildID: "g1"}}
	_, err = ProjectCapture(wrong, Scope{GuildIDs: []string{"g1"}}, ownership)
	require.ErrorIs(t, err, ErrProjection)
	// Guild removal still has dependent rows outside this channel-only contract.
	_, err = source.DB().ExecContext(ctx, `delete from guilds where id='g1'`)
	require.NoError(t, err)
	captured = capture(t, ctx, path, baseline)
	guild, err := OwnershipKey("guilds", []store.NativeValue{{Kind: "text", Text: "g1"}})
	require.NoError(t, err)
	_, err = ProjectCapture(captured, Scope{GuildIDs: []string{"g1"}}, ownershipFor(t, captured, map[string]bool{guild: true, channel: true}))
	require.ErrorIs(t, err, ErrReconcile)
}

func TestReconcileOwnershipRangeAndBounds(t *testing.T) {
	ctx, source, path, outbox, _, baseline := outboxFixture(t)
	channel := "c_%<>\"\\tail"
	require.NoError(t, source.UpsertChannel(ctx, store.ChannelRecord{ID: channel, GuildID: "g1", Name: "literal", Kind: "text", RawJSON: "{}"}))
	key, err := checkpointKey("channel:" + channel + ":old")
	require.NoError(t, err)
	other, err := checkpointKey("channel:" + channel + "x:unrelated")
	require.NoError(t, err)
	_, err = outbox.base.DB().ExecContext(ctx, `insert into owned_keys values(?),(?)`, key, other)
	require.NoError(t, err)
	_, err = outbox.Stage(ctx, capture(t, ctx, path, baseline), 1<<20)
	require.NoError(t, err)
	changes := finishCapture(t, outbox)
	require.Len(t, changes, 2)
	var count int
	require.NoError(t, outbox.base.DB().QueryRowContext(ctx, `select count(*) from owned_keys where key in (?,?)`, key, other).Scan(&count))
	require.Equal(t, 1, count, "only the literal channel's absent checkpoint is removed")
	state, err := outbox.State(ctx)
	require.NoError(t, err)
	large, err := checkpointKey("channel:" + channel + ":" + strings.Repeat("x", 1000))
	require.NoError(t, err)
	_, err = outbox.base.DB().ExecContext(ctx, `insert into owned_keys values(?)`, large)
	require.NoError(t, err)
	_, err = source.DB().ExecContext(ctx, `update channels set name='changed' where id=?`, channel)
	require.NoError(t, err)
	captured := capture(t, ctx, path, state.Baseline)
	_, err = outbox.Stage(ctx, captured, 128)
	require.ErrorIs(t, err, ErrOutboxLimit)
	after, err := outbox.State(ctx)
	require.NoError(t, err)
	require.Empty(t, after.PendingID)
	require.Equal(t, state.Baseline, after.Baseline)
	// Exceed the fixed 500*64 ownership-row bound before projection/encoding.
	_, err = outbox.base.DB().ExecContext(ctx, `with recursive n(x) as (values(1) union all select x+1 from n where x<32001)
		insert into owned_keys select json_array('sync_state','channel:cg1:k'||x) from n`)
	require.NoError(t, err)
	_, err = source.DB().ExecContext(ctx, `update channels set name='changed' where id='cg1'`)
	require.NoError(t, err)
	_, err = outbox.Stage(ctx, capture(t, ctx, path, state.Baseline), 8<<20)
	require.ErrorIs(t, err, ErrOutboxLimit)
	after, err = outbox.State(ctx)
	require.NoError(t, err)
	require.Empty(t, after.PendingID)
	require.Equal(t, state.Baseline, after.Baseline)
	encoded, err := json.Marshal(changes)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "unrelated")
}
