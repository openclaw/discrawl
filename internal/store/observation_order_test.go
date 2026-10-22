package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/messagetext"
	"github.com/stretchr/testify/require"
)

func observationOrderMessage(content, edited string) MessageRecord {
	return MessageRecord{
		ID: "100", GuildID: "g", ChannelID: "c", AuthorID: "u", Content: content,
		NormalizedContent: content, CreatedAt: "2026-10-01T00:00:00Z", EditedAt: edited,
		RawJSON: `{"id":"100","guild_id":"g","channel_id":"c","content":"` + content + `"}`,
	}
}

func TestProviderEditOrderSurvivesProjectionOnlyWrites(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{{Record: observationOrderMessage("first", "2026-10-01T00:01:00Z")}}))

	started := time.Now().UTC()
	// A projection-only write (text repair, attachment bookkeeping) advances
	// updated_at after the page request started without observing the provider.
	later := started.Add(time.Second).Format(timeLayout)
	_, err = s.DB().ExecContext(ctx, `update messages set updated_at=? where id='100'`, later)
	require.NoError(t, err)

	page := func(content, edited string) error {
		return s.UpsertMessages(ctx, []MessageMutation{{
			Record:  observationOrderMessage(content, edited),
			Options: WriteOptions{PreserveNewer: true, ObservationStartedAt: started},
		}})
	}
	content := func() string {
		var body string
		require.NoError(t, s.DB().QueryRowContext(ctx, `select content from messages where id='100'`).Scan(&body))
		return body
	}

	require.NoError(t, page("same revision refresh", "2026-10-01T00:01:00Z"))
	require.Equal(t, "first", content(), "equal provider revisions still yield to a later local write")
	require.NoError(t, page("older", "2026-10-01T00:00:30Z"))
	require.Equal(t, "first", content())
	require.NoError(t, page("strictly newer edit", "2026-10-01T00:02:00Z"))
	require.Equal(t, "strictly newer edit", content())
}

func TestZeroVersionWriteClearsSupersededTextParts(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	native := observationOrderMessage("native", "2026-10-01T00:01:00Z")
	native.TextVersion = messagetext.Version
	native.TextPartsJSON = `[{"kind":"authored","path":"/content","text":"native"}]`
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{{Record: native}}))

	// A writer without an attributed projection, such as a Desktop import,
	// replaces the message; the earlier native provenance no longer applies.
	desktop := observationOrderMessage("desktop edit", "2026-10-01T00:02:00Z")
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{{Record: desktop, Options: WriteOptions{PreserveNewer: true}}}))
	var parts string
	var version int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select text_parts_json,text_version from messages where id='100'`).Scan(&parts, &version))
	require.Equal(t, "[]", parts)
	require.Zero(t, version)
}

func TestRemovalEventFailureTracksEachEventTime(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	ref := FailureRef{Operation: "tail_metadata", Source: "discord", GuildID: "g", RelatedKind: "gateway_event", RelatedID: "GUILD_MEMBER_REMOVE:u"}
	firstSeen := func() time.Time {
		report, err := s.ListFailures(ctx, FailureListOptions{IncludeResolved: true}, time.Now())
		require.NoError(t, err)
		require.Len(t, report.Failures, 1)
		return report.Failures[0].FirstSeenAt
	}

	received := time.Now().UTC().Add(-time.Minute)
	require.NoError(t, s.RecordEventFailure(ctx, ref, errors.New("first removal"), received))
	first := firstSeen()
	require.True(t, first.Equal(received), "replay compares against the Gateway receipt time")
	time.Sleep(2 * time.Millisecond)
	// Replay retries keep the event time they are retrying.
	require.NoError(t, s.RecordFailure(ctx, ref, errors.New("replay retry")))
	require.Equal(t, first, firstSeen())

	require.NoError(t, s.ResolveFailures(ctx, ref))
	time.Sleep(2 * time.Millisecond)
	// A later removal of the same member is a new event, even after the
	// earlier failure resolved, so replay must not judge it by the first time.
	require.NoError(t, s.RecordEventFailure(ctx, ref, errors.New("second removal"), time.Time{}))
	require.True(t, firstSeen().After(first))
	require.Error(t, s.RecordEventFailure(ctx, ref, nil, time.Time{}))
}

func TestStaleMutationKeepsGenuineEventObservation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{{Record: observationOrderMessage("edited by repair", "2026-10-01T00:02:00Z")}}))

	// A queued Gateway create carrying the original payload arrives after REST
	// already stored the newer edit: canonical state stays newer, but the
	// genuine create observation is still retained.
	original := observationOrderMessage("original", "")
	create := MessageMutation{
		Record: original, EventType: "create", PayloadJSON: original.RawJSON,
		Options: WriteOptions{PreserveNewer: true, AppendEvent: true},
	}
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{create}))
	var content string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content from messages where id='100'`).Scan(&content))
	require.Equal(t, "edited by repair", content)
	var events int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events where message_id='100' and event_type='create'`).Scan(&events))
	require.Equal(t, 1, events)

	create.Options.DeduplicateEvent = true
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{create}))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events where message_id='100' and event_type='create'`).Scan(&events))
	require.Equal(t, 1, events, "deduplicated observations are not repeated")
}

func TestReplayResolvesOnlyTheGenerationItReconciled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	ref := FailureRef{Operation: "tail_metadata", Source: "discord", GuildID: "g", RelatedKind: "gateway_event", RelatedID: "GUILD_MEMBER_UPDATE:u"}
	require.NoError(t, s.RecordFailure(ctx, ref, errors.New("older role change")))
	report, err := s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	replayed := report.Failures[0]

	// A newer event for the same identity fails while replay is in flight.
	require.NoError(t, s.RecordFailure(ctx, ref, errors.New("newer role change")))
	require.NoError(t, s.ResolveFailureGeneration(ctx, replayed, "metadata_reconciled"))
	report, err = s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedCount, "the newer failure stays pending")

	require.NoError(t, s.ResolveFailureGeneration(ctx, report.Failures[0], "metadata_reconciled"))
	report, err = s.ListFailures(ctx, FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Zero(t, report.UnresolvedCount)
	require.Error(t, s.ResolveFailureGeneration(ctx, Failure{}, "metadata_reconciled"))
	require.Error(t, s.ResolveFailureGeneration(ctx, replayed, ""))
}
