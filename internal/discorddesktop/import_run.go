package discorddesktop

import (
	"context"
	"os"

	"github.com/openclaw/discrawl/internal/store"
)

type importRun struct {
	ctx               context.Context
	st                *store.Store
	opts              Options
	state             scanState
	rootFS            *os.Root
	channelLookup     map[string]store.ChannelRecord
	totals            scanTotals
	stats             *Stats
	base              snapshot
	pending           []fileCandidate
	pendingUnresolved unresolvedMessages
	pendingLookupSize int
	pendingRouteSize  int
}

func newImportRun(ctx context.Context, st *store.Store, opts Options, state scanState, rootFS *os.Root, stats *Stats) *importRun {
	return &importRun{
		ctx:               ctx,
		st:                st,
		opts:              opts,
		state:             state,
		rootFS:            rootFS,
		channelLookup:     copyChannelLookup(state.channels),
		totals:            newScanTotals(),
		stats:             stats,
		base:              newSnapshot(),
		pendingUnresolved: unresolvedMessages{},
		pendingLookupSize: -1,
		pendingRouteSize:  -1,
	}
}

func (r *importRun) scanContext(candidates []fileCandidate) error {
	if err := scanCandidates(r.ctx, r.rootFS, r.opts, candidates, r.base, r.channelLookup, r.stats); err != nil {
		return err
	}
	return r.finalizeAndCommit(candidates, r.base, false)
}

func (r *importRun) scanCacheBatches(candidates []fileCandidate) error {
	for start := 0; start < len(candidates); start += checkpointEveryFiles {
		end := min(start+checkpointEveryFiles, len(candidates))
		batchCandidates := candidates[start:end]
		batch := newSnapshotWithContext(r.base)
		if err := scanCandidates(r.ctx, r.rootFS, r.opts, batchCandidates, batch, r.channelLookup, r.stats); err != nil {
			return err
		}
		if err := r.finalizeAndCommit(batchCandidates, batch, false); err != nil {
			return err
		}
		mergeSnapshotContext(r.base, batch)
	}
	return nil
}

func (r *importRun) finalizeAndCommit(candidates []fileCandidate, snap snapshot, recordSkipped bool) error {
	filterExcludedMessages(snap, r.channelLookup, r.opts, r.totals, r.stats)
	unresolved := finalizeSnapshot(snap, r.channelLookup, r.totals, r.stats, recordSkipped)
	checkpoint := len(unresolved) == 0
	if !checkpoint {
		r.deferCandidates(candidates, unresolved)
	}
	if len(candidates) == 0 && !snapshotHasChanges(snap) {
		return nil
	}
	return commitSnapshot(r.ctx, r.st, r.opts, r.state, candidates, snap, checkpoint, r.stats)
}

func (r *importRun) deferCandidates(candidates []fileCandidate, unresolved unresolvedMessages) {
	r.pending = append(r.pending, candidates...)
	mergeUnresolved(r.pendingUnresolved, unresolved)
	if r.pendingLookupSize >= 0 {
		return
	}
	r.pendingLookupSize = len(r.channelLookup)
	r.pendingRouteSize = len(r.base.routes)
}

func (r *importRun) retryPending() error {
	if len(r.pending) == 0 {
		return nil
	}
	if !r.pendingCanResolve() {
		recordUnresolved(r.pendingUnresolved, r.totals, r.stats)
		return checkpointScannedCandidates(r.ctx, r.st, r.opts, r.state, r.pending, r.stats)
	}
	retry := newSnapshotWithContext(r.base)
	if err := scanCandidates(r.ctx, r.rootFS, r.opts, r.pending, retry, r.channelLookup, r.stats); err != nil {
		return err
	}
	filterExcludedMessages(retry, r.channelLookup, r.opts, r.totals, r.stats)
	unresolved := finalizeSnapshot(retry, r.channelLookup, r.totals, r.stats, true)
	checkpoint := len(unresolved) == 0
	if err := commitSnapshot(r.ctx, r.st, r.opts, r.state, r.pending, retry, checkpoint, r.stats); err != nil {
		return err
	}
	mergeSnapshotContext(r.base, retry)
	return nil
}

func (r *importRun) pendingCanResolve() bool {
	return len(r.channelLookup) > r.pendingLookupSize || len(r.base.routes) > r.pendingRouteSize
}
