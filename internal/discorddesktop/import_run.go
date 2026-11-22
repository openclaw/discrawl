package discorddesktop

import (
	"context"
	"os"
	"sort"
	"strings"

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
	return r.scanBatch(candidates, r.base, false)
}

func (r *importRun) scanCacheBatches(candidates []fileCandidate) error {
	for start := 0; start < len(candidates); start += checkpointEveryFiles {
		end := min(start+checkpointEveryFiles, len(candidates))
		batch := newSnapshotWithContext(r.base)
		if err := r.scanBatch(candidates[start:end], batch, false); err != nil {
			return err
		}
		mergeSnapshotContext(r.base, batch)
	}
	return nil
}

func (r *importRun) scanBatch(candidates []fileCandidate, snap snapshot, retry bool) error {
	files, err := scanCandidates(r.ctx, r.rootFS, r.opts, candidates, snap, r.channelLookup, r.stats)
	if err != nil {
		return err
	}
	unresolved := finalizeSnapshot(snap, r.channelLookup, r.totals, r.stats, retry)
	pendingKeys := make(map[string][]string)
	for id := range unresolved {
		for _, key := range files.messageSources[id] {
			pendingKeys[key] = append(pendingKeys[key], id)
		}
	}
	if !retry && len(pendingKeys) > 0 {
		if r.pendingLookupSize < 0 {
			r.pendingLookupSize = len(r.channelLookup)
			r.pendingRouteSize = len(r.base.routes)
		}
		for _, file := range files.files {
			if len(pendingKeys[file.relKey]) != 0 {
				r.pending = append(r.pending, file)
			}
		}
		mergeUnresolved(r.pendingUnresolved, unresolved)
	}
	// An unchanged partial file with the same pending IDs has no new
	// messages to write. Keep parsing it for context and late resolution.
	progress := make(map[string]bool, len(files.files))
	previousPending := make(map[string]map[string]bool)
	for _, file := range files.files {
		ids := pendingKeys[file.relKey]
		sort.Strings(ids)
		prior, ok := r.state.current[file.relKey]
		if !ok {
			prior = r.state.previous[file.relKey]
		}
		sameRevision := sameFileFingerprint(prior, file.fingerprint)
		progress[file.relKey] = prior.PendingIDs == "" || !sameRevision || prior.PendingIDs != strings.Join(ids, ",")
		if sameRevision && prior.PendingIDs != "" && len(ids) != 0 {
			previousPending[file.relKey] = make(map[string]bool)
			for id := range strings.SplitSeq(prior.PendingIDs, ",") {
				previousPending[file.relKey][id] = true
			}
		}
	}
	if !r.opts.DryRun {
		anyProgress := false
		for _, advances := range progress {
			anyProgress = anyProgress || advances
		}
		if !anyProgress {
			return nil
		}
	}
	// Fully resolved files can publish their events now. Messages sourced
	// only by pending files still defer events until their file is complete.
	for id, message := range snap.messages {
		sources := files.messageSources[id]
		complete, apply := false, false
		for _, key := range sources {
			if progress[key] && (previousPending[key] == nil || previousPending[key][id]) {
				apply = true
				complete = complete || len(pendingKeys[key]) == 0
			}
		}
		if !apply && !r.opts.DryRun {
			delete(snap.messages, id)
			continue
		}
		if !complete {
			message.Options.AppendEvent = false
			snap.messages[id] = message
		}
	}
	return commitSnapshot(r.ctx, r.st, r.opts, r.state, files.files, snap, pendingKeys, r.totals, r.stats)
}

func (r *importRun) retryPending() error {
	if len(r.pending) == 0 {
		return nil
	}
	if !r.pendingCanResolve() {
		recordUnresolved(r.pendingUnresolved, r.totals, r.stats)
		return nil
	}
	retry := newSnapshotWithContext(r.base)
	return r.scanBatch(r.pending, retry, true)
}

func (r *importRun) pendingCanResolve() bool {
	return len(r.channelLookup) > r.pendingLookupSize || len(r.base.routes) > r.pendingRouteSize
}
