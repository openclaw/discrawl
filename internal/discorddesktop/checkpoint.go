package discorddesktop

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/openclaw/discrawl/internal/store"
)

type fileFingerprint struct {
	Size      int64  `json:"size"`
	ModUnixNS int64  `json:"mod_unix_ns"`
	Status    string `json:"status,omitempty"`
}

type scanState struct {
	previous map[string]fileFingerprint
	current  map[string]fileFingerprint
	channels map[string]store.ChannelRecord
}

func loadScanState(ctx context.Context, st *store.Store, opts Options) (scanState, error) {
	state := scanState{
		previous: map[string]fileFingerprint{},
		current:  map[string]fileFingerprint{},
		channels: map[string]store.ChannelRecord{},
	}
	if st == nil || opts.DryRun {
		return state, nil
	}
	raw, err := st.GetSyncState(ctx, wiretapFileIndexScope)
	if err != nil {
		return state, err
	}
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &state.previous); err != nil {
			state.previous = map[string]fileFingerprint{}
		}
	} else {
		migrated, err := loadLegacyFileIndex(ctx, st)
		if err != nil {
			return state, err
		}
		state.previous = migrated
	}
	channels, err := st.Channels(ctx, "")
	if err != nil {
		return state, err
	}
	for _, channel := range channels {
		state.channels[channel.ID] = store.ChannelRecord{
			ID:      channel.ID,
			GuildID: channel.GuildID,
			Kind:    channel.Kind,
			Name:    channel.Name,
		}
	}
	return state, nil
}

// Both legacy indexes can certify unresolved files as imported. Recheck them
// once, retaining the index so upgrade is not mistaken for first-import pruning.
func loadLegacyFileIndex(ctx context.Context, st *store.Store) (map[string]fileFingerprint, error) {
	for _, scope := range []string{wiretapFileIndexScopeV2, wiretapFileIndexScopeV1} {
		raw, err := st.GetSyncState(ctx, scope)
		if err != nil {
			return nil, err
		}
		legacy := map[string]fileFingerprint{}
		if strings.TrimSpace(raw) == "" || json.Unmarshal([]byte(raw), &legacy) != nil {
			continue
		}
		for relKey, fingerprint := range legacy {
			legacy[relKey] = skippedFingerprint(fingerprint)
		}
		return legacy, nil
	}
	return map[string]fileFingerprint{}, nil
}

func saveFileIndex(ctx context.Context, st *store.Store, index map[string]fileFingerprint) error {
	body, err := json.Marshal(index)
	if err != nil {
		return err
	}
	return st.SetSyncState(ctx, wiretapFileIndexScope, string(body))
}

func sameFileFingerprint(a, b fileFingerprint) bool {
	return a.Size == b.Size && a.ModUnixNS == b.ModUnixNS
}

func isImportedFingerprint(fingerprint fileFingerprint) bool {
	return fingerprint.Status == "" || fingerprint.Status == fileStatusImported
}

func importedFingerprint(fingerprint fileFingerprint) fileFingerprint {
	fingerprint.Status = fileStatusImported
	return fingerprint
}

func skippedFingerprint(fingerprint fileFingerprint) fileFingerprint {
	fingerprint.Status = fileStatusSkipped
	return fingerprint
}

func snapshotHasChanges(snap snapshot) bool {
	return len(snap.guilds) > 0 || len(snap.channels) > 0 || len(snap.messages) > 0
}
