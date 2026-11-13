package remotedelivery

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"

	"github.com/openclaw/discrawl/internal/store"
)

func checkpointPrefix(channel string) (string, error) {
	if !validIdentity(channel) || strings.Contains(channel, ":") {
		return "", ErrProjection
	}
	return "channel:" + channel + ":", nil
}

func checkpointKey(scope string) (string, error) {
	return OwnershipKey("sync_state", []store.NativeValue{{Kind: "text", Text: scope}})
}

// The source supplies complete current rows; the outbox supplies complete old
// ownership. Their union withdraws absent/excluded rows without a source reopen.
func reconcileChannelCheckpoints(captured store.NativeCapture, ownership OwnershipSnapshot) (store.NativeCapture, error) {
	if len(captured.ChannelCoverage) == 0 {
		return captured, nil
	}
	captured.Changes = slices.Clone(captured.Changes)
	captured.CoverageGuilds = maps.Clone(captured.CoverageGuilds)
	if captured.CoverageGuilds == nil {
		captured.CoverageGuilds = map[string]string{}
	}
	index := map[string]int{}
	channels := map[string]store.NativeChange{}
	journalCheckpoints := map[string][]store.NativeChange{}
	for i, change := range captured.Changes {
		key, err := OwnershipKey(change.Table, change.Key)
		if err != nil {
			return store.NativeCapture{}, err
		}
		if _, duplicate := index[key]; duplicate {
			return store.NativeCapture{}, ErrProjection
		}
		index[key] = i
		if change.Table == "channels" {
			channels[change.Key[0].Text] = change
		} else if change.Table == "sync_state" {
			if rest, ok := strings.CutPrefix(change.Key[0].Text, "channel:"); ok {
				channel, _, _ := strings.Cut(rest, ":")
				journalCheckpoints[channel] = append(journalCheckpoints[channel], change)
			}
		}
	}
	for _, channel := range slices.Sorted(maps.Keys(captured.ChannelCoverage)) {
		entry := captured.ChannelCoverage[channel]
		parent, ok := channels[channel]
		if !ok || entry.Present != (parent.Operation == "upsert") {
			return store.NativeCapture{}, ErrProjection
		}
		if entry.Present {
			row, err := projectRow(parent)
			if err != nil || row[1].Value == nil || *row[1].Value != entry.GuildID {
				return store.NativeCapture{}, ErrProjection
			}
		} else if parent.Operation != "remove" || entry.GuildID != "" {
			return store.NativeCapture{}, ErrProjection
		}
		prefix, err := checkpointPrefix(channel)
		if err != nil {
			return store.NativeCapture{}, err
		}
		prior, complete := ownership.ChannelCheckpoints[channel]
		if !complete {
			return store.NativeCapture{}, ErrOwnership
		}
		current := map[string]bool{}
		for _, dependency := range entry.Checkpoints {
			if dependency.Table != "sync_state" || dependency.Operation != "upsert" || len(dependency.Key) != 1 || dependency.Key[0].Kind != "text" || !strings.HasPrefix(dependency.Key[0].Text, prefix) || len(dependency.Key[0].Text) == len(prefix) {
				return store.NativeCapture{}, ErrProjection
			}
			key, err := checkpointKey(dependency.Key[0].Text)
			if err != nil || current[key] {
				return store.NativeCapture{}, ErrProjection
			}
			current[key] = true
			if i, exists := index[key]; exists {
				original := captured.Changes[i]
				if original.Operation != dependency.Operation || !slices.Equal(original.Row, dependency.Row) || !slices.Equal(original.Columns, dependency.Columns) {
					return store.NativeCapture{}, ErrProjection
				}
			} else {
				index[key] = len(captured.Changes)
				captured.Changes = append(captured.Changes, dependency)
			}
			captured.CoverageGuilds[dependency.Key[0].Text] = entry.GuildID
		}
		// A journal upsert omitted from the complete current set is contradictory.
		for _, change := range journalCheckpoints[channel] {
			if change.Operation == "upsert" {
				key, _ := checkpointKey(change.Key[0].Text)
				if !current[key] {
					return store.NativeCapture{}, ErrProjection
				}
			}
		}
		seen := map[string]bool{}
		for _, key := range prior {
			var fields []string
			if json.Unmarshal([]byte(key), &fields) != nil || len(fields) != 2 || fields[0] != "sync_state" || !strings.HasPrefix(fields[1], prefix) || len(fields[1]) == len(prefix) || seen[key] || !ownership.Keys[key] {
				return store.NativeCapture{}, ErrOwnership
			}
			canonical, err := checkpointKey(fields[1])
			if err != nil || canonical != key {
				return store.NativeCapture{}, ErrOwnership
			}
			seen[key] = true
			if _, exists := index[key]; !exists {
				index[key] = len(captured.Changes)
				captured.Changes = append(captured.Changes, store.NativeChange{Table: "sync_state", Operation: "remove", Key: []store.NativeValue{{Kind: "text", Text: fields[1]}}})
			}
		}
	}
	return captured, nil
}
