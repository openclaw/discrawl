package discorddesktop

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

type snapshot struct {
	guilds     map[string]store.GuildRecord
	channels   map[string]store.ChannelRecord
	messages   map[string]store.MessageMutation
	routes     map[string]string
	userLabels map[string]userLabel
}

type scanTotals struct {
	guilds          map[string]struct{}
	channels        map[string]struct{}
	messages        map[string]struct{}
	dmMessages      map[string]struct{}
	guildMessages   map[string]struct{}
	dmChannels      map[string]struct{}
	skippedMessages map[string]struct{}
	skippedChannels map[string]struct{}
}

type unresolvedMessages map[string]string

const (
	wiretapFileIndexScope   = "wiretap:file_index:v3"
	wiretapFileIndexScopeV2 = "wiretap:file_index:v2"
	wiretapFileIndexScopeV1 = "wiretap:file_index:v1"
)

const (
	fileStatusImported = "imported"
	fileStatusSkipped  = "skipped"
)

func newScanTotals() scanTotals {
	return scanTotals{
		guilds:          map[string]struct{}{},
		channels:        map[string]struct{}{},
		messages:        map[string]struct{}{},
		dmMessages:      map[string]struct{}{},
		guildMessages:   map[string]struct{}{},
		dmChannels:      map[string]struct{}{},
		skippedMessages: map[string]struct{}{},
		skippedChannels: map[string]struct{}{},
	}
}

func finalizeSnapshot(snap snapshot, channelLookup map[string]store.ChannelRecord, totals scanTotals, stats *Stats, recordSkipped bool) unresolvedMessages {
	reconcileMessages(snap, channelLookup)
	inferDirectMessageNames(snap)
	reconcileMessages(snap, channelLookup)
	unresolved := unresolvedMessages{}
	for id, msg := range snap.messages {
		guildID := msg.Record.GuildID
		if guildID == "" {
			unresolved[id] = msg.Record.ChannelID
			if recordSkipped {
				totals.skippedMessages[id] = struct{}{}
				totals.skippedChannels[msg.Record.ChannelID] = struct{}{}
			}
			delete(snap.messages, id)
			continue
		}
		if _, ok := snap.guilds[guildID]; !ok {
			snap.guilds[guildID] = syntheticGuild(guildID, guildName(guildID))
		}
		if _, ok := snap.channels[msg.Record.ChannelID]; !ok {
			if channel, ok := channelLookup[msg.Record.ChannelID]; ok && channel.GuildID != "" {
				snap.channels[msg.Record.ChannelID] = channel
			} else {
				snap.channels[msg.Record.ChannelID] = syntheticChannel(msg.Record.ChannelID, guildID, msg.Record.ChannelName)
			}
		}
		snap.messages[id] = msg
	}
	for _, msg := range snap.messages {
		totals.messages[msg.Record.ID] = struct{}{}
		switch msg.Record.GuildID {
		case DirectMessageGuildID:
			totals.dmMessages[msg.Record.ID] = struct{}{}
			totals.dmChannels[msg.Record.ChannelID] = struct{}{}
		default:
			totals.guildMessages[msg.Record.ID] = struct{}{}
		}
	}
	for id, channel := range snap.channels {
		channelLookup[id] = channel
		totals.channels[id] = struct{}{}
	}
	for id := range snap.guilds {
		totals.guilds[id] = struct{}{}
	}
	stats.DMChannels = len(totals.dmChannels)
	stats.SkippedChannels = len(totals.skippedChannels)
	stats.Guilds = len(totals.guilds)
	stats.Channels = len(totals.channels)
	stats.Messages = len(totals.messages)
	stats.DMMessages = len(totals.dmMessages)
	stats.GuildMessages = len(totals.guildMessages)
	stats.SkippedMessages = len(totals.skippedMessages)
	return unresolved
}

func mergeUnresolved(dst, src unresolvedMessages) {
	maps.Copy(dst, src)
}

func recordUnresolved(unresolved unresolvedMessages, totals scanTotals, stats *Stats) {
	for messageID, channelID := range unresolved {
		totals.skippedMessages[messageID] = struct{}{}
		totals.skippedChannels[channelID] = struct{}{}
	}
	stats.SkippedChannels = len(totals.skippedChannels)
	stats.SkippedMessages = len(totals.skippedMessages)
}

func commitSnapshot(ctx context.Context, st *store.Store, opts Options, state scanState, candidates []fileCandidate, snap snapshot, checkpoint bool, stats *Stats) error {
	if opts.DryRun {
		return nil
	}
	if !checkpoint {
		if snapshotHasChanges(snap) {
			return writeSnapshot(ctx, st, snapshotWithoutMessageEvents(snap), false)
		}
		return nil
	}
	if snapshotHasChanges(snap) {
		if err := writeSnapshot(ctx, st, snap, false); err != nil {
			return err
		}
	} else if err := st.SetSyncState(ctx, "wiretap:last_import", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	for _, candidate := range candidates {
		state.current[candidate.relKey] = importedFingerprint(candidate.fingerprint)
	}
	if err := saveFileIndex(ctx, st, state.current); err != nil {
		return err
	}
	stats.Checkpoints++
	return nil
}

func checkpointScannedCandidates(ctx context.Context, st *store.Store, opts Options, state scanState, candidates []fileCandidate, stats *Stats) error {
	if opts.DryRun {
		return nil
	}
	if err := st.SetSyncState(ctx, "wiretap:last_import", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	for _, candidate := range candidates {
		state.current[candidate.relKey] = skippedFingerprint(candidate.fingerprint)
	}
	if err := saveFileIndex(ctx, st, state.current); err != nil {
		return err
	}
	stats.Checkpoints++
	return nil
}

func snapshotWithoutMessageEvents(snap snapshot) snapshot {
	out := snapshot{
		guilds:     snap.guilds,
		channels:   snap.channels,
		messages:   make(map[string]store.MessageMutation, len(snap.messages)),
		routes:     snap.routes,
		userLabels: snap.userLabels,
	}
	for id, message := range snap.messages {
		message.Options.AppendEvent = false
		out.messages[id] = message
	}
	return out
}

func newSnapshot() snapshot {
	return snapshot{
		guilds:     map[string]store.GuildRecord{},
		channels:   map[string]store.ChannelRecord{},
		messages:   map[string]store.MessageMutation{},
		routes:     map[string]string{},
		userLabels: map[string]userLabel{},
	}
}

func newSnapshotWithContext(base snapshot) snapshot {
	snap := newSnapshot()
	maps.Copy(snap.routes, base.routes)
	maps.Copy(snap.userLabels, base.userLabels)
	return snap
}

func mergeSnapshotContext(base snapshot, next snapshot) {
	for channelID, guildID := range next.routes {
		collectChannelRoute(base, channelID, guildID)
	}
	maps.Copy(base.userLabels, next.userLabels)
	maps.Copy(base.channels, next.channels)
}

func copyChannelLookup(in map[string]store.ChannelRecord) map[string]store.ChannelRecord {
	out := make(map[string]store.ChannelRecord, len(in))
	maps.Copy(out, in)
	return out
}

func writeSnapshot(ctx context.Context, st *store.Store, snap snapshot, prune bool) error {
	if prune {
		if err := st.DeleteGuildData(ctx, "@unknown"); err != nil {
			return err
		}
	}
	guilds := mapValues(snap.guilds)
	sort.Slice(guilds, func(i, j int) bool { return guilds[i].ID < guilds[j].ID })
	for _, guild := range guilds {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := st.UpsertGuild(ctx, guild); err != nil {
			return recordImportFailure(ctx, st, store.FailureRef{
				Operation: "import_guild",
				Source:    "wiretap",
				GuildID:   guild.ID,
			}, err)
		}
		if err := resolveImportFailures(ctx, st, store.FailureRef{
			Operation: "import_guild",
			Source:    "wiretap",
			GuildID:   guild.ID,
		}); err != nil {
			return err
		}
	}
	channels := mapValues(snap.channels)
	sort.Slice(channels, func(i, j int) bool { return channels[i].ID < channels[j].ID })
	for _, channel := range channels {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := st.UpsertChannel(ctx, channel); err != nil {
			return recordImportFailure(ctx, st, store.FailureRef{
				Operation: "import_channel",
				Source:    "wiretap",
				GuildID:   channel.GuildID,
				ChannelID: channel.ID,
			}, err)
		}
		if err := resolveImportFailures(ctx, st, store.FailureRef{
			Operation: "import_channel",
			Source:    "wiretap",
			GuildID:   channel.GuildID,
			ChannelID: channel.ID,
		}); err != nil {
			return err
		}
	}
	messages := mapValues(snap.messages)
	sort.Slice(messages, func(i, j int) bool { return messages[i].Record.ID < messages[j].Record.ID })
	if err := st.UpsertMessages(ctx, messages); err != nil {
		return recordImportFailure(ctx, st, store.FailureRef{
			Operation: "import_messages",
			Source:    "wiretap",
		}, err)
	}
	messageIDs := make([]string, 0, len(messages))
	for _, message := range messages {
		messageIDs = append(messageIDs, message.Record.ID)
	}
	if err := resolveImportFailureIdentity(ctx, st, store.FailureRef{
		Operation: "import_messages",
		Source:    "wiretap",
	}); err != nil {
		return err
	}
	if err := resolveImportMessageFailures(ctx, st, messageIDs); err != nil {
		return err
	}
	if prune {
		if err := st.DeleteOrphanChannels(ctx, DirectMessageGuildID); err != nil {
			return err
		}
	}
	return st.SetSyncState(ctx, "wiretap:last_import", time.Now().UTC().Format(time.RFC3339Nano))
}

func recordImportFailure(ctx context.Context, st *store.Store, ref store.FailureRef, failure error) error {
	ledgerCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := st.RecordFailure(ledgerCtx, ref, failure); err != nil {
		return fmt.Errorf("%w (record failure ledger: %w)", failure, err)
	}
	return failure
}

func resolveImportFailures(ctx context.Context, st *store.Store, ref store.FailureRef) error {
	ledgerCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return st.ResolveFailures(ledgerCtx, ref)
}

func resolveImportFailureIdentity(ctx context.Context, st *store.Store, ref store.FailureRef) error {
	ledgerCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return st.ResolveFailureIdentity(ledgerCtx, ref)
}

func resolveImportMessageFailures(ctx context.Context, st *store.Store, messageIDs []string) error {
	ledgerCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return st.ResolveMessageFailures(ledgerCtx, store.FailureRef{
		Operation: "import_messages",
		Source:    "wiretap",
	}, messageIDs)
}

func reconcileMessages(snap snapshot, channelLookup map[string]store.ChannelRecord) {
	for id, msg := range snap.messages {
		channel, ok := channelLookup[msg.Record.ChannelID]
		if !ok {
			if guildID := snap.routes[msg.Record.ChannelID]; guildID != "" {
				msg.Record.GuildID = guildID
				if guildID == DirectMessageGuildID {
					channel = syntheticChannel(msg.Record.ChannelID, guildID, "")
					snap.channels[msg.Record.ChannelID] = channel
					channelLookup[msg.Record.ChannelID] = channel
					ok = true
				}
			}
		}
		if !ok {
			if msg.Record.GuildID != "" {
				for i := range msg.Attachments {
					msg.Attachments[i].GuildID = msg.Record.GuildID
				}
				for i := range msg.Mentions {
					msg.Mentions[i].GuildID = msg.Record.GuildID
				}
				msg.Record.RawJSON = withRawGuildID(msg.Record.RawJSON, msg.Record.GuildID)
				msg.PayloadJSON = withRawGuildID(msg.PayloadJSON, msg.Record.GuildID)
			}
			snap.messages[id] = msg
			continue
		}
		if channel.GuildID != "" {
			msg.Record.GuildID = channel.GuildID
			for i := range msg.Attachments {
				msg.Attachments[i].GuildID = channel.GuildID
			}
			for i := range msg.Mentions {
				msg.Mentions[i].GuildID = channel.GuildID
			}
		}
		if channel.Name != "" {
			msg.Record.ChannelName = channel.Name
		}
		msg.Record.RawJSON = withRawGuildID(msg.Record.RawJSON, msg.Record.GuildID)
		msg.PayloadJSON = withRawGuildID(msg.PayloadJSON, msg.Record.GuildID)
		snap.messages[id] = msg
	}
}

func withRawGuildID(rawJSON, guildID string) string {
	if rawJSON == "" || guildID == "" {
		return rawJSON
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(rawJSON), &raw); err != nil {
		return rawJSON
	}
	raw["guild_id"] = guildID
	body, err := json.Marshal(raw)
	if err != nil {
		return rawJSON
	}
	return string(body)
}
