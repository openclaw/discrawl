package share

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/snapshot"
	"github.com/openclaw/discrawl/internal/store"
)

const (
	LastCheckSyncScope          = "share:last_check_at"
	LastMergeSyncScope          = "share:last_merge_at"
	LastMergeManifestSyncScope  = "share:last_merge_manifest_generated_at"
	LastMergeManifestJSONScope  = "share:last_merge_manifest_json"
	PendingReplacementSyncScope = "share:pending_replacement"
)

var ErrReplacementRequired = errors.New("share snapshot replacement required")

type ReplacementRequiredError struct {
	Reason string
	Tables []string
}

func (e *ReplacementRequiredError) Error() string {
	detail := strings.TrimSpace(e.Reason)
	if len(e.Tables) > 0 {
		detail = "tables: " + strings.Join(e.Tables, ", ")
	}
	if detail == "" {
		return ErrReplacementRequired.Error()
	}
	return ErrReplacementRequired.Error() + " (" + detail + ")"
}

func (e *ReplacementRequiredError) Unwrap() error {
	return ErrReplacementRequired
}

// MergeIfChanged refreshes canonical snapshot rows without deleting local rows.
// Exact reconciliation remains separate so callers can require explicit consent.
func MergeIfChanged(ctx context.Context, s *store.Store, opts Options) (Manifest, bool, error) {
	manifest, err := ReadManifest(opts.RepoPath)
	if err != nil {
		return Manifest{}, false, err
	}
	manifest = enrichManifestFromGit(ctx, opts.RepoPath, "HEAD", manifest)
	previous, ok := PreviousMergedManifest(ctx, s, opts)
	allowEventMerge := false
	if !ok {
		previous = Manifest{Version: manifest.Version}
		allowEventMerge, err = eventTablesEmpty(ctx, s)
		if err != nil {
			return Manifest{}, false, err
		}
	}
	plan := snapshot.PlanMergeImport(snapshotManifest(previous), snapshotManifest(manifest))
	plan, err = shareMergePlan(plan, snapshotManifest(previous), allowEventMerge)
	if err != nil {
		if markErr := MarkReplacementPending(ctx, s, manifest, err.Error()); markErr != nil {
			return Manifest{}, false, errors.Join(err, markErr)
		}
		return Manifest{}, false, err
	}
	return importMergePlan(ctx, s, opts, previous, manifest, plan)
}

func shareMergePlan(plan snapshot.ImportPlan, previous snapshot.Manifest, allowEventMerge bool) (snapshot.ImportPlan, error) {
	if plan.Full {
		return snapshot.ImportPlan{}, &ReplacementRequiredError{Reason: plan.Reason}
	}
	out := snapshot.ImportPlan{Tables: make([]snapshot.TableImportPlan, 0, len(plan.Tables))}
	var replacements []string
	for _, tablePlan := range plan.Tables {
		switch tablePlan.Table.Name {
		case "message_events", "mention_events":
			if allowEventMerge && tablePlan.Mode == snapshot.TableImportFiles {
				out.Tables = append(out.Tables, tablePlan)
				continue
			}
			// Event shards have generated IDs, so replay would duplicate history.
			// Only an explicitly forced exact import may replace these tables.
			tablePlan.Mode = snapshot.TableImportSkip
			tablePlan.Files = nil
			tablePlan.Reason = "force-only snapshot table"
			out.Tables = append(out.Tables, tablePlan)
			continue
		case "sync_state":
			tablePlan.Mode = snapshot.TableImportSkip
			tablePlan.Files = nil
			tablePlan.Reason = "local ownership"
			out.Tables = append(out.Tables, tablePlan)
			continue
		case "guilds", "channels", "members", "messages", "message_attachments":
		default:
			replacements = append(replacements, tablePlan.Table.Name)
			continue
		}
		if tablePlan.Mode == snapshot.TableImportReplace {
			if (tablePlan.Table.Name == "guilds" || tablePlan.Table.Name == "members") &&
				tablePlan.Reason == "columns changed" &&
				isTombstoneColumnAddition(manifestTable(previous, tablePlan.Table.Name).Columns, tablePlan.Table.Columns) {
				tablePlan.Mode = snapshot.TableImportFiles
				tablePlan.Reason = "merge tombstone-aware entity rows"
				out.Tables = append(out.Tables, tablePlan)
				continue
			}
			replacements = append(replacements, tablePlan.Table.Name)
			continue
		}
		out.Tables = append(out.Tables, tablePlan)
	}
	if len(replacements) > 0 {
		sort.Strings(replacements)
		return snapshot.ImportPlan{}, &ReplacementRequiredError{Tables: replacements}
	}
	return out, nil
}

func manifestTable(manifest snapshot.Manifest, name string) snapshot.TableManifest {
	for _, table := range manifest.Tables {
		if table.Name == name {
			return table
		}
	}
	return snapshot.TableManifest{}
}

func isTombstoneColumnAddition(previous, current []string) bool {
	if len(current) != len(previous)+3 {
		return false
	}
	base := make([]string, 0, len(previous))
	tombstones := map[string]bool{
		"deleted_at":      false,
		"deletion_source": false,
		"deletion_reason": false,
	}
	for _, column := range current {
		if _, ok := tombstones[column]; ok {
			if tombstones[column] {
				return false
			}
			tombstones[column] = true
			continue
		}
		base = append(base, column)
	}
	for _, found := range tombstones {
		if !found {
			return false
		}
	}
	return slices.Equal(previous, base)
}

func eventTablesEmpty(ctx context.Context, s *store.Store) (bool, error) {
	var rows int
	if err := s.DB().QueryRowContext(ctx, `select (select count(*) from message_events) + (select count(*) from mention_events)`).Scan(&rows); err != nil {
		return false, fmt.Errorf("count local share events: %w", err)
	}
	return rows == 0, nil
}

func mergePlanSearchRebuilds(plan snapshot.ImportPlan) (bool, bool) {
	for _, tablePlan := range plan.Tables {
		if tablePlan.Mode != snapshot.TableImportSkip && tablePlan.Table.Name == "members" {
			return false, true
		}
	}
	return false, false
}

func importMergeSnapshotRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any) error {
	channelID := ""
	previousName := ""
	if table == "channels" {
		channelID = stringValue(row["id"])
		if channelID != "" {
			err := tx.QueryRowContext(ctx, `select coalesce(name, '') from channels where id = ?`, channelID).Scan(&previousName)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("read message search channel %s: %w", channelID, err)
			}
		}
	}
	applied, err := upsertMergeSnapshotRow(ctx, tx, table, row)
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}
	if table == "messages" {
		messageID := stringValue(row["id"])
		if messageID != "" {
			return upsertMessageFTSRow(ctx, tx, messageID)
		}
		return nil
	}
	currentName := stringValue(row["name"])
	if channelID == "" || previousName == currentName {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `update message_fts set channel_name = ? where channel_id = ?`, currentName, channelID); err != nil {
		return fmt.Errorf("refresh message search channel %s: %w", channelID, err)
	}
	return nil
}

func ManifestAlreadyMerged(ctx context.Context, s *store.Store, manifest Manifest) bool {
	if manifest.GeneratedAt.IsZero() {
		return false
	}
	last, err := s.GetSyncState(ctx, LastMergeManifestSyncScope)
	if err != nil || strings.TrimSpace(last) == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339Nano, last)
	return err == nil && t.Equal(manifest.GeneratedAt)
}

func PreviousMergedManifest(ctx context.Context, s *store.Store, opts Options) (Manifest, bool) {
	body, err := s.GetSyncState(ctx, LastMergeManifestJSONScope)
	if err == nil && strings.TrimSpace(body) != "" {
		var manifest Manifest
		if json.Unmarshal([]byte(body), &manifest) == nil && !manifest.GeneratedAt.IsZero() {
			return manifest, true
		}
	}
	return PreviousImportedManifest(ctx, s, opts)
}

func MarkChecked(ctx context.Context, s *store.Store) error {
	return s.SetSyncState(ctx, LastCheckSyncScope, time.Now().UTC().Format(time.RFC3339Nano))
}

func MarkMerged(ctx context.Context, s *store.Store, manifest Manifest) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := s.SetSyncState(ctx, LastCheckSyncScope, now); err != nil {
		return err
	}
	if err := s.SetSyncState(ctx, LastMergeSyncScope, now); err != nil {
		return err
	}
	if !manifest.GeneratedAt.IsZero() {
		if err := s.SetSyncState(ctx, LastMergeManifestSyncScope, manifest.GeneratedAt.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		body, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("marshal merged manifest state: %w", err)
		}
		if err := s.SetSyncState(ctx, LastMergeManifestJSONScope, string(body)); err != nil {
			return err
		}
	}
	return s.SetSyncState(ctx, PendingReplacementSyncScope, "")
}

func MarkReplacementPending(ctx context.Context, s *store.Store, manifest Manifest, reason string) error {
	if err := MarkChecked(ctx, s); err != nil {
		return err
	}
	value := strings.TrimSpace(reason)
	if !manifest.GeneratedAt.IsZero() {
		value = manifest.GeneratedAt.Format(time.RFC3339Nano) + " " + value
	}
	return s.SetSyncState(ctx, PendingReplacementSyncScope, strings.TrimSpace(value))
}

func HasPendingReplacement(ctx context.Context, s *store.Store) bool {
	value, err := s.GetSyncState(ctx, PendingReplacementSyncScope)
	return err == nil && strings.TrimSpace(value) != ""
}
