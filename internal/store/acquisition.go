package store

import "errors"

// CheckAcquiredMessageRevision admits a creation-window reread without dropping
// the complete retained row. Refreshing actual edits is a different operation.
func CheckAcquiredMessageRevision(incoming, retained MessageRecord) error {
	if incoming.ID != retained.ID || incoming.GuildID != retained.GuildID || incoming.ChannelID != retained.ChannelID {
		return errors.New("retained message identity differs")
	}
	if retained.DeletedAt != "" {
		return nil
	}
	old, next := normalizeStoredTime(retained.EditedAt), normalizeStoredTime(incoming.EditedAt)
	if next > old {
		return errors.New("message edit requires the refresh protocol")
	}
	if next == old && incoming.Content != "" && incoming.Content != retained.Content {
		return errors.New("message content changed without a comparable revision")
	}
	return nil
}
