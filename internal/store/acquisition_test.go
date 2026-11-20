package store

import "testing"

func TestAcquiredMessageRevisionKeepsEnrichmentAndRejectsEdits(t *testing.T) {
	retained := MessageRecord{ID: "1", GuildID: "2", ChannelID: "3", Content: "body", EditedAt: "2026-11-19T02:00:00.125Z", TextVersion: 2, TextPartsJSON: `[{"text":"retained attachment"}]`}
	incoming := retained
	incoming.EditedAt = "2026-11-19T10:00:00.125+08:00"
	incoming.TextVersion = 0
	incoming.TextPartsJSON = ""
	if err := CheckAcquiredMessageRevision(incoming, retained); err != nil {
		t.Fatal("same provider revision rejected", err)
	}
	incoming.EditedAt = "2026-11-19T02:00:00.126Z"
	if err := CheckAcquiredMessageRevision(incoming, retained); err == nil {
		t.Fatal("actual edit admitted without refresh protocol")
	}
	incoming.EditedAt = retained.EditedAt
	incoming.Content = "changed without timestamp"
	if err := CheckAcquiredMessageRevision(incoming, retained); err == nil {
		t.Fatal("unversioned changed content admitted")
	}
	incoming = retained
	incoming.ChannelID = "4"
	if err := CheckAcquiredMessageRevision(incoming, retained); err == nil {
		t.Fatal("different identity admitted")
	}
}
