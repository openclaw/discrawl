package cli

import (
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

type unavailableMarkerCounts struct {
	Active     int
	Expired    int
	Unparsed   int
	OldestDays int
}

func countUnavailableMarkers(markers []store.SyncStateEntry, now time.Time) unavailableMarkerCounts {
	counts := unavailableMarkerCounts{}
	oldest := time.Time{}
	for _, marker := range markers {
		switch {
		case marker.UpdatedAt.IsZero():
			counts.Unparsed++
			continue
		case store.UnavailableMarkerActive(marker.UpdatedAt, now):
			counts.Active++
		default:
			counts.Expired++
		}
		if oldest.IsZero() || marker.UpdatedAt.Before(oldest) {
			oldest = marker.UpdatedAt
		}
	}
	if !oldest.IsZero() {
		counts.OldestDays = max(0, int(now.Sub(oldest).Hours()/24))
	}
	return counts
}
