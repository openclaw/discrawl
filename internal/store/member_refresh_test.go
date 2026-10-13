package store

import (
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestObservedMemberRefreshBatchesAndValidatesBeforeWriting(t *testing.T) {
	s, err := Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	started := time.Now()
	members := make([]MemberRecord, memberRefreshBatchSize+1)
	for i := range members {
		members[i] = MemberRecord{GuildID: "g", UserID: strconv.Itoa(i), Username: "fixture", RoleIDsJSON: `[]`, RawJSON: `{}`}
	}
	invalid := append([]MemberRecord(nil), members...)
	invalid[len(invalid)-1].GuildID = "other"
	_, err = s.MergeObservedMembers(t.Context(), "g", invalid, started)
	require.Error(t, err)
	var count int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from members`).Scan(&count))
	require.Zero(t, count)
	_, err = s.MergeObservedMembers(t.Context(), "g", append(slices.Clone(members), members[0]), started)
	require.ErrorContains(t, err, "duplicate")
	result, err := s.MergeObservedMembers(t.Context(), "g", members, started)
	require.NoError(t, err)
	require.Equal(t, len(members), result.Applied)
	require.Zero(t, result.NewerSkipped)
	_, err = s.MergeObservedMembers(t.Context(), "g", members, time.Time{})
	require.Error(t, err)
}
