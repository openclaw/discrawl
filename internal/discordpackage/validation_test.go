package discordpackage

import (
	"archive/zip"
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPackageInputFailures(t *testing.T) {
	_, err := Import(t.Context(), nil, Options{})
	require.ErrorContains(t, err, "store is required")
	_, err = Import(t.Context(), nil, Options{Path: filepath.Join(t.TempDir(), "missing"), DryRun: true})
	require.ErrorIs(t, err, fs.ErrNotExist)
	input := filepath.Join(t.TempDir(), "not-a-zip")
	require.NoError(t, os.WriteFile(input, []byte("not a ZIP"), 0o600))
	_, err = Import(t.Context(), nil, Options{Path: input, DryRun: true})
	require.ErrorContains(t, err, "open Discord package ZIP")
	_, err = Import(t.Context(), nil, Options{Path: t.TempDir(), DryRun: true})
	require.ErrorContains(t, err, "messages directory not found")
	_, err = Import(t.Context(), nil, Options{Path: writeFixture(t, fixtureFiles(), false), AccountID: "ambiguous", DryRun: true})
	require.ErrorContains(t, err, "invalid --account-id")
}

func TestPackageRejectsAmbiguousDirectories(t *testing.T) {
	for _, files := range []fstest.MapFS{
		{"messages/file": {}, "Messages/file": {}},
		{"messages": {Data: []byte("not a directory")}},
		{"one/messages/file": {}, "two/messages/file": {}},
		{"wrapper/messages/file": {}, "wrapper/Messages/file": {}},
	} {
		_, _, err := packageDirectories(files)
		require.Error(t, err)
	}
	_, _, err := packageDirectories(fstest.MapFS{})
	require.ErrorContains(t, err, "messages directory not found")
	_, err = childDirectory(fstest.MapFS{}, "missing", "messages")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestPackageRejectsMalformedChannelMetadata(t *testing.T) {
	for _, data := range []string{
		`not-json`,
		`{"id":"bad","type":1}`,
		`{"id":"` + testChannel + `","type":1,"guild":{"id":"1"}}`,
		`{"id":"` + testChannel + `","type":1,"guild_id":"1"}`,
		`{"id":"` + testChannel + `","type":1,"recipients":["invalid"]}`,
		`{"id":"` + testChannel + `","type":1,"recipients":null}`,
	} {
		files := fstest.MapFS{testChannel + "/channel.json": {Data: []byte(data)}}
		_, _, err := readChannel(files, testChannel, testAccount)
		require.Error(t, err)
	}
	_, _, err := readChannel(fstest.MapFS{}, testChannel, testAccount)
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestPackageRejectsMalformedTranscript(t *testing.T) {
	for _, data := range []string{
		``, `{}`, `[`, `[null]`,
		`[{"ID":"bad","Timestamp":"2026-01-01T00:00:00Z","Contents":""}]`,
		`[{"ID":"1","Timestamp":"2026-01-01T00:00:00Z","Contents":null}]`,
		`[{"ID":"1","Timestamp":"2026-01-01T00:00:00Z","Contents":"","Attachments":[]}]`,
		`[{"ID":"1","Timestamp":"2026-01-01T00:00:00Z","Contents":""},{"ID":"1","Timestamp":"2026-01-01T00:00:00Z","Contents":""}]`,
	} {
		_, _, err := readMessages(t.Context(), []byte(data), testChannel, testAccount)
		require.Error(t, err)
	}
	for _, data := range []string{``, `[]`, `{`, `{"id":`, `{"id":1,}`, `{} {}`} {
		_, err := objectFields([]byte(data))
		require.Error(t, err)
	}
	_, err := snowflake(json.RawMessage(`"unterminated`))
	require.Error(t, err)
}

func TestPackageRejectsDuplicateAndSymlinkZIPEntries(t *testing.T) {
	for _, mode := range []string{"duplicate", "symlink", "backslash", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "package.zip")
			f, err := os.Create(filename)
			require.NoError(t, err)
			w := zip.NewWriter(f)
			header := &zip.FileHeader{Name: "messages/file", Method: zip.Store}
			switch mode {
			case "symlink":
				header.SetMode(fs.ModeSymlink | 0o600)
			case "backslash":
				header.Name = `messages\file`
			case "oversized":
				header.UncompressedSize64 = maxFileBytes + 1
			}
			_, err = w.CreateRaw(header)
			require.NoError(t, err)
			if mode == "duplicate" {
				_, err = w.Create("messages/file")
				require.NoError(t, err)
			}
			require.NoError(t, w.Close())
			require.NoError(t, f.Close())
			_, err = Import(t.Context(), nil, Options{Path: filename, DryRun: true})
			require.ErrorContains(t, err, "unsafe, duplicate or oversized")
		})
	}
}

func TestPackageReportsCommittedProgressBeforeLaterFailure(t *testing.T) {
	files := fixtureFiles()
	later := "900719925474099399"
	files["messages/"+later+"/channel.json"] = `{"id":"` + later + `","type":1,"recipients":["` + testPeer + `"]}`
	files["messages/"+later+"/messages.json"] = `[invalid]`
	st, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	stats, err := Import(t.Context(), st, Options{Path: writeFixture(t, files, false)})
	require.ErrorContains(t, err, "invalid message array")
	// Directory iteration sorts IDs: numeric names precede c-prefixed names.
	require.Zero(t, stats.Inserted)
	delete(files, "messages/"+later+"/channel.json")
	delete(files, "messages/"+later+"/messages.json")
	files["messages/c"+later+"/channel.json"] = `{"id":"` + later + `","type":1,"recipients":["` + testPeer + `"]}`
	files["messages/c"+later+"/messages.json"] = `[invalid]`
	stats, err = Import(context.Background(), st, Options{Path: writeFixture(t, files, false)})
	require.Error(t, err)
	require.Equal(t, 1, stats.Inserted)
	var count int
	require.NoError(t, st.DB().QueryRow(`select count(*) from messages`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestPackageDoesNotTreatMissingAccountRecordAsEmptyIdentity(t *testing.T) {
	files := fstest.MapFS{"account/user.json": {Data: []byte(`{"id":"bad"}`)}}
	_, err := accountID(files, ".", "")
	require.ErrorContains(t, err, "invalid package account ID")
	delete(files, "account/user.json")
	files["account/other.json"] = &fstest.MapFile{Data: []byte(`{}`)}
	_, err = accountID(files, ".", testAccount)
	require.ErrorIs(t, err, fs.ErrNotExist)
	_, err = readFile(fstest.MapFS{"directory/file": {Data: []byte(strings.Repeat("x", 2))}}, "directory")
	require.ErrorContains(t, err, "not regular")
}
