package discordpackage

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

const (
	testAccount = "900719925474099301"
	testPeer    = "900719925474099302"
	testChannel = "900719925474099303"
	testMessage = "900719925474099304"
	testRows    = `[{"ID":900719925474099304,"Timestamp":"2026-01-02 03:04:05.123456+00:00","Contents":"exact message\nsecond line","Attachments":"https://cdn.discordapp.com/attachments/1/2/file.png"}]`
)

func fixtureFiles() map[string]string {
	return map[string]string{
		"account/user.json":                           `{"id":"` + testAccount + `","email":"not-imported@example.invalid","billing":"DO_NOT_IMPORT"}`,
		"messages/c" + testChannel + "/channel.json":  `{"id":"` + testChannel + `","type":1,"recipients":["` + testAccount + `","` + testPeer + `"]}`,
		"messages/c" + testChannel + "/messages.json": testRows,
	}
}

func writeFixture(t *testing.T, files map[string]string, zipped bool) string {
	t.Helper()
	root := t.TempDir()
	if !zipped {
		for name, body := range files {
			filename := filepath.Join(root, filepath.FromSlash(name))
			require.NoError(t, os.MkdirAll(filepath.Dir(filename), 0o700))
			require.NoError(t, os.WriteFile(filename, []byte(body), 0o600))
		}
		return root
	}
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, body := range files {
		f, err := w.Create("package/" + name)
		require.NoError(t, err)
		_, err = f.Write([]byte(body))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	filename := filepath.Join(root, "package.zip")
	require.NoError(t, os.WriteFile(filename, buf.Bytes(), 0o600))
	return filename
}

func TestImportPackageRoundTripAndRepeat(t *testing.T) {
	for _, zipped := range []bool{false, true} {
		t.Run(map[bool]string{false: "directory", true: "zip"}[zipped], func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			files := fixtureFiles()
			group := "900719925474099305"
			files["messages/"+group+"/channel.json"] = `{"id":"` + group + `","type":3,"recipients":["` + testPeer + `","900719925474099306"]}`
			files["messages/"+group+"/messages.json"] = `[]`
			files["messages/guild/channel.json"] = `{"type":0,"guild":{"id":"800719925474099300"}}`
			input := writeFixture(t, files, zipped)
			stats, err := Import(ctx, st, Options{Path: input})
			require.NoError(t, err)
			require.Equal(t, testAccount, stats.AccountID)
			require.Equal(t, 2, stats.Channels)
			require.Equal(t, 1, stats.SkippedChannels)
			require.Equal(t, 1, stats.Inserted)
			require.Equal(t, 1, stats.AttachmentLinks)
			require.Equal(t, "sent_only", stats.Coverage)
			require.False(t, stats.HistoryComplete)
			rows, err := st.ListMessages(ctx, store.MessageListOptions{GuildIDs: []string{store.DirectMessageGuildID}, Channel: testChannel})
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, testMessage, rows[0].MessageID)
			require.Equal(t, testAccount, rows[0].AuthorID)
			require.Equal(t, "exact message\nsecond line", rows[0].Content)
			var raw, event string
			require.NoError(t, st.DB().QueryRow(`select raw_json from messages where id=?`, testMessage).Scan(&raw))
			require.Contains(t, raw, "file.png")
			require.NotContains(t, raw, "DO_NOT_IMPORT")
			require.NoError(t, st.DB().QueryRow(`select payload_json from message_events where message_id=?`, testMessage).Scan(&event))
			require.Contains(t, event, "transcript_sha256")
			require.Contains(t, event, "sent_only")
			stats, err = Import(ctx, st, Options{Path: input})
			require.NoError(t, err)
			require.Zero(t, stats.Inserted)
			require.Equal(t, 1, stats.Existing)
			var count int
			require.NoError(t, st.DB().QueryRow(`select count(*) from message_events`).Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, st.DB().QueryRow(`select count(*) from embedding_jobs`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestImportPreservesExistingMessageAndTombstone(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{ID: testChannel, GuildID: "@me", Kind: "dm", Name: "cached name", Topic: "cached topic", RawJSON: `{"name":"cached name","topic":"cached topic"}`}))
	require.NoError(t, st.UpsertMessage(ctx, store.MessageRecord{ID: testMessage, GuildID: "@me", ChannelID: testChannel, AuthorID: testAccount, Content: "newer cached edit", NormalizedContent: "newer cached edit", RawJSON: `{"content":"newer cached edit"}`, CreatedAt: "2026-01-02T03:04:05Z", DeletedAt: "2026-02-01T00:00:00Z"}))
	stats, err := Import(ctx, st, Options{Path: writeFixture(t, fixtureFiles(), false)})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Existing)
	var content, deleted string
	require.NoError(t, st.DB().QueryRow(`select content,deleted_at from messages where id=?`, testMessage).Scan(&content, &deleted))
	require.Equal(t, "newer cached edit", content)
	require.NotEmpty(t, deleted)
	var name, topic string
	require.NoError(t, st.DB().QueryRow(`select name,topic from channels where id=?`, testChannel).Scan(&name, &topic))
	require.Equal(t, "cached name", name)
	require.Equal(t, "cached topic", topic)
}

func TestPackageIdentityConflictRollsBackChannel(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	require.NoError(t, st.UpsertMessage(ctx, store.MessageRecord{ID: testMessage, GuildID: "different", ChannelID: "elsewhere", AuthorID: testAccount, RawJSON: `{}`}))
	_, err = Import(ctx, st, Options{Path: writeFixture(t, fixtureFiles(), false)})
	require.ErrorContains(t, err, "conflicts with stored identity")
	var count int
	require.NoError(t, st.DB().QueryRow(`select count(*) from channels where id=?`, testChannel).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, st.DB().QueryRow(`select count(*) from message_events`).Scan(&count))
	require.Zero(t, count)
}

func TestPackageValidation(t *testing.T) {
	tests := []struct {
		name, filename, content, want string
	}{
		{"duplicate account", "account/user.json", `{"id":"` + testAccount + `","id":"` + testPeer + `"}`, "invalid package account JSON"},
		{"duplicate message ID", "messages/c" + testChannel + "/messages.json", `[{"ID":"` + testMessage + `","ID":"1"}]`, "duplicate JSON"},
		{"invalid timestamp", "messages/c" + testChannel + "/messages.json", `[{"ID":"` + testMessage + `","Contents":"secret","Timestamp":"bad"}]`, "invalid timestamp"},
		{"null messages", "messages/c" + testChannel + "/messages.json", `null`, "expected message array"},
		{"wrong recipients", "messages/c" + testChannel + "/channel.json", `{"id":"` + testChannel + `","type":1,"recipients":["` + testAccount + `"]}`, "recipient count"},
		{"wrong folder", "messages/c" + testChannel + "/channel.json", `{"id":"900719925474099399","type":1}`, "directory conflicts"},
		{"unknown channel", "messages/c" + testChannel + "/channel.json", `{"id":"` + testChannel + `"}`, "type is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files := fixtureFiles()
			files[tt.filename] = tt.content
			_, err := Import(context.Background(), nil, Options{Path: writeFixture(t, files, false), DryRun: true})
			require.ErrorContains(t, err, tt.want)
			require.NotContains(t, err.Error(), "secret")
		})
	}
	_, err := Import(context.Background(), nil, Options{Path: writeFixture(t, fixtureFiles(), false), AccountID: testPeer, DryRun: true})
	require.ErrorContains(t, err, "conflicts with package account")
}

func TestMessagesOnlyPackageRequiresExplicitIdentity(t *testing.T) {
	files := fixtureFiles()
	delete(files, "account/user.json")
	input := writeFixture(t, files, false)
	_, err := Import(context.Background(), nil, Options{Path: input, DryRun: true})
	require.ErrorContains(t, err, "--account-id")
	stats, err := Import(context.Background(), nil, Options{Path: input, AccountID: testAccount, DryRun: true})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)
	require.True(t, stats.DryRun)
}

func TestPackageRejectsUnsafeZIPAndSourceSymlink(t *testing.T) {
	input := writeFixture(t, map[string]string{"../escape": "bad"}, true)
	_, err := Import(context.Background(), nil, Options{Path: input, DryRun: true})
	require.ErrorContains(t, err, "unsafe")
	input = writeFixture(t, fixtureFiles(), false)
	account := filepath.Join(input, "account", "user.json")
	require.NoError(t, os.Remove(account))
	outside := filepath.Join(t.TempDir(), "user.json")
	require.NoError(t, os.WriteFile(outside, []byte(`{"id":"`+testAccount+`"}`), 0o600))
	if err := os.Symlink(outside, account); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	_, err = Import(context.Background(), nil, Options{Path: input, DryRun: true})
	require.Error(t, err)
}

func TestSnowflakesDoNotRoundThroughFloat(t *testing.T) {
	for _, input := range []string{testAccount, `"` + testAccount + `"`} {
		id, err := snowflake(json.RawMessage(input))
		require.NoError(t, err)
		require.Equal(t, testAccount, id)
	}
	for _, input := range []string{"9.007199254740993e17", "-1", "0", `"001"`, "18446744073709551616"} {
		_, err := snowflake(json.RawMessage(input))
		require.Error(t, err)
	}
}

func TestPackageCancellationAndLegacyFormat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Import(ctx, nil, Options{Path: writeFixture(t, fixtureFiles(), false), DryRun: true})
	require.ErrorIs(t, err, context.Canceled)
	files := fixtureFiles()
	delete(files, "messages/c"+testChannel+"/messages.json")
	files["messages/c"+testChannel+"/messages.csv"] = strings.ReplaceAll(testRows, "\n", " ")
	_, err = Import(context.Background(), nil, Options{Path: writeFixture(t, files, false), DryRun: true})
	require.ErrorContains(t, err, "JSON transcript required")
}
