package share

import (
	"path/filepath"
	"testing"

	"github.com/openclaw/crawlkit/snapshot"
	"github.com/stretchr/testify/require"
)

func TestPublicationAttachmentTextDiagnosticPreservesPreviousPublication(t *testing.T) {
	for _, column := range []string{
		"attachment_id", "message_id", "guild_id", "channel_id", "author_id",
		"filename", "content_type", "size", "url", "proxy_url", "text_content",
		"media_path", "content_sha256", "content_size", "fetched_at",
		"fetch_status", "fetch_error", "updated_at",
	} {
		t.Run(column, func(t *testing.T) {
			src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
			defer func() { _ = src.Close() }()
			require.NoError(t, addUncachedAttachment(t.Context(), src))
			opts := Options{RepoPath: filepath.Join(t.TempDir(), "publication"), Producer: fixtureProducer()}
			_, err := Export(t.Context(), src, opts)
			require.NoError(t, err)
			before := publicationBytes(t, opts.RepoPath)

			invalid := "payload-must-not-appear:\xff"
			_, err = src.DB().ExecContext(t.Context(), "update message_attachments set "+column+" = ?", invalid)
			require.NoError(t, err)
			result, err := Export(t.Context(), src, opts)
			require.EqualError(t, err, "filter table message_attachments: message_attachments."+column+" contains invalid UTF-8")
			require.Equal(t, Manifest{}, result)
			require.Equal(t, before, publicationBytes(t, opts.RepoPath))
			var retained []byte
			require.NoError(t, src.DB().QueryRowContext(t.Context(),
				"select cast("+column+" as blob) from message_attachments").Scan(&retained))
			require.Equal(t, []byte(invalid), retained)
		})
	}
}

func TestPublicationAttachmentBlobStillFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
		want string
	}{
		{"invalid-utf8", []byte("payload-not-for-errors:\xff"), "filter table message_attachments: message_attachments.text_content contains invalid UTF-8"},
		{"valid-utf8", []byte("ordinary blob bytes"), "encode table message_attachments: v1 snapshot cannot safely export BLOB column text_content; filter or explicitly replace its representation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
			defer func() { _ = src.Close() }()
			require.NoError(t, addUncachedAttachment(t.Context(), src))
			opts := Options{RepoPath: filepath.Join(t.TempDir(), "publication")}
			_, err := Export(t.Context(), src, opts)
			require.NoError(t, err)
			before := publicationBytes(t, opts.RepoPath)
			_, err = src.DB().ExecContext(t.Context(), `update message_attachments set text_content = ?`, tc.body)
			require.NoError(t, err)
			_, err = Export(t.Context(), src, opts)
			require.EqualError(t, err, tc.want)
			require.Equal(t, before, publicationBytes(t, opts.RepoPath))
			var body []byte
			var storageClass string
			require.NoError(t, src.DB().QueryRowContext(t.Context(),
				`select text_content, typeof(text_content) from message_attachments`).Scan(&body, &storageClass))
			require.Equal(t, tc.body, body)
			require.Equal(t, "blob", storageClass)
		})
	}
}

func TestPublicationAttachmentDiagnosticRunsAfterFiltering(t *testing.T) {
	for _, tc := range []struct {
		name   string
		dm     bool
		filter FilterOptions
	}{
		{"direct-message", true, FilterOptions{}},
		{"excluded-channel", false, FilterOptions{ExcludeChannelIDs: []string{"c1"}}},
		{"unselected-channel", false, FilterOptions{IncludeChannelIDs: []string{"other"}}},
		{"unknown-permissions", false, FilterOptions{PublicOnly: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
			defer func() { _ = src.Close() }()
			require.NoError(t, addUncachedAttachment(t.Context(), src))
			guild := "g1"
			if tc.dm {
				guild = directMessageGuildID
			}
			invalid := "excluded-payload-not-for-errors:\xff"
			_, err := src.DB().ExecContext(t.Context(),
				`update message_attachments set text_content = ?, guild_id = ?`, invalid, guild)
			require.NoError(t, err)
			result, err := Export(t.Context(), src, Options{
				RepoPath: filepath.Join(t.TempDir(), "publication"), Filter: tc.filter,
			})
			require.NoError(t, err)
			require.Zero(t, tableEntry(t, result, "message_attachments").Rows)
			var retained string
			require.NoError(t, src.DB().QueryRowContext(t.Context(),
				`select text_content from message_attachments`).Scan(&retained))
			require.Equal(t, invalid, retained)
		})
	}
}

func TestPublicationAttachmentDiagnosticUsesOnlyFixedSchemaNames(t *testing.T) {
	row := map[string]any{"untrusted-column-name": "payload-not-for-errors:\xff"}
	require.NoError(t, validateAttachmentSnapshotText(row))
	row["text_content"] = "payload-not-for-errors:\xff"
	require.EqualError(t, validateAttachmentSnapshotText(row), "message_attachments.text_content contains invalid UTF-8")
}

func TestPublicationTextOutsideAttachmentsKeepsSharedGuard(t *testing.T) {
	src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
	defer func() { _ = src.Close() }()
	_, err := src.DB().ExecContext(t.Context(), `update messages set content = ?`, "payload-not-for-errors:\xff")
	require.NoError(t, err)
	_, err = Export(t.Context(), src, Options{RepoPath: filepath.Join(t.TempDir(), "publication")})
	require.EqualError(t, err, "encode table messages: snapshot text is not valid UTF-8")
}

func TestSharedSnapshotStillRejectsInvalidAttachmentText(t *testing.T) {
	src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
	defer func() { _ = src.Close() }()
	require.NoError(t, addUncachedAttachment(t.Context(), src))
	_, err := src.DB().ExecContext(t.Context(), `update message_attachments set text_content = ?`, "payload-not-for-errors:\xff")
	require.NoError(t, err)
	_, err = snapshot.Export(t.Context(), snapshot.ExportOptions{
		DB: src.DB(), RootDir: t.TempDir(), Tables: []string{"message_attachments"},
	})
	require.EqualError(t, err, "encode table message_attachments: snapshot text is not valid UTF-8")
}
