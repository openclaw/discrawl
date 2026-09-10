package share

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/snapshot"
	"github.com/openclaw/discrawl/internal/report"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPublicationTablePathForms(t *testing.T) {
	generation := "0123456789abcdef0123456789abcdef"
	prefix := "tables/.generations/" + generation + "/messages/"
	tests := []struct {
		name string
		ok   bool
	}{
		{"tables/messages.jsonl", true},
		{"tables/messages.jsonl.gz", true},
		{"tables/messages/000001.jsonl.gz", true},
		{"tables/messages/1000000.jsonl.gz", true},
		{prefix + "000001.jsonl.gz", true},
		{prefix + "1000000.jsonl.gz", true},
		{strings.Replace(prefix, generation, strings.ToUpper(generation), 1) + "000001.jsonl.gz", false},
		{strings.Replace(prefix, generation, generation[:31], 1) + "000001.jsonl.gz", false},
		{strings.Replace(prefix, generation, generation+"0", 1) + "000001.jsonl.gz", false},
		{strings.Replace(prefix, generation, strings.Repeat("g", 32), 1) + "000001.jsonl.gz", false},
		{strings.Replace(prefix, "/messages/", "/members/", 1) + "000001.jsonl.gz", false},
		{strings.Replace(prefix, ".generations", "generations", 1) + "000001.jsonl.gz", false},
		{prefix + "00001.jsonl.gz", false},
		{prefix + "-00001.jsonl.gz", false},
		{prefix + "000001.jsonl", false},
		{prefix + "private.txt", false},
		{prefix + "extra/000001.jsonl.gz", false},
		{prefix + "../messages/000001.jsonl.gz", false},
		{prefix + "./000001.jsonl.gz", false},
		{prefix + "000001.jsonl.gz\n", false},
		{strings.Replace(prefix, "/messages/", "//messages/", 1) + "000001.jsonl.gz", false},
		{strings.Replace(prefix, "/messages/", "/.git/", 1) + "000001.jsonl.gz", false},
		{"/" + prefix + "000001.jsonl.gz", false},
		{strings.ReplaceAll(prefix, "/", "\\") + "000001.jsonl.gz", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Every legacy and current manifest field has the same ownership rule.
			for _, table := range []TableManifest{
				{Name: "messages", File: test.name},
				{Name: "messages", Files: []string{test.name}},
				{Name: "messages", FileManifests: []snapshot.FileManifest{{Path: test.name}}},
			} {
				files, err := publicationPaths(Manifest{Tables: []TableManifest{table}})
				if !test.ok {
					require.ErrorContains(t, err, "unowned publication path")
					continue
				}
				require.NoError(t, err)
				require.Equal(t, map[string]bool{ManifestName: true, test.name: true}, files)
			}
		})
	}
	_, err := publicationPaths(Manifest{Embeddings: []EmbeddingManifest{{
		Provider: "openai", Model: "fixture", InputVersion: "v1",
		Files: []string{prefix + "000001.jsonl.gz"},
	}}})
	require.ErrorContains(t, err, "unowned publication path")
}

func TestPublicationGenerationFilesRoundTripAndCleanup(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "archive.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{RepoPath: repo, Branch: "main"}
	previous, err := Export(ctx, src, opts)
	require.NoError(t, err)
	configureGitUser(t, repo)
	_, err = Commit(ctx, opts, "test: initial")
	require.NoError(t, err)

	generations := []string{strings.Repeat("a", 32), strings.Repeat("b", 32)}
	unlisted := "tables/.generations/" + generations[0] + "/messages/999999.jsonl.gz"
	require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(repo, unlisted)), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(repo, unlisted), []byte("staged note\n"), 0o600))
	testGitRun(t, ctx, repo, "add", "--", unlisted)
	require.NoError(t, os.WriteFile(filepath.Join(repo, unlisted), []byte("unstaged note\n"), 0o600))
	indexBefore := testGitOutput(t, ctx, repo, "diff", "--cached", "--binary", "--", unlisted)

	for _, generation := range generations {
		stage := filepath.Join(t.TempDir(), "stage")
		current, err := Export(ctx, src, Options{RepoPath: stage, Branch: "main"})
		require.NoError(t, err)
		// Relocate real exported bytes to exercise consumers, not a future producer.
		for i := range current.Tables {
			table := &current.Tables[i]
			if table.Name != "messages" {
				continue
			}
			require.NotEmpty(t, table.Files)
			for j, old := range table.Files {
				name := "tables/.generations/" + generation + "/messages/" + filepath.Base(old)
				require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(stage, name)), 0o700))
				require.NoError(t, os.Rename(filepath.Join(stage, old), filepath.Join(stage, name)))
				table.Files[j] = name
				if table.File == old {
					table.File = name
				}
				for k := range table.FileManifests {
					if table.FileManifests[k].Path == old {
						table.FileManifests[k].Path = name
					}
				}
				if hash, ok := current.Files[old]; ok {
					current.Files[name] = hash
					delete(current.Files, old)
				}
			}
		}
		writeShareManifest(t, stage, current)
		owned, err := previousPublicationPaths(ctx, repo, false)
		require.NoError(t, err)
		installed, err := installPublication(ctx, repo, stage, owned, current, nil)
		require.NoError(t, err)
		require.True(t, installed)
		for _, old := range tableEntry(t, previous, "messages").Files {
			require.NoFileExists(t, filepath.Join(repo, old))
			testGitRun(t, ctx, repo, "add", "-u", "--", old)
		}
		committed, err := Commit(ctx, opts, "test: generation")
		require.NoError(t, err)
		require.True(t, committed)
		tree := strings.Split(strings.TrimSpace(testGitOutput(t, ctx, repo, "ls-tree", "-r", "--name-only", "HEAD")), "\n")
		for _, name := range tableEntry(t, current, "messages").Files {
			require.Contains(t, tree, name)
		}
		for _, old := range tableEntry(t, previous, "messages").Files {
			require.NotContains(t, tree, old)
		}
		require.NotContains(t, tree, unlisted)
		require.Equal(t, indexBefore, testGitOutput(t, ctx, repo, "diff", "--cached", "--binary", "--", unlisted))
		body, err := os.ReadFile(filepath.Join(repo, unlisted))
		require.NoError(t, err)
		require.Equal(t, "unstaged note\n", string(body))

		dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "import.db"))
		require.NoError(t, err)
		_, err = Import(ctx, dst, opts)
		require.NoError(t, err)
		var content string
		require.NoError(t, dst.DB().QueryRowContext(ctx, `select content from messages where id = 'm1'`).Scan(&content))
		require.Equal(t, "launch checklist ready", content)
		require.NoError(t, dst.Close())
		previous = current
	}
	// Actual export must accept a generated prior manifest with the released pin.
	_, err = Export(ctx, src, opts)
	require.NoError(t, err)
	for _, old := range tableEntry(t, previous, "messages").Files {
		require.NoFileExists(t, filepath.Join(repo, old))
	}
	_, err = Commit(ctx, opts, "test: export after generation")
	require.NoError(t, err)
	require.Equal(t, indexBefore, testGitOutput(t, ctx, repo, "diff", "--cached", "--binary", "--", unlisted))
	require.FileExists(t, filepath.Join(repo, unlisted))
}

func TestPublicationOwnsExactFilesAndPreservesStagedWork(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "archive.db"))
	defer func() { _ = src.Close() }()
	blob, err := store.EncodeEmbeddingVector([]float32{1, 0})
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `insert into message_embeddings
		(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at)
		values ('m1','openai','fixture',?,2,?,?)`,
		store.EmbeddingInputVersion, blob, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{
		RepoPath: repo, Branch: "main", IncludeEmbeddings: true,
		EmbeddingProvider: "openai", EmbeddingModel: "fixture", EmbeddingInputVersion: store.EmbeddingInputVersion,
	}
	first, err := Export(ctx, src, opts)
	require.NoError(t, err)
	configureGitUser(t, repo)
	committed, err := Commit(ctx, opts, "test: initial")
	require.NoError(t, err)
	require.True(t, committed)
	priorEmbedding := first.Embeddings[0].Files[0]

	unrelated := []string{"private.txt", "tables/notes.txt", "media/notes.txt", "embeddings/notes.txt", "reports/report1.md"}
	for _, name := range unrelated {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(repo, name)), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte("staged private note\n"), 0o600))
		testGitRun(t, ctx, repo, "add", "--", name)
		require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte("unstaged private note\n"), 0o600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(repo, "untracked.txt"), []byte("untracked\n"), 0o600))
	indexBefore := testGitOutput(t, ctx, repo, "diff", "--cached", "--binary", "--", unrelated[0], unrelated[1], unrelated[2], unrelated[3], unrelated[4])
	// A generated report's brackets and wildcard are literal, not Git patterns.
	opts.ReadmePath = "reports/report[1]*.md"
	require.NoError(t, os.WriteFile(filepath.Join(repo, opts.ReadmePath), []byte("generated\n"), 0o600))
	opts.IncludeEmbeddings = false
	_, err = Export(ctx, src, opts)
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(repo, priorEmbedding))
	// An already-staged managed deletion must remain part of our commit.
	testGitRun(t, ctx, repo, "add", "-u", "--", priorEmbedding)
	committed, err = Commit(ctx, opts, "test: no vectors")
	require.NoError(t, err)
	require.True(t, committed)
	tree := testGitOutput(t, ctx, repo, "ls-tree", "-r", "--name-only", "HEAD")
	require.NotContains(t, tree, priorEmbedding)
	require.Contains(t, tree, opts.ReadmePath)
	for _, name := range unrelated {
		require.NotContains(t, strings.Split(strings.TrimSpace(tree), "\n"), name)
		body, err := os.ReadFile(filepath.Join(repo, name))
		require.NoError(t, err)
		require.Equal(t, "unstaged private note\n", string(body))
	}
	require.Equal(t, indexBefore, testGitOutput(t, ctx, repo, "diff", "--cached", "--binary", "--", unrelated[0], unrelated[1], unrelated[2], unrelated[3], unrelated[4]))
	require.FileExists(t, filepath.Join(repo, "untracked.txt"))
	require.NoError(t, os.Remove(filepath.Join(repo, opts.ReadmePath)))
	committed, err = Commit(ctx, opts, "test: remove generated report")
	require.NoError(t, err)
	require.True(t, committed)
	require.NotContains(t, testGitOutput(t, ctx, repo, "ls-tree", "-r", "--name-only", "HEAD"), opts.ReadmePath)
}

func TestFilteredPublicationOwnsOnlyFieldNotesDeletions(t *testing.T) {
	ctx := t.Context()
	s := seedStore(t, filepath.Join(t.TempDir(), "fixture.db"))
	t.Cleanup(func() { _ = s.Close() })
	repo := t.TempDir()
	opts := Options{RepoPath: repo, Branch: "main"}
	_, err := Export(ctx, s, opts)
	require.NoError(t, err)
	configureGitUser(t, repo)
	_, err = Commit(ctx, opts, "test: initial")
	require.NoError(t, err)
	require.NoError(t, os.Mkdir(filepath.Join(repo, "reports"), 0o700))
	for _, name := range []string{report.FieldNotesMarkdownPath, report.FieldNotesJSONPath, "AGENTS.md", "reports/manual.md"} {
		require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte("fixture\n"), 0o600))
	}
	testGitRun(t, ctx, repo, "-c", "commit.gpgsign=false", "add", ".")
	testGitRun(t, ctx, repo, "-c", "commit.gpgsign=false", "commit", "-m", "test: notes and docs")
	opts.Filter.IncludeChannelIDs = []string{"c1"}
	_, err = Commit(ctx, opts, "test: must refuse broad notes")
	require.ErrorContains(t, err, "must remove broader-scope field notes")
	require.NoError(t, report.RemoveFieldNotes(repo))
	testGitRun(t, ctx, repo, "add", "-u", "--", report.FieldNotesMarkdownPath)
	require.NoError(t, os.WriteFile(filepath.Join(repo, "AGENTS.md"), []byte("staged maintainer edit\n"), 0o600))
	testGitRun(t, ctx, repo, "add", "AGENTS.md")
	changed, err := Commit(ctx, opts, "test: remove only generated notes")
	require.NoError(t, err)
	require.True(t, changed)
	files := strings.Fields(testGitOutput(t, ctx, repo, "ls-tree", "-r", "--name-only", "HEAD"))
	require.NotContains(t, files, report.FieldNotesMarkdownPath)
	require.NotContains(t, files, report.FieldNotesJSONPath)
	require.Contains(t, files, "reports/manual.md")
	require.Equal(t, "fixture\n", testGitOutput(t, ctx, repo, "show", "HEAD:AGENTS.md"))
	require.Contains(t, testGitOutput(t, ctx, repo, "diff", "--cached", "--name-only"), "AGENTS.md")
}

func TestPublicationRejectsUnownedPreviousPaths(t *testing.T) {
	for _, name := range []string{"../private.txt", "tables/messages/private.txt", "tables/messages/../private.txt", "media/../../private.txt", "media/.git/config"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			src := seedStore(t, filepath.Join(t.TempDir(), "archive.db"))
			defer func() { _ = src.Close() }()
			repo := filepath.Join(t.TempDir(), "share")
			opts := Options{RepoPath: repo, Branch: "main"}
			manifest, err := Export(ctx, src, opts)
			require.NoError(t, err)
			manifest.Tables[0].Files = []string{name}
			body, err := json.Marshal(manifest)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(repo, ManifestName), body, 0o600))
			_, err = Export(ctx, src, opts)
			require.ErrorContains(t, err, "unowned publication path")
			after, err := os.ReadFile(filepath.Join(repo, ManifestName))
			require.NoError(t, err)
			require.Equal(t, body, after)
		})
	}
}

func TestPublicationRejectsSymlinkedDestination(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "archive.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{RepoPath: repo, Branch: "main"}
	require.NoError(t, EnsureRepo(ctx, opts))
	outside := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(repo, "tables")))
	_, err := Export(ctx, src, opts)
	require.ErrorContains(t, err, "symlinked")
	files, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Empty(t, files)
}
