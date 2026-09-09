package share

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func fixtureProducer() *PublicationProducer {
	return &PublicationProducer{Repository: "openclaw/discrawl", Revision: strings.Repeat("a", 40), RunID: 123, RunAttempt: 1}
}

func TestPublicationProducerEnvironment(t *testing.T) {
	valid := map[string]string{
		"DISCRAWL_PRODUCER_REPOSITORY":  "openclaw/discrawl",
		"DISCRAWL_PRODUCER_REVISION":    strings.Repeat("a", 40),
		"DISCRAWL_PRODUCER_RUN_ID":      "123",
		"DISCRAWL_PRODUCER_RUN_ATTEMPT": "1",
	}
	lookup := func(values map[string]string) func(string) (string, bool) {
		return func(name string) (string, bool) { value, ok := values[name]; return value, ok }
	}
	producer, err := PublicationProducerFromEnv(lookup(nil))
	require.NoError(t, err)
	require.Nil(t, producer)
	producer, err = PublicationProducerFromEnv(lookup(valid))
	require.NoError(t, err)
	require.Equal(t, fixtureProducer(), producer)
	for key := range valid {
		for _, bad := range []string{"", "0", "-1", "+1", "01", " 123", "private.invalid", strings.Repeat("x", 129)} {
			values := maps.Clone(valid)
			values[key] = bad
			_, err := PublicationProducerFromEnv(lookup(values))
			require.Error(t, err, "%s=%q", key, bad)
			require.NotContains(t, err.Error(), "private.invalid")
		}
		values := maps.Clone(valid)
		delete(values, key)
		_, err := PublicationProducerFromEnv(lookup(values))
		require.Error(t, err)
	}
}

func TestPublicationReceiptExactBinding(t *testing.T) {
	manifest := []byte("{\"version\":1}\n")
	body, err := encodePublicationReceipt(manifest, *fixtureProducer())
	require.NoError(t, err)
	producer, err := validatePublicationReceipt(manifest, body)
	require.NoError(t, err)
	require.Equal(t, *fixtureProducer(), producer)
	for _, changed := range [][]byte{bytes.TrimSuffix(manifest, []byte("\n")), append(bytes.Clone(manifest), ' '), []byte(`{"version":2}`)} {
		_, err := validatePublicationReceipt(changed, body)
		require.ErrorContains(t, err, "exact manifest bytes")
	}
	for _, bad := range [][]byte{
		bytes.Replace(body, []byte(`"schema": 1`), []byte(`"schema": 2`), 1),
		bytes.Replace(body, []byte(`"manifest.json"`), []byte(`"../manifest.json"`), 1),
		bytes.Replace(body, []byte(`"schema": 1`), []byte(`"private_host": "private.invalid", "schema": 1`), 1),
		append(bytes.Clone(body), []byte(`{}`)...),
		[]byte(strings.Repeat(" ", 4097)),
	} {
		_, err := validatePublicationReceipt(manifest, bad)
		require.Error(t, err)
	}
	var fields map[string]any
	require.NoError(t, json.Unmarshal(body, &fields))
	require.Len(t, fields, 4)
	require.Len(t, fields["producer"], 4)
}

func TestPublicationReceiptLifecycleAndRollback(t *testing.T) {
	src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{RepoPath: repo, Producer: fixtureProducer()}
	_, err := Export(t.Context(), src, opts)
	require.NoError(t, err)
	configureGitUser(t, repo)
	_, err = Commit(t.Context(), opts, "test: producer")
	require.NoError(t, err)
	head := testGitOutput(t, t.Context(), repo, "rev-parse", "HEAD")
	before := publicationBytes(t, repo)
	require.NoError(t, os.WriteFile(filepath.Join(repo, "private.txt"), []byte("private fixture"), 0o600))
	testGitRun(t, t.Context(), repo, "add", "private.txt")
	index := testGitOutput(t, t.Context(), repo, "diff", "--cached", "--binary")
	for _, phase := range []string{"prepare", "install", "manifest"} {
		fault := errors.New("receipt fixture failure")
		_, err := exportPublication(t.Context(), src, opts, func(at, name string) error {
			if at == phase && (name == producerName || at == "manifest") {
				return fault
			}
			return nil
		})
		require.ErrorIs(t, err, fault)
		after := publicationBytes(t, repo)
		for name, want := range before {
			require.Equal(t, want, after[name], name)
		}
	}
	require.Equal(t, head, testGitOutput(t, t.Context(), repo, "rev-parse", "HEAD"))
	_, err = Export(t.Context(), src, opts)
	require.NoError(t, err)
	_, _, err = workingPublicationBinding(repo, opts.Producer)
	require.NoError(t, err)
	_, err = Commit(t.Context(), opts, "test: next producer")
	require.NoError(t, err)
	require.Equal(t, index, testGitOutput(t, t.Context(), repo, "diff", "--cached", "--binary"))
	require.Contains(t, testGitOutput(t, t.Context(), repo, "ls-tree", "--name-only", "HEAD"), producerName)
	opts.Producer = nil
	_, err = Export(t.Context(), src, opts)
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(repo, producerName))
	_, err = Commit(t.Context(), opts, "test: retire producer")
	require.NoError(t, err)
	require.NotContains(t, testGitOutput(t, t.Context(), repo, "ls-tree", "--name-only", "HEAD"), producerName)
	require.Equal(t, index, testGitOutput(t, t.Context(), repo, "diff", "--cached", "--binary"))
}

func TestPublicationReceiptOwnership(t *testing.T) {
	src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
	defer func() { _ = src.Close() }()
	repo := filepath.Join(t.TempDir(), "share")
	_, err := Export(t.Context(), src, Options{RepoPath: repo})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(repo, producerName), []byte("unrelated"), 0o600))
	before := publicationBytes(t, repo)
	_, err = Export(t.Context(), src, Options{RepoPath: repo, Producer: fixtureProducer()})
	require.ErrorContains(t, err, "without declared")
	require.Equal(t, before, publicationBytes(t, repo))
	manifest, err := ReadManifest(repo)
	require.NoError(t, err)
	for _, name := range []string{"../producer.json", "media/producer.json", ""} {
		manifest.Files = map[string]string{"producer": name}
		_, err := publicationPaths(manifest)
		require.Error(t, err)
	}
	manifest.Files = map[string]string{"unknown": "private.txt"}
	paths, err := publicationPaths(manifest)
	require.NoError(t, err)
	require.NotContains(t, paths, "private.txt")
	manifest.Files = map[string]string{"producer": producerName}
	writeShareManifest(t, repo, manifest)
	require.NoError(t, os.Remove(filepath.Join(repo, producerName)))
	require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(repo, producerName)))
	_, err = Export(t.Context(), src, Options{RepoPath: repo, Producer: fixtureProducer()})
	require.Error(t, err)
}

func TestPublicationReceiptRetry(t *testing.T) {
	for _, changeManifest := range []bool{false, true} {
		t.Run(map[bool]string{false: "readme", true: "manifest"}[changeManifest], func(t *testing.T) {
			dir := t.TempDir()
			remote := filepath.Join(dir, "remote.git")
			testGitRun(t, t.Context(), dir, "init", "--bare", remote)
			src := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
			defer func() { _ = src.Close() }()
			opts := Options{RepoPath: filepath.Join(dir, "publisher"), Remote: remote, Branch: "main", Producer: fixtureProducer(), ReadmePath: "README.md"}
			_, err := Export(t.Context(), src, opts)
			require.NoError(t, err)
			configureGitUser(t, opts.RepoPath)
			require.NoError(t, os.WriteFile(filepath.Join(opts.RepoPath, "README.md"), []byte("first\n\nnotes: old\n"), 0o600))
			_, err = Commit(t.Context(), opts, "test: initial receipt")
			require.NoError(t, err)
			require.NoError(t, Push(t.Context(), opts))
			_, receipt, err := workingPublicationBinding(opts.RepoPath, opts.Producer)
			require.NoError(t, err)
			reporter := filepath.Join(dir, "reporter")
			testGitRun(t, t.Context(), dir, "clone", "--branch", "main", remote, reporter)
			configureGitUser(t, reporter)
			if changeManifest {
				name := filepath.Join(reporter, ManifestName)
				body, err := os.ReadFile(name)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(name, append(body, '\n'), 0o600))
			} else {
				require.NoError(t, os.WriteFile(filepath.Join(reporter, "README.md"), []byte("first\n\nnotes: new\n"), 0o600))
			}
			testGitRun(t, t.Context(), reporter, "commit", "-am", "test: remote update")
			testGitRun(t, t.Context(), reporter, "push")
			remoteBefore := testGitOutput(t, t.Context(), reporter, "rev-parse", "HEAD")
			require.NoError(t, os.WriteFile(filepath.Join(opts.RepoPath, "README.md"), []byte("second\n\nnotes: old\n"), 0o600))
			_, err = Commit(t.Context(), opts, "test: local report")
			require.NoError(t, err)
			err = Push(t.Context(), opts)
			if changeManifest {
				require.ErrorContains(t, err, "binding changed")
				require.Equal(t, remoteBefore, testGitOutput(t, t.Context(), remote, "rev-parse", "refs/heads/main"))
			} else {
				require.NoError(t, err)
				_, after, err := workingPublicationBinding(opts.RepoPath, opts.Producer)
				require.NoError(t, err)
				require.Equal(t, receipt, after, "report retry cannot remint producer identity")
				body, err := os.ReadFile(filepath.Join(opts.RepoPath, "README.md"))
				require.NoError(t, err)
				require.Contains(t, string(body), "second")
				require.Contains(t, string(body), "notes: new")
			}
		})
	}
}
