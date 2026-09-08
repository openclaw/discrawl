package share

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/openclaw/discrawl/internal/report"
)

var (
	publicationShard      = regexp.MustCompile(`^[0-9]{6,}\.jsonl\.gz$`)
	publicationGeneration = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

func publicationPaths(manifest Manifest) (map[string]bool, error) {
	files := map[string]bool{ManifestName: true}
	add := func(name, prefix string, shard bool) error {
		if !safePublicationPath(name) || !strings.HasPrefix(name, prefix+"/") ||
			(shard && (path.Dir(name) != prefix || !publicationShard.MatchString(path.Base(name)))) {
			return fmt.Errorf("unowned publication path %q", name)
		}
		files[name] = true
		return nil
	}
	for _, table := range manifest.Tables {
		if !slices.Contains(SnapshotTables, table.Name) {
			return nil, fmt.Errorf("unowned publication table %q", table.Name)
		}
		names := append([]string(nil), table.Files...)
		if table.File != "" {
			names = append(names, table.File)
		}
		for _, file := range table.FileManifests {
			names = append(names, file.Path)
		}
		for _, name := range names {
			// Accept legacy layouts and exact manifest-owned generation shards.
			if name == "tables/"+table.Name+".jsonl" || name == "tables/"+table.Name+".jsonl.gz" {
				files[name] = true
				continue
			}
			prefix := "tables/" + table.Name
			parts := strings.Split(name, "/")
			if len(parts) == 5 && parts[0] == "tables" && parts[1] == ".generations" &&
				publicationGeneration.MatchString(parts[2]) && parts[3] == table.Name {
				prefix = strings.Join(parts[:4], "/")
			}
			if err := add(name, prefix, true); err != nil {
				return nil, err
			}
		}
	}
	for _, embedding := range manifest.Embeddings {
		prefix := path.Join("embeddings", safePathSegment(embedding.Provider),
			safePathSegment(embedding.Model), safePathSegment(embedding.InputVersion))
		for _, name := range embedding.Files {
			if err := add(name, prefix, true); err != nil {
				return nil, err
			}
		}
	}
	if manifest.Media != nil {
		for _, file := range manifest.Media.Files {
			if err := add(file.Path, "media", false); err != nil {
				return nil, err
			}
		}
	}
	return files, nil
}

func safePublicationPath(name string) bool {
	if name == "" || name == "." || path.IsAbs(name) || path.Clean(name) != name ||
		strings.ContainsAny(name, "\\\x00\r\n") || strings.Contains(name, ":") {
		return false
	}
	for part := range strings.SplitSeq(name, "/") {
		if part == ".." || strings.EqualFold(part, ".git") {
			return false
		}
	}
	return true
}

func previousPublicationPaths(ctx context.Context, repo string, includeRemovedReadme bool) (map[string]bool, error) {
	files := map[string]bool{}
	if _, err := regularFileInRoot(repo, filepath.Join(repo, ManifestName), ManifestName, "publication"); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	current, err := ReadManifest(repo)
	if err != nil && !errors.Is(err, ErrNoManifest) {
		return nil, err
	}
	if err == nil {
		files, err = publicationPaths(current)
		if err != nil {
			return nil, err
		}
	}
	// Include HEAD ownership so a previous --no-commit export cannot conceal
	// managed deletions from the next publication commit.
	head, err := publicationGit(ctx, repo, nil, "rev-parse", "--verify", "--quiet", "HEAD")
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return files, nil // New, unborn archive repository.
		}
		return nil, err
	}
	tree, err := publicationGit(ctx, repo, nil, "ls-tree", "--name-only", strings.TrimSpace(string(head)), "--", ManifestName)
	if err != nil || len(tree) == 0 {
		return files, err
	}
	body, err := publicationGit(ctx, repo, nil, "show", strings.TrimSpace(string(head))+":"+ManifestName)
	if err != nil {
		return nil, err
	}
	prior, err := parseManifest(body)
	if err != nil {
		return nil, err
	}
	owned, err := publicationPaths(prior)
	if err != nil {
		return nil, err
	}
	for name := range owned {
		files[name] = true
	}
	if includeRemovedReadme {
		_, err := os.Lstat(filepath.Join(repo, "README.md"))
		if errors.Is(err, os.ErrNotExist) {
			tree, err := publicationGit(ctx, repo, nil, "ls-tree", "--name-only", strings.TrimSpace(string(head)), "--", "README.md")
			if err != nil {
				return nil, err
			}
			if len(tree) > 0 {
				body, err := publicationGit(ctx, repo, nil, "show", strings.TrimSpace(string(head))+":README.md")
				if err != nil {
					return nil, err
				}
				if strings.Contains(string(body), report.StartMarker) && strings.Contains(string(body), report.EndMarker) {
					files["README.md"] = true
				}
			}
		} else if err != nil {
			return nil, err
		}
	}
	return files, nil
}

func installPublication(repo, stage string, previous map[string]bool, manifest Manifest) error {
	current, err := publicationPaths(manifest)
	if err != nil {
		return err
	}
	all := map[string]bool{}
	for name := range previous {
		all[name] = true
	}
	for name := range current {
		all[name] = true
	}
	for name := range all {
		if _, err := regularFileInRoot(repo, filepath.Join(repo, filepath.FromSlash(name)), name, "publication"); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for _, name := range sortedPublicationPaths(current) {
		if name == ManifestName {
			continue
		}
		if err := copyFile(filepath.Join(repo, filepath.FromSlash(name)), filepath.Join(stage, filepath.FromSlash(name))); err != nil {
			return fmt.Errorf("install publication %s: %w", name, err)
		}
	}
	for name := range previous {
		if !current[name] {
			if err := os.Remove(filepath.Join(repo, filepath.FromSlash(name))); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove previous publication %s: %w", name, err)
			}
		}
	}
	return copyFile(filepath.Join(repo, ManifestName), filepath.Join(stage, ManifestName))
}

func commitPublication(ctx context.Context, opts Options, message string) (bool, error) {
	paths, err := previousPublicationPaths(ctx, opts.RepoPath, opts.Filter.Active())
	if err != nil {
		return false, err
	}
	if opts.ReadmePath != "" {
		if !safePublicationPath(opts.ReadmePath) || strings.TrimSpace(opts.ReadmePath) != opts.ReadmePath {
			return false, fmt.Errorf("invalid generated README path %q", opts.ReadmePath)
		}
		paths[opts.ReadmePath] = true
	}
	index, err := publicationGit(ctx, opts.RepoPath, nil, "ls-files", "-z")
	if err != nil {
		return false, err
	}
	indexed := map[string]bool{}
	for name := range strings.SplitSeq(string(index), "\x00") {
		indexed[name] = true
	}
	var selected []string
	for _, name := range sortedPublicationPaths(paths) {
		_, err := regularFileInRoot(opts.RepoPath, filepath.Join(opts.RepoPath, filepath.FromSlash(name)), name, "publication")
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		if err == nil || indexed[name] {
			selected = append(selected, name)
		}
	}
	deleted, err := publicationGit(ctx, opts.RepoPath, nil, "diff", "--cached", "--diff-filter=D", "--name-only", "--no-renames", "-z")
	if err != nil {
		return false, err
	}
	var restage []string
	for name := range strings.SplitSeq(string(deleted), "\x00") {
		if paths[name] && !indexed[name] {
			restage = append(restage, name)
			if !slices.Contains(selected, name) {
				selected = append(selected, name)
			}
		}
	}
	if len(selected) == 0 {
		return false, nil
	}
	// Reintroduce only owned deletions so Git add can find and stage their removal.
	if len(restage) > 0 {
		if _, err := publicationGit(ctx, opts.RepoPath, restage, "restore", "--staged", "--source=HEAD",
			"--pathspec-from-file=-", "--pathspec-file-nul"); err != nil {
			return false, err
		}
	}
	if err := runPublicationGit(ctx, opts.RepoPath, selected, "add", "--pathspec-from-file=-", "--pathspec-file-nul"); err != nil {
		return false, fmt.Errorf("stage publication: %w", err)
	}
	changed, err := publicationHasChanges(ctx, opts.RepoPath, paths)
	if err != nil || !changed {
		return false, err
	}
	if message == "" {
		message = "archive: update snapshot"
	}
	if err := runPublicationGit(ctx, opts.RepoPath, selected,
		"-c", "commit.gpgsign=false", "-c", "user.name=crawlkit", "-c", "user.email=crawlkit@example.invalid",
		"commit", "--only", "-m", message, "--pathspec-from-file=-", "--pathspec-file-nul"); err != nil {
		return false, fmt.Errorf("commit publication: %w", err)
	}
	return true, nil
}

func publicationHasChanges(ctx context.Context, repo string, paths map[string]bool) (bool, error) {
	cmd := publicationCommand(ctx, repo, nil, "diff", "--cached", "--name-only", "--no-renames", "-z")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return false, err
	}
	if err := cmd.Start(); err != nil {
		return false, err
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if end := bytes.IndexByte(data, 0); end >= 0 {
			return end + 1, data[:end], nil
		}
		if atEOF && len(data) > 0 {
			return 0, nil, errors.New("unterminated publication path")
		}
		return 0, nil, nil
	})
	changed := false
	for scanner.Scan() {
		changed = changed || paths[scanner.Text()]
	}
	// Drain valid records without retaining Git output; reject oversized records.
	readErr := scanner.Err()
	if readErr != nil {
		_ = stdout.Close()
	}
	if err := errors.Join(readErr, cmd.Wait(), ctx.Err()); err != nil {
		return false, fmt.Errorf("inspect staged publication: %w", err)
	}
	return changed, nil
}

func runPublicationGit(ctx context.Context, repo string, paths []string, args ...string) error {
	// Mutation output may contain archive names or hook diagnostics; do not relay it.
	err := publicationCommand(ctx, repo, paths, args...).Run()
	if err != nil {
		return errors.Join(err, ctx.Err())
	}
	return nil
}

func sortedPublicationPaths(paths map[string]bool) []string {
	names := make([]string, 0, len(paths))
	for name := range paths {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func publicationGit(ctx context.Context, repo string, paths []string, args ...string) ([]byte, error) {
	cmd := publicationCommand(ctx, repo, paths, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("publication git %s: %w: %s", args[0], err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func publicationCommand(ctx context.Context, repo string, paths []string, args ...string) *exec.Cmd {
	// Git 2.25 supports NUL path files for add/commit; names stay raw and literal.
	cmd := exec.CommandContext(ctx, "git", append([]string{"--literal-pathspecs", "-C", repo}, args...)...)
	if len(paths) > 0 {
		cmd.Stdin = strings.NewReader(strings.Join(paths, "\x00") + "\x00")
	}
	return cmd
}
