package share

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
	if name, ok := manifest.Files["producer"]; ok {
		if name != producerName {
			return nil, errors.New("invalid publication producer path")
		}
		files[producerName] = true
	}
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
				hasReport := strings.Contains(string(body), report.StartMarker) && strings.Contains(string(body), report.EndMarker)
				hasNotes := strings.Contains(string(body), report.FieldNotesStartMarker) && strings.Contains(string(body), report.FieldNotesEndMarker)
				hasLegacyNotes := strings.Contains(string(body), report.LegacyFieldNotesStartMarker) && strings.Contains(string(body), report.LegacyFieldNotesEndMarker)
				if hasReport || hasNotes || hasLegacyNotes {
					files["README.md"] = true
				}
			}
		} else if err != nil {
			return nil, err
		}
	}
	return files, nil
}

// The hook is private and per invocation, so failure tests cannot affect another
// export. It runs before an operation, never after a successful rename.
type publicationStep func(phase, name string) error

type preparedPublicationFile struct {
	name        string
	target      string
	dir         string
	replacement string
	backup      string
	changed     bool
}

func installPublication(ctx context.Context, repo, stage string, previous map[string]bool, manifest Manifest, before publicationStep) (manifestInstalled bool, err error) {
	current, err := publicationPaths(manifest)
	if err != nil {
		return false, err
	}
	all := map[string]bool{}
	for name := range previous {
		all[name] = true
	}
	for name := range current {
		all[name] = true
	}
	for name := range all {
		if !safePublicationPath(name) {
			return false, fmt.Errorf("unowned publication path %q", name)
		}
		if _, err := regularFileInRoot(repo, filepath.Join(repo, filepath.FromSlash(name)), name, "publication"); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		if current[name] {
			if _, err := regularFileInRoot(stage, filepath.Join(stage, filepath.FromSlash(name)), name, "publication"); err != nil {
				return false, err
			}
		}
	}
	step := func(phase, name string) error {
		if before != nil {
			if err := before(phase, name); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	files := make([]*preparedPublicationFile, 0, len(all))
	cleanup := func() error {
		var errs []error
		for _, file := range files {
			errs = append(errs, os.RemoveAll(file.dir))
		}
		return errors.Join(errs...)
	}
	retained := func() string {
		var dirs []string
		for _, file := range files {
			dirs = append(dirs, file.dir)
		}
		return strings.Join(dirs, ", ")
	}
	rollback := func(cause error) (bool, error) {
		var errs []error
		for _, file := range slices.Backward(files) {
			if !file.changed {
				continue
			}
			var restoreErr error
			if before != nil {
				restoreErr = before("rollback", file.name)
			}
			if restoreErr == nil {
				if file.backup != "" {
					restoreErr = os.Rename(file.backup, file.target)
				} else {
					restoreErr = os.Remove(file.target)
					if errors.Is(restoreErr, os.ErrNotExist) {
						restoreErr = nil
					}
				}
			}
			if restoreErr != nil {
				errs = append(errs, fmt.Errorf("restore publication %s: %w", file.name, restoreErr))
			}
		}
		if len(errs) > 0 {
			return false, errors.Join(cause, errors.Join(errs...), fmt.Errorf(
				"publication may be partially installed; backups retained in %s; restore each previous file to its named target (or remove newly created targets) before publishing again", retained()))
		}
		return false, errors.Join(cause, cleanup())
	}
	// Prepare the manifest first: a read-only archive root must fail before any
	// shard is replaced. Sibling private directories also work with mount points.
	names := []string{ManifestName}
	for _, name := range sortedPublicationPaths(all) {
		if name != ManifestName {
			names = append(names, name)
		}
	}
	for _, name := range names {
		if err := step("prepare", name); err != nil {
			return false, errors.Join(err, cleanup())
		}
		target := filepath.Join(repo, filepath.FromSlash(name))
		info, statErr := os.Lstat(target)
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return false, errors.Join(statErr, cleanup())
		}
		if !current[name] && errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return false, errors.Join(err, cleanup())
		}
		dir, err := os.MkdirTemp(filepath.Dir(target), ".discrawl-install-"+filepath.Base(target)+"-")
		if err != nil {
			return false, errors.Join(err, cleanup())
		}
		file := &preparedPublicationFile{name: name, target: target, dir: dir}
		files = append(files, file)
		if statErr == nil {
			file.backup = filepath.Join(dir, "previous")
			if err := preparePublicationFile(ctx, file.backup, target, info.Mode()); err != nil {
				return false, errors.Join(err, cleanup())
			}
		}
		if current[name] {
			file.replacement = filepath.Join(dir, "replacement")
			if err := preparePublicationFile(ctx, file.replacement, filepath.Join(stage, filepath.FromSlash(name)), 0o600); err != nil {
				return false, errors.Join(err, cleanup())
			}
		}
	}
	for _, file := range files {
		if file.name == ManifestName || file.replacement == "" {
			continue
		}
		if err := step("install", file.name); err != nil {
			return rollback(err)
		}
		if err := os.Rename(file.replacement, file.target); err != nil {
			return rollback(fmt.Errorf("install publication %s: %w", file.name, err))
		}
		file.changed = true
	}
	if err := step("manifest", ManifestName); err != nil {
		return rollback(err)
	}
	// os.Rename has no post-replacement work which could return a later error.
	if err := os.Rename(files[0].replacement, files[0].target); err != nil {
		return rollback(fmt.Errorf("install publication manifest: %w", err))
	}
	for _, file := range files {
		if current[file.name] {
			continue
		}
		if err := step("cleanup", file.name); err != nil {
			return true, fmt.Errorf("publication manifest installed; cleanup failed; backups retained in %s: %w", retained(), err)
		}
		if err := os.Remove(file.target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return true, fmt.Errorf("publication manifest installed; remove %s failed; backups retained in %s: %w", file.name, retained(), err)
		}
	}
	if err := cleanup(); err != nil {
		return true, fmt.Errorf("publication manifest installed; remove private preparation files: %w", err)
	}
	return true, nil
}

func preparePublicationFile(ctx context.Context, target, source string, mode os.FileMode) error {
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	// A fixed buffer bounds memory and provides cancellation points during large
	// shard copies. Rollback itself deliberately ignores the canceled context.
	buf := make([]byte, 64*1024)
	for err == nil {
		if err = ctx.Err(); err != nil {
			break
		}
		var n int
		n, err = src.Read(buf)
		if n > 0 {
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				err = writeErr
				break
			}
		}
		if errors.Is(err, io.EOF) {
			err = nil
			break
		}
	}
	return errors.Join(err, dst.Chmod(mode), dst.Close())
}

func commitPublication(ctx context.Context, opts Options, message string) (bool, error) {
	if _, _, err := workingPublicationBinding(opts.RepoPath, opts.Producer); err != nil {
		return false, err
	}
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
	if opts.Filter.Active() {
		for _, name := range []string{report.FieldNotesMarkdownPath, report.FieldNotesJSONPath} {
			_, err := regularFileInRoot(opts.RepoPath, filepath.Join(opts.RepoPath, filepath.FromSlash(name)), name, "publication")
			if err == nil {
				return false, errors.New("filtered publication must remove broader-scope field notes before commit")
			}
			if !errors.Is(err, os.ErrNotExist) {
				return false, err
			}
			paths[name] = true
		}
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
	// #nosec G204 -- fixed Git subcommands; repository/message are argv values and file paths use literal NUL stdin.
	cmd := exec.CommandContext(ctx, "git", append([]string{"--literal-pathspecs", "-C", repo}, args...)...)
	if len(paths) > 0 {
		cmd.Stdin = strings.NewReader(strings.Join(paths, "\x00") + "\x00")
	}
	return cmd
}
