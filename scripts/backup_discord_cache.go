//go:build ignore

// Temporary Linux-only backup transport. Build explicit files in the published
// crawlkit v0.15.0 module context with GOWORK=off and -mod=readonly.
// produce SOURCE NEW_WORK EXPECTATION_JSON PUBLIC_RECIPIENT
// verify CIPHER NEW_DEST EXPECTATION_JSON LOCAL_IDENTITY CIPHER_SHA256 CIPHER_SIZE
// Neither mode repairs data. A successful verify retains the decrypted evidence.
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"filippo.io/age"
)

const (
	sourceCap  = int64(10 << 30)
	copyCap    = int64(10 << 30)
	cipherCap  = int64(21 << 30)
	bindingCap = int64(64 << 10)
	reserve    = int64(8 << 30)
)

var names = [...]string{"original/discrawl.db", "original/discrawl.db-wal", "original/discrawl.db-shm", "consistent/before.sqlite"}

type cacheIdentity struct {
	ID      int64  `json:"id"`
	Key     string `json:"key"`
	Ref     string `json:"ref"`
	Version string `json:"version"`
	Created string `json:"created_at"`
	Size    int64  `json:"size_in_bytes"`
}

type expectation struct {
	Repository string        `json:"repository"`
	Revision   string        `json:"revision"`
	RunID      int64         `json:"run_id"`
	Attempt    int64         `json:"run_attempt"`
	Cache      cacheIdentity `json:"cache"`
}

type fileRecord struct {
	Name    string `json:"name"`
	Present bool   `json:"present"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

type binding struct {
	Schema int           `json:"schema_version"`
	Source expectation   `json:"source"`
	Files  [4]fileRecord `json:"files"`
}

type result struct {
	Complete   bool   `json:"complete"`
	Error      string `json:"error"`
	SHA256     string `json:"ciphertext_sha256,omitempty"`
	Size       int64  `json:"ciphertext_size,omitempty"`
	Repository string `json:"repository,omitempty"`
	Revision   string `json:"revision,omitempty"`
	RunID      int64  `json:"run_id,omitempty"`
	Attempt    int64  `json:"run_attempt,omitempty"`
}

// Faults and capacity observations belong to this invocation, never global state.
type operation struct {
	space func(string) (int64, error)
	fault func(string) error
}

func fail(reason string) error { return errors.New(reason) }

func (op operation) check(stage string) error {
	if op.fault != nil && op.fault(stage) != nil {
		return fail(stage)
	}
	return nil
}

func (op operation) admit(path string, additional int64) error {
	free := op.space
	if free == nil {
		free = func(path string) (int64, error) {
			var s syscall.Statfs_t
			if err := syscall.Statfs(path, &s); err != nil || s.Bsize <= 0 || s.Bavail > uint64((1<<63-1)/s.Bsize) {
				return 0, fail("capacity")
			}
			return int64(s.Bavail) * s.Bsize, nil
		}
	}
	n, err := free(path)
	if err != nil || additional < 0 || additional > 80<<30 || n < additional+reserve {
		return fail("capacity")
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	out := execute(ctx, os.Args[1:], operation{})
	if json.NewEncoder(os.Stdout).Encode(out) != nil || !out.Complete {
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string, op operation) (out result) {
	out.Error = "arguments"
	defer func() {
		if recover() != nil {
			out = result{Error: "internal"}
		}
	}()
	if len(args) != 5 && len(args) != 7 {
		return out
	}
	expected, err := readExpectation(args[3])
	if err != nil {
		return result{Error: "identity"}
	}
	var digest string
	var size int64
	switch {
	case len(args) == 5 && args[0] == "produce":
		recipient, e := age.ParseX25519Recipient(args[4])
		if e != nil {
			return result{Error: "recipient"}
		}
		ctx, cancel := context.WithTimeout(ctx, 60*time.Minute)
		defer cancel()
		digest, size, err = produce(ctx, args[1], args[2], expected, recipient, op)
	case len(args) == 7 && args[0] == "verify":
		key, e := readSmall(args[4], 1024, true)
		if e != nil {
			return result{Error: "identity"}
		}
		identity, e := age.ParseX25519Identity(strings.TrimSpace(string(key)))
		if e != nil {
			return result{Error: "identity"}
		}
		size, e = strconv.ParseInt(args[6], 10, 64)
		if e != nil || size <= 0 || size > cipherCap || !validHash(args[5]) {
			return out
		}
		digest = args[5]
		ctx, cancel := context.WithTimeout(ctx, 45*time.Minute)
		defer cancel()
		err = verify(ctx, args[1], args[2], expected, identity, digest, size, op)
	default:
		return out
	}
	if err != nil {
		reason := err.Error()
		switch reason {
		case "input_files", "file_budget", "copy", "capacity", "sqlite", "integrity", "binding",
			"original_changed", "encryption", "tar_close", "gzip_close", "age_close", "file_sync", "file_close",
			"ciphertext", "archive", "authentication", "time_limit":
			return result{Error: reason}
		default:
			return result{Error: "internal"}
		}
	}
	return result{true, "none", digest, size, expected.Repository, expected.Revision, expected.RunID, expected.Attempt}
}

func validHash(s string) bool { return regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(s) }

func readExpectation(path string) (expectation, error) {
	var e expectation
	data, err := readSmall(path, bindingCap, true)
	if err != nil || decode(data, &e) != nil {
		return e, fail("identity")
	}
	version := sha256.Sum256([]byte(".discrawl-ci/discrawl.db|.discrawl-ci/discrawl.db-shm|.discrawl-ci/discrawl.db-wal|zstd-without-long|1.0"))
	if e.Repository != "openclaw/discrawl" || !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(e.Revision) ||
		e.RunID <= 0 || e.Attempt <= 0 || e.Cache.ID <= 0 || e.Cache.Size <= 0 || e.Cache.Size > 8<<30 ||
		e.Cache.Ref != "refs/heads/main" || e.Cache.Version != hex.EncodeToString(version[:]) ||
		!regexp.MustCompile(`^discrawl-discord-db-Linux-main-[0-9]+-[0-9]+$`).MatchString(e.Cache.Key) ||
		!regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$`).MatchString(e.Cache.Created) {
		return e, fail("identity")
	}
	if _, err := time.Parse(time.RFC3339Nano, e.Cache.Created); err != nil {
		return e, fail("identity")
	}
	return e, nil
}

func decode(data []byte, into any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(into); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fail("binding")
	}
	return nil
}

func regular(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Sys().(*syscall.Stat_t).Nlink != 1 {
		return nil, fail("input_files")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fail("input_files")
	}
	actual, err := f.Stat()
	if err != nil || !os.SameFile(info, actual) {
		f.Close()
		return nil, fail("input_files")
	}
	return f, nil
}

func readSmall(path string, cap int64, private bool) ([]byte, error) {
	f, err := regular(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() > cap || (private && info.Mode().Perm() != 0600) {
		return nil, fail("input_files")
	}
	data, err := io.ReadAll(io.LimitReader(f, cap+1))
	if err != nil || int64(len(data)) > cap {
		return nil, fail("input_files")
	}
	return data, nil
}

func directory(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return fail("input_files")
	}
	real, err := filepath.EvalSymlinks(abs)
	info, statErr := os.Lstat(abs)
	if err != nil || statErr != nil || real != abs || !info.IsDir() {
		return fail("input_files")
	}
	return nil
}

func newWork(path string) error {
	if directory(filepath.Dir(path)) != nil || os.Mkdir(path, 0700) != nil {
		return fail("input_files")
	}
	return nil
}

func sameFilesystem(first, second string) bool {
	a, err1 := os.Stat(first)
	b, err2 := os.Stat(second)
	return err1 == nil && err2 == nil && a.Sys().(*syscall.Stat_t).Dev == b.Sys().(*syscall.Stat_t).Dev
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if r.ctx.Err() != nil {
		return 0, fail("time_limit")
	}
	return r.r.Read(p)
}

type cappedWriter struct {
	w   io.Writer
	n   int64
	max int64
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.max-w.n {
		return 0, fail("file_budget")
	}
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func inspect(source string) ([4]fileRecord, int64, error) {
	var records [4]fileRecord
	for i, name := range names {
		records[i].Name = name
	}
	if directory(source) != nil {
		return records, 0, fail("input_files")
	}
	d, err := os.Open(source)
	if err != nil {
		return records, 0, fail("input_files")
	}
	defer d.Close()
	entries, err := d.ReadDir(4)
	if (err != nil && err != io.EOF) || len(entries) > 3 {
		return records, 0, fail("input_files")
	}
	var total int64
	for _, entry := range entries {
		index := -1
		for i := 0; i < 3; i++ {
			if entry.Name() == filepath.Base(names[i]) {
				index = i
			}
		}
		if index < 0 {
			return records, 0, fail("input_files")
		}
		f, err := regular(filepath.Join(source, entry.Name()))
		if err != nil {
			return records, 0, err
		}
		info, err := f.Stat()
		f.Close()
		if err != nil || info.Size() < 0 || info.Size() > sourceCap-total {
			return records, 0, fail("file_budget")
		}
		records[index].Present, records[index].Size = true, info.Size()
		total += info.Size()
	}
	if !records[0].Present || records[0].Size == 0 {
		return records, 0, fail("input_files")
	}
	return records, total, nil
}

func copyFile(ctx context.Context, input, output string, size int64) (string, error) {
	in, err := regular(input)
	if err != nil {
		return "", err
	}
	defer in.Close()
	var out *os.File
	var dst io.Writer = io.Discard
	if output != "" {
		out, err = os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return "", fail("copy")
		}
		defer out.Close()
		dst = out
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, h), io.LimitReader(contextReader{ctx, in}, size+1))
	if err != nil || n != size {
		return "", fail("copy")
	}
	if out != nil {
		if out.Sync() != nil || out.Close() != nil {
			return "", fail("copy")
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// The source and retained cache archive already occupy disk. Admission below
// counts only additional writes, without assuming sparse/reflink savings.
func produce(ctx context.Context, source, work string, expected expectation, recipient age.Recipient, op operation) (string, int64, error) {
	source, _ = filepath.Abs(source)
	work, _ = filepath.Abs(work)
	if work == source || strings.HasPrefix(work, source+string(os.PathSeparator)) {
		return "", 0, fail("input_files")
	}
	records, total, err := inspect(source)
	if err != nil {
		return "", 0, err
	}
	if !sameFilesystem(source, filepath.Dir(work)) {
		return "", 0, fail("capacity")
	}
	if err := op.admit(filepath.Dir(work), total); err != nil {
		return "", 0, err
	}
	if newWork(work) != nil || os.Mkdir(filepath.Join(work, "source"), 0700) != nil || os.Mkdir(filepath.Join(work, "consistent"), 0700) != nil {
		return "", 0, fail("input_files")
	}
	copyCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	remaining := total
	for i := 0; i < 3; i++ {
		if !records[i].Present {
			continue
		}
		if err := op.admit(work, remaining); err != nil {
			return "", 0, err
		}
		name := filepath.Base(names[i])
		records[i].SHA256, err = copyFile(copyCtx, filepath.Join(source, name), filepath.Join(work, "source", name), records[i].Size)
		if err != nil {
			return "", 0, err
		}
		remaining -= records[i].Size
	}
	pages, err := sqlite(copyCtx, work, "pages", []string{"source/discrawl.db", "PRAGMA page_count; PRAGMA page_size;"}, "", 0, op)
	if err != nil {
		return "", 0, err
	}
	fields := strings.Fields(string(pages))
	if len(fields) != 2 {
		return "", 0, fail("sqlite")
	}
	count, err1 := strconv.ParseInt(fields[0], 10, 64)
	page, err2 := strconv.ParseInt(fields[1], 10, 64)
	if err1 != nil || err2 != nil || page < 512 || page > 65536 || page&(page-1) != 0 || count <= 0 || count > copyCap/page {
		return "", 0, fail("file_budget")
	}
	bound := count * page
	if err := op.admit(work, bound+cipherBound(total, bound)); err != nil {
		return "", 0, err
	}
	consistent := filepath.Join(work, names[3])
	if _, err := os.Lstat(consistent); !os.IsNotExist(err) {
		return "", 0, fail("input_files")
	}
	output, err := sqlite(copyCtx, work, "backup", []string{"-cmd", ".timeout 5000", "source/discrawl.db", ".backup main consistent/before.sqlite"}, consistent, bound, op)
	if err != nil || len(output) != 0 {
		return "", 0, fail("sqlite")
	}
	cancel()
	info, err := os.Lstat(consistent)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > bound || os.Chmod(consistent, 0600) != nil {
		return "", 0, fail("file_budget")
	}
	if err := integrity(ctx, work, "consistent/before.sqlite", op); err != nil {
		return "", 0, err
	}
	packCtx, packCancel := context.WithTimeout(ctx, 10*time.Minute)
	defer packCancel()
	records[3].Present, records[3].Size = true, info.Size()
	records[3].SHA256, err = copyFile(packCtx, consistent, "", info.Size())
	if err != nil {
		return "", 0, err
	}
	if err := op.admit(work, cipherBound(total, info.Size())); err != nil {
		return "", 0, err
	}
	b := binding{1, expected, records}
	paths := [4]string{filepath.Join(source, "discrawl.db"), filepath.Join(source, "discrawl.db-wal"), filepath.Join(source, "discrawl.db-shm"), consistent}
	digest, size, err := encrypt(packCtx, filepath.Join(work, "backup.tar.gz.age"), recipient, b, paths, op)
	if err != nil {
		return "", 0, err
	}
	after, _, err := inspect(source)
	for i := 0; err == nil && i < 3; i++ {
		if after[i].Present {
			after[i].SHA256, err = copyFile(packCtx, paths[i], "", after[i].Size)
		}
		if after[i] != records[i] {
			err = fail("original_changed")
		}
	}
	if err != nil || packCtx.Err() != nil {
		os.Remove(filepath.Join(work, "backup.tar.gz.age"))
		return "", 0, fail("original_changed")
	}
	return digest, size, nil
}

func cipherBound(s, c int64) int64 {
	plain := s + c + bindingCap + 5*1024 + 1024
	// Allow gzip expansion, not a compression ratio. The ciphertext writer also
	// enforces this measured budget, including X25519 and STREAM overhead.
	compressed := plain + plain/100 + 64<<10
	return compressed + 64<<10 + (compressed/(64<<10)+1)*16
}

func sqlite(ctx context.Context, work, stage string, args []string, watched string, maximum int64, op operation) ([]byte, error) {
	if op.check(stage) != nil || ctx.Err() != nil {
		return nil, fail("sqlite")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	files := make([]*os.File, 0, 2)
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	for _, suffix := range []string{"stdout", "stderr"} {
		f, err := os.OpenFile(filepath.Join(work, stage+"."+suffix+".private"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return nil, fail("sqlite")
		}
		files = append(files, f)
	}
	command := append([]string{"--signal=TERM", "--kill-after=30s", "600s", "/usr/bin/sqlite3", "-batch", "-bail", "-readonly", "-init", "/dev/null"}, args...)
	cmd := exec.CommandContext(ctx, "/usr/bin/timeout", command...)
	cmd.Dir, cmd.Env = work, []string{"PATH=/usr/bin:/bin", "HOME=" + work, "TMPDIR=" + work}
	cmd.Stdout, cmd.Stderr = &cappedWriter{w: files[0], max: 64 << 10}, &cappedWriter{w: files[1], max: 64 << 10}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 2 * time.Second
	done := make(chan struct{})
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				if watched != "" {
					info, err := os.Lstat(watched)
					if (err != nil && !os.IsNotExist(err)) || (err == nil && (!info.Mode().IsRegular() || info.Size() > maximum)) {
						cancel()
						return
					}
				}
			}
		}
	}()
	err := cmd.Run()
	close(done)
	<-monitorDone
	if err != nil || ctx.Err() != nil {
		return nil, fail("sqlite")
	}
	if _, err := files[0].Seek(0, io.SeekStart); err != nil {
		return nil, fail("sqlite")
	}
	output, err := io.ReadAll(io.LimitReader(files[0], 4097))
	if err != nil || len(output) > 4096 {
		return nil, fail("sqlite")
	}
	return output, nil
}

func integrity(ctx context.Context, work, file string, op operation) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	out, err := sqlite(ctx, work, "integrity", []string{file, "PRAGMA integrity_check;"}, "", 0, op)
	if err != nil || strings.TrimSpace(string(out)) != "ok" {
		return fail("integrity")
	}
	return nil
}

func header(name string, size int64) *tar.Header {
	// GNU regular headers support the approved 10 GiB bound without PAX records.
	return &tar.Header{Name: name, Mode: 0600, Size: size, Typeflag: tar.TypeReg, Format: tar.FormatGNU}
}

func encrypt(ctx context.Context, path string, recipient age.Recipient, b binding, paths [4]string, op operation) (digest string, size int64, err error) {
	data, err := json.Marshal(b)
	if err != nil || int64(len(data)) > bindingCap {
		return "", 0, fail("binding")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", 0, fail("encryption")
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(path)
		}
	}()
	hash := sha256.New()
	var sourceSize int64
	for _, record := range b.Files[:3] {
		if record.Present {
			sourceSize += record.Size
		}
	}
	dst := &cappedWriter{w: io.MultiWriter(f, hash), max: min(cipherCap, cipherBound(sourceSize, b.Files[3].Size))}
	encrypted, err := age.Encrypt(dst, recipient)
	if err != nil {
		return "", 0, fail("encryption")
	}
	compressed := gzip.NewWriter(encrypted)
	tw := tar.NewWriter(compressed)
	for i, record := range b.Files {
		if !record.Present {
			continue
		}
		input, e := regular(paths[i])
		if e != nil {
			return "", 0, fail("input_files")
		}
		h := sha256.New()
		e = tw.WriteHeader(header(record.Name, record.Size))
		if e == nil {
			_, e = io.Copy(io.MultiWriter(tw, h), io.LimitReader(contextReader{ctx, input}, record.Size+1))
		}
		input.Close()
		if e != nil || hex.EncodeToString(h.Sum(nil)) != record.SHA256 {
			return "", 0, fail("original_changed")
		}
	}
	if tw.WriteHeader(header("binding.json", int64(len(data)))) != nil {
		return "", 0, fail("encryption")
	}
	if _, err := tw.Write(data); err != nil {
		return "", 0, fail("encryption")
	}
	for _, step := range []struct {
		name string
		do   func() error
	}{{"tar_close", tw.Close}, {"gzip_close", compressed.Close}, {"age_close", encrypted.Close}, {"file_sync", f.Sync}, {"file_close", f.Close}} {
		e := step.do()
		if op.check(step.name) != nil || e != nil {
			return "", 0, fail(step.name)
		}
	}
	if ctx.Err() != nil {
		return "", 0, fail("time_limit")
	}
	return hex.EncodeToString(hash.Sum(nil)), dst.n, nil
}

// Re-encoding to a hash catches extensions, links, noncanonical padding and
// trailing bytes that archive/tar may otherwise skip. No second tar is stored.
func readEnvelope(ctx context.Context, cipher string, identity age.Identity, expected expectation, digest string, size int64, destination string, op operation) (binding, error) {
	var b binding
	f, err := regular(cipher)
	if err != nil {
		return b, fail("ciphertext")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() != size || size <= 0 || size > cipherCap {
		return b, fail("ciphertext")
	}
	cipherHash := sha256.New()
	plain, err := age.Decrypt(io.TeeReader(contextReader{ctx, f}, cipherHash), identity)
	if err != nil {
		return b, fail("authentication")
	}
	// ByteReader plus Multistream(false) leaves the exact gzip-member boundary
	// visible, including an empty second member or compressed trailing junk.
	compressed := bufio.NewReader(contextReader{ctx, plain})
	expanded, err := gzip.NewReader(compressed)
	if err != nil {
		return b, fail("archive")
	}
	defer expanded.Close()
	expanded.Multistream(false)
	rawHash, canonicalHash := sha256.New(), sha256.New()
	raw := io.TeeReader(io.LimitReader(contextReader{ctx, expanded}, sourceCap+copyCap+bindingCap+8193), rawHash)
	tr, canonical := tar.NewReader(raw), tar.NewWriter(canonicalHash)
	var observed [4]fileRecord
	for i, name := range names {
		observed[i].Name = name
	}
	last, foundBinding := -1, false
	var total int64
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil || foundBinding || h.Typeflag != tar.TypeReg || h.Format != tar.FormatGNU || h.Size < 0 {
			return b, fail("archive")
		}
		index, cap := -1, bindingCap
		for i, name := range names {
			if h.Name == name {
				index = i
				cap = sourceCap
			}
		}
		if h.Name != "binding.json" && index < 0 || h.Size > cap || (index >= 0 && index <= last) {
			return b, fail("archive")
		}
		if index >= 0 && index < 3 && h.Size > sourceCap-total {
			return b, fail("file_budget")
		}
		if canonical.WriteHeader(header(h.Name, h.Size)) != nil {
			return b, fail("archive")
		}
		var buffer bytes.Buffer
		var output *os.File
		var target io.Writer = io.Discard
		if index == -1 {
			target = &buffer
		} else if destination != "" {
			if err := op.admit(destination, h.Size); err != nil {
				return b, err
			}
			output, err = os.OpenFile(filepath.Join(destination, h.Name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return b, fail("copy")
			}
			target = output
		}
		hash := sha256.New()
		n, err := io.Copy(io.MultiWriter(target, canonical, hash), tr)
		if output != nil {
			err = errors.Join(err, output.Sync(), output.Close())
		}
		if err != nil || n != h.Size {
			return b, fail("archive")
		}
		if index >= 0 {
			observed[index] = fileRecord{h.Name, true, n, hex.EncodeToString(hash.Sum(nil))}
			last = index
			if index < 3 {
				total += n
				if total > sourceCap {
					return b, fail("file_budget")
				}
			}
		} else {
			if decode(buffer.Bytes(), &b) != nil {
				return b, fail("binding")
			}
			data, err := json.Marshal(b)
			if err != nil || !bytes.Equal(data, buffer.Bytes()) {
				return b, fail("binding")
			}
			foundBinding = true
			if destination != "" {
				if err := os.WriteFile(filepath.Join(destination, "binding.json"), data, 0600); err != nil {
					return b, fail("copy")
				}
			}
		}
	}
	// Tar EOF is not gzip EOF: consume the CRC/size trailer and reject extra tar
	// bytes. Gzip Close alone does not validate the trailer.
	n, err := io.Copy(io.Discard, raw)
	if err != nil || n != 0 || canonical.Close() != nil || !bytes.Equal(rawHash.Sum(nil), canonicalHash.Sum(nil)) {
		return b, fail("authentication")
	}
	// The bounded reader must not manufacture a successful EOF.
	var extra [1]byte
	nextra, err := expanded.Read(extra[:])
	if nextra != 0 || err != io.EOF || expanded.Close() != nil {
		return b, fail("archive")
	}
	// One gzip member is not authenticated age EOF. Reject any remaining
	// compressed bytes and require the complete outer ciphertext digest.
	nextra, err = compressed.Read(extra[:])
	if nextra != 0 || err != io.EOF || hex.EncodeToString(cipherHash.Sum(nil)) != digest {
		return b, fail("authentication")
	}
	if !foundBinding || b.Schema != 1 || b.Source != expected || b.Files != observed ||
		!observed[0].Present || observed[0].Size <= 0 || !observed[3].Present || observed[3].Size <= 0 {
		return b, fail("binding")
	}
	return b, nil
}

func verify(ctx context.Context, cipher, destination string, expected expectation, identity age.Identity, digest string, size int64, op operation) error {
	if directory(filepath.Dir(destination)) != nil {
		return fail("input_files")
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		return fail("input_files")
	}
	if !sameFilesystem(cipher, filepath.Dir(destination)) {
		return fail("capacity")
	}
	readCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	// Authenticate once without extracting to obtain measured S/C for admission.
	b, err := readEnvelope(readCtx, cipher, identity, expected, digest, size, "", op)
	if err != nil {
		return err
	}
	var s int64
	for _, f := range b.Files[:3] {
		s += f.Size
	}
	c := b.Files[3].Size
	// Cipher is verified resident; still reserve one A for its download container.
	if err := op.admit(filepath.Dir(destination), size+s+2*c); err != nil {
		return err
	}
	if newWork(destination) != nil {
		return fail("input_files")
	}
	for _, name := range []string{"original", "consistent", "verification"} {
		if os.Mkdir(filepath.Join(destination, name), 0700) != nil {
			return fail("copy")
		}
	}
	again, err := readEnvelope(readCtx, cipher, identity, expected, digest, size, destination, op)
	if err != nil || again != b {
		return fail("authentication")
	}
	if err := op.admit(destination, c); err != nil {
		return err
	}
	verifyCtx, verifyCancel := context.WithTimeout(ctx, 10*time.Minute)
	defer verifyCancel()
	h, err := copyFile(verifyCtx, filepath.Join(destination, names[3]), filepath.Join(destination, "verification", "before.sqlite"), c)
	if err != nil || h != b.Files[3].SHA256 {
		return fail("copy")
	}
	if err := integrity(verifyCtx, destination, "verification/before.sqlite", op); err != nil {
		return err
	}
	for _, path := range []string{"original", "consistent", "verification", "."} {
		dir, err := os.Open(filepath.Join(destination, path))
		if err != nil {
			return fail("copy")
		}
		if errors.Join(dir.Sync(), dir.Close()) != nil {
			return fail("copy")
		}
	}
	// A marker is written only after both authenticated passes and copy integrity.
	marker, err := os.OpenFile(filepath.Join(destination, "VERIFIED"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fail("copy")
	}
	verified := false
	defer func() {
		if !verified {
			os.Remove(filepath.Join(destination, "VERIFIED"))
		}
	}()
	_, writeErr := fmt.Fprintln(marker, digest)
	if errors.Join(writeErr, marker.Sync(), marker.Close()) != nil {
		return fail("copy")
	}
	for _, path := range []string{destination, filepath.Dir(destination)} {
		dir, err := os.Open(path)
		if err != nil {
			return fail("copy")
		}
		if errors.Join(dir.Sync(), dir.Close()) != nil {
			return fail("copy")
		}
	}
	verified = true
	return nil
}
