//go:build ignore

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func TestIncompleteTailsAndBuckets(t *testing.T) {
	tails := [][]byte{{0xc2}, {0xe2}, {0xe2, 0x82}, {0xf0}, {0xf0, 0x9f}, {0xf0, 0x9f, 0x92}}
	for _, tail := range tails {
		for _, size := range []int{8191, 8192, 8193} {
			body := append(bytes.Repeat([]byte("a"), size-len(tail)), tail...)
			out := emptyAggregate()
			classify(body, &out)
			if out.InvalidText != 1 || out.ValidText != 0 || out.OtherMalformed != 0 {
				t.Fatal("incomplete tail misclassified")
			}
			var expected buckets
			expected.add(int64(size))
			actual := []buckets{out.Incomplete1, out.Incomplete2, out.Incomplete3}[len(tail)-1]
			if actual != expected || (out.StrictCandidates == 1) != (size == 8192) {
				t.Fatal("tail length/bucket or strict candidate mismatch")
			}
		}
	}
}

func TestUnicodeAndMalformedClasses(t *testing.T) {
	for _, body := range [][]byte{nil, []byte("plain"), []byte("\ufffd"), []byte("\u4e2d\U0001f600")} {
		out := emptyAggregate()
		classify(body, &out)
		if out.ValidText != 1 || out.InvalidText != 0 {
			t.Fatal("valid UTF-8, including U+FFFD, rejected")
		}
	}
	for _, body := range [][]byte{
		{0x80}, {0xff}, {0xc0, 0xaf}, {0xc1, 0xbf}, {0xe0, 0x80, 0x80},
		{0xed, 0xa0, 0x80}, {0xf4, 0x90, 0x80, 0x80}, {0xf5},
		{0xe2, 0x82, 'x'}, {'a', 0xff, 'b'}, {0xff, 0xf0, 0x9f},
		{0xe0, 0x80}, {0xed, 0xa0}, {0xf4, 0x90},
	} {
		out := emptyAggregate()
		classify(append(bytes.Repeat([]byte("a"), 8192-len(body)), body...), &out)
		if out.OtherMalformed != 1 || out.InvalidText != 1 || out.StrictCandidates != 0 {
			t.Fatal("complete/interior malformed bytes treated as an incomplete tail")
		}
	}
}

func openFixture(t *testing.T, directory, schema string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(directory, inputNames[0]))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	if schema != "" {
		mustExec(t, db, schema)
	}
	return db
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func fixture(t *testing.T, bodies ...[]byte) string {
	t.Helper()
	directory := t.TempDir()
	db := openFixture(t, directory, "CREATE TABLE message_attachments(text_content TEXT)")
	for _, body := range bodies {
		mustExec(t, db, "INSERT INTO message_attachments VALUES(CAST(? AS TEXT))", body)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return directory
}

func sourceBytes(t *testing.T, directory string) map[string][32]byte {
	t.Helper()
	result := make(map[string][32]byte)
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		result[entry.Name()] = sha256.Sum256(data)
	}
	return result
}

func runFixture(t *testing.T, source string, limit bounds) aggregate {
	t.Helper()
	before := sourceBytes(t, source)
	scratch := t.TempDir()
	result := execute(context.Background(), []string{source, scratch}, limit)
	if !reflect.DeepEqual(before, sourceBytes(t, source)) {
		t.Fatal("original file presence or bytes changed")
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatal("working copy was not removed")
	}
	return result
}

func TestDiagnosticRoundTrip(t *testing.T) {
	source := fixture(t, []byte(""), []byte("\ufffd"), []byte("\u4e2d\U0001f600"),
		append(bytes.Repeat([]byte("a"), 8189), 0xf0, 0x9f, 0x92),
		[]byte{'a', 0xff, 'b'}, bytes.Repeat([]byte("b"), 8191), bytes.Repeat([]byte("c"), 8193))
	out := runFixture(t, source, approvedBounds())
	if !out.Complete || !out.OriginalUnchanged || out.AbortReason != "none" || out.TotalCells != 7 ||
		out.Storage.Text != 7 || out.ValidText != 5 || out.InvalidText != 2 ||
		out.OtherMalformed != 1 || out.StrictCandidates != 1 || out.Incomplete3.Equal != 1 ||
		out.Lengths != (buckets{5, 1, 1}) {
		t.Fatalf("unexpected aggregate: %+v", out)
	}
}

func TestAllSQLiteStorageClasses(t *testing.T) {
	source := t.TempDir()
	db := openFixture(t, source, "CREATE TABLE message_attachments(text_content)")
	mustExec(t, db, "INSERT INTO message_attachments VALUES('text'), (CAST(x'ff' AS BLOB)), (NULL), (42), (1.5)")
	// Simulate legacy physical records while retaining the expected current TEXT declaration.
	mustExec(t, db, "PRAGMA writable_schema=ON")
	mustExec(t, db, "UPDATE sqlite_schema SET sql='CREATE TABLE message_attachments(text_content TEXT)' WHERE name='message_attachments'")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	out := runFixture(t, source, approvedBounds())
	if !out.Complete || out.Storage != (storageCounts{1, 1, 1, 1, 1}) || out.ValidText != 1 || out.InvalidText != 0 || out.TotalCells != 5 {
		t.Fatalf("storage classes lost: %+v", out)
	}
}

func TestPreflightCapsBeforeAnyMaterialization(t *testing.T) {
	for _, test := range []struct {
		name   string
		bodies [][]byte
		adjust func(*bounds)
		reason string
	}{
		{"rows", [][]byte{[]byte("a"), []byte("b"), []byte("c")}, func(b *bounds) { b.rows = 2 }, "row_limit"},
		{"total-bytes", [][]byte{[]byte("a"), []byte("bcde")}, func(b *bounds) { b.bytes = 4 }, "byte_limit"},
		{"cell", [][]byte{[]byte("a"), []byte("bcde")}, func(b *bounds) { b.cell = 3 }, "cell_limit"},
		{"real-cell-cap", [][]byte{[]byte("a"), bytes.Repeat([]byte("x"), (1<<20)+1)}, func(*bounds) {}, "cell_limit"},
		{"sqlite-length-limit", [][]byte{[]byte("a"), bytes.Repeat([]byte("x"), 3<<20)}, func(*bounds) {}, "sqlite"},
	} {
		t.Run(test.name, func(t *testing.T) {
			limit := approvedBounds()
			test.adjust(&limit)
			out := runFixture(t, fixture(t, test.bodies...), limit)
			if out.Complete || out.AbortReason != test.reason || out.ValidText != 0 || out.InvalidText != 0 || !out.OriginalUnchanged {
				t.Fatalf("preflight failed to stop before bodies: %+v", out)
			}
		})
	}
	t.Run("exact-exhaustion", func(t *testing.T) {
		limit := approvedBounds()
		limit.rows, limit.bytes, limit.cell = 2, 3, 2
		out := runFixture(t, fixture(t, []byte("a"), []byte("bc")), limit)
		if !out.Complete || out.TotalCells != 2 {
			t.Fatal("row-limit-plus-one query failed to establish exact exhaustion")
		}
	})
}

func TestFileBudgetsAndCancellation(t *testing.T) {
	source := fixture(t, []byte("test"))
	info, err := os.Stat(filepath.Join(source, inputNames[0]))
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []bounds{
		{time.Second, 10, 1024, 1024, info.Size() - 1, maxCombinedBytes},
		{time.Second, 10, 1024, 1024, maxRestoredBytes, info.Size()*2 - 1},
	} {
		if out := runFixture(t, source, limit); out.Complete || out.AbortReason != "file_budget" {
			t.Fatalf("file budget ignored: %+v", out)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := execute(ctx, []string{source, t.TempDir()}, approvedBounds())
	if out.Complete || out.AbortReason != "time_limit" {
		t.Fatal("cancelled scan was not incomplete")
	}
	limit := approvedBounds()
	limit.duration = time.Nanosecond
	if out := runFixture(t, source, limit); out.Complete || out.AbortReason != "time_limit" {
		t.Fatal("deadline was not enforced")
	}
	limit = approvedBounds()
	limit.cell++
	if out := execute(context.Background(), []string{source, t.TempDir()}, limit); out.AbortReason != "arguments" {
		t.Fatal("caller could raise approved scan caps")
	}
}

func TestMeasuredCapacityBounds(t *testing.T) {
	limit := approvedBounds()
	if limit != (bounds{240 * time.Second, 1_000_000, 256 << 20, 1 << 20, 10 << 30, 20 << 30}) {
		t.Fatal("approved capacity or unchanged scan limits drifted")
	}
	for _, field := range []string{"files", "combined"} {
		raised := limit
		if field == "files" {
			raised.files++
		} else {
			raised.combined++
		}
		if out := execute(context.Background(), []string{"unused-source", "unused-scratch"}, raised); out.AbortReason != "arguments" {
			t.Fatal("caller exceeded approved capacity")
		}
	}
	source := fixture(t, []byte("small boundary fixture"))
	info, err := os.Stat(filepath.Join(source, inputNames[0]))
	if err != nil {
		t.Fatal(err)
	}
	limit.files, limit.combined = info.Size(), info.Size()*2
	if out := runFixture(t, source, limit); !out.Complete || !out.OriginalUnchanged {
		t.Fatal("exact restored/combined boundary rejected")
	}
}

func TestMeasuredCapacitySparseMetadata(t *testing.T) {
	for _, test := range []struct {
		name string
		size int64
		ok   bool
	}{
		{"measured", 9089302528, true},
		{"exact-limit", 10 << 30, true},
		{"over-limit", (10 << 30) + 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			file, err := os.OpenFile(filepath.Join(directory, inputNames[0]), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			truncateErr := file.Truncate(test.size)
			closeErr := file.Close()
			if truncateErr != nil || closeErr != nil {
				t.Fatal("sparse metadata fixture creation failed")
			}
			root, err := os.OpenRoot(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			// Only inspect metadata: never pass these sparse files to hashing or copying.
			files, total, reason := inspectFiles(root, maxRestoredBytes)
			if (reason == "") != test.ok || (!test.ok && reason != "file_budget") {
				t.Fatal("restored-capacity boundary incorrect")
			}
			if test.ok && (total != test.size || len(files) != 1) {
				t.Fatal("logical size lost during metadata inspection")
			}
			if _, _, oldReason := inspectFiles(root, 8<<30); oldReason != "file_budget" {
				t.Fatal("previous restored limit unexpectedly accepted measured size")
			}
		})
	}
}

func TestSchemaEncodingAndErrorPrivacy(t *testing.T) {
	for _, test := range []struct {
		name, schema, reason string
	}{
		{"missing", "CREATE TABLE other(value TEXT)", "schema"},
		{"wrong-column", "CREATE TABLE message_attachments(other TEXT)", "schema"},
		{"wrong-type", "CREATE TABLE message_attachments(text_content BLOB)", "schema"},
		{"view", "CREATE VIEW message_attachments AS SELECT 'PRIVATE_PAYLOAD_MARKER' AS text_content", "schema"},
		{"generated", "CREATE TABLE message_attachments(value TEXT, text_content TEXT GENERATED ALWAYS AS(value))", "schema"},
		{"encoding", "PRAGMA encoding='UTF-16le'; CREATE TABLE message_attachments(text_content TEXT)", "encoding"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := t.TempDir()
			db := openFixture(t, source, test.schema)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			out := runFixture(t, source, approvedBounds())
			if out.Complete || out.AbortReason != test.reason {
				t.Fatalf("unexpected abort: %+v", out)
			}
			assertPrivateOutput(t, out)
		})
	}
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, inputNames[0]), []byte("PRIVATE_PAYLOAD_MARKER\xff"), 0600); err != nil {
		t.Fatal(err)
	}
	out := runFixture(t, source, approvedBounds())
	if out.Complete || out.AbortReason != "sqlite" {
		t.Fatal("malformed database unexpectedly accepted")
	}
	assertPrivateOutput(t, out)
	out = execute(context.Background(), []string{"PRIVATE_PAYLOAD_MARKER"}, approvedBounds())
	assertPrivateOutput(t, out)
}

func assertPrivateOutput(t *testing.T, out aggregate) {
	t.Helper()
	data, err := json.Marshal(out)
	if err != nil || len(data) > 4096 || bytes.Contains(data, []byte("PRIVATE_PAYLOAD_MARKER")) ||
		bytes.Contains(data, []byte("/tmp/")) || bytes.Contains(data, []byte("/home/")) || bytes.Contains(data, []byte("http")) {
		t.Fatal("aggregate exposed a value or exceeded output bound")
	}
}

func TestInputFileSafety(t *testing.T) {
	for _, scenario := range []string{"symlink", "hardlink", "extra", "directory", "fifo"} {
		t.Run(scenario, func(t *testing.T) {
			source := fixture(t, []byte("a"))
			main := filepath.Join(source, inputNames[0])
			var err error
			switch scenario {
			case "symlink":
				err = os.Symlink(main, filepath.Join(source, inputNames[1]))
			case "hardlink":
				err = os.Link(main, filepath.Join(source, inputNames[2]))
			case "extra":
				err = os.WriteFile(filepath.Join(source, "unexpected"), nil, 0600)
			case "directory":
				err = os.Mkdir(filepath.Join(source, inputNames[1]), 0700)
			case "fifo":
				err = syscall.Mkfifo(filepath.Join(source, inputNames[2]), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			out := execute(context.Background(), []string{source, t.TempDir()}, approvedBounds())
			if out.Complete || out.AbortReason != "input_files" {
				t.Fatal("unsafe file set accepted")
			}
		})
	}
	source := fixture(t, []byte("a"))
	inside := filepath.Join(source, "scratch")
	if err := os.Mkdir(inside, 0700); err != nil {
		t.Fatal(err)
	}
	if out := execute(context.Background(), []string{source, inside}, approvedBounds()); out.AbortReason != "arguments" {
		t.Fatal("working copy could be created inside source")
	}
}

func TestWALOnlyCommitsAndMissingSHM(t *testing.T) {
	for _, withSHM := range []bool{true, false} {
		live := t.TempDir()
		db := openFixture(t, live, "")
		mustExec(t, db, "PRAGMA journal_mode=WAL")
		mustExec(t, db, "PRAGMA wal_autocheckpoint=0")
		mustExec(t, db, "CREATE TABLE message_attachments(text_content TEXT)")
		mustExec(t, db, "INSERT INTO message_attachments VALUES('WAL_ONLY')")
		// Freeze the quiescent available set; the classifier sees no live connection.
		frozen := t.TempDir()
		for _, name := range inputNames {
			if !withSHM && name == inputNames[1] {
				continue
			}
			data, err := os.ReadFile(filepath.Join(live, name))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(frozen, name), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		out := runFixture(t, frozen, approvedBounds())
		if !out.Complete || !out.OriginalUnchanged || out.TotalCells != 1 || out.ValidText != 1 {
			t.Fatalf("WAL-only commit lost or original changed: %+v", out)
		}
		db.Close()
	}
}

func TestPrivateCopyAndPresenceVerification(t *testing.T) {
	source := fixture(t, []byte("a"))
	root, err := os.OpenRoot(source)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	before, _, reason := inspectFiles(root, maxRestoredBytes)
	if reason != "" {
		t.Fatal(reason)
	}
	for name, stamp := range before {
		stamp.hash, err = hashFile(context.Background(), root, name, stamp.info)
		if err != nil {
			t.Fatal(err)
		}
		before[name] = stamp
	}
	destination := t.TempDir()
	work, err := os.OpenRoot(destination)
	if err != nil {
		t.Fatal(err)
	}
	defer work.Close()
	for name, stamp := range before {
		if err := copyFile(context.Background(), root, work, name, stamp); err != nil {
			t.Fatal(err)
		}
		info, err := work.Stat(name)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("copy permissions not 0600")
		}
	}
	if !unchanged(context.Background(), root, before, maxRestoredBytes) {
		t.Fatal("unchanged source rejected")
	}
	if err := os.WriteFile(filepath.Join(source, inputNames[1]), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if unchanged(context.Background(), root, before, maxRestoredBytes) {
		t.Fatal("new optional file was not detected")
	}
	os.Remove(filepath.Join(source, inputNames[1]))
	if err := os.WriteFile(filepath.Join(source, inputNames[0]), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if unchanged(context.Background(), root, before, maxRestoredBytes) {
		t.Fatal("changed main file was not detected")
	}
}

type workflowStep struct {
	Name    string         `yaml:"name"`
	ID      string         `yaml:"id"`
	Uses    string         `yaml:"uses"`
	Run     string         `yaml:"run"`
	If      string         `yaml:"if"`
	With    map[string]any `yaml:"with"`
	Env     map[string]any `yaml:"env"`
	Timeout int            `yaml:"timeout-minutes"`
}

type workflowDocument struct {
	On          map[string]any    `yaml:"on"`
	Permissions map[string]string `yaml:"permissions"`
	Concurrency struct {
		Group  string `yaml:"group"`
		Cancel bool   `yaml:"cancel-in-progress"`
	} `yaml:"concurrency"`
	Jobs map[string]struct {
		Runner  string            `yaml:"runs-on"`
		Timeout int               `yaml:"timeout-minutes"`
		Env     map[string]string `yaml:"env"`
		Steps   []workflowStep    `yaml:"steps"`
	} `yaml:"jobs"`
}

func readWorkflow(t *testing.T) (workflowDocument, []byte) {
	t.Helper()
	var data []byte
	var err error
	for _, prefix := range []string{".", ".."} {
		data, err = os.ReadFile(filepath.Join(prefix, ".github/workflows/publish-discord-backup.yml"))
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	var document workflowDocument
	if err := yaml.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	return document, data
}

func TestWorkflowSafetyContract(t *testing.T) {
	document, raw := readWorkflow(t)
	if len(document.On) != 1 || document.On["workflow_dispatch"] == nil || len(document.Jobs) != 1 ||
		!reflect.DeepEqual(document.Permissions, map[string]string{"contents": "read", "actions": "read"}) ||
		document.Concurrency.Group != "discrawl-cache-diagnostic-20260909" || document.Concurrency.Cancel {
		t.Fatal("workflow widened triggers, jobs, permissions or concurrency")
	}
	job, ok := document.Jobs["diagnostic"]
	if !ok || job.Runner != "ubuntu-latest" || job.Timeout != 20 || job.Env["GOWORK"] != "off" || job.Env["GOFLAGS"] != "-mod=readonly" {
		t.Fatal("job resource or build contract changed")
	}
	for _, forbidden := range []string{"secrets.", "secrets:", "cache/save", "upload-artifact", "restore-keys:", "git push", "go run ./cmd/", "sudo ", "mount ", "schedule:"} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Fatalf("forbidden workflow operation: %s", forbidden)
		}
	}
	allowed := map[string]bool{
		"actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1":      true,
		"actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e":      true,
		"actions/github-script@ed597411d8f924073f98dfc5c65a23a2325f34cd": true,
	}
	var scripts, wrappers []string
	var restoreIndices []int
	buildIndex, runtimeIndex, stockIndex := -1, -1, -1
	for index, step := range job.Steps {
		if step.Uses != "" && !allowed[step.Uses] {
			t.Fatal("unpinned/unapproved action")
		}
		if strings.HasPrefix(step.Uses, "actions/checkout@") && step.With["persist-credentials"] != false {
			t.Fatal("checkout retains credentials")
		}
		if step.With["repository"] == "actions/cache" {
			stockIndex = index
			if step.With["ref"] != "55cc8345863c7cc4c66a329aec7e433d2d1c52a9" ||
				step.With["path"] != ".diagnostic-cache-action" || step.With["fetch-depth"] != 1 || step.With["fetch-tags"] != false {
				t.Fatal("stock restore checkout identity/depth changed")
			}
		}
		if step.ID == "cache-runtime" {
			runtimeIndex = index
			script := step.With["script"].(string)
			for _, required := range []string{`process.versions.node.split(".")[0] !== "24"`,
				`git(["rev-parse", "HEAD"]) !== "55cc8345863c7cc4c66a329aec7e433d2d1c52a9"`,
				`"dist/restore-only/index.js", "2427fc716c959214d22297fbf24a33c040b968bb", 3377934, true`,
				`"package.json", "b409614a4fd77b5eb49ee1d36bbf5936dab2792f"`,
				`"restore/action.yml", "93352f50541fca3b088080dba6b102051220dda2"`,
				`crypto.createHash("sha1")`, `fs.lstatSync(file)`, `pkg.type !== "module"`,
				`core.setOutput("node_binary", process.execPath)`} {
				if !strings.Contains(script, required) {
					t.Fatal("stock entrypoint or trusted Node verification missing")
				}
			}
		}
		if strings.HasPrefix(step.Uses, "actions/setup-go@") && step.With["cache"] != false {
			t.Fatal("setup-go cache save enabled")
		}
		if step.ID == "build" {
			buildIndex = index
			if !strings.Contains(step.Run, "24n*1024n**3n") || !strings.Contains(step.Run, "go test -count=1 scripts/diagnose_discord_cache.go") ||
				!strings.Contains(step.Run, `env -i PATH=/usr/bin:/bin "$NODE_BINARY" --version >/dev/null`) ||
				step.Env["NODE_BINARY"] != "${{ steps.cache-runtime.outputs.node_binary }}" {
				t.Fatal("pre-restore build/test/disk guard missing")
			}
		}
		if strings.HasPrefix(step.Uses, "actions/github-script@") && (step.ID == "cache-before" || step.ID == "") {
			scripts = append(scripts, step.With["script"].(string))
		}
		if step.ID == "lookup" || step.ID == "restore" {
			restoreIndices = append(restoreIndices, index)
			wrappers = append(wrappers, step.With["script"].(string))
			if step.Uses != "actions/github-script@ed597411d8f924073f98dfc5c65a23a2325f34cd" ||
				step.Env["NODE_BINARY"] != "${{ steps.cache-runtime.outputs.node_binary }}" ||
				step.Env["EXPECTED_KEY"] != "${{ steps.cache-before.outputs.key }}" ||
				step.Env["PRIVATE_TEMP"] != "${{ steps.build.outputs.private_temp }}" {
				t.Fatal("restore containment boundary changed")
			}
			if step.ID == "restore" && (step.Timeout != 10 || step.Env["LOOKUP_ONLY"] != "false") {
				t.Fatal("restore transfer bounds changed")
			}
			if step.ID == "lookup" && (step.Env["LOOKUP_ONLY"] != "true" || step.Timeout != 2) {
				t.Fatal("lookup downloads private data")
			}
		}
	}
	if len(scripts) != 2 || scripts[0] != scripts[1] || len(wrappers) != 2 || wrappers[0] != wrappers[1] ||
		len(restoreIndices) != 2 || stockIndex < 0 || runtimeIndex <= stockIndex || buildIndex <= runtimeIndex || buildIndex >= restoreIndices[0] {
		t.Fatal("metadata pre/post or build-before-restore contract changed")
	}
	if !strings.Contains(scripts[0], "expected.size_in_bytes > 8 * 1024 ** 3") {
		t.Fatal("compressed-cache metadata ceiling changed")
	}
	for _, forbidden := range []string{"dist/restore/index.js", "npm install", "stdio:\"inherit\"", ".pipe(process.stdout)", ".pipe(process.stderr)"} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Fatal("stock restore containment bypass")
		}
	}
	last := job.Steps[len(job.Steps)-1].Run
	if !strings.Contains(last, "240s env -i") || !strings.Contains(last, "2>/dev/null") || !strings.Contains(last, "statSync(path).size > 4096") {
		t.Fatal("classifier environment/output boundary missing")
	}
}

func TestWorkflowContainedRestore(t *testing.T) {
	document, _ := readWorkflow(t)
	var script string
	for _, step := range document.Jobs["diagnostic"].Steps {
		if step.ID == "lookup" {
			script = step.With["script"].(string)
		}
	}
	function, boundary, ok := strings.Cut(script, "\ntry {\n  if (process.platform")
	if !ok {
		t.Fatal("restore execution boundary missing")
	}
	boundary = "\ntry {\n  if (process.platform" + boundary
	cases := map[string]string{
		"multiline": "", "single": "", "descendant": "", "lookup-false": "",
		"duplicate": "restore_outputs", "wrong": "restore_outputs", "missing": "restore_outputs",
		"malformed": "restore_outputs", "unexpected": "restore_outputs", "invalid-utf8": "restore_outputs",
		"nonzero": "restore_exit", "file-command": "restore_commands",
		"capture-cap": "restore_capture_cap", "command-cap": "restore_capture_cap",
		"timeout": "restore_timeout", "cancelled": "restore_cancelled", "spawn-error": "restore_spawn",
	}
	for scenario, expectedReason := range cases {
		t.Run(scenario, func(t *testing.T) {
			directory := t.TempDir()
			entryDirectory := filepath.Join(directory, ".diagnostic-cache-action", "dist", "restore-only")
			if err := os.MkdirAll(entryDirectory, 0700); err != nil {
				t.Fatal(err)
			}
			const key = "discrawl-discord-db-Linux-main-100-1"
			fixture := `const fs=require("node:fs"), cp=require("node:child_process");
const scenario=` + strconv.Quote(scenario) + `, key=` + strconv.Quote(key) + `;
const marker="PRIVATE_PAYLOAD_MARKER";
process.stdout.write("https://example.invalid/"+marker+"\n");
process.stderr.write("service/body/tool "+marker+"\n");
if(process.cwd()!==process.env.GITHUB_WORKSPACE ||
   process.env.INPUT_PATH!==".discrawl-ci/discrawl.db\n.discrawl-ci/discrawl.db-shm\n.discrawl-ci/discrawl.db-wal" ||
   process.env.INPUT_KEY!==key || process.env["INPUT_RESTORE-KEYS"]!=="" ||
   process.env["INPUT_FAIL-ON-CACHE-MISS"]!=="true" || process.env.INPUT_ENABLECROSSOSARCHIVE!=="false" ||
   process.env["INPUT_LOOKUP-ONLY"]!==(scenario==="lookup-false"?"false":"true") ||
   process.env.SEGMENT_DOWNLOAD_TIMEOUT_MINS!=="2" ||
   process.env.ACTIONS_RUNTIME_TOKEN!=="placeholder" || process.env.RUNNER_TRACKING_ID!=="placeholder" ||
   process.env.GITHUB_TOKEN || process.env.NODE_OPTIONS || process.env.GITHUB_FUTURE_COMMAND ||
   process.env.EXTRA_PROVIDER_TOKEN) process.exit(4);
for(const name of ["GITHUB_OUTPUT","GITHUB_ENV","GITHUB_PATH","GITHUB_STATE","GITHUB_STEP_SUMMARY"]) {
  if(!fs.lstatSync(process.env[name]).isFIFO()) process.exit(5);
}
const output=(name,value)=>{
  const delimiter="ghadelimiter_11111111-2222-4333-8444-555555555555";
  fs.appendFileSync(process.env.GITHUB_OUTPUT,scenario==="single"?name+"="+value+"\n":name+"<<"+delimiter+"\n"+value+"\n"+delimiter+"\n");
};
output("cache-primary-key",key);
if(scenario!=="missing") output("cache-matched-key",scenario==="wrong"?"other":key);
output("cache-hit","true");
if(scenario==="duplicate") output("cache-hit","true");
if(scenario==="unexpected") output("unapproved-command","value");
if(scenario==="malformed") fs.appendFileSync(process.env.GITHUB_OUTPUT,"cache-hit<<ghadelimiter_bad\ntrue\n");
if(scenario==="invalid-utf8") fs.appendFileSync(process.env.GITHUB_OUTPUT,Buffer.from([255,10]));
if(scenario==="file-command") {
  for(const name of ["GITHUB_ENV","GITHUB_PATH","GITHUB_STATE","GITHUB_STEP_SUMMARY"])
    fs.appendFileSync(process.env[name],"INJECTED="+marker+"\n");
}
if(scenario==="capture-cap") {
  process.stdout.write(marker.repeat(1000));
  process.stderr.write(marker.repeat(1000));
}
if(scenario==="command-cap") fs.appendFileSync(process.env.GITHUB_OUTPUT,marker.repeat(1000));
if(["timeout","descendant"].includes(scenario)) {
  const child=cp.spawn(process.execPath,["-e",'process.stdout.write("PRIVATE_PAYLOAD_MARKER"); process.stderr.write("PRIVATE_PAYLOAD_MARKER"); setInterval(()=>{},1000)'],{stdio:["ignore","inherit","inherit"]});
  fs.writeFileSync("descendant.pid",String(child.pid));
  if(scenario==="descendant") process.exit(0);
}
if(scenario==="cancelled") process.kill(process.ppid,"SIGTERM");
if(["timeout","cancelled","capture-cap","command-cap"].includes(scenario)) setInterval(()=>{},1000);
else if(scenario==="nonzero") throw new Error(marker);
`
			if err := os.WriteFile(filepath.Join(entryDirectory, "index.js"), []byte(fixture), 0600); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"OUTPUT", "ENV", "PATH", "STATE", "STEP_SUMMARY", "FUTURE_COMMAND"} {
				if err := os.WriteFile(filepath.Join(directory, "parent-"+name), []byte("parent-unchanged"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			payload, _ := json.Marshal(map[string]any{"directory": directory, "scenario": scenario, "key": key})
			program := `const fs=require("node:fs"),path=require("node:path");
const input=JSON.parse(process.argv[1]), result={failed:[],outputs:[]};
const core={setFailed:reason=>result.failed.push(reason),setOutput:(name,value)=>result.outputs.push([name,value])};
Object.assign(process.env,{GITHUB_WORKSPACE:input.directory,PRIVATE_TEMP:input.directory,NODE_BINARY:process.execPath,
  EXPECTED_KEY:input.key,LOOKUP_ONLY:input.scenario==="lookup-false"?"false":"true",
  ACTIONS_RUNTIME_TOKEN:"placeholder",RUNNER_TRACKING_ID:"placeholder",GITHUB_TOKEN:"placeholder",
  EXTRA_PROVIDER_TOKEN:"placeholder",NODE_OPTIONS:"--not-forwarded"});
for(const name of ["OUTPUT","ENV","PATH","STATE","STEP_SUMMARY","FUTURE_COMMAND"])
  process.env["GITHUB_"+name]=path.join(input.directory,"parent-"+name);
` + function + `
const actualContainedRestore=containedRestore;
containedRestore=options=>actualContainedRestore({...options,timeoutMs:600,
  node:input.scenario==="spawn-error"?path.join(input.directory,"missing-node"):options.node,
  streamCap:input.scenario==="capture-cap"?128:4096,commandCap:1024});
(async()=>{` + boundary + `})().then(()=>process.stdout.write(JSON.stringify(result)));`
			output, err := runNode(t, program, string(payload))
			if err != nil || bytes.Contains(output, []byte("PRIVATE_PAYLOAD_MARKER")) || len(output) > 1024 {
				t.Fatal("restore child output escaped containment")
			}
			var result struct {
				Failed  []string   `json:"failed"`
				Outputs [][]string `json:"outputs"`
			}
			if err := json.Unmarshal(output, &result); err != nil {
				t.Fatal("unexpected public wrapper output")
			}
			if expectedReason == "" {
				expected := [][]string{{"cache-primary-key", key}, {"cache-matched-key", key}, {"cache-hit", "true"}}
				if len(result.Failed) != 0 || !reflect.DeepEqual(result.Outputs, expected) {
					t.Fatalf("valid exact outputs were not relayed: %s", output)
				}
			} else if !reflect.DeepEqual(result.Failed, []string{expectedReason}) || len(result.Outputs) != 0 {
				t.Fatalf("wrong fixed failure disposition: %s", output)
			}
			for _, name := range []string{"OUTPUT", "ENV", "PATH", "STATE", "STEP_SUMMARY", "FUTURE_COMMAND"} {
				data, err := os.ReadFile(filepath.Join(directory, "parent-"+name))
				if err != nil || string(data) != "parent-unchanged" {
					t.Fatal("child reached parent runner command file")
				}
			}
			directories, err := filepath.Glob(filepath.Join(directory, "contained-*"))
			if err != nil || len(directories) != 1 {
				t.Fatal("private capture directory missing")
			}
			info, err := os.Stat(directories[0])
			if err != nil || info.Mode().Perm() != 0700 {
				t.Fatal("capture directory is not private")
			}
			files, err := os.ReadDir(directories[0])
			if err != nil {
				t.Fatal(err)
			}
			var streams, commands int64
			for _, file := range files {
				info, err := file.Info()
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatal("capture file is not private")
				}
				if file.Name() == "stdout" || file.Name() == "stderr" {
					streams += info.Size()
				} else if strings.HasSuffix(file.Name(), ".capture") {
					commands += info.Size()
				}
			}
			streamLimit := int64(4096)
			if scenario == "capture-cap" {
				streamLimit = 128
			}
			if streams > streamLimit || commands > 1024 {
				t.Fatal("private capture exceeded byte cap")
			}
			if data, err := os.ReadFile(filepath.Join(directory, "descendant.pid")); err == nil {
				pid, err := strconv.Atoi(string(data))
				if err != nil || pid <= 0 {
					t.Fatal("invalid synthetic descendant PID")
				}
				stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
				if err == nil {
					_, tail, ok := strings.Cut(string(stat), ") ")
					if !ok || !strings.HasPrefix(tail, "Z ") {
						syscall.Kill(pid, syscall.SIGKILL)
						t.Fatal("owned synthetic descendant survived wrapper termination")
					}
				} else if !os.IsNotExist(err) {
					t.Fatal("could not verify synthetic descendant termination")
				}
			}
		})
	}
}

func runNode(t *testing.T, program string, arguments ...string) ([]byte, error) {
	t.Helper()
	node := os.Getenv("NODE_BINARY")
	if node == "" {
		node = "node"
	}
	cmd := exec.Command(node, append([]string{"-e", program}, arguments...)...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir()}
	return cmd.CombinedOutput()
}

func TestWorkflowCapturedNodeBinary(t *testing.T) {
	node := os.Getenv("NODE_BINARY")
	if node == "" {
		var err error
		node, err = exec.LookPath("node")
		if err != nil {
			t.Fatal("existing Node is required")
		}
	}
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "node"), []byte("#!/bin/sh\nexit 91\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NODE_BINARY", node)
	t.Setenv("PATH", directory)
	output, err := runNode(t, `process.stdout.write("captured-node")`)
	if err != nil || string(output) != "captured-node" {
		t.Fatal("workflow tests did not use the captured Node binary")
	}
}

func TestWorkflowSizeOnlyContract(t *testing.T) {
	document, _ := readWorkflow(t)
	dispatch := document.On["workflow_dispatch"].(map[string]any)
	inputs := dispatch["inputs"].(map[string]any)
	operation := inputs["operation"].(map[string]any)
	if !reflect.DeepEqual(operation["options"], []any{"aggregate-only", "size-only"}) {
		t.Fatal("operation choices widened")
	}
	steps := document.Jobs["diagnostic"].Steps
	var size workflowStep
	for index, step := range steps {
		if step.ID == "size-only" {
			size = step
			if index != len(steps)-2 || step.If != "${{ inputs.operation == 'size-only' }}" || step.Timeout != 1 ||
				step.Env["NODE_BINARY"] != "${{ steps.cache-runtime.outputs.node_binary }}" ||
				step.Env["PRIVATE_TEMP"] != "${{ steps.build.outputs.private_temp }}" {
				t.Fatal("size-only scheduling or environment changed")
			}
		}
	}
	if steps[len(steps)-1].If != "${{ inputs.operation == 'aggregate-only' }}" {
		t.Fatal("size-only can invoke classifier")
	}
	for _, required := range []string{"29s env -i", "--kill-after=1s", "2>/dev/null", "Buffer.byteLength(output) <= 2048",
		`SOURCE_DIR="$GITHUB_WORKSPACE/.discrawl-ci"`, `fs.statfsSync(scratch, {bigint:true})`,
		`fs.opendirSync(source, {bufferSize:4})`, "i <= names.length", "size_timeout", "size_runtime"} {
		if !strings.Contains(size.Run, required) {
			t.Fatal("size-only bounded metadata contract missing")
		}
	}
	for _, forbidden := range []string{"readFile", "createReadStream", "copyFile", "createHash", "sqlite", "/diagnose", "core.", "GITHUB_TOKEN"} {
		if strings.Contains(size.Run, forbidden) {
			t.Fatal("size-only reads contents or widens its boundary")
		}
	}
}

func TestWorkflowSizeOnlyMetadata(t *testing.T) {
	document, _ := readWorkflow(t)
	var run string
	for _, step := range document.Jobs["diagnostic"].Steps {
		if step.ID == "size-only" {
			run = step.Run
		}
	}
	_, script, ok := strings.Cut(run, "<<'JS' 2>/dev/null\n")
	if !ok {
		t.Fatal("size-only script missing")
	}
	script, _, ok = strings.Cut(script, "\nJS\n")
	if !ok {
		t.Fatal("size-only script delimiter missing")
	}
	for _, scenario := range []string{
		"oversized", "db-only", "db-wal", "db-shm", "empty-optional", "missing-db", "empty-db",
		"unexpected", "duplicate", "too-many", "symlink", "hardlink", "special", "negative", "unsafe-size",
		"sum-overflow", "source-symlink", "scratch-symlink", "not-directory", "stat-error",
		"directory-error", "close-error", "free-error", "negative-free", "zero-block", "free-overflow",
	} {
		t.Run(scenario, func(t *testing.T) {
			program := `const scenario=globalThis.process.argv[1], marker="PRIVATE_PAYLOAD_MARKER";
const source="/synthetic/cache", scratch="/synthetic/work";
const names=["discrawl.db","discrawl.db-shm","discrawl.db-wal"], maximum=BigInt(Number.MAX_SAFE_INTEGER);
let entries=[...names], reads=0, closed=false, sized=false;
if(scenario==="db-only") entries=[names[0]];
if(scenario==="db-wal") entries=[names[0],names[2]];
if(scenario==="db-shm") entries=[names[0],names[1]];
if(scenario==="missing-db") entries=[names[1]];
if(scenario==="unexpected") entries=[names[0],marker];
if(scenario==="duplicate") entries=[names[0],names[0]];
if(scenario==="too-many") entries=[...names,marker];
const metadata={
  lstatSync:(file,options)=>{
    if(options.bigint!==true) throw new Error(marker);
    if(file===source||file===scratch) return {
      isDirectory:()=>scenario!=="not-directory",
      isSymbolicLink:()=>scenario===(file===source?"source-symlink":"scratch-symlink")
    };
    if(!names.some(name=>file===source+"/"+name)) throw new Error(marker);
    if(scenario==="stat-error") throw new Error(marker);
    const primary=file===source+"/"+names[0];
    let size=primary?9n*1024n**3n:4096n;
    if(scenario==="empty-db"&&primary || scenario==="empty-optional"&&!primary) size=0n;
    if(scenario==="negative") size=-1n;
    if(scenario==="unsafe-size") size=maximum+1n;
    if(scenario==="sum-overflow") size=maximum;
    return {size,nlink:scenario==="hardlink"?2n:1n,
      isFile:()=>!["symlink","special"].includes(scenario),isSymbolicLink:()=>scenario==="symlink"};
  },
  opendirSync:(file,options)=>{
    if(file!==source||options.bufferSize!==4) throw new Error(marker);
    if(scenario==="directory-error") throw new Error(marker);
    return {
      readSync:()=>{if(++reads>4) throw new Error(marker); const name=entries.shift(); return name===undefined?null:{name};},
      closeSync:()=>{closed=true;if(scenario==="close-error") throw new Error(marker);}
    };
  },
  statfsSync:(file,options)=>{
    if(file!==scratch||options.bigint!==true||!closed) throw new Error(marker);
    sized=true;
    if(scenario==="free-error") throw new Error(marker);
    return {bavail:scenario==="negative-free"?-1n:scenario==="free-overflow"?maximum:1024n,
      bsize:scenario==="zero-block"?0n:4096n};
  }
};
const fs=new Proxy(metadata,{get:(target,key)=>{
  if(!(key in target)) throw new Error(marker);
  return target[key];
}});
const require=name=>{if(name!=="node:fs") throw new Error(marker);return fs;};
const process={env:{SOURCE_DIR:source,PRIVATE_TEMP:scratch},stdout:{write:text=>globalThis.process.stdout.write(text)},exitCode:0};
` + script + `
globalThis.process.exitCode=process.exitCode;
if(reads>4 || (process.exitCode===0 && (!closed||!sized))) globalThis.process.exitCode=3;
`
			output, err := runNode(t, program, scenario)
			valid := scenario == "oversized" || scenario == "db-only" || scenario == "db-wal" ||
				scenario == "db-shm" || scenario == "empty-optional"
			if valid != (err == nil) || len(output) > 2048 || bytes.Contains(output, []byte("PRIVATE_PAYLOAD_MARKER")) ||
				bytes.Contains(output, []byte("/synthetic")) || bytes.Contains(output, []byte("discrawl.db")) {
				t.Fatal("size-only metadata/privacy result invalid")
			}
			var result map[string]any
			if json.Unmarshal(output, &result) != nil || len(result) != 7 ||
				result["schema_version"] != float64(1) || result["scope"] != "restored_cache.file_set" ||
				result["complete"] != valid {
				t.Fatal("unexpected size-only output schema")
			}
			if valid {
				count := float64(3)
				if scenario == "db-only" {
					count = 1
				} else if scenario == "db-wal" || scenario == "db-shm" {
					count = 2
				}
				total := float64(9 << 30)
				if scenario != "empty-optional" {
					total += (count - 1) * 4096
				}
				if result["error"] != "none" || result["file_count"] != count ||
					result["total_logical_bytes"] != total || result["free_bytes"] != float64(1024*4096) {
					t.Fatal("size-only totals or working-filesystem free space incorrect")
				}
			} else if result["error"] != "size_metadata" || result["file_count"] != float64(0) ||
				result["total_logical_bytes"] != float64(0) || result["free_bytes"] != float64(0) {
				t.Fatal("size-only failure exposed partial/unsafe metadata")
			}
		})
	}
}

func TestWorkflowSizeOnlyShellFailures(t *testing.T) {
	document, _ := readWorkflow(t)
	var run string
	for _, step := range document.Jobs["diagnostic"].Steps {
		if step.ID == "size-only" {
			run = step.Run
		}
	}
	for _, status := range []int{124, 137, 2} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			directory := t.TempDir()
			node := filepath.Join(directory, "node")
			fixture := "#!/bin/sh\nprintf '%s\\n' PRIVATE_PAYLOAD_MARKER >&2\nexit " + strconv.Itoa(status) + "\n"
			if err := os.WriteFile(node, []byte(fixture), 0700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", "-euo", "pipefail", "-c", run)
			cmd.Env = []string{"PATH=/usr/bin:/bin", "GITHUB_WORKSPACE=" + directory,
				"PRIVATE_TEMP=" + directory, "NODE_BINARY=" + node}
			output, err := cmd.CombinedOutput()
			expected := "size_timeout"
			if status == 2 {
				expected = "size_runtime"
			}
			var result map[string]any
			if err == nil || len(output) > 2048 || bytes.Contains(output, []byte("PRIVATE_PAYLOAD_MARKER")) ||
				json.Unmarshal(output, &result) != nil || len(result) != 7 || result["complete"] != false ||
				result["error"] != expected || result["scope"] != "restored_cache.file_set" ||
				result["total_logical_bytes"] != float64(0) || result["free_bytes"] != float64(0) ||
				result["file_count"] != float64(0) || result["schema_version"] != float64(1) {
				t.Fatal("size-only shell failure escaped fixed output")
			}
		})
	}
}

func TestWorkflowCacheMetadataMocks(t *testing.T) {
	document, _ := readWorkflow(t)
	var script string
	for _, step := range document.Jobs["diagnostic"].Steps {
		if step.ID == "cache-before" {
			script = step.With["script"].(string)
		}
	}
	version := sha256.Sum256([]byte(".discrawl-ci/discrawl.db|.discrawl-ci/discrawl.db-shm|.discrawl-ci/discrawl.db-wal|zstd-without-long|1.0"))
	expected := map[string]any{"id": 42, "key": "discrawl-discord-db-Linux-main-100-1", "ref": "refs/heads/main",
		"version": hex.EncodeToString(version[:]), "created_at": "2026-01-01T00:00:00.000000000Z", "size_in_bytes": 1024}
	for _, scenario := range []string{"match", "last-access", "id", "key", "ref", "version", "created_at", "size_in_bytes", "duplicate", "branch-shadow", "prefix-match", "post-restore-drift", "missing", "pagination", "api-error", "bad-input"} {
		t.Run(scenario, func(t *testing.T) {
			actual := make(map[string]any)
			for key, value := range expected {
				actual[key] = value
			}
			total := 1
			caches := []map[string]any{actual}
			link := ""
			switch scenario {
			case "match":
			case "last-access":
				actual["last_accessed_at"] = "later"
			case "duplicate":
				caches = append(caches, actual)
				total = 2
			case "branch-shadow":
				caches = append(caches, map[string]any{"id": 43, "key": expected["key"], "ref": "refs/heads/maintenance/example"})
				total = 2
			case "prefix-match":
				actual["key"] = expected["key"].(string) + "-extra"
			case "post-restore-drift":
				actual["size_in_bytes"] = 2048
			case "missing":
				caches, total = nil, 0
			case "pagination":
				link = `<https://example.invalid/next>; rel="next"`
			case "api-error", "bad-input":
			default:
				actual[scenario] = "different"
			}
			input := map[string]any{"expected": expected, "caches": caches, "total": total, "link": link, "scenario": scenario}
			payload, _ := json.Marshal(input)
			selectedScript := script
			if scenario == "post-restore-drift" {
				for _, step := range document.Jobs["diagnostic"].Steps {
					if strings.HasPrefix(step.Uses, "actions/github-script@") && step.ID == "" {
						selectedScript = step.With["script"].(string)
					}
				}
			}
			program := `const input=JSON.parse(globalThis.process.argv[1]); const calls=[], result={failed:[],outputs:[]};
const process={env:{EXPECTED_CACHE_IDENTITY:input.scenario==="bad-input"?"PRIVATE_PAYLOAD_MARKER":JSON.stringify(input.expected)}};
const github={rest:{actions:{getActionsCacheList:async args=>{
  calls.push(args); if(input.scenario==="api-error") throw new Error("PRIVATE_PAYLOAD_MARKER");
  return {data:{total_count:input.total,actions_caches:input.caches},headers:{link:input.link}};
}}}};
const core={setFailed:reason=>result.failed.push(reason),setOutput:(key,value)=>result.outputs.push([key,value])};
(async()=>{` + selectedScript + `})().then(()=>{result.calls=calls; globalThis.process.stdout.write(JSON.stringify(result));});`
			output, err := runNode(t, program, string(payload))
			if err != nil || bytes.Contains(output, []byte("PRIVATE_PAYLOAD_MARKER")) {
				t.Fatalf("metadata guard leaked/failed: %s", output)
			}
			var result struct {
				Failed  []string         `json:"failed"`
				Outputs [][]string       `json:"outputs"`
				Calls   []map[string]any `json:"calls"`
			}
			if err := json.Unmarshal(output, &result); err != nil {
				t.Fatal(err)
			}
			valid := scenario == "match" || scenario == "last-access"
			if valid != (len(result.Failed) == 0) || (!valid && !reflect.DeepEqual(result.Failed, []string{"cache_identity"})) {
				t.Fatalf("metadata scenario accepted incorrectly: %s", scenario)
			}
			if valid && (len(result.Outputs) != 1 || result.Outputs[0][1] != expected["key"]) {
				t.Fatal("validated exact key not returned")
			}
			for _, call := range result.Calls {
				if call["key"] != expected["key"] || call["per_page"] != float64(2) || call["ref"] != nil {
					t.Fatal("metadata query widened key or hid branch shadows")
				}
			}
		})
	}
}

func TestWorkflowExecutionContextGuard(t *testing.T) {
	document, _ := readWorkflow(t)
	guard := document.Jobs["diagnostic"].Steps[0].Run
	base := map[string]string{
		"GITHUB_REPOSITORY": "openclaw/discrawl",
		"RUNNER_OS":         "Linux",
		"GITHUB_EVENT_NAME": "workflow_dispatch",
		"GITHUB_REF":        "refs/heads/maintenance/discord-cache-diagnostic-20260909",
		"OPERATION":         "aggregate-only",
		"EXPECTED_SHA":      strings.Repeat("a", 40),
		"GITHUB_SHA":        strings.Repeat("a", 40),
		"WORKFLOW_SHA":      strings.Repeat("a", 40),
		"WORKFLOW_REF":      "openclaw/discrawl/.github/workflows/publish-discord-backup.yml@refs/heads/maintenance/discord-cache-diagnostic-20260909",
	}
	cases := map[string]string{
		"valid": "", "size-only": "", "GITHUB_REPOSITORY": "other/repository", "GITHUB_EVENT_NAME": "schedule",
		"GITHUB_REF": "refs/heads/main", "OPERATION": "publish", "EXPECTED_SHA": "$(echo PRIVATE_PAYLOAD_MARKER)",
		"GITHUB_SHA": strings.Repeat("b", 40), "WORKFLOW_SHA": strings.Repeat("c", 40),
		"WORKFLOW_REF": "other", "RUNNER_OS": "Windows", "RUNNER_DEBUG": "1", "ACTIONS_STEP_DEBUG": "true", "ACTIONS_RUNNER_DEBUG": "true",
	}
	for key, value := range cases {
		t.Run(key, func(t *testing.T) {
			env := map[string]string{"PATH": "/usr/bin:/bin"}
			for k, v := range base {
				env[k] = v
			}
			if key == "size-only" {
				env["OPERATION"] = "size-only"
			} else if key != "valid" {
				env[key] = value
			}
			cmd := exec.Command("bash", "-euo", "pipefail", "-c", guard)
			for k, v := range env {
				cmd.Env = append(cmd.Env, k+"="+v)
			}
			output, err := cmd.CombinedOutput()
			valid := key == "valid" || key == "size-only"
			if (err == nil) != valid || bytes.Contains(output, []byte("PRIVATE_PAYLOAD_MARKER")) {
				t.Fatal("execution guard accepted wrong context or interpolated input")
			}
			if !valid && string(output) != "::error::execution_context\n" {
				t.Fatal("execution guard printed unexpected details")
			}
		})
	}
}

func TestWorkflowOutputValidatorPrivacy(t *testing.T) {
	document, _ := readWorkflow(t)
	steps := document.Jobs["diagnostic"].Steps
	run := steps[len(steps)-1].Run
	_, script, found := strings.Cut(run, "\"$NODE_BINARY\" <<'JS'\n")
	if !found {
		t.Fatal("output validator missing")
	}
	script, _, found = strings.Cut(script, "\nJS\n")
	if !found {
		t.Fatal("output validator delimiter missing")
	}
	valid := emptyAggregate()
	valid.Complete, valid.OriginalUnchanged = true, true
	validJSON, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"valid", "aborted", "raw-error", "extra-value", "huge", "bad-count", "negative", "unverified-original"} {
		t.Run(scenario, func(t *testing.T) {
			payload := append([]byte(nil), validJSON...)
			switch scenario {
			case "valid":
			case "aborted":
				out := emptyAggregate()
				out.AbortReason = "sqlite"
				payload, _ = json.Marshal(out)
			case "raw-error":
				payload = []byte("PRIVATE_PAYLOAD_MARKER")
			case "huge":
				payload = bytes.Repeat([]byte("PRIVATE_PAYLOAD_MARKER"), 1000)
			default:
				var object map[string]any
				if err := json.Unmarshal(payload, &object); err != nil {
					t.Fatal(err)
				}
				switch scenario {
				case "extra-value":
					object["sample"] = "PRIVATE_PAYLOAD_MARKER"
				case "bad-count":
					object["total_cells"] = 1
				case "negative":
					object["valid_text"] = -1
				case "unverified-original":
					object["original_unchanged"] = false
				}
				payload, _ = json.Marshal(object)
			}
			directory := t.TempDir()
			if err := os.WriteFile(filepath.Join(directory, "aggregate.json"), payload, 0600); err != nil {
				t.Fatal(err)
			}
			node, err := exec.LookPath("node")
			if err != nil {
				t.Fatal("existing Node is required for synthetic output validation")
			}
			cmd := exec.Command("env", "-i", "PATH=/usr/bin:/bin", "PRIVATE_TEMP="+directory, node, "-e", script)
			output, err := cmd.CombinedOutput()
			accepted := scenario == "valid" || scenario == "aborted"
			if accepted != (err == nil) || bytes.Contains(output, []byte("PRIVATE_PAYLOAD_MARKER")) || len(output) > 4096 {
				t.Fatalf("output validator privacy/acceptance failed: %s", output)
			}
			if !accepted && string(output) != "{\"schema_version\":1,\"complete\":false,\"abort_reason\":\"output_validation\"}\n" {
				t.Fatal("validator did not return fixed failure enum")
			}
		})
	}
}
