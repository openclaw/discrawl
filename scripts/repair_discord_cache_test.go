//go:build ignore

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
	"go.yaml.in/yaml/v3"
)

func repairFixture(t *testing.T, wal bool) (string, *sql.DB) {
	t.Helper()
	return repairFixtureLayout(t, wal, false)
}

func repairFixtureLayout(t *testing.T, wal, migrated bool) (string, *sql.DB) {
	t.Helper()
	source := filepath.Join(t.TempDir(), ".discrawl-ci")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(source, inputNames[0])
	if migrated {
		legacy, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		// Seed the app's pre-media schema, then let store.Open run its migrations.
		_, createErr := legacy.Exec(`create table message_attachments (
			attachment_id text primary key, message_id text not null, guild_id text not null,
			channel_id text not null, author_id text, filename text not null, content_type text,
			size integer not null default 0, url text, proxy_url text, text_content text not null default '',
			updated_at text not null)`)
		closeErr := legacy.Close()
		if createErr != nil || closeErr != nil {
			t.Fatal("synthetic legacy schema creation failed")
		}
	}
	s, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatal("synthetic store creation failed")
	}
	db := s.DB()
	t.Cleanup(func() { s.Close() })
	if migrated {
		var ddl string
		if db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name='message_attachments'`).Scan(&ddl) != nil ||
			normalizedDDL(ddl) != normalizedDDL(migratedAttachmentDDL) ||
			normalizedDDL(ddl) == normalizedDDL(attachmentDDL) {
			t.Fatal("app migration did not produce the exact legacy layout")
		}
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO message_attachments
		(attachment_id,message_id,guild_id,channel_id,filename,text_content,updated_at)
		VALUES(?,?,?,?,?,CAST(? AS TEXT),?)`)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < int(expectedCells); index++ {
		value := []byte("valid \ufffd")
		if index < 25 {
			value = append(bytes.Repeat([]byte("a"), 8191), 0xc2)
		} else if index < 46 {
			value = append(bytes.Repeat([]byte("b"), 8190), 0xe2, 0x82)
		} else if index < 378 {
			value = bytes.Repeat([]byte("v"), 8192)
		}
		if _, err = stmt.Exec(strconv.Itoa(index), "message", "guild", "channel", "fixture", value, "unchanged"); err != nil {
			t.Fatal("synthetic attachment insertion failed")
		}
	}
	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, table := range share.SnapshotTables {
		if table == "message_attachments" {
			continue
		}
		columns, err := db.Query(`SELECT name,type FROM pragma_table_info(?)`, table)
		if err != nil {
			t.Fatal(err)
		}
		var names, placeholders []string
		var values []any
		for columns.Next() {
			var name, kind string
			if columns.Scan(&name, &kind) != nil {
				t.Fatal("synthetic schema read failed")
			}
			names = append(names, `"`+name+`"`)
			placeholders = append(placeholders, "?")
			if strings.Contains(strings.ToLower(kind), "int") {
				values = append(values, 1)
			} else {
				values = append(values, "{}")
			}
		}
		if err = columns.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(`INSERT INTO "`+table+`" (`+strings.Join(names, ",")+`) VALUES (`+strings.Join(placeholders, ",")+")", values...); err != nil {
			t.Fatal("synthetic snapshot table insertion failed")
		}
	}
	if !wal {
		if _, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec("PRAGMA journal_mode=DELETE"); err != nil {
			t.Fatal(err)
		}
	}
	return source, db
}

func logicalTable(t *testing.T, db *sql.DB, table string) []byte {
	t.Helper()
	rows, err := db.Query(`SELECT * FROM "` + table + `" ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(values))
		for index := range pointers {
			pointers[index] = &values[index]
		}
		if rows.Scan(pointers...) != nil {
			t.Fatal("synthetic comparison read failed")
		}
		for index, value := range values {
			if text, ok := value.(string); ok {
				values[index] = []byte(text)
			}
		}
		result = append(result, values)
	}
	if rows.Err() != nil {
		t.Fatal("synthetic comparison incomplete")
	}
	out, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestExactRepairStandaloneExport(t *testing.T) {
	t.Run("fresh", func(t *testing.T) { testExactRepairStandaloneExport(t, false) })
	t.Run("migrated", func(t *testing.T) { testExactRepairStandaloneExport(t, true) })
}

func testExactRepairStandaloneExport(t *testing.T, migrated bool) {
	t.Helper()
	source, db := repairFixtureLayout(t, false, migrated)
	expectedAttachments := attachmentFixtureState(t, db, true)
	before := map[string][]byte{}
	for _, table := range share.SnapshotTables {
		if table != "message_attachments" {
			before[table] = logicalTable(t, db, table)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	result := runRepair(context.Background(), []string{source, scratch})
	if !result.Complete || result.Error != "none" || !result.OriginalUnchanged ||
		result.ChangedCells != 46 || result.RemovedBytes != 67 || result.Tails != [3]int64{25, 21, 0} ||
		len(result.ExportRows) != 8 || result.ExportRows["message_attachments"] != expectedCells {
		t.Fatalf("repair proof failed: stage=%s error=%s", result.Stage, result.Error)
	}
	assertResultPrivacy(t, result)
	files, err := os.ReadDir(source)
	if err != nil || len(files) != 1 || files[0].Name() != inputNames[0] {
		t.Fatal("standalone output retained sidecars")
	}
	ready, err := openDatabase(filepath.Join(source, inputNames[0]), true)
	if err != nil {
		t.Fatal(err)
	}
	defer ready.Close()
	if !bytes.Equal(expectedAttachments, attachmentFixtureState(t, ready, false)) {
		t.Fatal("attachment fields or storage classes changed beyond the exact suffixes")
	}
	var count, removed int64
	if err = ready.QueryRow(`SELECT count(*),sum(8192-length(CAST(text_content AS BLOB)))
		FROM message_attachments WHERE CAST(attachment_id AS INTEGER)<46`).Scan(&count, &removed); err != nil ||
		count != 46 || removed != 67 {
		t.Fatal("exact suffix totals changed")
	}
	var replaced, updated int
	if ready.QueryRow(`SELECT count(*) FROM message_attachments WHERE text_content='valid `+"\ufffd"+`'`).Scan(&replaced) != nil ||
		replaced != int(expectedCells)-378 ||
		ready.QueryRow(`SELECT count(*) FROM message_attachments WHERE updated_at!='unchanged'`).Scan(&updated) != nil || updated != 0 {
		t.Fatal("valid replacement character or unrelated field changed")
	}
	for table, expected := range before {
		if !bytes.Equal(expected, logicalTable(t, ready, table)) {
			t.Fatal("unrelated table changed")
		}
	}
	if _, err = os.Stat(filepath.Join(scratch, "originals", inputNames[0])); err != nil {
		t.Fatal("original file set was not retained")
	}
}

func attachmentFixtureState(t *testing.T, db *sql.DB, repaired bool) []byte {
	t.Helper()
	var selectColumns []string
	for _, column := range attachmentColumns {
		selectColumns = append(selectColumns, "typeof("+column+")", "CAST("+column+" AS BLOB)")
	}
	rows, err := db.Query("SELECT " + strings.Join(selectColumns, ",") +
		" FROM message_attachments ORDER BY CAST(attachment_id AS INTEGER)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var state [][]any
	for row := 0; rows.Next(); row++ {
		values := make([]any, len(selectColumns))
		destinations := make([]any, len(values))
		for index := range destinations {
			destinations[index] = &values[index]
		}
		if rows.Scan(destinations...) != nil {
			t.Fatal("synthetic attachment state read failed")
		}
		if repaired && row < 46 {
			text := values[21].([]byte)
			drop := 1
			if row >= 25 {
				drop = 2
			}
			values[21] = text[:len(text)-drop]
		}
		state = append(state, values)
	}
	if rows.Err() != nil {
		t.Fatal("synthetic attachment state incomplete")
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestAttachmentSchemaLayouts(t *testing.T) {
	for name, ddl := range map[string]string{"fresh": attachmentDDL, "migrated": migratedAttachmentDDL} {
		t.Run(name, func(t *testing.T) {
			for _, scenario := range []string{
				"accepted", "constraint", "default", "extra-column", "trigger",
				"partial-index", "expression-index", "text-index",
			} {
				t.Run(scenario, func(t *testing.T) {
					db, err := sql.Open("sqlite", ":memory:")
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					definition := ddl
					switch scenario {
					case "constraint":
						definition = strings.Replace(definition, "filename text not null", "filename text", 1)
					case "default":
						definition = strings.Replace(definition, "fetch_status text not null default ''",
							"fetch_status text not null default 'changed'", 1)
					}
					if _, err = db.Exec(definition); err != nil {
						t.Fatal(err)
					}
					statement := map[string]string{
						"extra-column":     "ALTER TABLE message_attachments ADD COLUMN unexpected TEXT",
						"trigger":          "CREATE TRIGGER unexpected AFTER UPDATE ON message_attachments BEGIN SELECT 1; END",
						"partial-index":    "CREATE INDEX unexpected ON message_attachments(message_id) WHERE size > 0",
						"expression-index": "CREATE INDEX unexpected ON message_attachments(length(message_id))",
						"text-index":       "CREATE INDEX unexpected ON message_attachments(text_content)",
					}[scenario]
					if statement != "" {
						if _, err = db.Exec(statement); err != nil {
							t.Fatal(err)
						}
					}
					conn, err := db.Conn(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					err = validateSchema(context.Background(), conn)
					if (err == nil) != (scenario == "accepted") {
						t.Fatal("exact schema admission changed")
					}
				})
			}
		})
	}
}

func TestRepairRollbackAndAdmission(t *testing.T) {
	for _, scenario := range []string{
		"interior", "overlong", "surrogate", "continuation", "wrong-length",
		"missing-candidate", "blob", "schema", "trigger", "text-index", "cas", "mid-transaction",
	} {
		t.Run(scenario, func(t *testing.T) {
			source, db := repairFixture(t, false)
			switch scenario {
			case "interior", "overlong", "surrogate", "continuation", "wrong-length", "missing-candidate":
				body := append(bytes.Repeat([]byte("a"), 8191), 0xff)
				switch scenario {
				case "overlong":
					body = append(bytes.Repeat([]byte("a"), 8190), 0xc0, 0xaf)
				case "surrogate":
					body = append(bytes.Repeat([]byte("a"), 8189), 0xed, 0xa0, 0x80)
				case "continuation":
					body = append(bytes.Repeat([]byte("a"), 8191), 0x80)
				case "wrong-length":
					body = append(bytes.Repeat([]byte("a"), 8190), 0xc2)
				case "missing-candidate":
					body = bytes.Repeat([]byte("a"), 8192)
				}
				if _, err := db.Exec("UPDATE message_attachments SET text_content=CAST(? AS TEXT) WHERE rowid=1", body); err != nil {
					t.Fatal(err)
				}
			case "blob":
				if _, err := db.Exec("UPDATE message_attachments SET text_content=CAST(text_content AS BLOB) WHERE rowid=1"); err != nil {
					t.Fatal(err)
				}
			case "schema":
				if _, err := db.Exec("ALTER TABLE message_attachments ADD COLUMN unexpected TEXT"); err != nil {
					t.Fatal(err)
				}
			case "trigger":
				if _, err := db.Exec("CREATE TRIGGER unexpected AFTER UPDATE ON message_attachments BEGIN UPDATE sync_state SET cursor='changed'; END"); err != nil {
					t.Fatal(err)
				}
			case "text-index":
				if _, err := db.Exec("CREATE INDEX unexpected ON message_attachments(text_content)"); err != nil {
					t.Fatal(err)
				}
			}
			before := logicalTable(t, db, "message_attachments")
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var hook func(*sql.Conn, int) error
			if scenario == "cas" {
				hook = func(c *sql.Conn, index int) error {
					if index == 0 {
						_, err := c.ExecContext(context.Background(), "UPDATE message_attachments SET text_content='changed' WHERE rowid=1")
						return err
					}
					return nil
				}
			} else if scenario == "mid-transaction" {
				hook = func(_ *sql.Conn, index int) error {
					if index == 23 {
						return errors.New("PRIVATE_PAYLOAD_MARKER")
					}
					return nil
				}
			}
			_, repairErr := repairTransaction(context.Background(), conn, hook)
			conn.Close()
			if repairErr == nil || !bytes.Equal(before, logicalTable(t, db, "message_attachments")) {
				t.Fatal("rejected repair did not roll back atomically")
			}
			if strings.Contains(repairErr.Error(), "PRIVATE_PAYLOAD_MARKER") {
				t.Fatal("injected private error escaped")
			}
			_ = source
		})
	}
}

func TestStrictExportRejectsOtherInvalidColumn(t *testing.T) {
	source, db := repairFixture(t, false)
	if _, err := db.Exec("UPDATE messages SET content=CAST(x'ff' AS TEXT)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	result := runRepair(context.Background(), []string{source, t.TempDir()})
	if result.Complete || result.Stage != "export" || result.Error != "strict_export" || !result.OriginalUnchanged {
		t.Fatalf("invalid unrelated text admitted: stage=%s error=%s", result.Stage, result.Error)
	}
	assertResultPrivacy(t, result)
}

func TestWALOriginalFilesPreserved(t *testing.T) {
	source, db := repairFixture(t, true)
	before, _, err := inspectOriginals(source)
	if err != nil || before[inputNames[2]].info == nil || before[inputNames[2]].info.Size() == 0 {
		t.Fatal("synthetic WAL commits missing")
	}
	for name, stamp := range before {
		stamp.hash, err = hashOriginal(context.Background(), source, name, stamp.info)
		if err != nil {
			t.Fatal(err)
		}
		before[name] = stamp
	}
	result := runRepair(context.Background(), []string{source, t.TempDir()})
	if !result.Complete || !result.OriginalUnchanged {
		t.Fatalf("WAL-inclusive repair failed: stage=%s error=%s", result.Stage, result.Error)
	}
	db.Close()
}

func TestStrictCandidateClassifier(t *testing.T) {
	for _, test := range []struct {
		tail []byte
		want int
	}{
		{[]byte{0xc2}, 1},
		{[]byte{0xe2, 0x82}, 2},
		{[]byte{0xf0, 0x9f, 0x92}, 3},
		{[]byte{0xff}, 0},
		{[]byte{0xc0, 0xaf}, 0},
		{[]byte{0xed, 0xa0, 0x80}, 0},
		{[]byte("\ufffd"), 0},
	} {
		value := append(bytes.Repeat([]byte("a"), 8192-len(test.tail)), test.tail...)
		if got := incompleteTail(value); got != test.want {
			t.Fatal("strict byte predicate changed")
		}
		if test.want > 0 && !utf8.Valid(value[:len(value)-test.want]) {
			t.Fatal("candidate prefix is not valid UTF-8")
		}
	}
}

func assertResultPrivacy(t *testing.T, result repairResult) {
	t.Helper()
	data, err := json.Marshal(result)
	if err != nil || len(data) > 4096 || bytes.Contains(data, []byte("PRIVATE_PAYLOAD_MARKER")) ||
		bytes.Contains(data, []byte("/home/")) || bytes.Contains(data, []byte("attachment_id")) {
		t.Fatal("repair output escaped fixed aggregate schema")
	}
}

type workflowStep struct {
	Name string         `yaml:"name"`
	ID   string         `yaml:"id"`
	Uses string         `yaml:"uses"`
	If   string         `yaml:"if"`
	Run  string         `yaml:"run"`
	With map[string]any `yaml:"with"`
	Env  map[string]any `yaml:"env"`
}

type workflowJob struct {
	If          string            `yaml:"if"`
	Needs       string            `yaml:"needs"`
	Timeout     int               `yaml:"timeout-minutes"`
	Permissions map[string]string `yaml:"permissions"`
	Concurrency map[string]any    `yaml:"concurrency"`
	Steps       []workflowStep    `yaml:"steps"`
}

type workflowDocument struct {
	On          map[string]any         `yaml:"on"`
	Permissions map[string]string      `yaml:"permissions"`
	Jobs        map[string]workflowJob `yaml:"jobs"`
}

func readWorkflow(t *testing.T) (workflowDocument, []byte) {
	t.Helper()
	var raw []byte
	var err error
	for _, prefix := range []string{".", ".."} {
		raw, err = os.ReadFile(filepath.Join(prefix, ".github/workflows/repair-discord-cache.yml"))
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	var document workflowDocument
	if yaml.Unmarshal(raw, &document) != nil {
		t.Fatal("workflow is not valid YAML")
	}
	return document, raw
}

func stepScript(t *testing.T, document workflowDocument, id string) string {
	t.Helper()
	for _, step := range document.Jobs["repair"].Steps {
		if step.ID == id {
			return step.With["script"].(string)
		}
	}
	t.Fatal("expected workflow script missing")
	return ""
}

func runNode(t *testing.T, program string, args ...string) ([]byte, error) {
	t.Helper()
	binary := os.Getenv("NODE_BINARY")
	if binary == "" {
		binary = "node"
	}
	command := exec.Command(binary, append([]string{"-e", program}, args...)...)
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir()}
	return command.CombinedOutput()
}

func TestWorkflowContract(t *testing.T) {
	document, raw := readWorkflow(t)
	if len(document.On) != 3 || document.On["pull_request"] == nil || document.On["push"] == nil ||
		document.On["workflow_dispatch"] == nil || len(document.Jobs) != 2 ||
		!reflect.DeepEqual(document.Permissions, map[string]string{"contents": "read"}) {
		t.Fatal("workflow trigger/permission boundary widened")
	}
	paths := []any{
		".github/workflows/repair-discord-cache.yml", "scripts/repair_discord_cache.go",
		"scripts/repair_discord_cache_test.go", "go.mod", "go.sum", "internal/store/**", "internal/share/**",
		".github/workflows/publish-discord-backup.yml", ".github/workflows/discord-backup-report.yml",
	}
	for _, event := range []string{"pull_request", "push"} {
		trigger := document.On[event].(map[string]any)
		if !reflect.DeepEqual(trigger["paths"], paths) ||
			(event == "push" && !reflect.DeepEqual(trigger["branches"], []any{"main"})) {
			t.Fatal("explicit helper validation path coverage changed")
		}
	}
	repair := document.Jobs["repair"]
	if repair.Needs != "validate" || repair.Timeout != 60 ||
		!reflect.DeepEqual(repair.Concurrency, map[string]any{"group": "discord-backup-repo", "cancel-in-progress": false, "queue": "max"}) {
		t.Fatal("writer admission or shared queue changed")
	}
	for _, guard := range []string{
		"github.event_name == 'workflow_dispatch'", "github.ref == 'refs/heads/main'",
		"github.actor == 'vincentkoc'", "github.triggering_actor == 'vincentkoc'",
		"github.run_attempt == '1'", "inputs.root_durable_ack == true", "needs.validate.result == 'success'",
	} {
		if !strings.Contains(repair.If, guard) {
			t.Fatal("data job guard missing")
		}
	}
	validation := document.Jobs["validate"]
	if validation.Concurrency != nil {
		t.Fatal("read-only validation acquired the writer group")
	}
	var proof string
	for _, step := range validation.Steps {
		proof += step.Run
		if strings.Contains(step.Uses, "cache") {
			t.Fatal("validation may access app caches")
		}
	}
	for _, command := range []string{
		"go test -count=1 scripts/repair_discord_cache.go scripts/repair_discord_cache_test.go",
		"go test -count=1 -race scripts/repair_discord_cache.go scripts/repair_discord_cache_test.go",
		"go vet scripts/repair_discord_cache.go scripts/repair_discord_cache_test.go", "go build -trimpath",
		"git diff --exit-code -- go.mod go.sum",
	} {
		if !strings.Contains(proof, command) {
			t.Fatal("explicit-file helper CI proof missing")
		}
	}
	for _, forbidden := range []string{
		"secrets.", "upload-artifact", "schedule:", "workflow_run:", "pull_request_target:",
		"always()", "cache/save@", "cache/restore@", "git push", "go run ./cmd/", "sudo ", "/rerun", "/cancel",
	} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Fatal("forbidden maintenance route")
		}
	}
	wrappers, guards := []string{}, []string{}
	saveIndex, helperIndex, gateIndex := -1, -1, -1
	for index, step := range repair.Steps {
		if strings.HasPrefix(step.Uses, "actions/checkout@") && step.With["persist-credentials"] != false {
			t.Fatal("checkout retained credentials")
		}
		if strings.HasPrefix(step.Uses, "actions/setup-go@") && step.With["cache"] != false {
			t.Fatal("setup-go enabled save hooks")
		}
		if step.ID == "helper" {
			helperIndex = index
		}
		if step.ID == "save-gate" {
			gateIndex = index
		}
		if step.ID == "save" {
			saveIndex = index
			if !strings.Contains(step.If, "steps.helper.outputs.cache_ready == 'true'") ||
				!strings.Contains(step.If, "steps.save-gate.outcome == 'success'") {
				t.Fatal("save escaped helper/current-winner gates")
			}
		}
		if step.ID == "lookup" || step.ID == "restore" || step.ID == "save" {
			wrappers = append(wrappers, step.With["script"].(string))
		}
		if step.Env["PHASE"] != nil {
			guards = append(guards, step.With["script"].(string))
		}
	}
	if len(wrappers) != 3 || wrappers[0] != wrappers[1] || wrappers[0] != wrappers[2] ||
		len(guards) != 4 || guards[0] != guards[1] || guards[0] != guards[2] || guards[0] != guards[3] ||
		helperIndex < 0 || gateIndex <= helperIndex || saveIndex <= gateIndex {
		t.Fatal("shared guarded restore/save route drifted")
	}
}

func fixtureInputs() map[string]any {
	version := sha256.Sum256([]byte(".discrawl-ci/discrawl.db|.discrawl-ci/discrawl.db-shm|.discrawl-ci/discrawl.db-wal|zstd-without-long|1.0"))
	cache, _ := json.Marshal(map[string]any{
		"id": 42, "key": "discrawl-discord-db-Linux-main-100-1",
		"ref": "refs/heads/main", "version": hex.EncodeToString(version[:]),
		"created_at": "2026-01-01T00:00:00.000000000Z", "size_in_bytes": 1024,
	})
	return map[string]any{
		"expected_sha": strings.Repeat("a", 40), "cache_identity": string(cache),
		"backup_run_id": "200", "backup_run_attempt": "1", "backup_source_sha": strings.Repeat("b", 40),
		"backup_artifact_id": "300", "backup_artifact_digest": "sha256:" + strings.Repeat("c", 64),
		"backup_ciphertext_sha256": strings.Repeat("d", 64), "root_durable_ack": true,
	}
}

func TestContextGuards(t *testing.T) {
	document, _ := readWorkflow(t)
	script := stepScript(t, document, "context")
	for _, scenario := range []string{
		"valid", "ack", "actor", "triggering-actor", "attempt", "event", "ref", "sha",
		"workflow-sha", "workflow-ref", "debug", "unknown", "missing", "malformed-cache", "private-hash", "source-key-nonnumeric",
	} {
		t.Run(scenario, func(t *testing.T) {
			input := fixtureInputs()
			env := map[string]string{
				"GITHUB_REPOSITORY": "openclaw/discrawl", "RUNNER_OS": "Linux", "GITHUB_EVENT_NAME": "workflow_dispatch",
				"GITHUB_REF": "refs/heads/main", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_ACTOR": "vincentkoc",
				"TRIGGERING_ACTOR": "vincentkoc", "GITHUB_SHA": strings.Repeat("a", 40), "EXPECTED_SHA": strings.Repeat("a", 40),
				"WORKFLOW_SHA": strings.Repeat("a", 40), "WORKFLOW_REF": "openclaw/discrawl/.github/workflows/repair-discord-cache.yml@refs/heads/main",
			}
			switch scenario {
			case "ack":
				input["root_durable_ack"] = false
			case "actor":
				env["GITHUB_ACTOR"] = "other"
			case "triggering-actor":
				env["TRIGGERING_ACTOR"] = "other"
			case "attempt":
				env["GITHUB_RUN_ATTEMPT"] = "2"
			case "event":
				env["GITHUB_EVENT_NAME"] = "pull_request"
			case "ref":
				env["GITHUB_REF"] = "refs/heads/maintenance/example"
			case "sha":
				env["GITHUB_SHA"] = strings.Repeat("e", 40)
			case "workflow-sha":
				env["WORKFLOW_SHA"] = strings.Repeat("e", 40)
			case "workflow-ref":
				env["WORKFLOW_REF"] = "other"
			case "debug":
				env["RUNNER_DEBUG"] = "1"
			case "unknown", "private-hash":
				input["private_hash"] = "PRIVATE_PAYLOAD_MARKER"
			case "missing":
				delete(input, "backup_ciphertext_sha256")
			case "malformed-cache":
				input["cache_identity"] = "PRIVATE_PAYLOAD_MARKER"
			case "source-key-nonnumeric":
				var cache map[string]any
				if json.Unmarshal([]byte(input["cache_identity"].(string)), &cache) != nil {
					t.Fatal("synthetic cache input invalid")
				}
				cache["key"] = "discrawl-discord-db-Linux-main-unexpected"
				raw, _ := json.Marshal(cache)
				input["cache_identity"] = string(raw)
			}
			raw, _ := json.Marshal(input)
			env["DISPATCH_INPUTS"] = string(raw)
			payload, _ := json.Marshal(env)
			program := `const env=JSON.parse(globalThis.process.argv[1]),failed=[];
const process={env},core={setFailed:value=>failed.push(value)};
` + script + `
globalThis.process.stdout.write(JSON.stringify(failed));`
			output, err := runNode(t, program, string(payload))
			want := `["execution_context"]`
			if scenario == "valid" {
				want = "[]"
			}
			if err != nil || string(output) != want || bytes.Contains(output, []byte("PRIVATE_PAYLOAD_MARKER")) {
				t.Fatal("context guard accepted unsafe input or leaked details")
			}
		})
	}
}

func TestMetadataWinnerAndSaveReadback(t *testing.T) {
	document, _ := readWorkflow(t)
	script := stepScript(t, document, "before")
	for _, scenario := range []string{
		"before", "copy", "save", "after", "main-drift", "newer-winner", "last-access",
		"incomplete", "duplicate", "tie", "cache-drift", "backup-failed", "artifact-mismatch", "artifact-expired",
		"active-writer", "new-key-exists", "save-warning-no-cache", "bad-new-version", "old-new-time",
		"pending-normal", "queued-normal", "waiting-normal", "requested-normal", "queued-writer", "after-queued-publisher",
		"validation-running", "validation-only", "writer-missing", "jobs-incomplete", "jobs-next-page", "jobs-duplicate",
		"jobs-wrong-attempt", "jobs-wrong-run", "jobs-wrong-source", "jobs-unknown", "jobs-unknown-status",
		"run-attempt-invalid", "jobs-error", "newer-nonnumeric-winner", "older-nonnumeric-key",
	} {
		t.Run(scenario, func(t *testing.T) {
			payload, _ := json.Marshal(fixtureInputs())
			program := `const input=JSON.parse(globalThis.process.argv[1]),scenario=globalThis.process.argv[2];
const expected=JSON.parse(input.cache_identity),created="2026-01-02T00:00:00.000000000Z";
const saved={...expected,id:43,key:"discrawl-discord-db-Linux-main-400-1",created_at:created};
const phase=["after","after-queued-publisher","save-warning-no-cache","bad-new-version","old-new-time"].includes(scenario)?"after":
  ["copy","save"].includes(scenario)?scenario:"before";
const env={DISPATCH_INPUTS:JSON.stringify(input),GITHUB_RUN_ID:"400",GITHUB_RUN_ATTEMPT:"1",GITHUB_SHA:input.expected_sha,PHASE:phase};
let caches=[{...expected}];
if(phase==="after"&&scenario!=="save-warning-no-cache")caches.push(saved);
if(scenario==="main-drift")env.GITHUB_SHA="different";
if(scenario==="newer-winner")caches.push({...saved,key:"discrawl-discord-db-Linux-main-350-1"});
if(scenario==="newer-nonnumeric-winner")caches.push({...saved,key:"discrawl-discord-db-Linux-main-unexpected"});
if(scenario==="older-nonnumeric-key")caches.push({...expected,id:44,key:"discrawl-discord-db-Linux-main-legacy",
  created_at:"2025-01-01T00:00:00.000000000Z"});
if(scenario==="last-access")caches[0].last_accessed_at="2099-01-01T00:00:00Z";
if(scenario==="duplicate")caches.push({...expected});
if(scenario==="tie")caches.push({...expected,id:44,key:"discrawl-discord-db-Linux-main-150-1"});
if(scenario==="cache-drift")caches[0].size_in_bytes++;
if(scenario==="new-key-exists")caches.push(saved);
if(scenario==="bad-new-version")saved.version="different";
if(scenario==="old-new-time")saved.created_at="2025-01-01T00:00:00Z";
const waiting={"pending-normal":"pending","queued-normal":"queued","waiting-normal":"waiting",
  "requested-normal":"requested","after-queued-publisher":"queued"};
const repairRun=["validation-running","validation-only","writer-missing"].includes(scenario);
const hasOther=Object.hasOwn(waiting,scenario)||repairRun||scenario.startsWith("jobs-")||
  ["active-writer","queued-writer","run-attempt-invalid"].includes(scenario);
const other={id:999,status:waiting[scenario]||"in_progress",run_attempt:scenario==="run-attempt-invalid"?0:2,
  head_sha:"e".repeat(40)};
const writer={id:1001,run_id:999,run_attempt:2,head_sha:other.head_sha,
  name:repairRun?"Manually acknowledged repair and cache install":"publish",
  status:scenario==="active-writer"?"in_progress":"queued"};
let jobs=[writer],jobRequests=0;
if(repairRun)jobs.unshift({...writer,id:1000,name:"Repair helper synthetic validation",
  status:scenario==="writer-missing"?"completed":"in_progress"});
if(["validation-only","writer-missing"].includes(scenario))jobs.pop();
if(scenario==="jobs-duplicate")jobs.push({...writer});
if(scenario==="jobs-wrong-attempt")writer.run_attempt=1;
if(scenario==="jobs-wrong-run")writer.run_id=998;
if(scenario==="jobs-wrong-source")writer.head_sha="f".repeat(40);
if(scenario==="jobs-unknown")writer.name="unknown";
if(scenario==="jobs-unknown-status")writer.status="unknown";
const github={rest:{
  git:{getRef:async()=>({data:{object:{sha:input.expected_sha}}})},
  actions:{
    getWorkflowRun:async({run_id})=>({data:run_id===400?
      {head_sha:input.expected_sha,run_attempt:1,run_started_at:"2026-01-01T12:00:00Z"}:
      {id:200,repository:{full_name:"openclaw/discrawl"},head_sha:input.backup_source_sha,run_attempt:1,
       status:"completed",conclusion:scenario==="backup-failed"?"failure":"success",path:".github/workflows/publish-discord-backup.yml"}}),
    getArtifact:async()=>({data:{id:300,workflow_run:{id:200,head_sha:input.backup_source_sha},expired:scenario==="artifact-expired",
      size_in_bytes:1024,digest:scenario==="artifact-mismatch"?"different":input.backup_artifact_digest}}),
    listWorkflowRuns:async({workflow_id,status})=>{
      const found=hasOther&&status===other.status&&workflow_id===(repairRun?"repair-discord-cache.yml":"publish-discord-backup.yml");
      return {data:{total_count:found?1:0,workflow_runs:found?[other]:[]},headers:{}};
    },
    listJobsForWorkflowRunAttempt:async({run_id,attempt_number,per_page,page})=>{
      jobRequests++;
      if(run_id!==999||attempt_number!==2||per_page!==100||page!==1||scenario==="jobs-error")
        throw new Error("PRIVATE_PAYLOAD_MARKER");
      return {data:{total_count:jobs.length+(scenario==="jobs-incomplete"?1:0),jobs},
        headers:scenario==="jobs-next-page"?{link:'placeholder; rel="next"'}:{}};
    },
    getActionsCacheList:async()=>({data:{total_count:caches.length+(scenario==="incomplete"?1:0),actions_caches:caches},headers:{}})
  }
}};
const failed=[],outputs=[],info=[];
const process={env},core={setFailed:x=>failed.push(x),setOutput:(k,v)=>outputs.push([k,v]),info:x=>info.push(x)};
(async()=>{` + script + `})().then(()=>globalThis.process.stdout.write(JSON.stringify({failed,outputs,info,jobRequests})));`
			output, err := runNode(t, program, string(payload), scenario)
			var result struct {
				Failed      []string `json:"failed"`
				Outputs     [][]any  `json:"outputs"`
				Info        []string `json:"info"`
				JobRequests int      `json:"jobRequests"`
			}
			valid := scenario == "before" || scenario == "copy" || scenario == "save" || scenario == "after" || scenario == "last-access" ||
				scenario == "pending-normal" || scenario == "queued-normal" || scenario == "waiting-normal" || scenario == "requested-normal" ||
				scenario == "queued-writer" || scenario == "after-queued-publisher" || scenario == "validation-running" ||
				scenario == "validation-only" || scenario == "older-nonnumeric-key"
			if err != nil || json.Unmarshal(output, &result) != nil || valid != (len(result.Failed) == 0) ||
				bytes.Contains(output, []byte("PRIVATE_PAYLOAD_MARKER")) {
				t.Fatalf("metadata disposition incorrect for %s", scenario)
			}
			jobLookup := strings.HasPrefix(scenario, "jobs-") || scenario == "active-writer" || scenario == "queued-writer" ||
				scenario == "validation-running" || scenario == "validation-only" || scenario == "writer-missing"
			expectedRequests := 0
			if jobLookup {
				expectedRequests = 1
			}
			if result.JobRequests != expectedRequests {
				t.Fatal("writer inspection did not use one complete exact-attempt job response")
			}
			after := scenario == "save-warning-no-cache" || scenario == "bad-new-version" || scenario == "old-new-time"
			if !valid && (!reflect.DeepEqual(result.Failed, []string{"metadata_gate"}) ||
				len(result.Info) != map[bool]int{false: 0, true: 1}[after] || len(result.Outputs) != 0) {
				t.Fatal("failed metadata emitted success outputs")
			}
		})
	}
}

func TestRestoreCapacityAdmission(t *testing.T) {
	document, _ := readWorkflow(t)
	var run string
	contextIndex, buildIndex, lookupIndex := -1, -1, -1
	for index, step := range document.Jobs["repair"].Steps {
		switch step.ID {
		case "context":
			contextIndex = index
		case "build":
			buildIndex, run = index, step.Run
		case "lookup":
			lookupIndex = index
		}
	}
	_, check, start := strings.Cut(run, "<<'CAPACITY'\n")
	check, _, end := strings.Cut(check, "\nCAPACITY\n")
	if !start || !end || contextIndex < 0 || buildIndex <= contextIndex || lookupIndex <= buildIndex ||
		strings.Index(run, "go build ") > strings.Index(run, "<<'CAPACITY'") ||
		!strings.Contains(run, `env -i PATH=/usr/bin:/bin DISPATCH_INPUTS="$DISPATCH_INPUTS" "$NODE_BINARY" <<'CAPACITY'`) {
		t.Fatal("validated measured capacity admission moved before build or after private access")
	}
	const gib = uint64(1 << 30)
	for _, test := range []struct {
		name string
		k    uint64
		free uint64
		want bool
	}{
		{"below", gib, 19*gib - 1, false},
		{"at", gib, 19 * gib, true},
		{"above", gib, 19*gib + 1, true},
		{"larger-cache", 2 * gib, 19 * gib, false},
		{"largest-cache", 8 * gib, 26 * gib, true},
		{"zero-cache", 0, 18 * gib, false},
		{"oversized-cache", 8*gib + 1, 27 * gib, false},
		{"unsafe-integer", 1 << 53, 1 << 54, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := fixtureInputs()
			cache, _ := json.Marshal(map[string]uint64{"size_in_bytes": test.k})
			input["cache_identity"] = string(cache)
			raw, _ := json.Marshal(input)
			program := `process.env.DISPATCH_INPUTS=process.argv[1];
require("node:fs").statfsSync=(path,options)=>{
  if(path!=="."||options.bigint!==true)throw new Error();
  return {bsize:1n,bavail:BigInt(process.argv[2])};
};
` + check
			output, err := runNode(t, program, string(raw), strconv.FormatUint(test.free, 10))
			if (err == nil) != test.want || (test.want && len(output) != 0) ||
				(!test.want && string(output) != "::error::restore_capacity\n") {
				t.Fatal("compressed cache plus source/reserve capacity boundary changed")
			}
		})
	}
}

func TestContainedSaveAndRestore(t *testing.T) {
	document, _ := readWorkflow(t)
	script := stepScript(t, document, "lookup")
	function, _, ok := strings.Cut(script, "\ntry {\n")
	if !ok {
		t.Fatal("capture function boundary missing")
	}
	for _, scenario := range []string{"restore", "save", "save-warning", "save-output", "command", "nonzero", "capture-cap", "timeout"} {
		t.Run(scenario, func(t *testing.T) {
			directory := t.TempDir()
			entry := filepath.Join(directory, "entry.cjs")
			child := `const fs=require("node:fs"),mode=` + strconv.Quote(scenario) + `;
process.stdout.write("PRIVATE_PAYLOAD_MARKER\n");process.stderr.write("PRIVATE_PAYLOAD_MARKER\n");
if(process.env.GITHUB_TOKEN||process.env.EXTRA_PROVIDER_TOKEN||process.env.NODE_OPTIONS)process.exit(3);
if(process.env.INPUT_PATH!==".discrawl-ci/discrawl.db\n.discrawl-ci/discrawl.db-shm\n.discrawl-ci/discrawl.db-wal")process.exit(3);
if(mode==="restore"){
  for(const [name,value]of[["cache-primary-key",process.env.INPUT_KEY],["cache-matched-key",process.env.INPUT_KEY],["cache-hit","true"]])
    fs.appendFileSync(process.env.GITHUB_OUTPUT,name+"="+value+"\n");
}else if(process.env["INPUT_LOOKUP-ONLY"]!==undefined)process.exit(3);
if(mode==="save-warning")process.stdout.write("warning: no cache saved");
if(mode==="save-output")fs.appendFileSync(process.env.GITHUB_OUTPUT,"cache-id=123\n");
if(mode==="command")fs.appendFileSync(process.env.GITHUB_ENV,"INJECTED=PRIVATE_PAYLOAD_MARKER\n");
if(mode==="nonzero")process.exit(2);
if(mode==="capture-cap")process.stdout.write("PRIVATE_PAYLOAD_MARKER".repeat(1000));
if(mode==="timeout")setInterval(()=>{},1000);
`
			if os.WriteFile(entry, []byte(child), 0o600) != nil {
				t.Fatal("synthetic child creation failed")
			}
			payload, _ := json.Marshal(map[string]any{"directory": directory, "entry": entry, "scenario": scenario})
			program := `const input=JSON.parse(process.argv[1]);
Object.assign(process.env,{GITHUB_TOKEN:"placeholder",EXTRA_PROVIDER_TOKEN:"placeholder",NODE_OPTIONS:"not-forwarded"});
` + function + `
containedCache({node:process.execPath,entry:input.entry,workspace:input.directory,privateRoot:input.directory,
key:"discrawl-discord-db-Linux-main-100-1",lookupOnly:"false",saveOnly:input.scenario!=="restore",
timeoutMs:500,streamCap:input.scenario==="capture-cap"?64:4096,commandCap:1024})
.then(output=>process.stdout.write(JSON.stringify({ok:true,output})))
.catch(error=>process.stdout.write(JSON.stringify({ok:false,error:error.message})));`
			output, err := runNode(t, program, string(payload))
			if err != nil || bytes.Contains(output, []byte("PRIVATE_PAYLOAD_MARKER")) || len(output) > 1024 {
				t.Fatal("stock action output escaped capture")
			}
			var result struct {
				OK     bool              `json:"ok"`
				Output map[string]string `json:"output"`
				Error  string            `json:"error"`
			}
			if json.Unmarshal(output, &result) != nil {
				t.Fatal("capture output invalid")
			}
			valid := scenario == "restore" || scenario == "save" || scenario == "save-warning"
			if result.OK != valid || ((scenario == "save" || scenario == "save-warning") && len(result.Output) != 0) {
				t.Fatal("capture/save output contract violated")
			}
		})
	}
}

func TestHelperAdmissionPrivacy(t *testing.T) {
	result := runRepair(context.Background(), []string{"PRIVATE_PAYLOAD_MARKER"})
	if result.Complete || result.Error != "arguments" {
		t.Fatal("invalid invocation accepted")
	}
	assertResultPrivacy(t, result)
}

func TestStandaloneSaveBoundary(t *testing.T) {
	document, _ := readWorkflow(t)
	_, boundary, ok := strings.Cut(stepScript(t, document, "save"), "\ntry {\n")
	if !ok {
		t.Fatal("save boundary missing")
	}
	for _, scenario := range []string{"valid", "wal", "empty", "symlink", "hardlink", "mode", "wrong-key", "unsafe-size"} {
		t.Run(scenario, func(t *testing.T) {
			program := `const fs=require("node:fs"),os=require("node:os"),path=require("node:path");
const scenario=process.argv[1],workspace=fs.mkdtempSync(path.join(os.tmpdir(),"repair-save-test-"));
const root=path.join(workspace,".discrawl-ci"),db=path.join(root,"discrawl.db");
fs.mkdirSync(root,{mode:0o700});fs.writeFileSync(db,"synthetic",{mode:0o600});
if(scenario==="wal")fs.writeFileSync(path.join(root,"discrawl.db-wal"),"synthetic");
if(scenario==="empty")fs.truncateSync(db,0);
if(scenario==="mode")fs.chmodSync(db,0o644);
if(scenario==="symlink"){fs.unlinkSync(db);fs.symlinkSync("missing",db);}
if(scenario==="hardlink")fs.linkSync(db,path.join(workspace,"other"));
if(scenario==="unsafe-size"){
  const lstat=fs.lstatSync;
  fs.lstatSync=(file,...args)=>{const info=lstat(file,...args);if(file===db)info.size=10*1024**3+1;return info;};
}
Object.assign(process.env,{CACHE_MODE:"save",NODE_BINARY:process.execPath,GITHUB_WORKSPACE:workspace,
  GITHUB_RUN_ID:"400",EXPECTED_KEY:"discrawl-discord-db-Linux-main-"+(scenario==="wrong-key"?"399":"400")+"-1"});
const failed=[];let called=0;
const core={setFailed:value=>failed.push(value),setOutput:()=>{throw new Error("unexpected output");}};
async function containedCache(){called++;return {};}
(async()=>{try {
` + boundary + `
})().then(()=>{fs.rmSync(workspace,{recursive:true});process.stdout.write(JSON.stringify({called,failed}));});`
			output, err := runNode(t, program, scenario)
			var result struct {
				Called int      `json:"called"`
				Failed []string `json:"failed"`
			}
			if err != nil || json.Unmarshal(output, &result) != nil ||
				(scenario == "valid" && (result.Called != 1 || len(result.Failed) != 0)) ||
				(scenario != "valid" && (result.Called != 0 || !reflect.DeepEqual(result.Failed, []string{"restore_context"}))) {
				t.Fatal("unsafe standalone directory or key reached cache save")
			}
		})
	}
}

func TestHelperOutputGate(t *testing.T) {
	document, _ := readWorkflow(t)
	var run string
	for _, step := range document.Jobs["repair"].Steps {
		if step.ID == "helper" {
			run = step.Run
		}
	}
	_, validator, ok := strings.Cut(run, "<<'JS'\n")
	validator, _, end := strings.Cut(validator, "\nJS\n")
	if !ok || !end {
		t.Fatal("helper output validator missing")
	}
	for _, scenario := range []string{"valid", "failure", "exit", "counts", "unknown", "malformed", "oversized", "private-error"} {
		t.Run(scenario, func(t *testing.T) {
			result := repairResult{
				SchemaVersion: 1, Scope: "restored_cache.message_attachments.text_content",
				Complete: true, Stage: "cache_ready", Error: "none", OriginalUnchanged: true,
				ChangedCells: 46, RemovedBytes: 67, Tails: [3]int64{25, 21, 0}, ExportRows: map[string]int64{},
			}
			for _, table := range share.SnapshotTables {
				result.ExportRows[table] = 1
			}
			result.ExportRows["message_attachments"] = expectedCells
			if scenario == "failure" || scenario == "private-error" {
				result.Complete, result.Stage, result.Error = false, "export", "strict_export"
				if scenario == "private-error" {
					result.Error = "PRIVATE_PAYLOAD_MARKER"
				}
			}
			if scenario == "counts" {
				result.ChangedCells--
			}
			raw, _ := json.Marshal(result)
			switch scenario {
			case "unknown":
				raw = append(raw[:len(raw)-1], []byte(`,"private":"PRIVATE_PAYLOAD_MARKER"}`)...)
			case "malformed":
				raw = []byte("PRIVATE_PAYLOAD_MARKER")
			case "oversized":
				raw = bytes.Repeat([]byte("PRIVATE_PAYLOAD_MARKER"), 300)
			}
			directory := t.TempDir()
			if os.WriteFile(filepath.Join(directory, "result.json"), raw, 0o600) != nil {
				t.Fatal("synthetic result creation failed")
			}
			program := `process.env.PRIVATE_TEMP=process.argv[1];process.env.HELPER_STATUS=process.argv[2];` + validator
			status := "0"
			if scenario == "exit" {
				status = "1"
			}
			output, err := runNode(t, program, directory, status)
			if (err == nil) != (scenario == "valid") || bytes.Contains(output, []byte("PRIVATE_PAYLOAD_MARKER")) {
				t.Fatal("failed helper result admitted or private output escaped")
			}
			if scenario != "valid" && scenario != "failure" && string(output) != "::error::repair_output\n" {
				t.Fatal("invalid result did not emit the fixed error")
			}
			if scenario == "failure" && string(output) != `{"schema_version":1,"complete":false,"stage":"export","error":"strict_export","original_unchanged":true}`+"\n" {
				t.Fatal("helper failure was not reduced to fixed public fields")
			}
		})
	}
}

func TestSchemaLiteralWhitespaceNotIgnored(t *testing.T) {
	changed := strings.Replace(attachmentDDL, "default ''", "default ' '", 1)
	if normalizedDDL(changed) == normalizedDDL(attachmentDDL) {
		t.Fatal("schema literal changed during normalization")
	}
}

func TestCheckpointAdmissionBeforeWALConsolidation(t *testing.T) {
	for _, scenario := range []string{"reject-checkpoint", "reject-journal", "admit"} {
		t.Run(scenario, func(t *testing.T) {
			original := filepath.Join(t.TempDir(), "fixture.db")
			db, err := sql.Open("sqlite", original)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
				CREATE TABLE fixture(value BLOB); INSERT INTO fixture VALUES(zeroblob(131072));`); err != nil {
				t.Fatal(err)
			}
			logical, err := logicalBytes(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			scratch := t.TempDir()
			file := filepath.Join(scratch, "working.db")
			before := map[string][]byte{}
			for _, suffix := range []string{"", "-wal", "-shm"} {
				body, err := os.ReadFile(original + suffix)
				if err != nil || os.WriteFile(file+suffix, body, 0o600) != nil {
					t.Fatal("synthetic WAL copy failed")
				}
				before[suffix] = body
			}
			if int64(len(before[""])) >= logical {
				t.Fatal("fixture does not require checkpoint growth")
			}
			var requests []int64
			admit := func(directory string, additional int64) error {
				if directory != scratch {
					t.Fatal("capacity checked on the wrong filesystem")
				}
				requests = append(requests, additional)
				if len(requests) == 1 && scenario == "reject-checkpoint" ||
					len(requests) == 2 && scenario == "reject-journal" {
					return errors.New("capacity")
				}
				return nil
			}
			working, size, err := prepareWorkingDatabase(context.Background(), file, scratch, admit)
			if scenario == "admit" {
				if err != nil || working == nil || size != logical {
					t.Fatal("measured checkpoint admission failed")
				}
				working.Close()
			} else if err == nil || err.Error() != "capacity" || working != nil {
				t.Fatal("failed capacity admission retained a writable DB")
			}
			expected := []int64{logical, 2 * logical}
			if scenario == "reject-checkpoint" {
				expected = expected[:1]
				for _, suffix := range []string{"", "-wal"} {
					after, err := os.ReadFile(file + suffix)
					if err != nil || !bytes.Equal(after, before[suffix]) {
						t.Fatal("rejected admission checkpointed the copied WAL")
					}
				}
			}
			if !reflect.DeepEqual(requests, expected) {
				t.Fatal("checkpoint or journal/backup headroom was not admitted")
			}
		})
	}
}
