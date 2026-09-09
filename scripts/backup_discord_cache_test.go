//go:build ignore

package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	_ "modernc.org/sqlite"
)

func syntheticExpectation() expectation {
	version := sha256.Sum256([]byte(".discrawl-ci/discrawl.db|.discrawl-ci/discrawl.db-shm|.discrawl-ci/discrawl.db-wal|zstd-without-long|1.0"))
	return expectation{
		Repository: "openclaw/discrawl", Revision: strings.Repeat("a", 40), RunID: 123, Attempt: 1,
		Cache: cacheIdentity{1, "discrawl-discord-db-Linux-main-100-1", "refs/heads/main", hex.EncodeToString(version[:]), "2026-09-09T00:00:00.000Z", 100},
	}
}

func ampleSpace() operation {
	return operation{space: func(string) (int64, error) { return 100 << 30, nil }}
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func ephemeralIdentity(t *testing.T) *age.X25519Identity {
	t.Helper()
	// Synthetic in-memory recipient only; never persisted or used operationally.
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func fixture(t *testing.T) (string, map[string][]byte) {
	t.Helper()
	source := filepath.Join(t.TempDir(), "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(source, "discrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0",
		"CREATE TABLE payload (text_value TEXT, blob_value BLOB)",
		"INSERT INTO payload VALUES (CAST(x'0061ff80e282' AS TEXT), x'ff000180f09f')",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	// Keep the connection idle, so WAL remains present without concurrent writes.
	original := make(map[string][]byte)
	for _, name := range names[:3] {
		data, err := os.ReadFile(filepath.Join(source, filepath.Base(name)))
		if err != nil || len(data) == 0 {
			t.Fatalf("missing WAL fixture member: %v", err)
		}
		original[name] = data
	}
	return source, original
}

func assertOriginals(t *testing.T, source string, original map[string][]byte) {
	t.Helper()
	for name, want := range original {
		got, err := os.ReadFile(filepath.Join(source, filepath.Base(name)))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("original changed: %s", name)
		}
	}
}

func TestBackupWALAndArbitraryBytesRoundTrip(t *testing.T) {
	source, original := fixture(t)
	id := ephemeralIdentity(t)
	root := t.TempDir()
	work, destination := filepath.Join(root, "work"), filepath.Join(root, "retained")
	digest, size, err := produce(context.Background(), source, work, syntheticExpectation(), id.Recipient(), ampleSpace())
	if err != nil {
		t.Fatal(err)
	}
	assertOriginals(t, source, original)
	cipher := filepath.Join(work, "backup.tar.gz.age")
	b, err := readEnvelope(context.Background(), cipher, id, syntheticExpectation(), digest, size, "", ampleSpace())
	if err != nil {
		t.Fatal(err)
	}
	if b.Files[1].Size == 0 || b.Files[2].Size == 0 {
		t.Fatal("WAL/SHM not bound")
	}
	if err := verify(context.Background(), cipher, destination, syntheticExpectation(), id, digest, size, ampleSpace()); err != nil {
		t.Fatal(err)
	}
	for name, want := range original {
		got, err := os.ReadFile(filepath.Join(destination, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("original did not round trip: %s", name)
		}
	}
	before := filepath.Join(destination, "consistent", "before.sqlite")
	// Query only another disposable copy, not the retained evidence.
	queryDir := t.TempDir()
	if _, err := copyFile(context.Background(), before, filepath.Join(queryDir, "query.sqlite"), b.Files[3].Size); err != nil {
		t.Fatal(err)
	}
	out, err := sqlite(context.Background(), queryDir, "query", []string{"query.sqlite", "SELECT typeof(text_value),hex(CAST(text_value AS BLOB)),typeof(blob_value),hex(blob_value) FROM payload;"}, "", 0, ampleSpace())
	if err != nil || string(out) != "text|0061FF80E282|blob|FF000180F09F\n" {
		t.Fatalf("coherent backup lost WAL or changed bytes: %q, %v", out, err)
	}
	for _, path := range []string{cipher, before, filepath.Join(destination, "binding.json"), filepath.Join(destination, "VERIFIED")} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("private file mode: %v", err)
		}
	}
	encrypted, err := os.ReadFile(cipher)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"original/discrawl.db", "binding.json", b.Files[0].SHA256, syntheticExpectation().Cache.Key, "text_value"} {
		if bytes.Contains(encrypted, []byte(private)) {
			t.Fatal("private envelope metadata exposed")
		}
	}
	assertOriginals(t, source, original)
}

func TestOptionalOriginalsAndExistingDestinations(t *testing.T) {
	source, _ := fixture(t)
	// Build a standalone synthetic DB through the approved backup operation.
	id := ephemeralIdentity(t)
	work := filepath.Join(t.TempDir(), "first")
	_, _, err := produce(context.Background(), source, work, syntheticExpectation(), id.Recipient(), ampleSpace())
	if err != nil {
		t.Fatal(err)
	}
	secondSource := t.TempDir()
	info, err := os.Stat(filepath.Join(work, names[3]))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copyFile(context.Background(), filepath.Join(work, names[3]), filepath.Join(secondSource, "discrawl.db"), info.Size()); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(t.TempDir(), "second")
	digest, size, err := produce(context.Background(), secondSource, second, syntheticExpectation(), id.Recipient(), ampleSpace())
	if err != nil {
		t.Fatal(err)
	}
	b, err := readEnvelope(context.Background(), filepath.Join(second, "backup.tar.gz.age"), id, syntheticExpectation(), digest, size, "", ampleSpace())
	if err != nil || b.Files[1].Present || b.Files[2].Present {
		t.Fatalf("absent sidecars misrepresented: %v", err)
	}
	if _, _, err := produce(context.Background(), secondSource, second, syntheticExpectation(), id.Recipient(), ampleSpace()); err == nil {
		t.Fatal("reused producer destination")
	}
	if err := verify(context.Background(), filepath.Join(second, "backup.tar.gz.age"), second, syntheticExpectation(), id, digest, size, ampleSpace()); err == nil {
		t.Fatal("reused retained destination")
	}
}

func tinyBinding() (binding, [4][]byte) {
	data := [4][]byte{[]byte("synthetic-main"), []byte("synthetic-wal"), nil, []byte("synthetic-consistent")}
	b := binding{Schema: 1, Source: syntheticExpectation()}
	for i, name := range names {
		b.Files[i] = fileRecord{Name: name}
		if data[i] != nil {
			sum := sha256.Sum256(data[i])
			b.Files[i] = fileRecord{name, true, int64(len(data[i])), hex.EncodeToString(sum[:])}
		}
	}
	return b, data
}

func canonicalTar(t *testing.T, b binding, data [4][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for i, record := range b.Files {
		if !record.Present {
			continue
		}
		if err := tw.WriteHeader(header(record.Name, record.Size)); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(data[i]); err != nil {
			t.Fatal(err)
		}
	}
	bindingData, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(header("binding.json", int64(len(bindingData)))); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(bindingData); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func seal(t *testing.T, plaintext []byte, id *age.X25519Identity) []byte {
	t.Helper()
	return sealCompressed(t, gzipBytes(t, plaintext, nil), id)
}

func gzipBytes(t *testing.T, plaintext, extra []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	w.Extra = extra
	if _, err := w.Write(plaintext); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func sealCompressed(t *testing.T, compressed []byte, id *age.X25519Identity) []byte {
	t.Helper()
	var out bytes.Buffer
	w, err := age.Encrypt(&out, id.Recipient())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(compressed); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func readSynthetic(t *testing.T, ciphertext []byte, id *age.X25519Identity, expected expectation) error {
	t.Helper()
	path := filepath.Join(t.TempDir(), "backup.tar.gz.age")
	mustWrite(t, path, ciphertext)
	h := sha256.Sum256(ciphertext)
	_, err := readEnvelope(context.Background(), path, id, expected, hex.EncodeToString(h[:]), int64(len(ciphertext)), "", ampleSpace())
	return err
}

func TestAuthenticatedEOFAndTampering(t *testing.T) {
	id := ephemeralIdentity(t)
	b, data := tinyBinding()
	// Align the gzip-member EOF with a STREAM boundary. A complete tar and
	// gzip trailer must not hide a missing final authenticated age chunk.
	plain := canonicalTar(t, b, data)
	compressed := gzipBytes(t, plain, nil)
	compressed = gzipBytes(t, plain, make([]byte, (64<<10)-len(compressed)-2))
	if len(compressed) != 64<<10 {
		t.Fatal("fixture does not align gzip EOF with STREAM boundary")
	}
	sealed := sealCompressed(t, compressed, id)
	if err := readSynthetic(t, sealed, id, syntheticExpectation()); err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"truncated_final_byte": sealed[:len(sealed)-1],
		"truncated_chunk":      sealed[:len(sealed)-16],
		"trailing_plaintext":   seal(t, append(append([]byte{}, plain...), []byte("trailer")...), id),
		"trailing_zero_block":  seal(t, append(append([]byte{}, plain...), make([]byte, 512)...), id),
		"trailing_ciphertext":  append(append([]byte{}, sealed...), 0),
	}
	withFinalChunk := sealCompressed(t, append(append([]byte{}, compressed...), 0), id)
	missingFinal := withFinalChunk[:len(withFinalChunk)-17]
	cases["missing_final_after_gzip_eof"] = missingFinal
	unsafePlain, err := age.Decrypt(bytes.NewReader(missingFinal), id)
	if err != nil {
		t.Fatal(err)
	}
	unsafeGzip, err := gzip.NewReader(bufio.NewReader(unsafePlain))
	if err != nil {
		t.Fatal(err)
	}
	defer unsafeGzip.Close()
	unsafeGzip.Multistream(false)
	if got, err := io.ReadAll(unsafeGzip); err != nil || !bytes.Equal(got, plain) {
		t.Fatal("fixture does not discriminate gzip-only validation:", err)
	}
	tampered := append([]byte{}, sealed...)
	tampered[len(tampered)-20] ^= 1
	cases["tampered"] = tampered
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if err := readSynthetic(t, input, id, syntheticExpectation()); err == nil {
				t.Fatal("accepted unauthenticated or noncanonical envelope")
			}
		})
	}
	if err := readSynthetic(t, sealed, ephemeralIdentity(t), syntheticExpectation()); err == nil {
		t.Fatal("accepted wrong identity")
	}
	expected := syntheticExpectation()
	expected.Attempt++
	if err := readSynthetic(t, sealed, id, expected); err == nil {
		t.Fatal("accepted wrong run binding")
	}
}

func TestGzipIntegrityAndSingleMember(t *testing.T) {
	id := ephemeralIdentity(t)
	b, data := tinyBinding()
	plain := canonicalTar(t, b, data)
	compressed := gzipBytes(t, plain, nil)
	if err := readSynthetic(t, sealCompressed(t, compressed, id), id, syntheticExpectation()); err != nil {
		t.Fatal(err)
	}
	badCRC := append([]byte{}, compressed...)
	badCRC[len(badCRC)-8] ^= 1
	badSize := append([]byte{}, compressed...)
	badSize[len(badSize)-4] ^= 1
	emptyMember := append(append([]byte{}, compressed...), gzipBytes(t, nil, nil)...)
	// Default multistream behavior would silently accept an empty second member.
	unsafe, err := gzip.NewReader(bytes.NewReader(emptyMember))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(unsafe); err != nil || !bytes.Equal(got, plain) {
		t.Fatal("fixture does not discriminate default multistream behavior:", err)
	}
	unsafe.Close()
	for name, input := range map[string][]byte{
		"crc":                 badCRC,
		"size_trailer":        badSize,
		"truncated_header":    compressed[:9],
		"truncated_deflate":   compressed[:len(compressed)/2],
		"truncated_trailer":   compressed[:len(compressed)-1],
		"empty_second_member": emptyMember,
		"second_member":       append(append([]byte{}, compressed...), gzipBytes(t, plain, nil)...),
		"trailing_junk":       append(append([]byte{}, compressed...), []byte("fixture-only-trailer")...),
		"trailing_zero":       append(append([]byte{}, compressed...), 0),
		"raw_tar":             plain,
	} {
		t.Run(name, func(t *testing.T) {
			sealed := sealCompressed(t, input, id)
			// Every malformed gzip input has a valid age envelope and digest.
			// Rejection must come from the inner format, not stale ciphertext proof.
			if err := readSynthetic(t, sealed, id, syntheticExpectation()); err == nil {
				t.Fatal("accepted invalid gzip transport")
			}
			root := t.TempDir()
			cipher := filepath.Join(root, "backup.tar.gz.age")
			destination := filepath.Join(root, "retained")
			mustWrite(t, cipher, sealed)
			sum := sha256.Sum256(sealed)
			if err := verify(context.Background(), cipher, destination, syntheticExpectation(), id, hex.EncodeToString(sum[:]), int64(len(sealed)), ampleSpace()); err == nil {
				t.Fatal("invalid gzip passed local verification")
			}
			if _, err := os.Lstat(destination); !os.IsNotExist(err) {
				t.Fatal("extracted before gzip and age validation")
			}
		})
	}
}

func TestGzipExpansionPreservesTarCaps(t *testing.T) {
	id := ephemeralIdentity(t)
	for _, name := range []string{"original", "consistent", "binding"} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			tw := tar.NewWriter(&out)
			file, size := names[0], sourceCap+1
			switch name {
			case "consistent":
				file, size = names[3], copyCap+1
			case "binding":
				file, size = "binding.json", bindingCap+1
			}
			if err := tw.WriteHeader(header(file, size)); err != nil {
				t.Fatal(err)
			}
			// A tiny valid gzip stream must not admit an oversized expanded entry.
			compressed := gzipBytes(t, out.Bytes(), nil)
			if len(compressed) >= 512 {
				t.Fatal("fixture is not compressed")
			}
			if err := readSynthetic(t, sealCompressed(t, compressed, id), id, syntheticExpectation()); err == nil {
				t.Fatal("compressed size bypassed expanded entry cap")
			}
		})
	}
	b, data := tinyBinding()
	plain := append(canonicalTar(t, b, data), make([]byte, 2<<20)...)
	compressed := gzipBytes(t, plain, nil)
	if len(compressed) >= len(plain)/100 {
		t.Fatal("fixture does not exercise substantial expansion")
	}
	if err := readSynthetic(t, sealCompressed(t, compressed, id), id, syntheticExpectation()); err == nil {
		t.Fatal("compressed trailing expansion passed canonical tar validation")
	}
}

func TestGzipBudgetDoesNotAssumeCompression(t *testing.T) {
	id := ephemeralIdentity(t)
	random := rand.New(rand.NewPCG(1, 2))
	for _, size := range []int{0, 1, 16383, 16384, 65535, 65536, 1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(random.Uint32())
			}
			compressed := gzipBytes(t, payload, nil)
			cipher := sealCompressed(t, compressed, id)
			if int64(len(cipher)) > cipherBound(int64(size), 0) {
				t.Fatal("incompressible input exceeded the expansion budget")
			}
		})
	}
}

func TestStrictArchiveAndBinding(t *testing.T) {
	id := ephemeralIdentity(t)
	b, data := tinyBinding()
	for _, name := range []string{"duplicate", "unexpected", "traversal", "absolute", "symlink", "hardlink", "pax", "oversize", "binding_cap", "missing", "hash", "presence", "source", "metadata"} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			tw := tar.NewWriter(&out)
			bad := b
			if name == "hash" {
				bad.Files[0].SHA256 = strings.Repeat("0", 64)
			}
			if name == "presence" {
				bad.Files[2].Present = true
			}
			if name == "source" {
				bad.Source.Cache.ID++
			}
			for i, record := range b.Files {
				if !record.Present || (name == "missing" && i == 0) {
					continue
				}
				h := header(record.Name, record.Size)
				if i == 0 {
					switch name {
					case "unexpected":
						h.Name = "extra"
					case "traversal":
						h.Name = "../escape"
					case "absolute":
						h.Name = "/escape"
					case "symlink":
						h.Typeflag, h.Linkname, h.Size = tar.TypeSymlink, "escape", 0
					case "hardlink":
						h.Typeflag, h.Linkname, h.Size = tar.TypeLink, "escape", 0
					case "pax":
						h.Format, h.PAXRecords = tar.FormatPAX, map[string]string{"private": "fixture-only"}
					case "metadata":
						h.Uname = "fixture-owner"
					case "oversize":
						h.Size = sourceCap + 1
					}
				}
				if err := tw.WriteHeader(h); err != nil {
					t.Fatal(err)
				}
				if h.Typeflag == tar.TypeReg {
					if _, err := tw.Write(data[i]); err != nil {
						t.Fatal(err)
					}
				}
				if name == "oversize" {
					break
				}
				if name == "duplicate" && i == 0 {
					tw.WriteHeader(header(record.Name, record.Size))
					tw.Write(data[i])
				}
			}
			if name != "oversize" {
				payload, err := json.Marshal(bad)
				if err != nil {
					t.Fatal(err)
				}
				if name == "binding_cap" {
					payload = bytes.Repeat([]byte(" "), int(bindingCap+1))
				}
				tw.WriteHeader(header("binding.json", int64(len(payload))))
				tw.Write(payload)
				if err := tw.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := readSynthetic(t, seal(t, out.Bytes(), id), id, syntheticExpectation()); err == nil {
				t.Fatal("accepted invalid archive")
			}
		})
	}
}

func TestCloseFailuresRejectCiphertext(t *testing.T) {
	b, data := tinyBinding()
	id := ephemeralIdentity(t)
	var paths [4]string
	root := t.TempDir()
	for i, payload := range data {
		if payload != nil {
			paths[i] = filepath.Join(root, filepath.Base(names[i]))
			mustWrite(t, paths[i], payload)
		}
	}
	stages := []string{"tar_close", "gzip_close", "age_close", "file_sync", "file_close"}
	for index, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			called := false
			var observed []string
			op := ampleSpace()
			op.fault = func(at string) error {
				observed = append(observed, at)
				if at == stage {
					called = true
					return errors.New("fixture-only-private-diagnostic")
				}
				return nil
			}
			path := filepath.Join(t.TempDir(), "backup.tar.gz.age")
			_, _, err := encrypt(context.Background(), path, id.Recipient(), b, paths, op)
			if !called || err == nil || err.Error() != stage {
				t.Fatalf("close failure not propagated: %v", err)
			}
			if strings.Join(observed, ",") != strings.Join(stages[:index+1], ",") {
				t.Fatal("incorrect finalization order")
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatal("failed ciphertext eligible for upload")
			}
		})
	}
}

func TestMeasuredCapacityAndFailuresPreserveOriginals(t *testing.T) {
	source, original := fixture(t)
	id := ephemeralIdentity(t)
	for _, stage := range []string{"capacity", "pages", "backup", "integrity", "cancelled"} {
		t.Run(stage, func(t *testing.T) {
			op := ampleSpace()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stage == "capacity" {
				op.space = func(string) (int64, error) { return reserve, nil }
			} else if stage == "cancelled" {
				cancel()
			} else {
				op.fault = func(at string) error {
					if at == stage {
						return errors.New("fixture-only-private-path-and-value")
					}
					return nil
				}
			}
			work := filepath.Join(t.TempDir(), "work")
			_, _, err := produce(ctx, source, work, syntheticExpectation(), id.Recipient(), op)
			if err == nil || strings.Contains(err.Error(), "fixture-only") {
				t.Fatalf("private error or false success: %v", err)
			}
			if _, err := os.Stat(filepath.Join(work, "backup.tar.gz.age")); !os.IsNotExist(err) {
				t.Fatal("failure left upload-eligible artifact")
			}
			assertOriginals(t, source, original)
		})
	}
	// A tiny fixture fits far below the cap-case 59 GiB; admission is measured.
	op := operation{space: func(string) (int64, error) { return reserve + 2<<20, nil }}
	work := filepath.Join(t.TempDir(), "small")
	digest, size, err := produce(context.Background(), source, work, syntheticExpectation(), id.Recipient(), op)
	if err != nil {
		t.Fatal("used cap-case admission instead of measured sizes:", err)
	}
	dest := filepath.Join(t.TempDir(), "retain")
	if err := verify(context.Background(), filepath.Join(work, "backup.tar.gz.age"), dest, syntheticExpectation(), id, digest, size, operation{space: func(string) (int64, error) { return reserve, nil }}); err == nil {
		t.Fatal("accepted insufficient local capacity")
	}
	if _, err := os.Lstat(dest); !os.IsNotExist(err) {
		t.Fatal("extracted before local admission")
	}
}

func TestSourceFileBoundsAndLinks(t *testing.T) {
	for _, variant := range []string{"extra", "symlink", "hardlink", "oversize"} {
		t.Run(variant, func(t *testing.T) {
			source := t.TempDir()
			mustWrite(t, filepath.Join(source, "discrawl.db"), []byte("synthetic"))
			switch variant {
			case "extra":
				mustWrite(t, filepath.Join(source, "unexpected"), nil)
			case "symlink":
				if err := os.Symlink("discrawl.db", filepath.Join(source, "discrawl.db-wal")); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(filepath.Join(source, "discrawl.db"), filepath.Join(source, "discrawl.db-wal")); err != nil {
					t.Fatal(err)
				}
			case "oversize":
				if err := os.Truncate(filepath.Join(source, "discrawl.db"), sourceCap+1); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := inspect(source); err == nil {
				t.Fatal("accepted unsafe source")
			}
		})
	}
}

func TestPublicErrorsAndIdentityValidation(t *testing.T) {
	root := t.TempDir()
	expect := filepath.Join(root, "expectation.json")
	e := syntheticExpectation()
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, expect, data)
	if _, err := readExpectation(expect); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*expectation){
		func(e *expectation) { e.Repository = "private.invalid/source" },
		func(e *expectation) { e.Revision = "main" },
		func(e *expectation) { e.Attempt = 0 },
		func(e *expectation) { e.Cache.Version = strings.Repeat("0", 64) },
	} {
		bad := e
		mutate(&bad)
		data, _ := json.Marshal(bad)
		mustWrite(t, expect, data)
		if _, err := readExpectation(expect); err == nil {
			t.Fatal("accepted invalid source binding")
		}
	}
	mustWrite(t, expect, data)
	out := execute(context.Background(), []string{"produce", filepath.Join(root, "private-source"), filepath.Join(root, "work"), expect, "not-a-recipient"}, ampleSpace())
	encoded, _ := json.Marshal(out)
	if out.Complete || out.Error != "recipient" || strings.Contains(string(encoded), root) || strings.Contains(string(encoded), "private-source") {
		t.Fatal("unsafe public result")
	}
	if _, err := os.Lstat(filepath.Join(root, "work")); !os.IsNotExist(err) {
		t.Fatal("mutated before recipient validation")
	}
}

func TestSQLitePrivateErrorsAndTimeout(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "not.sqlite"), []byte("fixture-only-private-payload"))
	_, err := sqlite(context.Background(), root, "bad", []string{"not.sqlite", "PRAGMA integrity_check;"}, "", 0, ampleSpace())
	if err == nil || err.Error() != "sqlite" {
		t.Fatalf("unexpected diagnostic: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = sqlite(ctx, root, "timeout", []string{"not.sqlite", "PRAGMA integrity_check;"}, "", 0, ampleSpace())
	if err == nil {
		t.Fatal("timeout/bad DB was accepted")
	}
}

func TestCipherBoundAndWriter(t *testing.T) {
	if cipherBound(sourceCap, copyCap) > cipherCap || cipherBound(100, 100) >= 1<<20 {
		t.Fatal("incorrect bounded size admission")
	}
	var b bytes.Buffer
	w := cappedWriter{w: &b, max: 4}
	if n, err := w.Write([]byte("1234")); n != 4 || err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("5")); err == nil || b.String() != "1234" {
		t.Fatal("write exceeded cap")
	}
	r := contextReader{ctx: context.Background(), r: bytes.NewReader([]byte("x"))}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyIntegrityFailureRetainsUnmodifiedEvidence(t *testing.T) {
	b, data := tinyBinding()
	id := ephemeralIdentity(t)
	root := t.TempDir()
	cipher := filepath.Join(root, "backup.tar.gz.age")
	sealed := seal(t, canonicalTar(t, b, data), id)
	mustWrite(t, cipher, sealed)
	sum := sha256.Sum256(sealed)
	destination := filepath.Join(root, "retained")
	if err := verify(context.Background(), cipher, destination, syntheticExpectation(), id, hex.EncodeToString(sum[:]), int64(len(sealed)), ampleSpace()); err == nil || err.Error() != "integrity" {
		t.Fatalf("bad SQLite backup was accepted: %v", err)
	}
	for i, record := range b.Files {
		if !record.Present {
			continue
		}
		got, err := os.ReadFile(filepath.Join(destination, record.Name))
		if err != nil || !bytes.Equal(got, data[i]) {
			t.Fatal("verification changed retained evidence")
		}
	}
	if _, err := os.Stat(filepath.Join(destination, "VERIFIED")); !os.IsNotExist(err) {
		t.Fatal("failed integrity has a success marker")
	}
}

func TestOriginalDriftRejectsClosedOutput(t *testing.T) {
	source, _ := fixture(t)
	id := ephemeralIdentity(t)
	op := ampleSpace()
	op.fault = func(stage string) error {
		if stage == "file_close" {
			// Only this synthetic source is mutated, after encryption finishes.
			return os.WriteFile(filepath.Join(source, "unexpected"), []byte("changed"), 0600)
		}
		return nil
	}
	work := filepath.Join(t.TempDir(), "work")
	if _, _, err := produce(context.Background(), source, work, syntheticExpectation(), id.Recipient(), op); err == nil || err.Error() != "original_changed" {
		t.Fatalf("original drift not detected: %v", err)
	}
	if _, err := os.Stat(filepath.Join(work, "backup.tar.gz.age")); !os.IsNotExist(err) {
		t.Fatal("drift left an upload-eligible ciphertext")
	}
}

func TestWorkflowBackupOnlyWiring(t *testing.T) {
	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source path unavailable")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(here), "..", ".github", "workflows", "publish-discord-backup.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, forbidden := range []string{"schedule:", "secrets.", "actions/cache/save", "contents: write", "actions: write", "git push", "repair_discord", "upload-pages-artifact"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("unexpected maintenance authority: %s", forbidden)
		}
	}
	for _, required := range []string{
		"timeout-minutes: 60", "GOWORK: \"off\"", "GOFLAGS: -mod=readonly",
		"go mod download github.com/openclaw/crawlkit@v0.15.0",
		"maintenance/discord-cache-backup-20260909", "WORKFLOW_SHA", "backup-only",
		"actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
		"path: ${{ steps.build.outputs.private_temp }}/work/backup.tar.gz.age",
		"compression-level: 0", "retention-days: 1", "overwrite: false",
		"steps.backup.outputs.closed == 'true'", "RESTORE_DEADLINE_MS",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("missing backup contract: %s", required)
		}
	}
	if strings.Count(text, "uses: actions/upload-artifact@") != 1 ||
		strings.Index(text, "go test -count=1") > strings.Index(text, "- name: Restore only the approved cache") {
		t.Fatal("upload count or pre-restore test ordering")
	}
}
