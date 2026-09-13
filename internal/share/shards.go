package share

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const shardFlushRows = 1024

var maxShardBytes int64 = 40 * 1024 * 1024

type tableShardWriter struct {
	rootDir     string
	relDir      string
	label       string
	nextShard   int
	rowsInShard int
	files       []string
	file        *os.File
	counter     *countingWriter
	gz          *gzip.Writer
}

func (w *tableShardWriter) open() error {
	rel := filepath.ToSlash(filepath.Join(w.relDir, fmt.Sprintf("%06d.jsonl.gz", w.nextShard)))
	path := filepath.Join(w.rootDir, filepath.FromSlash(rel))
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", rel, err)
	}
	w.nextShard++
	w.rowsInShard = 0
	w.files = append(w.files, rel)
	w.file = file
	w.counter = &countingWriter{w: file}
	w.gz = gzip.NewWriter(w.counter)
	return nil
}

func (w *tableShardWriter) Write(p []byte) (int, error) {
	return w.gz.Write(p)
}

func (w *tableShardWriter) rotateIfNeeded() error {
	if maxShardBytes <= 0 || w.rowsInShard == 0 || w.counter.n < maxShardBytes {
		return nil
	}
	if err := w.close(); err != nil {
		return err
	}
	return w.open()
}

func (w *tableShardWriter) finishRow() error {
	w.rowsInShard++
	if maxShardBytes > 1024*1024 && w.rowsInShard%shardFlushRows != 0 {
		return nil
	}
	if err := w.gz.Flush(); err != nil {
		return fmt.Errorf("flush %s shard: %w", w.label, err)
	}
	return nil
}

func (w *tableShardWriter) close() error {
	var closeErr error
	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			closeErr = err
		}
		w.gz = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		w.file = nil
	}
	if closeErr != nil {
		return fmt.Errorf("close %s shard: %w", w.label, closeErr)
	}
	return nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func exportValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

func importValue(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v.String(), 64); err == nil {
			return f
		}
		return v.String()
	default:
		return v
	}
}

func safePathSegment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "_"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	clean := b.String()
	if clean == "." || clean == ".." {
		return "_"
	}
	return clean
}
