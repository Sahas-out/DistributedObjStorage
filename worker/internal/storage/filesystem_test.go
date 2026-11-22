package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func newStore(t *testing.T, dir string) *Filesystem {
	t.Helper()
	f, err := NewFilesystem(context.Background(), dir, nil)
	if err != nil {
		t.Fatalf("NewFilesystem: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// completeVersion reports whether got is exactly one "<id>-writer<w>-round<r>"
// payload, as written by the concurrency test.
func completeVersion(got []byte, id string) bool {
	return regexp.MustCompile(`^` + regexp.QuoteMeta(id) + `-writer\d+-round\d+$`).Match(got)
}

func put(t *testing.T, f *Filesystem, id, obj string, seq int, data []byte) ChunkMetadata {
	t.Helper()
	meta, err := f.Put(context.Background(), ChunkMetadata{ChunkID: id, ObjectID: obj, SequenceNo: seq}, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put %s: %v", id, err)
	}
	return meta
}

func read(t *testing.T, f *Filesystem, id string) ([]byte, ChunkMetadata) {
	t.Helper()
	rc, meta, err := f.Open(context.Background(), id)
	if err != nil {
		t.Fatalf("Open %s: %v", id, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return data, meta
}

// files lists the names in <dir>/<sub>.
func files(t *testing.T, dir, sub string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, sub))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

func TestPutOpenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	f := newStore(t, dir)
	data := bytes.Repeat([]byte{0x00, 0xFF, 0x7F}, 1000)

	before := time.Now().UTC()
	meta := put(t, f, "obj1_2", "obj1", 2, data)

	if meta.ChunkID != "obj1_2" || meta.ObjectID != "obj1" || meta.SequenceNo != 2 {
		t.Fatalf("identity fields = %+v", meta)
	}
	if meta.Size != int64(len(data)) {
		t.Fatalf("size = %d, want %d", meta.Size, len(data))
	}
	if meta.CreatedAt.Before(before) || meta.CreatedAt.Location() != time.UTC {
		t.Fatalf("CreatedAt = %v", meta.CreatedAt)
	}
	if !strings.HasPrefix(meta.FilePath, "chunks/obj1_2.") {
		t.Fatalf("FilePath = %q", meta.FilePath)
	}
	if onDisk, err := os.ReadFile(filepath.Join(dir, meta.FilePath)); err != nil || !bytes.Equal(onDisk, data) {
		t.Fatalf("file at FilePath: err=%v equal=%v", err, bytes.Equal(onDisk, data))
	}

	got, gotMeta := read(t, f, "obj1_2")
	if !bytes.Equal(got, data) {
		t.Fatal("Open returned different bytes")
	}
	if !gotMeta.CreatedAt.Equal(meta.CreatedAt) {
		t.Fatalf("CreatedAt round trip: %v != %v", gotMeta.CreatedAt, meta.CreatedAt)
	}
	gotMeta.CreatedAt, meta.CreatedAt = time.Time{}, time.Time{}
	if gotMeta != meta {
		t.Fatalf("Open metadata = %+v, want %+v", gotMeta, meta)
	}
	if tmp := files(t, dir, tmpDir); len(tmp) != 0 {
		t.Fatalf("tmp not empty: %v", tmp)
	}
}

func TestPutEmptyChunk(t *testing.T) {
	f := newStore(t, t.TempDir())
	meta := put(t, f, "empty", "o", 0, nil)
	if meta.Size != 0 {
		t.Fatalf("meta = %+v", meta)
	}
	if got, _ := read(t, f, "empty"); len(got) != 0 {
		t.Fatalf("got %d bytes", len(got))
	}
}

func TestPutOverwriteReplacesDataAndRemovesOldFile(t *testing.T) {
	dir := t.TempDir()
	f := newStore(t, dir)
	first := put(t, f, "c1", "objA", 0, []byte("first"))
	second := put(t, f, "c1", "objB", 7, []byte("second version"))

	if first.FilePath == second.FilePath {
		t.Fatal("overwrite reused the same file")
	}
	if _, err := os.Stat(filepath.Join(dir, first.FilePath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old file still present: %v", err)
	}
	got, meta := read(t, f, "c1")
	if string(got) != "second version" || meta.ObjectID != "objB" || meta.SequenceNo != 7 {
		t.Fatalf("after overwrite: %q %+v", got, meta)
	}
	if n := len(files(t, dir, chunksDir)); n != 1 {
		t.Fatalf("%d chunk files, want 1", n)
	}
	if list, _ := f.List(context.Background()); len(list) != 1 {
		t.Fatalf("List has %d rows, want 1", len(list))
	}
}

func TestOpenReaderSurvivesOverwriteAndDelete(t *testing.T) {
	f := newStore(t, t.TempDir())
	put(t, f, "c1", "o", 0, []byte("original"))

	rc, _, err := f.Open(context.Background(), "c1")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	put(t, f, "c1", "o", 0, []byte("replacement"))
	if err := f.Delete(context.Background(), "c1"); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	if err != nil || string(got) != "original" {
		t.Fatalf("open reader got %q, %v", got, err)
	}
}

func TestInvalidChunkIDs(t *testing.T) {
	dir := t.TempDir()
	f := newStore(t, dir)
	ctx := context.Background()
	for _, id := range []string{"", "a.b", "../x", "a/b", strings.Repeat("a", 129)} {
		if _, err := f.Put(ctx, ChunkMetadata{ChunkID: id, ObjectID: "o"}, strings.NewReader("x")); !errors.Is(err, ErrInvalidChunkID) {
			t.Errorf("Put(%q) err = %v", id, err)
		}
		if _, _, err := f.Open(ctx, id); !errors.Is(err, ErrInvalidChunkID) {
			t.Errorf("Open(%q) err = %v", id, err)
		}
		if err := f.Delete(ctx, id); !errors.Is(err, ErrInvalidChunkID) {
			t.Errorf("Delete(%q) err = %v", id, err)
		}
	}
	if n := len(files(t, dir, chunksDir)) + len(files(t, dir, tmpDir)); n != 0 {
		t.Fatalf("%d files written for invalid IDs", n)
	}
}

type failingReader struct{ n int }

func (r *failingReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, errors.New("connection dropped")
	}
	k := min(len(p), r.n)
	r.n -= k
	return k, nil
}

func TestPutFailuresLeaveNothingBehind(t *testing.T) {
	t.Run("source read error", func(t *testing.T) {
		dir := t.TempDir()
		f := newStore(t, dir)
		_, err := f.Put(context.Background(), ChunkMetadata{ChunkID: "c1", ObjectID: "o"}, &failingReader{n: 100})
		if err == nil {
			t.Fatal("Put succeeded")
		}
		assertEmpty(t, f, dir)
	})

	t.Run("canceled context", func(t *testing.T) {
		dir := t.TempDir()
		f := newStore(t, dir)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := f.Put(ctx, ChunkMetadata{ChunkID: "c1", ObjectID: "o"}, strings.NewReader("data"))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		assertEmpty(t, f, dir)
	})

	t.Run("failed overwrite keeps the previous version", func(t *testing.T) {
		dir := t.TempDir()
		f := newStore(t, dir)
		put(t, f, "c1", "o", 0, []byte("good"))
		_, err := f.Put(context.Background(), ChunkMetadata{ChunkID: "c1", ObjectID: "o"}, &failingReader{n: 3})
		if err == nil {
			t.Fatal("Put succeeded")
		}
		if got, _ := read(t, f, "c1"); string(got) != "good" {
			t.Fatalf("got %q after failed overwrite", got)
		}
		if n := len(files(t, dir, chunksDir)); n != 1 {
			t.Fatalf("%d chunk files, want 1", n)
		}
	})
}

func assertEmpty(t *testing.T, f *Filesystem, dir string) {
	t.Helper()
	if list, err := f.List(context.Background()); err != nil || len(list) != 0 {
		t.Fatalf("List = %v, %v", list, err)
	}
	if c, tmp := files(t, dir, chunksDir), files(t, dir, tmpDir); len(c)+len(tmp) != 0 {
		t.Fatalf("leftover files: chunks=%v tmp=%v", c, tmp)
	}
}

func TestOpenMissing(t *testing.T) {
	f := newStore(t, t.TempDir())
	if _, _, err := f.Open(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestOpenWithMissingDataFileIsNotFound(t *testing.T) {
	dir := t.TempDir()
	f := newStore(t, dir)
	meta := put(t, f, "c1", "o", 0, []byte("x"))
	if err := os.Remove(filepath.Join(dir, meta.FilePath)); err != nil {
		t.Fatal(err)
	}
	_, _, err := f.Open(context.Background(), "c1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	// Putting the chunk again repairs it.
	put(t, f, "c1", "o", 0, []byte("x"))
	rc, _, err := f.Open(context.Background(), "c1")
	if err != nil {
		t.Fatalf("Open after re-put: %v", err)
	}
	rc.Close()
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	f := newStore(t, dir)
	put(t, f, "keep", "o", 0, []byte("k"))
	meta := put(t, f, "gone", "o", 1, []byte("g"))

	if err := f.Delete(context.Background(), "gone"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, meta.FilePath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file still present: %v", err)
	}
	if _, _, err := f.Open(context.Background(), "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Open after delete: %v", err)
	}
	if err := f.Delete(context.Background(), "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete: %v, want ErrNotFound", err)
	}
	if got, _ := read(t, f, "keep"); string(got) != "k" {
		t.Fatal("other chunk affected")
	}
}

func TestList(t *testing.T) {
	f := newStore(t, t.TempDir())
	list, err := f.List(context.Background())
	if err != nil || list == nil || len(list) != 0 {
		t.Fatalf("empty List = %#v, %v", list, err)
	}
	for _, id := range []string{"c", "a", "b"} {
		put(t, f, id, "obj-"+id, len(id), []byte(id))
	}
	list, err = f.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, m := range list {
		ids = append(ids, m.ChunkID)
		if m.ObjectID != "obj-"+m.ChunkID || m.Size != int64(len(m.ChunkID)) || m.FilePath == "" {
			t.Errorf("row %+v", m)
		}
	}
	if strings.Join(ids, ",") != "a,b,c" {
		t.Fatalf("order = %v", ids)
	}
}

func TestPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	f, err := NewFilesystem(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := put(t, f, "c1", "obj", 4, []byte("durable"))
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f2 := newStore(t, dir)
	got, meta := read(t, f2, "c1")
	if string(got) != "durable" {
		t.Fatalf("data after restart = %q", got)
	}
	if !meta.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("CreatedAt %v != %v", meta.CreatedAt, want.CreatedAt)
	}
	meta.CreatedAt, want.CreatedAt = time.Time{}, time.Time{}
	if meta != want {
		t.Fatalf("metadata after restart = %+v, want %+v", meta, want)
	}
}

func TestStartupReconcile(t *testing.T) {
	dir := t.TempDir()
	f, err := NewFilesystem(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	put(t, f, "intact", "o", 0, []byte("intact"))
	lost := put(t, f, "lost", "o", 1, []byte("lost"))
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash and a damaged volume:
	// - an orphan chunk file (renamed into place, metadata never committed)
	// - a half-written temp file
	// - a row whose data file disappeared
	orphan := filepath.Join(dir, chunksDir, "orphan.0123456789abcdef")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, tmpDir, "x.123.part"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, lost.FilePath)); err != nil {
		t.Fatal(err)
	}

	f2 := newStore(t, dir)
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan file not removed: %v", err)
	}
	if tmp := files(t, dir, tmpDir); len(tmp) != 0 {
		t.Errorf("tmp not emptied: %v", tmp)
	}
	list, err := f2.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ChunkID != "intact" {
		t.Fatalf("List after reconcile = %+v", list)
	}
	if got, _ := read(t, f2, "intact"); string(got) != "intact" {
		t.Fatal("intact chunk damaged by reconcile")
	}
	if _, _, err := f2.Open(context.Background(), "lost"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lost chunk Open err = %v, want ErrNotFound", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	f := newStore(t, dir)
	ctx := context.Background()

	const ids, writersPerID, rounds = 4, 4, 5
	var wg sync.WaitGroup
	errs := make(chan error, ids*writersPerID*rounds*3)
	for i := range ids {
		id := fmt.Sprintf("c%d", i)
		for w := range writersPerID {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for r := range rounds {
					data := fmt.Appendf(nil, "%s-writer%d-round%d", id, w, r)
					if _, err := f.Put(ctx, ChunkMetadata{ChunkID: id, ObjectID: "o", SequenceNo: i}, bytes.NewReader(data)); err != nil {
						errs <- fmt.Errorf("Put %s: %w", id, err)
						return
					}
					// Every read must see one complete version: bytes written by
					// a single Put, with the size recorded alongside them.
					rc, meta, err := f.Open(ctx, id)
					if err != nil {
						errs <- fmt.Errorf("Open %s: %w", id, err)
						return
					}
					got, err := io.ReadAll(rc)
					rc.Close()
					if err != nil || !completeVersion(got, id) || int64(len(got)) != meta.Size {
						errs <- fmt.Errorf("Open %s: torn read (err=%v)", id, err)
						return
					}
					if _, err := f.List(ctx); err != nil {
						errs <- fmt.Errorf("List: %w", err)
						return
					}
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	list, err := f.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != ids {
		t.Fatalf("List has %d rows, want %d", len(list), ids)
	}
	var want []string
	for _, m := range list {
		want = append(want, filepath.Base(m.FilePath))
	}
	got := files(t, dir, chunksDir)
	sort.Strings(want)
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("chunk files %v don't match metadata %v", got, want)
	}
}

func TestPing(t *testing.T) {
	f := newStore(t, t.TempDir())
	if err := f.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	f.Close()
	if err := f.Ping(context.Background()); err == nil {
		t.Fatal("Ping after Close succeeded")
	}
}
