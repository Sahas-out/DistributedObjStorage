package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
	"github.com/Sahas-out/DistributedObjectStorage/worker/internal/storage"
)

// mockStorage lets each test inject behaviour and records calls.
type mockStorage struct {
	putFn    func(ctx context.Context, meta storage.ChunkMetadata, src io.Reader) (storage.ChunkMetadata, error)
	openFn   func(ctx context.Context, id string) (io.ReadCloser, storage.ChunkMetadata, error)
	deleteFn func(ctx context.Context, id string) error
	listFn   func(ctx context.Context) ([]storage.ChunkMetadata, error)

	mu    sync.Mutex
	calls []string
}

func (m *mockStorage) record(c string) {
	m.mu.Lock()
	m.calls = append(m.calls, c)
	m.mu.Unlock()
}

func (m *mockStorage) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockStorage) Put(ctx context.Context, meta storage.ChunkMetadata, src io.Reader) (storage.ChunkMetadata, error) {
	m.record("Put " + meta.ChunkID)
	if m.putFn == nil {
		panic("unexpected Put")
	}
	return m.putFn(ctx, meta, src)
}

func (m *mockStorage) Open(ctx context.Context, id string) (io.ReadCloser, storage.ChunkMetadata, error) {
	m.record("Open " + id)
	if m.openFn == nil {
		panic("unexpected Open")
	}
	return m.openFn(ctx, id)
}

func (m *mockStorage) Delete(ctx context.Context, id string) error {
	m.record("Delete " + id)
	if m.deleteFn == nil {
		panic("unexpected Delete")
	}
	return m.deleteFn(ctx, id)
}

func (m *mockStorage) List(ctx context.Context) ([]storage.ChunkMetadata, error) {
	m.record("List")
	if m.listFn == nil {
		panic("unexpected List")
	}
	return m.listFn(ctx)
}

const testMaxChunk = 1 << 20

func newTestHandler(store storage.Storage) http.Handler {
	return NewServer(store, "worker-test", testMaxChunk, nil).Handler()
}

func do(t *testing.T, h http.Handler, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func sum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// chunkBody encodes a protocol.Chunk PUT body.
func chunkBody(t testing.TB, objectID string, seq int, data []byte) []byte {
	t.Helper()
	b, err := json.Marshal(protocol.Chunk{ObjectID: objectID, SequenceNo: seq, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// echoPut is a Put that reads the body and returns correct metadata.
func echoPut(_ context.Context, meta storage.ChunkMetadata, src io.Reader) (storage.ChunkMetadata, error) {
	b, err := io.ReadAll(src)
	if err != nil {
		return storage.ChunkMetadata{}, err
	}
	meta.Size = int64(len(b))
	meta.Checksum = sum(b)
	return meta, nil
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) protocol.ErrorBody {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("error Content-Type = %q, want application/json", ct)
	}
	var e protocol.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	if e.Error.Code == "" || e.Error.Message == "" {
		t.Fatalf("error body missing code/message: %q", rec.Body.String())
	}
	return e.Error
}

func expectError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body %q)", rec.Code, status, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != code {
		t.Fatalf("error code = %q, want %q", got, code)
	}
}

var invalidIDPaths = []struct {
	name string
	path string
}{
	{"dot", "a.b"},
	{"encoded slash", "a%2Fb"},
	{"encoded dot-dot", "%2E%2E"},
	{"encoded traversal", "..%2Fetc%2Fpasswd"},
	{"space", "a%20b"},
	{"too long", strings.Repeat("a", 129)},
	{"unicode", "%C3%A9"},
}

// ---------- PUT ----------

func TestPutChunk(t *testing.T) {
	data := []byte("hello chunk \x00\x01\xff")
	body := chunkBody(t, "obj1", 3, data)

	t.Run("happy path returns 201 and ChunkMetadata", func(t *testing.T) {
		var stored storage.ChunkMetadata
		m := &mockStorage{putFn: func(ctx context.Context, meta storage.ChunkMetadata, src io.Reader) (storage.ChunkMetadata, error) {
			stored = meta
			return echoPut(ctx, meta, src)
		}}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/obj1_3", body, nil)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
		}
		if stored.ChunkID != "obj1_3" || stored.ObjectID != "obj1" || stored.SequenceNo != 3 {
			t.Fatalf("storage got metadata %+v", stored)
		}
		var got protocol.ChunkMetadata
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		want := protocol.ChunkMetadata{ChunkID: "obj1_3", ObjectID: "obj1", SequenceNo: 3}
		if got != want {
			t.Fatalf("response = %+v, want %+v", got, want)
		}
		if h := rec.Header().Get(protocol.HeaderChunkChecksum); h != sum(data) {
			t.Fatalf("%s = %q, want %q", protocol.HeaderChunkChecksum, h, sum(data))
		}
	})

	t.Run("storage receives the decoded bytes", func(t *testing.T) {
		var got []byte
		m := &mockStorage{putFn: func(ctx context.Context, meta storage.ChunkMetadata, src io.Reader) (storage.ChunkMetadata, error) {
			b, _ := io.ReadAll(src)
			got = b
			return echoPut(ctx, meta, bytes.NewReader(b))
		}}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/c1", body, nil)
		if rec.Code != http.StatusCreated || !bytes.Equal(got, data) {
			t.Fatalf("status = %d, stored %q, want %q", rec.Code, got, data)
		}
	})

	t.Run("sequence 0 accepted", func(t *testing.T) {
		m := &mockStorage{putFn: echoPut}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/c0", chunkBody(t, "o", 0, data), nil)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
		}
	})

	t.Run("max-length id accepted", func(t *testing.T) {
		m := &mockStorage{putFn: echoPut}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/"+strings.Repeat("a", 128), body, nil)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201", rec.Code)
		}
	})

	t.Run("empty data accepted", func(t *testing.T) {
		m := &mockStorage{putFn: echoPut}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/empty", chunkBody(t, "o", 0, []byte{}), nil)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
		}
	})

	for _, tc := range invalidIDPaths {
		t.Run("invalid id "+tc.name, func(t *testing.T) {
			m := &mockStorage{}
			rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/"+tc.path, body, nil)
			expectError(t, rec, http.StatusBadRequest, protocol.CodeInvalidChunkID)
			if m.callCount() != 0 {
				t.Fatalf("storage was called: %v", m.calls)
			}
		})
	}

	t.Run("reserved id metadata rejected", func(t *testing.T) {
		m := &mockStorage{}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/metadata", body, nil)
		expectError(t, rec, http.StatusBadRequest, protocol.CodeInvalidChunkID)
		if m.callCount() != 0 {
			t.Fatalf("storage was called: %v", m.calls)
		}
	})

	for _, tc := range []struct {
		name string
		body string
	}{
		{"not json", "raw bytes"},
		{"json array", `[1,2]`},
		{"data not base64", `{"object_id":"o","sequence_no":0,"data":"!!!"}`},
		{"sequence_no not a number", `{"object_id":"o","sequence_no":"1","data":""}`},
		{"missing object_id", `{"sequence_no":0,"data":"aGk="}`},
		{"empty object_id", `{"object_id":"","sequence_no":0,"data":"aGk="}`},
		{"invalid object_id", `{"object_id":"a/b","sequence_no":0,"data":"aGk="}`},
		{"missing sequence_no", `{"object_id":"o","data":"aGk="}`},
		{"negative sequence_no", `{"object_id":"o","sequence_no":-1,"data":"aGk="}`},
		{"missing data", `{"object_id":"o","sequence_no":0}`},
		{"null data", `{"object_id":"o","sequence_no":0,"data":null}`},
	} {
		t.Run("invalid body "+tc.name, func(t *testing.T) {
			m := &mockStorage{}
			rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/c1", []byte(tc.body), nil)
			expectError(t, rec, http.StatusBadRequest, protocol.CodeInvalidRequest)
			if m.callCount() != 0 {
				t.Fatalf("storage was called: %v", m.calls)
			}
		})
	}

	t.Run("rejected PUTs still consume the whole body", func(t *testing.T) {
		// Replying while body bytes are unread makes the server close the
		// connection, and the TCP reset can destroy the response in transit.
		big := chunkBody(t, "o", 0, make([]byte, testMaxChunk))
		for _, tc := range []struct{ path, checksum string }{
			{"/internals/chunks/bad.id", ""},
			{"/internals/chunks/c1", "not-a-digest"},
			{"/internals/chunks/c1", sum([]byte("other"))},
		} {
			body := bytes.NewReader(big)
			req := httptest.NewRequest(http.MethodPut, tc.path, body)
			if tc.checksum != "" {
				req.Header.Set(protocol.HeaderChunkChecksum, tc.checksum)
			}
			rec := httptest.NewRecorder()
			newTestHandler(&mockStorage{}).ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s: status = %d, want 400", tc.path, rec.Code)
			}
			if body.Len() != 0 {
				t.Fatalf("%s: %d body bytes left unread", tc.path, body.Len())
			}
		}
	})

	t.Run("exactly max size data accepted", func(t *testing.T) {
		m := &mockStorage{putFn: echoPut}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/big",
			chunkBody(t, "o", 0, make([]byte, testMaxChunk)), nil)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
		}
	})

	t.Run("oversized data returns 413", func(t *testing.T) {
		m := &mockStorage{}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/big",
			chunkBody(t, "o", 0, make([]byte, testMaxChunk+1)), nil)
		expectError(t, rec, http.StatusRequestEntityTooLarge, protocol.CodeChunkTooLarge)
		if m.callCount() != 0 {
			t.Fatalf("storage was called: %v", m.calls)
		}
	})

	t.Run("oversized body returns 413", func(t *testing.T) {
		m := &mockStorage{}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/big",
			chunkBody(t, "o", 0, make([]byte, 2*testMaxChunk)), nil)
		expectError(t, rec, http.StatusRequestEntityTooLarge, protocol.CodeChunkTooLarge)
		if m.callCount() != 0 {
			t.Fatalf("storage was called: %v", m.calls)
		}
	})

	t.Run("matching checksum header accepted (case-insensitive)", func(t *testing.T) {
		m := &mockStorage{putFn: echoPut}
		hdr := map[string]string{protocol.HeaderChunkChecksum: strings.ToUpper(sum(data))}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/c1", body, hdr)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
		}
	})

	t.Run("mismatched checksum header returns 400 and does not store", func(t *testing.T) {
		m := &mockStorage{}
		hdr := map[string]string{protocol.HeaderChunkChecksum: sum([]byte("other"))}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/c1", body, hdr)
		expectError(t, rec, http.StatusBadRequest, protocol.CodeChecksumMismatch)
		if m.callCount() != 0 {
			t.Fatalf("storage was called: %v", m.calls)
		}
	})

	t.Run("malformed checksum header returns 400", func(t *testing.T) {
		m := &mockStorage{}
		hdr := map[string]string{protocol.HeaderChunkChecksum: "not-a-digest"}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/c1", body, hdr)
		expectError(t, rec, http.StatusBadRequest, protocol.CodeInvalidRequest)
		if m.callCount() != 0 {
			t.Fatalf("storage was called: %v", m.calls)
		}
	})

	t.Run("storage failure returns 500 without leaking details", func(t *testing.T) {
		m := &mockStorage{putFn: func(context.Context, storage.ChunkMetadata, io.Reader) (storage.ChunkMetadata, error) {
			return storage.ChunkMetadata{}, errors.New("disk on fire at /secret/path")
		}}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/c1", body, nil)
		expectError(t, rec, http.StatusInternalServerError, protocol.CodeStorageError)
		if strings.Contains(rec.Body.String(), "/secret/path") {
			t.Fatalf("internal error leaked: %q", rec.Body.String())
		}
	})

	t.Run("storage invalid id error returns 400", func(t *testing.T) {
		m := &mockStorage{putFn: func(context.Context, storage.ChunkMetadata, io.Reader) (storage.ChunkMetadata, error) {
			return storage.ChunkMetadata{}, fmt.Errorf("wrapped: %w", storage.ErrInvalidChunkID)
		}}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/c1", body, nil)
		expectError(t, rec, http.StatusBadRequest, protocol.CodeInvalidChunkID)
	})

	t.Run("storage returning inconsistent info returns 500", func(t *testing.T) {
		m := &mockStorage{putFn: func(_ context.Context, meta storage.ChunkMetadata, src io.Reader) (storage.ChunkMetadata, error) {
			_, _ = io.ReadAll(src)
			meta.Size, meta.Checksum = 1, sum([]byte("x"))
			return meta, nil
		}}
		rec := do(t, newTestHandler(m), http.MethodPut, "/internals/chunks/c1", body, nil)
		expectError(t, rec, http.StatusInternalServerError, protocol.CodeStorageError)
	})
}

// ---------- GET ----------

func openReturning(data []byte, meta storage.ChunkMetadata) func(context.Context, string) (io.ReadCloser, storage.ChunkMetadata, error) {
	return func(context.Context, string) (io.ReadCloser, storage.ChunkMetadata, error) {
		return io.NopCloser(bytes.NewReader(data)), meta, nil
	}
}

type trackingCloser struct {
	io.Reader
	closed bool
}

func (c *trackingCloser) Close() error { c.closed = true; return nil }

func TestGetChunk(t *testing.T) {
	data := []byte("some chunk bytes \x00\x01\x02")
	good := storage.ChunkMetadata{
		ChunkID: "c1", ObjectID: "obj1", SequenceNo: 2,
		Size: int64(len(data)), Checksum: sum(data),
	}

	t.Run("happy path returns Chunk JSON and headers", func(t *testing.T) {
		tc := &trackingCloser{Reader: bytes.NewReader(data)}
		m := &mockStorage{openFn: func(context.Context, string) (io.ReadCloser, storage.ChunkMetadata, error) {
			return tc, good, nil
		}}
		rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/c1", nil, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
		}
		var got protocol.Chunk
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.ObjectID != "obj1" || got.SequenceNo != 2 || !bytes.Equal(got.Data, data) {
			t.Fatalf("body = %+v", got)
		}
		h := rec.Header()
		if got := h.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		if got := h.Get("Content-Length"); got != fmt.Sprint(rec.Body.Len()) {
			t.Errorf("Content-Length = %q, body is %d bytes", got, rec.Body.Len())
		}
		if got := h.Get(protocol.HeaderChunkChecksum); got != good.Checksum {
			t.Errorf("%s = %q", protocol.HeaderChunkChecksum, got)
		}
		if got := h.Get(protocol.HeaderChunkSize); got != fmt.Sprint(len(data)) {
			t.Errorf("%s = %q", protocol.HeaderChunkSize, got)
		}
		if !tc.closed {
			t.Errorf("reader was not closed")
		}
	})

	t.Run("HEAD returns headers without body", func(t *testing.T) {
		m := &mockStorage{openFn: openReturning(data, good)}
		rec := do(t, newTestHandler(m), http.MethodHead, "/internals/chunks/c1", nil, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if rec.Body.Len() != 0 {
			t.Fatalf("HEAD returned a body")
		}
		if rec.Header().Get(protocol.HeaderChunkChecksum) != good.Checksum {
			t.Fatalf("missing checksum header")
		}
	})

	t.Run("missing chunk returns 404", func(t *testing.T) {
		m := &mockStorage{openFn: func(context.Context, string) (io.ReadCloser, storage.ChunkMetadata, error) {
			return nil, storage.ChunkMetadata{}, fmt.Errorf("open: %w", storage.ErrNotFound)
		}}
		rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/nope", nil, nil)
		expectError(t, rec, http.StatusNotFound, protocol.CodeChunkNotFound)
	})

	t.Run("corrupted content returns 500 chunk_corrupted", func(t *testing.T) {
		corrupted := append([]byte(nil), data...)
		corrupted[0] ^= 0xFF
		m := &mockStorage{openFn: openReturning(corrupted, good)}
		rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/c1", nil, nil)
		expectError(t, rec, http.StatusInternalServerError, protocol.CodeChunkCorrupted)
		if strings.Contains(rec.Body.String(), `"data"`) {
			t.Fatalf("corrupted data was returned: %q", rec.Body.String())
		}
	})

	t.Run("truncated content returns 500 chunk_corrupted", func(t *testing.T) {
		m := &mockStorage{openFn: openReturning(data[:5], good)}
		rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/c1", nil, nil)
		expectError(t, rec, http.StatusInternalServerError, protocol.CodeChunkCorrupted)
	})

	t.Run("empty recorded checksum returns 500 chunk_corrupted", func(t *testing.T) {
		noSum := good
		noSum.Checksum = ""
		m := &mockStorage{openFn: openReturning(data, noSum)}
		rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/c1", nil, nil)
		expectError(t, rec, http.StatusInternalServerError, protocol.CodeChunkCorrupted)
	})

	t.Run("missing checksum metadata returns 500 storage_error", func(t *testing.T) {
		m := &mockStorage{openFn: func(context.Context, string) (io.ReadCloser, storage.ChunkMetadata, error) {
			return nil, storage.ChunkMetadata{}, storage.ErrMissingChecksum
		}}
		rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/c1", nil, nil)
		expectError(t, rec, http.StatusInternalServerError, protocol.CodeStorageError)
	})

	t.Run("read failure returns 500", func(t *testing.T) {
		m := &mockStorage{openFn: func(context.Context, string) (io.ReadCloser, storage.ChunkMetadata, error) {
			return io.NopCloser(io.MultiReader(bytes.NewReader(data[:3]), errReader{})), good, nil
		}}
		rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/c1", nil, nil)
		expectError(t, rec, http.StatusInternalServerError, protocol.CodeStorageError)
	})

	for _, tc := range invalidIDPaths {
		t.Run("invalid id "+tc.name, func(t *testing.T) {
			m := &mockStorage{}
			rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/"+tc.path, nil, nil)
			expectError(t, rec, http.StatusBadRequest, protocol.CodeInvalidChunkID)
			if m.callCount() != 0 {
				t.Fatalf("storage was called: %v", m.calls)
			}
		})
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

// ---------- DELETE ----------

func TestDeleteChunk(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode int
	}{
		{"existing chunk", nil, http.StatusNoContent},
		{"missing chunk is idempotent", storage.ErrNotFound, http.StatusNoContent},
		{"wrapped missing chunk", fmt.Errorf("x: %w", storage.ErrNotFound), http.StatusNoContent},
		{"storage failure", errors.New("boom"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &mockStorage{deleteFn: func(context.Context, string) error { return tc.err }}
			rec := do(t, newTestHandler(m), http.MethodDelete, "/internals/chunks/c1", nil, nil)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantCode)
			}
			if tc.wantCode == http.StatusNoContent && rec.Body.Len() != 0 {
				t.Fatalf("204 with body %q", rec.Body.String())
			}
			if tc.wantCode == http.StatusInternalServerError {
				expectError(t, rec, tc.wantCode, protocol.CodeStorageError)
			}
		})
	}

	t.Run("invalid id", func(t *testing.T) {
		m := &mockStorage{}
		rec := do(t, newTestHandler(m), http.MethodDelete, "/internals/chunks/a.b", nil, nil)
		expectError(t, rec, http.StatusBadRequest, protocol.CodeInvalidChunkID)
	})

	t.Run("reserved id metadata rejected", func(t *testing.T) {
		m := &mockStorage{}
		rec := do(t, newTestHandler(m), http.MethodDelete, "/internals/chunks/metadata", nil, nil)
		expectError(t, rec, http.StatusBadRequest, protocol.CodeInvalidChunkID)
		if m.callCount() != 0 {
			t.Fatalf("storage was called: %v", m.calls)
		}
	})
}

// ---------- LIST (GET /internals/chunks/metadata) ----------

func TestListChunkMetadata(t *testing.T) {
	t.Run("empty store returns empty array", func(t *testing.T) {
		m := &mockStorage{listFn: func(context.Context) ([]storage.ChunkMetadata, error) { return nil, nil }}
		rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/metadata", nil, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		if got := strings.TrimSpace(rec.Body.String()); got != "[]" {
			t.Fatalf("expected empty array, got %q", got)
		}
	})

	t.Run("returns metadata DTOs only", func(t *testing.T) {
		metas := []storage.ChunkMetadata{
			{ChunkID: "a", ObjectID: "o1", SequenceNo: 0, Size: 1, Checksum: sum([]byte("a")), FilePath: "/data/a"},
			{ChunkID: "b", ObjectID: "o1", SequenceNo: 1, Size: 2, Checksum: sum([]byte("bb")), FilePath: "/data/b"},
		}
		m := &mockStorage{listFn: func(context.Context) ([]storage.ChunkMetadata, error) { return metas, nil }}
		rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/metadata", nil, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		var got []protocol.ChunkMetadata
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		want := []protocol.ChunkMetadata{
			{ChunkID: "a", ObjectID: "o1", SequenceNo: 0},
			{ChunkID: "b", ObjectID: "o1", SequenceNo: 1},
		}
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("list = %+v, want %+v", got, want)
		}
		// Internal fields must not leak onto the wire.
		for _, field := range []string{"file_path", "FilePath", "/data/a", "checksum"} {
			if strings.Contains(rec.Body.String(), field) {
				t.Fatalf("response leaks %q: %s", field, rec.Body.String())
			}
		}
	})

	t.Run("storage failure returns 500", func(t *testing.T) {
		m := &mockStorage{listFn: func(context.Context) ([]storage.ChunkMetadata, error) { return nil, errors.New("boom") }}
		rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/metadata", nil, nil)
		expectError(t, rec, http.StatusInternalServerError, protocol.CodeStorageError)
	})
}

// ---------- misc ----------

func TestHealth(t *testing.T) {
	rec := do(t, newTestHandler(&mockStorage{}), http.MethodGet, "/healthz", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" || body["worker_id"] != "worker-test" {
		t.Fatalf("unexpected body %v", body)
	}
}

func TestRouting(t *testing.T) {
	h := newTestHandler(&mockStorage{})
	if rec := do(t, h, http.MethodPost, "/internals/chunks/c1", []byte("x"), nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST chunk: status = %d, want 405", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/internals/chunks/", nil, nil); rec.Code != http.StatusNotFound {
		t.Errorf("empty id: status = %d, want 404", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/nope", nil, nil); rec.Code != http.StatusNotFound {
		t.Errorf("unknown route: status = %d, want 404", rec.Code)
	}
	// Old routes are gone.
	for _, p := range []string{"/internal/chunks/c1", "/internal/workers/w/chunks"} {
		if rec := do(t, h, http.MethodGet, p, nil, nil); rec.Code != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want 404", p, rec.Code)
		}
	}
}

func TestPanicRecovery(t *testing.T) {
	m := &mockStorage{listFn: func(context.Context) ([]storage.ChunkMetadata, error) { panic("kaboom") }}
	rec := do(t, newTestHandler(m), http.MethodGet, "/internals/chunks/metadata", nil, nil)
	expectError(t, rec, http.StatusInternalServerError, protocol.CodeInternalError)
}

// ---------- round trip against the real filesystem store ----------

func TestRoundTripWithFilesystemStore(t *testing.T) {
	store, err := storage.NewFilesystem(context.Background(), t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	srv := httptest.NewServer(newTestHandler(store))
	defer srv.Close()
	client := srv.Client()

	data := bytes.Repeat([]byte{0xAB, 0x00, 0x7F}, 200_000) // ~600 KB of binary data
	url := srv.URL + "/internals/chunks/obj_0"

	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(chunkBody(t, "obj", 0, data)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(protocol.HeaderChunkChecksum, sum(data))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT status = %d", resp.StatusCode)
	}

	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var chunk protocol.Chunk
	decErr := json.NewDecoder(resp.Body).Decode(&chunk)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || decErr != nil {
		t.Fatalf("GET status = %d, decode error = %v", resp.StatusCode, decErr)
	}
	if chunk.ObjectID != "obj" || chunk.SequenceNo != 0 || !bytes.Equal(chunk.Data, data) {
		t.Fatalf("GET returned object_id=%q sequence_no=%d, data equal = %v",
			chunk.ObjectID, chunk.SequenceNo, bytes.Equal(chunk.Data, data))
	}
	if resp.Header.Get(protocol.HeaderChunkChecksum) != sum(data) {
		t.Fatalf("checksum header mismatch")
	}

	resp, err = client.Get(srv.URL + "/internals/chunks/metadata")
	if err != nil {
		t.Fatal(err)
	}
	var list []protocol.ChunkMetadata
	_ = json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	want := protocol.ChunkMetadata{ChunkID: "obj_0", ObjectID: "obj", SequenceNo: 0}
	if len(list) != 1 || list[0] != want {
		t.Fatalf("unexpected list %+v", list)
	}

	for i := 0; i < 2; i++ { // second delete is idempotent
		req, _ = http.NewRequest(http.MethodDelete, url, nil)
		resp, err = client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE #%d status = %d", i+1, resp.StatusCode)
		}
	}

	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d", resp.StatusCode)
	}
}
