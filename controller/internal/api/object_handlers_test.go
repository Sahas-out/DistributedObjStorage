package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/object"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

const testChunkSize = 8

type stubWorker struct {
	chunks map[string]protocol.Chunk
	putErr error
	getErr map[string]error
}

func (s *stubWorker) PutChunk(_ context.Context, id, obj string, seq int, data []byte, _ string) (protocol.ChunkMetadata, error) {
	if s.putErr != nil {
		return protocol.ChunkMetadata{}, s.putErr
	}
	s.chunks[id] = protocol.Chunk{ObjectID: obj, SequenceNo: seq, Data: bytes.Clone(data)}
	return protocol.ChunkMetadata{ChunkID: id, ObjectID: obj, SequenceNo: seq}, nil
}

func (s *stubWorker) GetChunk(_ context.Context, id string) (protocol.Chunk, error) {
	if err := s.getErr[id]; err != nil {
		return protocol.Chunk{}, err
	}
	return s.chunks[id], nil
}

func (s *stubWorker) DeleteChunk(_ context.Context, id string) error {
	delete(s.chunks, id)
	return nil
}

func newTestServer(t *testing.T) (*httptest.Server, *stubWorker) {
	t.Helper()
	store, err := metadata.Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	worker := &stubWorker{chunks: map[string]protocol.Chunk{}, getErr: map[string]error{}}
	svc := object.NewService(worker, store, testChunkSize, nil)
	srv := httptest.NewServer(NewServer(svc, store.Ping, nil).Handler())
	t.Cleanup(srv.Close)
	return srv, worker
}

func do(t *testing.T, method, url string, body io.Reader) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func upload(t *testing.T, base string, data []byte) protocol.ObjectInfo {
	t.Helper()
	resp := do(t, http.MethodPut, base+"/api/v1/objects", bytes.NewReader(data))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload status = %d", resp.StatusCode)
	}
	var info protocol.ObjectInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatal(err)
	}
	return info
}

func wantError(t *testing.T, resp *http.Response, status int, code string) {
	t.Helper()
	var body protocol.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if resp.StatusCode != status || body.Error.Code != code {
		t.Fatalf("got %d %q, want %d %q", resp.StatusCode, body.Error.Code, status, code)
	}
}

func TestUploadThenDownload(t *testing.T) {
	srv, _ := newTestServer(t)
	data := []byte(strings.Repeat("distributed!", 5))
	info := upload(t, srv.URL, data)
	if info.Size != int64(len(data)) || info.ChunkCount != 8 {
		t.Fatalf("info = %+v", info)
	}

	resp := do(t, http.MethodGet, srv.URL+"/api/v1/objects/"+info.ObjectID, nil)
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("status %d, body %q", resp.StatusCode, got)
	}
	if resp.Header.Get("Content-Length") != strconv.Itoa(len(data)) || resp.Header.Get("ETag") != `"`+info.Checksum+`"` {
		t.Fatalf("headers = %v", resp.Header)
	}
}

func TestHeadReturnsHeadersWithoutWorker(t *testing.T) {
	srv, worker := newTestServer(t)
	info := upload(t, srv.URL, []byte("hello world"))
	clear(worker.chunks)
	resp := do(t, http.MethodHead, srv.URL+"/api/v1/objects/"+info.ObjectID, nil)
	if resp.StatusCode != http.StatusOK || resp.ContentLength != 11 {
		t.Fatalf("status %d, length %d", resp.StatusCode, resp.ContentLength)
	}
}

func TestErrorResponses(t *testing.T) {
	srv, worker := newTestServer(t)
	objects := srv.URL + "/api/v1/objects"

	t.Run("malformed id", func(t *testing.T) {
		wantError(t, do(t, http.MethodGet, objects+"/not-an-id", nil), http.StatusBadRequest, protocol.CodeInvalidObjectID)
	})
	t.Run("unknown object", func(t *testing.T) {
		wantError(t, do(t, http.MethodGet, objects+"/"+strings.Repeat("a", 32), nil), http.StatusNotFound, protocol.CodeObjectNotFound)
	})
	t.Run("wrong method", func(t *testing.T) {
		wantError(t, do(t, http.MethodDelete, objects, nil), http.StatusMethodNotAllowed, protocol.CodeMethodNotAllowed)
	})
	t.Run("unknown route", func(t *testing.T) {
		wantError(t, do(t, http.MethodGet, srv.URL+"/nope", nil), http.StatusNotFound, protocol.CodeNotFound)
	})
	t.Run("corrupted first chunk", func(t *testing.T) {
		info := upload(t, srv.URL, []byte("some data"))
		worker.getErr[info.ObjectID+"_0"] = workers.ErrChunkCorrupted
		wantError(t, do(t, http.MethodGet, objects+"/"+info.ObjectID, nil), http.StatusInternalServerError, protocol.CodeObjectCorrupted)
	})
	t.Run("worker down on download", func(t *testing.T) {
		info := upload(t, srv.URL, []byte("some data"))
		worker.getErr[info.ObjectID+"_0"] = workers.ErrUnavailable
		wantError(t, do(t, http.MethodGet, objects+"/"+info.ObjectID, nil), http.StatusServiceUnavailable, protocol.CodeWorkerUnavailable)
	})
	t.Run("worker down on upload", func(t *testing.T) {
		worker.putErr = workers.ErrUnavailable
		defer func() { worker.putErr = nil }()
		wantError(t, do(t, http.MethodPut, objects, bytes.NewReader(make([]byte, 100))), http.StatusServiceUnavailable, protocol.CodeWorkerUnavailable)
	})
}

// A failure after the 200 was sent must not look like a complete download.
func TestMidStreamFailureAbortsConnection(t *testing.T) {
	srv, worker := newTestServer(t)
	info := upload(t, srv.URL, make([]byte, 3*testChunkSize))
	worker.getErr[info.ObjectID+"_1"] = workers.ErrUnavailable

	// Small responses may still be buffered when the connection is aborted, so
	// the error can surface on the request or while reading the body.
	resp, err := http.Get(srv.URL + "/api/v1/objects/" + info.ObjectID)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("reading the body succeeded, want an error from the aborted connection")
	}
}
