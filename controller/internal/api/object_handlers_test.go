package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/cluster"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/events"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/object"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

const testChunkSize = 8

// stubWorker simulates every worker at once; chunk IDs are unique across
// workers, so the address can be ignored.
type stubWorker struct {
	mu     sync.Mutex
	chunks map[string]protocol.Chunk
	putErr error
	getErr map[string]error
}

func (s *stubWorker) PutChunk(_ context.Context, _, id, obj string, seq int, data []byte, _ string) (protocol.ChunkMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.putErr != nil {
		return protocol.ChunkMetadata{}, s.putErr
	}
	s.chunks[id] = protocol.Chunk{ObjectID: obj, SequenceNo: seq, Data: bytes.Clone(data)}
	return protocol.ChunkMetadata{ChunkID: id, ObjectID: obj, SequenceNo: seq}, nil
}

func (s *stubWorker) GetChunk(_ context.Context, _, id string) (protocol.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.getErr[id]; err != nil {
		return protocol.Chunk{}, err
	}
	c, ok := s.chunks[id]
	if !ok {
		return protocol.Chunk{}, workers.ErrChunkNotFound
	}
	return c, nil
}

func (s *stubWorker) DeleteChunk(_ context.Context, _, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.chunks, id)
	return nil
}

func (s *stubWorker) ListChunks(context.Context, string) ([]protocol.ChunkMetadata, error) {
	return nil, nil
}

func (s *stubWorker) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.chunks)
}

// failChunk makes every replica of one chunk fail with err.
func (s *stubWorker) failChunk(reg *cluster.Registry, objectID string, seq int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range reg.ObjectReplicas(objectID) {
		if l.SeqNo == seq {
			s.getErr[l.ChunkID] = err
		}
	}
}

type testEnv struct {
	srv    *httptest.Server
	worker *stubWorker
	reg    *cluster.Registry
	stats  *stubStats
}

// newTestServer serves the API with workers ready workers and replication
// factor 2.
func newTestServer(t *testing.T, ready ...int) testEnv {
	t.Helper()
	n := 2
	if len(ready) > 0 {
		n = ready[0]
	}
	store, err := metadata.Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	worker := &stubWorker{chunks: map[string]protocol.Chunk{}, getErr: map[string]error{}}
	reg := cluster.NewRegistry(cluster.LeastLoaded{})
	for i := range n {
		id := "w" + strconv.Itoa(i)
		res, err := reg.Announce(id, "http://10.0.0."+strconv.Itoa(i)+":8080")
		if err != nil {
			t.Fatal(err)
		}
		reg.CompleteSync(id, res.SyncGen, nil)
	}
	members := cluster.NewMembership(reg, worker, store, 30*time.Second, time.Second, nil)
	t.Cleanup(members.Close)
	svc := object.NewService(worker, store, reg, 2, testChunkSize, nil)
	feed := events.New(events.DefaultCapacity)
	svc.SetEvents(feed)
	members.SetEvents(feed)
	stats := &stubStats{byAddr: map[string]protocol.WorkerStats{}}
	api := NewServer(svc, members, store.Ping, nil)
	api.EnableDashboard(Dashboard{Stats: stats, Events: feed, ReplicationFactor: 2, AnnounceInterval: 30 * time.Second, StartedAt: time.Now()})
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return testEnv{srv: srv, worker: worker, reg: reg, stats: stats}
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

// formPart is one field of a multipart/form-data upload. A part with a
// filename is a file field.
type formPart struct {
	field, filename, contentType, body string
}

func filePart(name string, data []byte) formPart {
	return formPart{field: "file", filename: name, contentType: "application/octet-stream", body: string(data)}
}

func metaPart(json string) formPart { return formPart{field: "metadata", body: json} }

// postForm sends parts as a multipart/form-data POST to /api/v1/objects.
func postForm(t *testing.T, base string, parts ...formPart) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for _, p := range parts {
		h := textproto.MIMEHeader{}
		cd := fmt.Sprintf(`form-data; name=%q`, p.field)
		if p.filename != "" {
			cd += fmt.Sprintf(`; filename=%q`, p.filename)
		}
		h.Set("Content-Disposition", cd)
		if p.contentType != "" {
			h.Set("Content-Type", p.contentType)
		}
		w, err := mw.CreatePart(h)
		if err != nil {
			t.Fatal(err)
		}
		io.WriteString(w, p.body)
	}
	mw.Close()
	req, err := http.NewRequest(http.MethodPost, base+"/api/v1/objects", &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func uploadParts(t *testing.T, base string, parts ...formPart) protocol.ObjectInfo {
	t.Helper()
	resp := postForm(t, base, parts...)
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload status = %d: %s", resp.StatusCode, body)
	}
	var info protocol.ObjectInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatal(err)
	}
	return info
}

func upload(t *testing.T, base string, data []byte) protocol.ObjectInfo {
	t.Helper()
	return uploadParts(t, base, filePart("data.bin", data))
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
	env := newTestServer(t)
	srv := env.srv
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
	env := newTestServer(t)
	srv := env.srv
	info := upload(t, srv.URL, []byte("hello world"))
	env.worker.mu.Lock()
	clear(env.worker.chunks)
	env.worker.mu.Unlock()
	resp := do(t, http.MethodHead, srv.URL+"/api/v1/objects/"+info.ObjectID, nil)
	if resp.StatusCode != http.StatusOK || resp.ContentLength != 11 {
		t.Fatalf("status %d, length %d", resp.StatusCode, resp.ContentLength)
	}
}

func TestErrorResponses(t *testing.T) {
	env := newTestServer(t)
	srv, worker := env.srv, env.worker
	objects := srv.URL + "/api/v1/objects"

	t.Run("malformed id", func(t *testing.T) {
		wantError(t, do(t, http.MethodGet, objects+"/not-an-id", nil), http.StatusBadRequest, protocol.CodeInvalidObjectID)
	})
	t.Run("unknown object", func(t *testing.T) {
		wantError(t, do(t, http.MethodGet, objects+"/"+strings.Repeat("a", 32), nil), http.StatusNotFound, protocol.CodeObjectNotFound)
	})
	t.Run("wrong method", func(t *testing.T) {
		wantError(t, do(t, http.MethodDelete, objects, nil), http.StatusMethodNotAllowed, protocol.CodeMethodNotAllowed)
		wantError(t, do(t, http.MethodPost, objects+"/"+strings.Repeat("a", 32), nil), http.StatusMethodNotAllowed, protocol.CodeMethodNotAllowed)
	})
	t.Run("upload with PUT", func(t *testing.T) {
		wantError(t, do(t, http.MethodPut, objects, strings.NewReader("data")), http.StatusMethodNotAllowed, protocol.CodeMethodNotAllowed)
	})
	t.Run("unknown route", func(t *testing.T) {
		wantError(t, do(t, http.MethodGet, srv.URL+"/nope", nil), http.StatusNotFound, protocol.CodeNotFound)
	})
	t.Run("every copy of the first chunk corrupted", func(t *testing.T) {
		info := upload(t, srv.URL, []byte("some data"))
		worker.failChunk(env.reg, info.ObjectID, 0, workers.ErrChunkCorrupted)
		wantError(t, do(t, http.MethodGet, objects+"/"+info.ObjectID, nil), http.StatusInternalServerError, protocol.CodeObjectCorrupted)
	})
	t.Run("every copy unreachable on download", func(t *testing.T) {
		info := upload(t, srv.URL, []byte("some data"))
		worker.failChunk(env.reg, info.ObjectID, 0, workers.ErrUnavailable)
		wantError(t, do(t, http.MethodGet, objects+"/"+info.ObjectID, nil), http.StatusServiceUnavailable, protocol.CodeObjectUnavailable)
	})
	t.Run("worker down on upload", func(t *testing.T) {
		worker.mu.Lock()
		worker.putErr = workers.ErrUnavailable
		worker.mu.Unlock()
		defer func() {
			worker.mu.Lock()
			worker.putErr = nil
			worker.mu.Unlock()
		}()
		wantError(t, postForm(t, srv.URL, filePart("x.bin", make([]byte, 100))), http.StatusServiceUnavailable, protocol.CodeWorkerUnavailable)
	})
}

// A failure after the 200 was sent must not look like a complete download.
func TestMidStreamFailureAbortsConnection(t *testing.T) {
	env := newTestServer(t)
	srv := env.srv
	info := upload(t, srv.URL, make([]byte, 3*testChunkSize))
	env.worker.failChunk(env.reg, info.ObjectID, 1, workers.ErrUnavailable)

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

func TestUploadWithTooFewWorkers(t *testing.T) {
	env := newTestServer(t, 1)
	resp := postForm(t, env.srv.URL, filePart("x.bin", []byte("data")))
	wantError(t, resp, http.StatusServiceUnavailable, protocol.CodeInsufficientWorkers)
}

func TestListAndDeleteObjects(t *testing.T) {
	env := newTestServer(t, 3)
	objects := env.srv.URL + "/api/v1/objects"

	resp := do(t, http.MethodGet, objects, nil)
	var list []protocol.ObjectInfo
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil || resp.StatusCode != http.StatusOK || list == nil || len(list) != 0 {
		t.Fatalf("empty list: status %d, %v, %v", resp.StatusCode, list, err)
	}

	a := upload(t, env.srv.URL, []byte("first object"))
	b := upload(t, env.srv.URL, []byte("second"))
	resp = do(t, http.MethodGet, objects, nil)
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ObjectID != a.ObjectID || list[1].ObjectID != b.ObjectID || list[0].ChunkCount != 2 {
		t.Fatalf("list = %+v", list)
	}

	resp = do(t, http.MethodDelete, objects+"/"+a.ObjectID, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d", resp.StatusCode)
	}
	wantError(t, do(t, http.MethodDelete, objects+"/"+a.ObjectID, nil), http.StatusNotFound, protocol.CodeObjectNotFound)
	wantError(t, do(t, http.MethodGet, objects+"/"+a.ObjectID, nil), http.StatusNotFound, protocol.CodeObjectNotFound)
	wantError(t, do(t, http.MethodDelete, objects+"/BAD", nil), http.StatusBadRequest, protocol.CodeInvalidObjectID)
	if n := env.worker.count(); n != 2 { // b's single chunk, twice
		t.Fatalf("workers hold %d chunks after delete, want 2", n)
	}
}

func TestReplicasEndpoint(t *testing.T) {
	env := newTestServer(t, 3)
	info := upload(t, env.srv.URL, make([]byte, 2*testChunkSize))
	resp := do(t, http.MethodGet, env.srv.URL+"/internal/objects/"+info.ObjectID+"/replicas", nil)
	var body struct {
		protocol.ObjectInfo
		Replicas []protocol.ReplicaInfo `json:"replicas"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, %v", resp.StatusCode, err)
	}
	if body.ObjectID != info.ObjectID || len(body.Replicas) != 4 {
		t.Fatalf("body = %+v", body)
	}
	for _, r := range body.Replicas {
		if r.Status != string(cluster.ReplicaActive) || r.WorkerAddr == "" || r.ChunkID == "" {
			t.Fatalf("replica = %+v", r)
		}
	}
	wantError(t, do(t, http.MethodGet, env.srv.URL+"/internal/objects/"+strings.Repeat("b", 32)+"/replicas", nil), http.StatusNotFound, protocol.CodeObjectNotFound)
}

func TestWorkerRegistrationEndpoints(t *testing.T) {
	env := newTestServer(t, 0)
	base := env.srv.URL + "/internal/workers"

	post := func(body string) *http.Response {
		return do(t, http.MethodPost, base+"/register", strings.NewReader(body))
	}

	resp := post(`{"port": 8080}`)
	var reg protocol.RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&reg); err != nil || resp.StatusCode != http.StatusOK || reg.WorkerID == "" {
		t.Fatalf("register: status %d, %+v, %v", resp.StatusCode, reg, err)
	}
	if reg.AnnounceIntervalSeconds != 30 {
		t.Fatalf("announce interval = %d", reg.AnnounceIntervalSeconds)
	}

	// The controller derives the address from the connection (127.0.0.1 here).
	deadline := time.Now().Add(2 * time.Second)
	var ws []protocol.WorkerInfo
	for {
		resp = do(t, http.MethodGet, base, nil)
		if err := json.NewDecoder(resp.Body).Decode(&ws); err != nil {
			t.Fatal(err)
		}
		if len(ws) == 1 && ws[0].Status == string(cluster.WorkerReady) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workers = %+v; want one READY worker", ws)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ws[0].WorkerID != reg.WorkerID || ws[0].Addr != "http://127.0.0.1:8080" {
		t.Fatalf("worker = %+v", ws[0])
	}

	wantError(t, post(`not json`), http.StatusBadRequest, protocol.CodeInvalidRequest)
	wantError(t, post(`{"port": 0}`), http.StatusBadRequest, protocol.CodeInvalidRequest)
	wantError(t, do(t, http.MethodGet, base+"/register", nil), http.StatusMethodNotAllowed, protocol.CodeMethodNotAllowed)

	if resp := do(t, http.MethodDelete, base+"/"+reg.WorkerID, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("deregister status = %d", resp.StatusCode)
	}
	wantError(t, do(t, http.MethodDelete, base+"/"+reg.WorkerID, nil), http.StatusNotFound, protocol.CodeWorkerNotFound)
	wantError(t, post(`{"worker_id": "`+reg.WorkerID+`", "port": 8080}`), http.StatusGone, protocol.CodeWorkerRetired)
}
