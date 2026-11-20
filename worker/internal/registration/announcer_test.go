package registration

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

type memStore struct {
	mu sync.Mutex
	id string
}

func (m *memStore) LoadWorkerID(context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.id, nil
}

func (m *memStore) SaveWorkerID(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.id = id
	return nil
}

func (m *memStore) get() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.id
}

// fakeController records register requests. failFirst requests answer 503;
// after retireAfter successful ones (if > 0) it answers 410 worker_retired.
type fakeController struct {
	failFirst   int32
	retireAfter int32

	calls    atomic.Int32
	ok       atomic.Int32
	mu       sync.Mutex
	requests []protocol.RegisterRequest
}

func (f *fakeController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != RegisterPath {
		http.NotFound(w, r)
		return
	}
	var req protocol.RegisterRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()

	if f.calls.Add(1) <= f.failFirst {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if f.retireAfter > 0 && f.ok.Load() >= f.retireAfter {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGone)
		_ = json.NewEncoder(w).Encode(protocol.ErrorResponse{Error: protocol.ErrorBody{Code: protocol.CodeWorkerRetired}})
		return
	}
	f.ok.Add(1)
	id := req.WorkerID
	if id == "" {
		id = "assigned-1"
	}
	_ = json.NewEncoder(w).Encode(protocol.RegisterResponse{WorkerID: id, Status: "AVAILABLE"})
}

func (f *fakeController) snapshot() []protocol.RegisterRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]protocol.RegisterRequest(nil), f.requests...)
}

func newAnnouncer(t *testing.T, url string, store IdentityStore) *Announcer {
	t.Helper()
	a, err := New(context.Background(), Config{
		ControllerURL: url,
		Port:          8080,
		Interval:      20 * time.Millisecond,
		RetryMin:      5 * time.Millisecond,
		RetryMax:      10 * time.Millisecond,
	}, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestRegistersRetriesAndKeepsAnnouncing(t *testing.T) {
	fc := &fakeController{failFirst: 2}
	srv := httptest.NewServer(fc)
	defer srv.Close()
	store := &memStore{}
	a := newAnnouncer(t, srv.URL, store)
	if a.WorkerID() != "" {
		t.Fatalf("fresh worker has id %q", a.WorkerID())
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Two failures, then the registration, then at least two re-announcements.
	waitFor(t, func() bool { return fc.ok.Load() >= 3 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}

	if a.WorkerID() != "assigned-1" || store.get() != "assigned-1" {
		t.Fatalf("worker id = %q, stored %q; want assigned-1", a.WorkerID(), store.get())
	}
	reqs := fc.snapshot()
	for i, r := range reqs {
		want := "assigned-1"
		if i <= 2 { // the failed attempts and the first success had no id yet
			want = ""
		}
		if r.WorkerID != want || r.Port != 8080 {
			t.Fatalf("request %d = %+v, want worker_id %q port 8080", i, r, want)
		}
	}
}

func TestSendsStoredID(t *testing.T) {
	fc := &fakeController{}
	srv := httptest.NewServer(fc)
	defer srv.Close()
	a := newAnnouncer(t, srv.URL, &memStore{id: "existing"})
	if a.WorkerID() != "existing" {
		t.Fatalf("WorkerID = %q before registering; want the stored id", a.WorkerID())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.Run(ctx) //nolint:errcheck
	waitFor(t, func() bool { return fc.ok.Load() >= 1 })
	if got := fc.snapshot()[0].WorkerID; got != "existing" {
		t.Fatalf("first request sent worker_id %q, want existing", got)
	}
}

func TestStopsWhenRetired(t *testing.T) {
	fc := &fakeController{retireAfter: 1}
	srv := httptest.NewServer(fc)
	defer srv.Close()
	a := newAnnouncer(t, srv.URL, &memStore{})

	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrRetired) {
			t.Fatalf("Run returned %v, want ErrRetired", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after being retired")
	}
}
