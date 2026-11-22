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

// fakeController records register requests. failFirst requests answer 503;
// after retireAfter successful ones (if > 0) it answers 410 worker_retired.
type fakeController struct {
	failFirst   int32
	retireAfter int32
	intervalSec int // announce_interval_seconds in responses; 0 omits it

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
	_ = json.NewEncoder(w).Encode(protocol.RegisterResponse{WorkerID: id, Status: "AVAILABLE", AnnounceIntervalSeconds: f.intervalSec})
}

func (f *fakeController) snapshot() []protocol.RegisterRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]protocol.RegisterRequest(nil), f.requests...)
}

func newAnnouncer(t *testing.T, url string) *Announcer {
	t.Helper()
	return New(Config{
		ControllerURL: url,
		Port:          8080,
		Interval:      20 * time.Millisecond,
		RetryMin:      5 * time.Millisecond,
		RetryMax:      10 * time.Millisecond,
	}, nil)
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
	a := newAnnouncer(t, srv.URL)
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

	if a.WorkerID() != "assigned-1" {
		t.Fatalf("worker id = %q, want assigned-1", a.WorkerID())
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

func TestStopsWhenRetired(t *testing.T) {
	fc := &fakeController{retireAfter: 1}
	srv := httptest.NewServer(fc)
	defer srv.Close()
	a := newAnnouncer(t, srv.URL)

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

func TestAdoptsControllerInterval(t *testing.T) {
	fc := &fakeController{intervalSec: 7}
	srv := httptest.NewServer(fc)
	defer srv.Close()
	a := newAnnouncer(t, srv.URL)
	if got := a.Interval(); got != 20*time.Millisecond {
		t.Fatalf("initial interval = %s", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()
	waitFor(t, func() bool { return a.Interval() == 7*time.Second })
	// With a 7s interval there is no second announcement during the test.
	time.Sleep(100 * time.Millisecond)
	if n := fc.ok.Load(); n != 1 {
		t.Fatalf("announcements = %d, want 1", n)
	}
}
