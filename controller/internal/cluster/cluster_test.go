package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

func seqIDs() func() (string, error) {
	var n atomic.Int64
	return func() (string, error) { return fmt.Sprintf("c%d", n.Add(1)), nil }
}

// readyRegistry returns a registry with n READY workers w0..w(n-1).
func readyRegistry(t *testing.T, n int) *Registry {
	t.Helper()
	r := NewRegistry(LeastLoaded{})
	for i := range n {
		id := fmt.Sprintf("w%d", i)
		res, err := r.Announce(id, fmt.Sprintf("http://10.0.0.%d:8080", i))
		if err != nil {
			t.Fatal(err)
		}
		if ok, _ := r.CompleteSync(id, res.SyncGen, nil); !ok {
			t.Fatalf("CompleteSync %s failed", id)
		}
	}
	return r
}

func counts(r *Registry) map[string]int {
	out := map[string]int{}
	for _, w := range r.Workers() {
		out[w.ID] = w.ChunkCount
	}
	return out
}

func TestPlaceUsesDistinctWorkersAndStaysBalanced(t *testing.T) {
	r := readyRegistry(t, 5)
	ids := seqIDs()
	for seq := range 1001 {
		locs, err := r.Place("obj", seq, 2, ids)
		if err != nil {
			t.Fatal(err)
		}
		if len(locs) != 2 || locs[0].WorkerID == locs[1].WorkerID {
			t.Fatalf("seq %d placed on %+v; want 2 distinct workers", seq, locs)
		}
		for _, l := range locs {
			if l.Status != ReplicaPending || l.Addr == "" {
				t.Fatalf("placed replica = %+v; want PENDING with an address", l)
			}
		}
		minC, maxC := 1<<30, 0
		for _, c := range counts(r) {
			minC, maxC = min(minC, c), max(maxC, c)
		}
		if maxC-minC > 1 {
			t.Fatalf("after %d placements chunk counts are %v; want within 1", seq+1, counts(r))
		}
	}
}

func TestPlaceNeedsEnoughReadyWorkers(t *testing.T) {
	r := readyRegistry(t, 1)
	// An AVAILABLE worker doesn't count.
	if _, err := r.Announce("pending", "http://10.0.0.99:8080"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Place("obj", 0, 2, seqIDs()); !errors.Is(err, ErrInsufficientWorkers) {
		t.Fatalf("error = %v, want ErrInsufficientWorkers", err)
	}
	if n := len(r.ObjectReplicas("obj")); n != 0 {
		t.Fatalf("failed placement left %d replicas", n)
	}
}

func TestReadableOnlyReturnsActiveReplicasOnReadyWorkers(t *testing.T) {
	r := readyRegistry(t, 3)
	locs, err := r.Place("obj", 0, 2, seqIDs())
	if err != nil {
		t.Fatal(err)
	}
	if n := len(r.Readable("obj", 0)); n != 0 {
		t.Fatalf("pending replicas readable: %d", n)
	}
	for _, l := range locs {
		r.SetStatus(l.ChunkID, ReplicaActive)
	}
	if n := len(r.Readable("obj", 0)); n != 2 {
		t.Fatalf("readable = %d, want 2", n)
	}
	r.SetStatus(locs[0].ChunkID, ReplicaCorrupted)
	got := r.Readable("obj", 0)
	if len(got) != 1 || got[0].ChunkID != locs[1].ChunkID {
		t.Fatalf("readable after corruption = %+v", got)
	}
}

func TestRemoveKeepsIndexesConsistent(t *testing.T) {
	r := readyRegistry(t, 2)
	locs, _ := r.Place("obj", 0, 2, seqIDs())
	for _, l := range locs {
		r.Remove(l.ChunkID)
	}
	r.Remove("never-existed")
	if n := len(r.ObjectReplicas("obj")); n != 0 {
		t.Fatalf("replicas left: %d", n)
	}
	for id, c := range counts(r) {
		if c != 0 {
			t.Fatalf("worker %s count = %d, want 0", id, c)
		}
	}
	if len(r.chunks) != 0 || len(r.byObject) != 0 {
		t.Fatalf("index not empty: chunks=%d byObject=%d", len(r.chunks), len(r.byObject))
	}
}

func TestAnnounceLifecycle(t *testing.T) {
	r := NewRegistry(LeastLoaded{})

	res, err := r.Announce("w", "http://a:1")
	if err != nil || !res.New || res.SyncGen == 0 || res.Worker.Status != WorkerAvailable {
		t.Fatalf("first announce = %+v, %v", res, err)
	}
	gen := res.SyncGen

	// Announcing again while the sync runs must not start another one.
	if res, _ := r.Announce("w", "http://a:1"); res.SyncGen != 0 || res.New {
		t.Fatalf("announce during sync = %+v", res)
	}

	// A failed sync is retried on the next announcement.
	r.FailSync("w", gen)
	res, _ = r.Announce("w", "http://a:1")
	if res.SyncGen == 0 {
		t.Fatal("announce after failed sync did not request a sync")
	}
	gen = res.SyncGen
	if ok, _ := r.CompleteSync("w", gen, []Replica{{ChunkID: "c1", ObjectID: "o", SeqNo: 0, Status: ReplicaActive}}); !ok {
		t.Fatal("CompleteSync failed")
	}
	if w, _ := r.Worker("w"); w.Status != WorkerReady || w.ChunkCount != 1 {
		t.Fatalf("after sync worker = %+v", w)
	}

	// Steady state: only LastSeen changes.
	if res, _ := r.Announce("w", "http://a:1"); res.SyncGen != 0 || res.Worker.Status != WorkerReady {
		t.Fatalf("steady announce = %+v", res)
	}

	// New address: replicas dropped, back to AVAILABLE, sync requested, and a
	// sync from the old generation is discarded.
	res, _ = r.Announce("w", "http://b:1")
	if !res.Moved || res.SyncGen == 0 || res.Worker.Status != WorkerAvailable || res.Worker.ChunkCount != 0 {
		t.Fatalf("moved announce = %+v", res)
	}
	if ok, _ := r.CompleteSync("w", gen, nil); ok {
		t.Fatal("stale sync was applied")
	}
	if ok, _ := r.CompleteSync("w", res.SyncGen, nil); !ok {
		t.Fatal("current sync was rejected")
	}
}

func TestCompleteSyncKeepsExistingEntries(t *testing.T) {
	r := readyRegistry(t, 2)
	locs, _ := r.Place("obj", 0, 2, seqIDs()) // PENDING on w0 and w1

	// w1 moves and reports its chunks, including the pending one and one
	// whose ID is registered on w0.
	var onW0, onW1 string
	for _, l := range locs {
		if l.WorkerID == "w0" {
			onW0 = l.ChunkID
		} else {
			onW1 = l.ChunkID
		}
	}
	res, _ := r.Announce("w1", "http://new:1")
	ok, conflicts := r.CompleteSync("w1", res.SyncGen, []Replica{
		{ChunkID: onW0, ObjectID: "obj", SeqNo: 0, Status: ReplicaActive},
		{ChunkID: onW1, ObjectID: "obj", SeqNo: 0, Status: ReplicaActive},
		{ChunkID: "x", ObjectID: "gone", SeqNo: 3, Status: ReplicaOrphan},
	})
	if !ok || len(conflicts) != 1 || conflicts[0] != onW0 {
		t.Fatalf("CompleteSync = %v, conflicts %v", ok, conflicts)
	}
	if w, _ := r.Worker("w1"); w.ChunkCount != 2 {
		t.Fatalf("w1 count = %d, want 2", w.ChunkCount)
	}
	if got := r.ObjectReplicas("gone"); len(got) != 1 || got[0].Status != ReplicaOrphan {
		t.Fatalf("orphan replica = %+v", got)
	}
}

func TestRemoveWorkerRetires(t *testing.T) {
	r := readyRegistry(t, 2)
	if _, err := r.Place("obj", 0, 2, seqIDs()); err != nil {
		t.Fatal(err)
	}
	if err := r.RemoveWorker("w0", true); err != nil {
		t.Fatal(err)
	}
	if err := r.RemoveWorker("w0", true); !errors.Is(err, ErrUnknownWorker) {
		t.Fatalf("second remove: %v", err)
	}
	if got := r.ObjectReplicas("obj"); len(got) != 1 || got[0].WorkerID != "w1" {
		t.Fatalf("replicas after removing w0 = %+v", got)
	}
	if _, err := r.Announce("w0", "http://x:1"); !errors.Is(err, ErrWorkerRetired) {
		t.Fatalf("retired announce: %v", err)
	}
}

// ---------- Membership ----------

type fakeLister struct {
	mu     sync.Mutex
	chunks map[string][]protocol.ChunkMetadata
	fail   map[string]bool
	calls  map[string]int
}

func (f *fakeLister) ListChunks(_ context.Context, addr string) ([]protocol.ChunkMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[addr]++
	if f.fail[addr] {
		return nil, errors.New("unreachable")
	}
	return f.chunks[addr], nil
}

func (f *fakeLister) setFail(addr string, fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[addr] = fail
}

type fakeIndex map[string]int // object -> chunk count

func (f fakeIndex) HasChunk(_ context.Context, objectID string, seq int) (bool, error) {
	n, ok := f[objectID]
	return ok && seq < n, nil
}

func waitStatus(t *testing.T, r *Registry, id string, want WorkerStatus) Worker {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if w, ok := r.Worker(id); ok && w.Status == want {
			return w
		}
		if time.Now().After(deadline) {
			w, _ := r.Worker(id)
			t.Fatalf("worker %s status = %s, want %s", id, w.Status, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRegisterLoadsChunkListAndClassifiesOrphans(t *testing.T) {
	addr := "http://10.1.1.1:8080"
	lister := &fakeLister{chunks: map[string][]protocol.ChunkMetadata{addr: {
		{ChunkID: "a", ObjectID: "obj", SequenceNo: 0},
		{ChunkID: "b", ObjectID: "obj", SequenceNo: 1},
		{ChunkID: "c", ObjectID: "obj", SequenceNo: 9}, // past the object's end
		{ChunkID: "d", ObjectID: "deleted", SequenceNo: 0},
	}}, fail: map[string]bool{}}
	r := NewRegistry(LeastLoaded{})
	m := NewMembership(r, lister, fakeIndex{"obj": 2}, 30*time.Second, time.Second, nil)
	defer m.Close()

	resp, err := m.Register(protocol.RegisterRequest{Port: 8080}, "10.1.1.1")
	if err != nil {
		t.Fatal(err)
	}
	if resp.WorkerID == "" || resp.Status != string(WorkerAvailable) || resp.AnnounceIntervalSeconds != 30 {
		t.Fatalf("response = %+v", resp)
	}
	w := waitStatus(t, r, resp.WorkerID, WorkerReady)
	if w.Addr != addr || w.ChunkCount != 4 {
		t.Fatalf("worker = %+v", w)
	}
	statuses := map[string]ReplicaStatus{}
	for _, obj := range []string{"obj", "deleted"} {
		for _, l := range r.ObjectReplicas(obj) {
			statuses[l.ChunkID] = l.Status
		}
	}
	want := map[string]ReplicaStatus{"a": ReplicaActive, "b": ReplicaActive, "c": ReplicaOrphan, "d": ReplicaOrphan}
	for id, s := range want {
		if statuses[id] != s {
			t.Errorf("chunk %s status = %s, want %s", id, statuses[id], s)
		}
	}

	// Re-announcing with the assigned ID keeps the same identity.
	again, err := m.Register(protocol.RegisterRequest{WorkerID: resp.WorkerID, Port: 8080}, "10.1.1.1")
	if err != nil || again.WorkerID != resp.WorkerID || again.Status != string(WorkerReady) {
		t.Fatalf("re-announce = %+v, %v", again, err)
	}
}

func TestRegisterRetriesFailedSync(t *testing.T) {
	addr := "http://10.1.1.2:9000"
	lister := &fakeLister{chunks: map[string][]protocol.ChunkMetadata{}, fail: map[string]bool{addr: true}}
	r := NewRegistry(LeastLoaded{})
	m := NewMembership(r, lister, fakeIndex{}, time.Second, time.Second, nil)
	defer m.Close()

	req := protocol.RegisterRequest{WorkerID: "known", Port: 9000}
	if _, err := m.Register(req, "10.1.1.2"); err != nil {
		t.Fatal(err)
	}
	// Wait for the failing sync to finish.
	deadline := time.Now().Add(2 * time.Second)
	for {
		r.mu.RLock()
		done := r.workers["known"].syncGen == 0
		r.mu.RUnlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sync did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	if w, _ := r.Worker("known"); w.Status != WorkerAvailable {
		t.Fatalf("status after failed sync = %s", w.Status)
	}

	lister.setFail(addr, false)
	if _, err := m.Register(req, "10.1.1.2"); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, r, "known", WorkerReady)
}

func TestRegisterValidationAndDeregister(t *testing.T) {
	r := NewRegistry(LeastLoaded{})
	m := NewMembership(r, &fakeLister{fail: map[string]bool{}}, fakeIndex{}, time.Second, time.Second, nil)
	defer m.Close()

	bad := []struct {
		req protocol.RegisterRequest
		ip  string
	}{
		{protocol.RegisterRequest{Port: 0}, "10.0.0.1"},
		{protocol.RegisterRequest{Port: 70000}, "10.0.0.1"},
		{protocol.RegisterRequest{WorkerID: "../etc", Port: 80}, "10.0.0.1"},
		{protocol.RegisterRequest{Port: 80}, "not-an-ip"},
	}
	for _, b := range bad {
		if _, err := m.Register(b.req, b.ip); !errors.Is(err, ErrInvalidRegistration) {
			t.Errorf("Register(%+v, %q) error = %v, want ErrInvalidRegistration", b.req, b.ip, err)
		}
	}

	resp, err := m.Register(protocol.RegisterRequest{Port: 80}, "10.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Deregister(resp.WorkerID); err != nil {
		t.Fatal(err)
	}
	if err := m.Deregister(resp.WorkerID); !errors.Is(err, ErrUnknownWorker) {
		t.Fatalf("second deregister: %v", err)
	}
	if _, err := m.Register(protocol.RegisterRequest{WorkerID: resp.WorkerID, Port: 80}, "10.0.0.1"); !errors.Is(err, ErrWorkerRetired) {
		t.Fatalf("register after deregister: %v", err)
	}
}
