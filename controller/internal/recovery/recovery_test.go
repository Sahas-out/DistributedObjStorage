package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/cluster"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

const deadAfter = 30 * time.Millisecond

// fakeWorkers simulates the workers' disks, keyed by address.
type fakeWorkers struct {
	mu      sync.Mutex
	chunks  map[string]map[string]protocol.Chunk // addr -> chunk ID -> chunk
	down    map[string]bool                      // addr -> every call fails
	onPut   func(chunkID string)                 // called after a successful put
	deletes atomic.Int32
}

func (f *fakeWorkers) PutChunk(_ context.Context, addr, id, obj string, seq int, data []byte, _ string) (protocol.ChunkMetadata, error) {
	f.mu.Lock()
	if f.down[addr] {
		f.mu.Unlock()
		return protocol.ChunkMetadata{}, workers.ErrUnavailable
	}
	if f.chunks[addr] == nil {
		f.chunks[addr] = map[string]protocol.Chunk{}
	}
	f.chunks[addr][id] = protocol.Chunk{ObjectID: obj, SequenceNo: seq, Data: bytes.Clone(data)}
	hook := f.onPut
	f.mu.Unlock()
	if hook != nil {
		hook(id)
	}
	return protocol.ChunkMetadata{ChunkID: id, ObjectID: obj, SequenceNo: seq}, nil
}

func (f *fakeWorkers) GetChunk(_ context.Context, addr, id string) (protocol.Chunk, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[addr] {
		return protocol.Chunk{}, workers.ErrUnavailable
	}
	c, ok := f.chunks[addr][id]
	if !ok {
		return protocol.Chunk{}, workers.ErrChunkNotFound
	}
	return c, nil
}

func (f *fakeWorkers) DeleteChunk(_ context.Context, addr, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[addr] {
		return workers.ErrUnavailable
	}
	delete(f.chunks[addr], id)
	f.deletes.Add(1)
	return nil
}

func (f *fakeWorkers) data(addr, id string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.chunks[addr][id]
	return c.Data, ok
}

func (f *fakeWorkers) corrupt(addr, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.chunks[addr][id]
	c.Data = bytes.Repeat([]byte{0xFF}, len(c.Data))
	f.chunks[addr][id] = c
}

func (f *fakeWorkers) remove(addr, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.chunks[addr], id)
}

// fakeMeta holds object chunks and deletes objects like object.Service:
// metadata first, then every replica in the registry.
type fakeMeta struct {
	mu      sync.Mutex
	chunks  map[cluster.ChunkKey]metadata.ChunkRef
	reg     *cluster.Registry
	deleted []string
}

func (m *fakeMeta) AllChunks(context.Context) ([]metadata.ChunkRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []metadata.ChunkRef
	for _, c := range m.chunks {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ObjectID != out[j].ObjectID {
			return out[i].ObjectID < out[j].ObjectID
		}
		return out[i].SequenceNo < out[j].SequenceNo
	})
	return out, nil
}

func (m *fakeMeta) HasChunk(_ context.Context, obj string, seq int) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.chunks[cluster.ChunkKey{ObjectID: obj, SeqNo: seq}]
	return ok, nil
}

func (m *fakeMeta) Delete(_ context.Context, obj string) error {
	m.mu.Lock()
	found := false
	for k := range m.chunks {
		if k.ObjectID == obj {
			delete(m.chunks, k)
			found = true
		}
	}
	if found {
		m.deleted = append(m.deleted, obj)
	}
	m.mu.Unlock()
	if !found {
		return metadata.ErrNotFound
	}
	for _, l := range m.reg.ObjectReplicas(obj) {
		m.reg.Remove(l.ChunkID)
	}
	return nil
}

func (m *fakeMeta) deletedObjects() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.deleted...)
}

// copySpec is a copy a test worker holds when it reports its chunks.
type copySpec struct {
	obj     string
	seq     int
	chunkID string
	status  cluster.ReplicaStatus // ACTIVE if empty
}

func cp(obj string, seq int, chunkID string) copySpec {
	return copySpec{obj: obj, seq: seq, chunkID: chunkID}
}

type harness struct {
	t     *testing.T
	reg   *cluster.Registry
	fw    *fakeWorkers
	meta  *fakeMeta
	rec   *Recovery
	data  map[cluster.ChunkKey][]byte
	addrs map[string]string // worker ID -> address
}

func newHarness(t *testing.T) *harness {
	reg := cluster.NewRegistry(cluster.LeastLoaded{})
	fw := &fakeWorkers{chunks: map[string]map[string]protocol.Chunk{}, down: map[string]bool{}}
	meta := &fakeMeta{chunks: map[cluster.ChunkKey]metadata.ChunkRef{}, reg: reg}
	rec := New(reg, fw, meta, meta, Config{ReplicationFactor: 2, Interval: time.Hour, DeadAfter: deadAfter}, nil)
	var n atomic.Int32
	rec.newID = func() (string, error) { return fmt.Sprintf("new%d", n.Add(1)), nil }
	return &harness{t: t, reg: reg, fw: fw, meta: meta, rec: rec,
		data: map[cluster.ChunkKey][]byte{}, addrs: map[string]string{}}
}

// object records an object with the given number of chunks in the metadata.
func (h *harness) object(obj string, chunks int) {
	for seq := range chunks {
		k := cluster.ChunkKey{ObjectID: obj, SeqNo: seq}
		data := []byte(fmt.Sprintf("%s-chunk-%d", obj, seq))
		sum := sha256.Sum256(data)
		h.data[k] = data
		h.meta.chunks[k] = metadata.ChunkRef{ObjectID: obj, Chunk: metadata.Chunk{
			SequenceNo: seq, Size: int64(len(data)), Checksum: hex.EncodeToString(sum[:]),
		}}
	}
}

// worker registers a READY worker that holds the given copies. A copy of a
// chunk the metadata doesn't know is stored with filler data.
func (h *harness) worker(id string, copies ...copySpec) {
	h.workerAt(id, fmt.Sprintf("http://%s:8080", id), copies...)
}

func (h *harness) workerAt(id, addr string, copies ...copySpec) {
	h.t.Helper()
	h.addrs[id] = addr
	res, err := h.reg.Announce(id, addr)
	if err != nil {
		h.t.Fatal(err)
	}
	var reps []cluster.Replica
	for _, c := range copies {
		data := h.data[cluster.ChunkKey{ObjectID: c.obj, SeqNo: c.seq}]
		if data == nil {
			data = []byte("orphan data")
		}
		if _, err := h.fw.PutChunk(context.Background(), addr, c.chunkID, c.obj, c.seq, data, ""); err != nil {
			h.t.Fatal(err)
		}
		st := c.status
		if st == "" {
			st = cluster.ReplicaActive
		}
		reps = append(reps, cluster.Replica{ChunkID: c.chunkID, ObjectID: c.obj, SeqNo: c.seq, Status: st})
	}
	if res.SyncGen != 0 {
		if ok, _ := h.reg.CompleteSync(id, res.SyncGen, reps); !ok {
			h.t.Fatalf("CompleteSync %s failed", id)
		}
	}
}

// kill lets the given workers miss their heartbeats while every other
// registered worker keeps announcing.
func (h *harness) kill(ids ...string) {
	h.t.Helper()
	time.Sleep(deadAfter + 10*time.Millisecond)
	dying := map[string]bool{}
	for _, id := range ids {
		dying[id] = true
	}
	for _, w := range h.reg.Workers() {
		if !dying[w.ID] && w.Status != cluster.WorkerDead {
			if _, err := h.reg.Announce(w.ID, w.Addr); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	for _, id := range ids {
		h.fw.down[h.addrs[id]] = true
	}
}

func (h *harness) tick() { h.rec.tick(context.Background()) }

// copies returns the status of every replica of one chunk, by worker ID.
func (h *harness) copies(obj string, seq int) map[string]cluster.ReplicaStatus {
	out := map[string]cluster.ReplicaStatus{}
	for _, l := range h.reg.ObjectReplicas(obj) {
		if l.SeqNo == seq {
			out[l.WorkerID] = l.Status
		}
	}
	return out
}

func (h *harness) chunkID(obj string, seq int, worker string) string {
	for _, l := range h.reg.ObjectReplicas(obj) {
		if l.SeqNo == seq && l.WorkerID == worker {
			return l.ChunkID
		}
	}
	h.t.Fatalf("no copy of %s/%d on %s", obj, seq, worker)
	return ""
}

// assertGoodOn checks that the chunk has exactly the given ACTIVE copies and
// that each worker's disk holds the right data for them.
func (h *harness) assertGoodOn(obj string, seq int, workerIDs ...string) {
	h.t.Helper()
	got := h.copies(obj, seq)
	want := map[string]cluster.ReplicaStatus{}
	for _, id := range workerIDs {
		want[id] = cluster.ReplicaActive
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		h.t.Fatalf("copies of %s/%d = %v; want %v", obj, seq, got, want)
	}
	for _, id := range workerIDs {
		data, ok := h.fw.data(h.addrs[id], h.chunkID(obj, seq, id))
		if !ok || !bytes.Equal(data, h.data[cluster.ChunkKey{ObjectID: obj, SeqNo: seq}]) {
			h.t.Fatalf("worker %s holds %q for %s/%d", id, data, obj, seq)
		}
	}
}

func TestRestoresCopiesOfDeadWorker(t *testing.T) {
	h := newHarness(t)
	h.object("o", 2)
	h.worker("w0", cp("o", 0, "a0"), cp("o", 1, "a1"))
	h.worker("w1", cp("o", 0, "b0"))
	h.worker("w2", cp("o", 1, "c1"))

	h.kill("w0")
	h.tick()

	if w, _ := h.reg.Worker("w0"); w.Status != cluster.WorkerDead || w.ChunkCount != 0 {
		t.Fatalf("w0 = %+v; want DEAD with its LOST copies dropped", w)
	}
	h.assertGoodOn("o", 0, "w1", "w2")
	h.assertGoodOn("o", 1, "w1", "w2")
	if st := h.rec.Status(); st.DegradedChunks != 0 || st.UnavailableChunks != 0 {
		t.Fatalf("status = %+v", st)
	}
}

func TestRepairsCorruptedAndMissingCopiesInPlace(t *testing.T) {
	h := newHarness(t)
	h.object("o", 2)
	h.worker("w0", cp("o", 0, "a0"), cp("o", 1, "a1"))
	h.worker("w1", cp("o", 0, "b0"), cp("o", 1, "b1"))
	h.worker("w2")

	// Downloads found a0 corrupted and b1 missing.
	h.fw.corrupt(h.addrs["w0"], "a0")
	h.reg.SetStatus("a0", cluster.ReplicaCorrupted)
	h.fw.remove(h.addrs["w1"], "b1")
	h.reg.SetStatus("b1", cluster.ReplicaLost)

	h.tick()

	// Same workers, same chunk IDs, good data again; w2 got nothing.
	h.assertGoodOn("o", 0, "w0", "w1")
	h.assertGoodOn("o", 1, "w0", "w1")
	if h.chunkID("o", 0, "w0") != "a0" || h.chunkID("o", 1, "w1") != "b1" {
		t.Fatal("repaired copies changed chunk ID")
	}
}

func TestFindsCorruptionWhileCopying(t *testing.T) {
	h := newHarness(t)
	h.object("o", 1)
	h.worker("w0", cp("o", 0, "a0"))
	h.worker("w1", cp("o", 0, "b0"))
	h.worker("w2")
	// w0 dies; w1's copy is silently corrupted, so the chunk can't be
	// restored from it.
	h.fw.corrupt(h.addrs["w1"], "b0")
	h.kill("w0")
	h.tick()
	if got := h.copies("o", 0); got["w1"] != cluster.ReplicaCorrupted || got["w0"] != cluster.ReplicaLost || len(got) != 2 {
		t.Fatalf("copies = %v; want w1 CORRUPTED, w0 LOST, nothing new", got)
	}
	// Next tick: the only other copy is on a dead worker, so the object is
	// kept and reported unavailable.
	h.tick()
	if st := h.rec.Status(); st.UnavailableChunks != 1 {
		t.Fatalf("status = %+v; want 1 unavailable chunk", st)
	}
	if d := h.meta.deletedObjects(); len(d) != 0 {
		t.Fatalf("deleted %v while a copy may come back", d)
	}
}

func TestUnavailableChunkRecoversWhenWorkerReturns(t *testing.T) {
	h := newHarness(t)
	h.object("o", 1)
	h.worker("w0", cp("o", 0, "a0"))
	h.worker("w1", cp("o", 0, "b0"))
	h.worker("w2")

	h.kill("w0", "w1")
	h.tick()
	h.tick()
	if st := h.rec.Status(); st.UnavailableChunks != 1 {
		t.Fatalf("status = %+v; want 1 unavailable chunk", st)
	}
	if d := h.meta.deletedObjects(); len(d) != 0 {
		t.Fatalf("object deleted: %v", d)
	}

	// w0 restarts: a new process (new ID) at the same address, with its disk.
	h.fw.down[h.addrs["w0"]] = false
	h.workerAt("w0b", h.addrs["w0"], cp("o", 0, "a0"))
	h.tick()

	h.assertGoodOn("o", 0, "w0b", "w2")
	if st := h.rec.Status(); st.UnavailableChunks != 0 {
		t.Fatalf("status = %+v", st)
	}
}

func TestTrimsExtraCopiesWhenDeadWorkerReturns(t *testing.T) {
	h := newHarness(t)
	h.object("o", 1)
	h.worker("w0", cp("o", 0, "a0"))
	h.worker("w1", cp("o", 0, "b0"))
	h.worker("w2")
	h.kill("w0")
	h.tick() // restored on w2
	h.assertGoodOn("o", 0, "w1", "w2")

	// w0 comes back with the same ID and its old copy: three copies now.
	h.fw.down[h.addrs["w0"]] = false
	res, err := h.reg.Announce("w0", h.addrs["w0"])
	if err != nil || !res.Revived {
		t.Fatalf("announce = %+v, %v", res, err)
	}
	h.reg.CompleteSync("w0", res.SyncGen, []cluster.Replica{{ChunkID: "a0", ObjectID: "o", SeqNo: 0, Status: cluster.ReplicaActive}})
	if n := len(h.copies("o", 0)); n != 3 {
		t.Fatalf("copies = %d; want 3 before trimming", n)
	}
	h.tick()
	if got := h.copies("o", 0); len(got) != 2 {
		t.Fatalf("copies after trim = %v; want 2", got)
	}
	if h.fw.deletes.Load() != 1 {
		t.Fatalf("deletes = %d; want 1", h.fw.deletes.Load())
	}
}

func TestDeletesObjectWhenEveryCopyIsCorrupted(t *testing.T) {
	h := newHarness(t)
	h.object("bad", 2)
	h.object("fine", 1)
	h.worker("w0", cp("bad", 0, "a0"), cp("bad", 1, "a1"), cp("fine", 0, "f0"))
	h.worker("w1", cp("bad", 0, "b0"), cp("bad", 1, "b1"), cp("fine", 0, "f1"))
	h.reg.SetStatus("a1", cluster.ReplicaCorrupted)
	h.reg.SetStatus("b1", cluster.ReplicaCorrupted)

	h.tick()

	if d := h.meta.deletedObjects(); len(d) != 1 || d[0] != "bad" {
		t.Fatalf("deleted = %v; want [bad]", d)
	}
	if n := len(h.reg.ObjectReplicas("bad")); n != 0 {
		t.Fatalf("%d copies of the deleted object left", n)
	}
	h.assertGoodOn("fine", 0, "w0", "w1")
}

func TestNothingDeletedWhileAWorkerIsSyncing(t *testing.T) {
	h := newHarness(t)
	h.object("o", 1)
	h.worker("w0", cp("o", 0, "a0"))
	h.worker("w1", cp("o", 0, "b0"))
	h.reg.SetStatus("a0", cluster.ReplicaCorrupted)
	h.reg.SetStatus("b0", cluster.ReplicaCorrupted)
	// A worker whose chunk list isn't loaded yet may hold a good copy.
	if _, err := h.reg.Announce("w2", "http://w2:8080"); err != nil {
		t.Fatal(err)
	}
	h.tick()
	if d := h.meta.deletedObjects(); len(d) != 0 {
		t.Fatalf("deleted %v while a worker was syncing", d)
	}
}

func TestRemovesOrphans(t *testing.T) {
	h := newHarness(t)
	h.object("o", 1)
	orphan := copySpec{obj: "gone", seq: 0, chunkID: "x", status: cluster.ReplicaOrphan}
	h.worker("w0", cp("o", 0, "a0"), orphan)
	h.worker("w1", cp("o", 0, "b0"))
	h.worker("w2", copySpec{obj: "gone", seq: 1, chunkID: "y", status: cluster.ReplicaOrphan})
	h.kill("w2") // an orphan on a dead worker is only dropped from the table

	h.tick()

	if _, ok := h.fw.data(h.addrs["w0"], "x"); ok {
		t.Fatal("orphan still on w0's disk")
	}
	if n := len(h.reg.ObjectReplicas("gone")); n != 0 {
		t.Fatalf("%d orphan entries left", n)
	}
	if st := h.rec.Status(); st.OrphanReplicas != 0 {
		t.Fatalf("status = %+v", st)
	}
	h.assertGoodOn("o", 0, "w0", "w1")
}

func TestStartupGraceDelaysRepairsButNotDetection(t *testing.T) {
	h := newHarness(t)
	h.rec.cfg.StartupGrace = time.Hour
	h.object("o", 1)
	h.worker("w0", cp("o", 0, "a0"))
	h.worker("w1", cp("o", 0, "b0"))
	h.worker("w2")
	h.kill("w0")
	h.tick()
	if w, _ := h.reg.Worker("w0"); w.Status != cluster.WorkerDead {
		t.Fatalf("w0 = %s; want DEAD during the grace period", w.Status)
	}
	if got := h.copies("o", 0); len(got) != 2 || got["w0"] != cluster.ReplicaLost {
		t.Fatalf("copies = %v; want no repair during the grace period", got)
	}
}

func TestCopyForObjectDeletedMeanwhileIsRemoved(t *testing.T) {
	h := newHarness(t)
	h.object("o", 1)
	h.worker("w0", cp("o", 0, "a0"))
	h.worker("w1", cp("o", 0, "b0"))
	h.worker("w2")
	h.kill("w0")
	// The client deletes the object while recovery writes the new copy.
	h.fw.onPut = func(string) { _ = h.meta.Delete(context.Background(), "o") }
	h.tick()
	if n := len(h.reg.ObjectReplicas("o")); n != 0 {
		t.Fatalf("%d copies left of a deleted object", n)
	}
	if _, ok := h.fw.data(h.addrs["w2"], "new1"); ok {
		t.Fatal("new copy left on w2's disk")
	}
}
