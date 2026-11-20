package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/cluster"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

const testChunkSize = 8

// fakeWorkers simulates several workers, keyed by address.
type fakeWorkers struct {
	mu        sync.Mutex
	chunks    map[string]map[string]protocol.Chunk // addr -> chunk ID -> chunk
	failPut   map[string]bool                      // addr -> PutChunk fails
	failGet   map[string]bool                      // addr -> GetChunk fails
	failDel   map[string]bool                      // addr -> DeleteChunk fails
	failPutAt int                                  // sequence number whose PutChunk fails on one worker; -1 for none
	gets      map[string]int                       // addr -> GetChunk calls
}

func newFakeWorkers() *fakeWorkers {
	return &fakeWorkers{
		chunks:    map[string]map[string]protocol.Chunk{},
		failPut:   map[string]bool{},
		failGet:   map[string]bool{},
		failDel:   map[string]bool{},
		failPutAt: -1,
		gets:      map[string]int{},
	}
}

func (f *fakeWorkers) PutChunk(_ context.Context, addr, id, obj string, seq int, data []byte, _ string) (protocol.ChunkMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPut[addr] {
		return protocol.ChunkMetadata{}, workers.ErrUnavailable
	}
	if seq == f.failPutAt {
		f.failPutAt = -1 // fail only one of the replicas
		return protocol.ChunkMetadata{}, workers.ErrUnavailable
	}
	if f.chunks[addr] == nil {
		f.chunks[addr] = map[string]protocol.Chunk{}
	}
	f.chunks[addr][id] = protocol.Chunk{ObjectID: obj, SequenceNo: seq, Data: bytes.Clone(data)}
	return protocol.ChunkMetadata{ChunkID: id, ObjectID: obj, SequenceNo: seq}, nil
}

func (f *fakeWorkers) GetChunk(_ context.Context, addr, id string) (protocol.Chunk, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets[addr]++
	if f.failGet[addr] {
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
	if f.failDel[addr] {
		return workers.ErrUnavailable
	}
	delete(f.chunks[addr], id)
	return nil
}

func (f *fakeWorkers) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.chunks {
		n += len(m)
	}
	return n
}

// corrupt flips the data of the given replica.
func (f *fakeWorkers) corrupt(addr, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.chunks[addr][id]
	c.Data = bytes.Repeat([]byte{9}, len(c.Data))
	f.chunks[addr][id] = c
}

type fakeMeta struct {
	mu        sync.Mutex
	objects   map[string]metadata.Object
	createErr error
}

func (f *fakeMeta) Create(_ context.Context, o metadata.Object) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return f.createErr
	}
	f.objects[o.ObjectID] = o
	return nil
}

func (f *fakeMeta) Get(_ context.Context, id string) (metadata.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[id]
	if !ok {
		return metadata.Object{}, metadata.ErrNotFound
	}
	return o, nil
}

func (f *fakeMeta) List(context.Context) ([]metadata.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []metadata.Object{}
	for _, o := range f.objects {
		o.Chunks = nil
		out = append(out, o)
	}
	return out, nil
}

func (f *fakeMeta) Delete(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects[id]; !ok {
		return metadata.ErrNotFound
	}
	delete(f.objects, id)
	return nil
}

func addr(i int) string { return fmt.Sprintf("http://10.0.0.%d:8080", i) }

// setup builds a Service with n READY workers and replication factor 2.
func setup(t *testing.T, n int) (*Service, *fakeWorkers, *fakeMeta, *cluster.Registry) {
	t.Helper()
	reg := cluster.NewRegistry(cluster.LeastLoaded{})
	for i := range n {
		id := fmt.Sprintf("w%d", i)
		res, err := reg.Announce(id, addr(i))
		if err != nil {
			t.Fatal(err)
		}
		reg.CompleteSync(id, res.SyncGen, nil)
	}
	w, m := newFakeWorkers(), &fakeMeta{objects: map[string]metadata.Object{}}
	return NewService(w, m, reg, 2, testChunkSize, nil), w, m, reg
}

func download(t *testing.T, svc *Service, id string) ([]byte, error) {
	t.Helper()
	dl, err := svc.OpenDownload(context.Background(), id)
	if err != nil {
		return nil, err
	}
	var got bytes.Buffer
	err = dl.Stream(&got)
	return got.Bytes(), err
}

func TestUploadDownloadRoundTrip(t *testing.T) {
	for _, length := range []int{0, 1, testChunkSize, testChunkSize + 1, 5*testChunkSize + 3} {
		svc, w, _, reg := setup(t, 3)
		data := bytes.Repeat([]byte{0, 1, 2, 0xff, 7}, 10)[:length]

		obj, err := svc.Upload(context.Background(), bytes.NewReader(data))
		if err != nil {
			t.Fatalf("length %d: Upload: %v", length, err)
		}
		wantSum := sha256.Sum256(data)
		if obj.Size != int64(length) || obj.Checksum != hex.EncodeToString(wantSum[:]) || obj.ChunkCount != len(obj.Chunks) {
			t.Errorf("length %d: object = %+v", length, obj)
		}
		if w.total() != 2*obj.ChunkCount {
			t.Errorf("length %d: workers hold %d replicas, want %d", length, w.total(), 2*obj.ChunkCount)
		}

		got, err := download(t, svc, obj.ObjectID)
		if err != nil {
			t.Fatalf("length %d: download: %v", length, err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("length %d: downloaded data differs", length)
		}
		for _, l := range reg.ObjectReplicas(obj.ObjectID) {
			if l.Status != cluster.ReplicaActive {
				t.Errorf("length %d: replica %s is %s after commit", length, l.ChunkID, l.Status)
			}
		}
	}
}

func TestEachChunkHasTwoCopiesOnDistinctWorkers(t *testing.T) {
	svc, _, _, reg := setup(t, 4)
	obj, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 10*testChunkSize)))
	if err != nil {
		t.Fatal(err)
	}
	bySeq := map[int][]cluster.Location{}
	ids := map[string]bool{}
	for _, l := range reg.ObjectReplicas(obj.ObjectID) {
		bySeq[l.SeqNo] = append(bySeq[l.SeqNo], l)
		if ids[l.ChunkID] {
			t.Fatalf("chunk id %s used twice", l.ChunkID)
		}
		ids[l.ChunkID] = true
	}
	for seq := range obj.ChunkCount {
		ls := bySeq[seq]
		if len(ls) != 2 || ls[0].WorkerID == ls[1].WorkerID {
			t.Fatalf("seq %d replicas = %+v; want 2 on distinct workers", seq, ls)
		}
	}
	// 20 replicas across 4 workers: 5 each.
	for _, w := range reg.Workers() {
		if w.ChunkCount != 5 {
			t.Errorf("worker %s holds %d replicas, want 5", w.ID, w.ChunkCount)
		}
	}
}

func TestUploadNeedsTwoReadyWorkers(t *testing.T) {
	svc, w, m, _ := setup(t, 1)
	_, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 3)))
	if !errors.Is(err, cluster.ErrInsufficientWorkers) {
		t.Fatalf("error = %v, want ErrInsufficientWorkers", err)
	}
	if len(m.objects) != 0 || w.total() != 0 {
		t.Fatal("failed upload left data behind")
	}
}

func TestFailedReplicaLeavesNothingBehind(t *testing.T) {
	svc, w, m, reg := setup(t, 3)
	w.failPutAt = 2
	_, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 5*testChunkSize)))
	if !errors.Is(err, workers.ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
	if len(m.objects) != 0 || w.total() != 0 {
		t.Fatalf("left behind %d objects and %d chunks", len(m.objects), w.total())
	}
	for _, wk := range reg.Workers() {
		if wk.ChunkCount != 0 {
			t.Fatalf("registry still counts %d replicas on %s", wk.ChunkCount, wk.ID)
		}
	}
}

func TestTruncatedUploadIsNotCommitted(t *testing.T) {
	svc, w, m, _ := setup(t, 2)
	src := io.MultiReader(bytes.NewReader(make([]byte, testChunkSize+3)), iotest.ErrReader(io.ErrUnexpectedEOF))
	if _, err := svc.Upload(context.Background(), src); !errors.Is(err, ErrIncompleteUpload) {
		t.Fatalf("error = %v, want ErrIncompleteUpload", err)
	}
	if len(m.objects) != 0 || w.total() != 0 {
		t.Fatalf("left behind %d objects and %d chunks", len(m.objects), w.total())
	}
}

func TestMetadataFailureRemovesChunks(t *testing.T) {
	svc, w, m, _ := setup(t, 2)
	m.createErr = errors.New("disk full")
	if _, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 20))); err == nil {
		t.Fatal("Upload succeeded, want error")
	}
	if w.total() != 0 {
		t.Fatalf("left behind %d chunks", w.total())
	}
}

func TestDownloadFallsBackFromCorruptedReplica(t *testing.T) {
	svc, w, _, reg := setup(t, 3)
	data := bytes.Repeat([]byte("abcdefgh"), 3)
	obj, err := svc.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt one copy of every chunk.
	var bad []cluster.Location
	seen := map[int]bool{}
	for _, l := range reg.ObjectReplicas(obj.ObjectID) {
		if !seen[l.SeqNo] {
			seen[l.SeqNo] = true
			w.corrupt(l.Addr, l.ChunkID)
			bad = append(bad, l)
		}
	}

	// Replicas are tried in random order, so a single read may never touch a
	// corrupted copy. Read enough times that every corrupted copy is tried
	// (the chance one is missed in 50 reads is 2^-50); every read must still
	// return the original data.
	for i := range 50 {
		got, err := download(t, svc, obj.ObjectID)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("download %d = %q, %v; want the original data", i, got, err)
		}
	}
	// Each corrupted copy has been detected, marked and taken out of service.
	for _, l := range bad {
		for _, r := range reg.Readable(obj.ObjectID, l.SeqNo) {
			if r.ChunkID == l.ChunkID {
				t.Fatalf("corrupted replica %s is still readable", l.ChunkID)
			}
		}
	}
}

func TestDownloadFallsBackFromUnreachableWorker(t *testing.T) {
	svc, w, _, _ := setup(t, 2)
	data := []byte("0123456789abcdef!")
	obj, err := svc.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	w.failGet[addr(0)] = true
	if got, err := download(t, svc, obj.ObjectID); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download = %q, %v", got, err)
	}

	// With both workers unreachable the object is unavailable.
	w.failGet[addr(1)] = true
	if _, err := download(t, svc, obj.ObjectID); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("error = %v, want ErrObjectUnavailable", err)
	}
}

func TestDownloadWithEveryReplicaCorrupted(t *testing.T) {
	svc, w, _, reg := setup(t, 2)
	obj, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 2*testChunkSize)))
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range reg.ObjectReplicas(obj.ObjectID) {
		if l.SeqNo == 1 {
			w.corrupt(l.Addr, l.ChunkID)
		}
	}
	got, err := download(t, svc, obj.ObjectID)
	if !errors.Is(err, workers.ErrChunkCorrupted) {
		t.Fatalf("error = %v, want ErrChunkCorrupted", err)
	}
	if len(got) != testChunkSize {
		t.Fatalf("wrote %d bytes, want only the verified first chunk", len(got))
	}
}

func TestDeleteRemovesReplicas(t *testing.T) {
	svc, w, m, reg := setup(t, 3)
	obj, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 4*testChunkSize)))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Delete(context.Background(), obj.ObjectID); err != nil {
		t.Fatal(err)
	}
	if len(m.objects) != 0 || w.total() != 0 || len(reg.ObjectReplicas(obj.ObjectID)) != 0 {
		t.Fatalf("after delete: %d objects, %d chunks, %d replicas", len(m.objects), w.total(), len(reg.ObjectReplicas(obj.ObjectID)))
	}
	if err := svc.Delete(context.Background(), obj.ObjectID); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("second delete: %v, want ErrNotFound", err)
	}
}

func TestDeleteMarksUnremovableReplicasOrphan(t *testing.T) {
	svc, w, m, reg := setup(t, 2)
	obj, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 2*testChunkSize)))
	if err != nil {
		t.Fatal(err)
	}
	w.failDel[addr(0)] = true
	if err := svc.Delete(context.Background(), obj.ObjectID); err != nil {
		t.Fatalf("Delete: %v; want success despite the failing worker", err)
	}
	if len(m.objects) != 0 {
		t.Fatal("metadata not deleted")
	}
	left := reg.ObjectReplicas(obj.ObjectID)
	if len(left) != 2 {
		t.Fatalf("replicas left = %d, want the 2 on the failing worker", len(left))
	}
	for _, l := range left {
		if l.WorkerID != "w0" || l.Status != cluster.ReplicaOrphan {
			t.Fatalf("leftover replica = %+v; want ORPHAN on w0", l)
		}
	}
}

func TestDownloadTriesAFailedWorkerOnlyOnce(t *testing.T) {
	svc, w, _, _ := setup(t, 2) // every chunk has a copy on both workers
	data := bytes.Repeat([]byte("0123456789"), 20)
	obj, err := svc.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	w.failGet[addr(0)] = true
	if got, err := download(t, svc, obj.ObjectID); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download = %v", err)
	}
	if n := w.gets[addr(0)]; n > 1 {
		t.Fatalf("unreachable worker was tried %d times in one download of %d chunks; want at most once", n, obj.ChunkCount)
	}
}
