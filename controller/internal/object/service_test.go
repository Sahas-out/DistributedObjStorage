package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

const testChunkSize = 8

type fakeWorker struct {
	mu        sync.Mutex
	chunks    map[string]protocol.Chunk
	failPutAt int // sequence number whose PutChunk fails; -1 for none
	getErr    map[string]error
}

func newFakeWorker() *fakeWorker {
	return &fakeWorker{chunks: map[string]protocol.Chunk{}, failPutAt: -1, getErr: map[string]error{}}
}

func (f *fakeWorker) PutChunk(_ context.Context, id, obj string, seq int, data []byte, _ string) (protocol.ChunkMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if seq == f.failPutAt {
		return protocol.ChunkMetadata{}, workers.ErrUnavailable
	}
	f.chunks[id] = protocol.Chunk{ObjectID: obj, SequenceNo: seq, Data: bytes.Clone(data)}
	return protocol.ChunkMetadata{ChunkID: id, ObjectID: obj, SequenceNo: seq}, nil
}

func (f *fakeWorker) GetChunk(_ context.Context, id string) (protocol.Chunk, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.getErr[id]; err != nil {
		return protocol.Chunk{}, err
	}
	c, ok := f.chunks[id]
	if !ok {
		return protocol.Chunk{}, workers.ErrChunkNotFound
	}
	return c, nil
}

func (f *fakeWorker) DeleteChunk(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.chunks, id)
	return nil
}

type fakeMeta struct {
	objects   map[string]metadata.Object
	createErr error
}

func (f *fakeMeta) Create(_ context.Context, o metadata.Object) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.objects[o.ObjectID] = o
	return nil
}

func (f *fakeMeta) Get(_ context.Context, id string) (metadata.Object, error) {
	o, ok := f.objects[id]
	if !ok {
		return metadata.Object{}, metadata.ErrNotFound
	}
	return o, nil
}

func setup() (*Service, *fakeWorker, *fakeMeta) {
	w, m := newFakeWorker(), &fakeMeta{objects: map[string]metadata.Object{}}
	return NewService(w, m, testChunkSize, nil), w, m
}

func TestUploadDownloadRoundTrip(t *testing.T) {
	for _, length := range []int{0, 1, testChunkSize, testChunkSize + 1, 5*testChunkSize + 3} {
		svc, _, _ := setup()
		data := bytes.Repeat([]byte{0, 1, 2, 0xff, 7}, 10)[:length]

		obj, err := svc.Upload(context.Background(), bytes.NewReader(data))
		if err != nil {
			t.Fatalf("length %d: Upload: %v", length, err)
		}
		wantSum := sha256.Sum256(data)
		if obj.Size != int64(length) || obj.Checksum != hex.EncodeToString(wantSum[:]) {
			t.Errorf("length %d: object = %+v", length, obj)
		}

		dl, err := svc.OpenDownload(context.Background(), obj.ObjectID)
		if err != nil {
			t.Fatalf("length %d: OpenDownload: %v", length, err)
		}
		var got bytes.Buffer
		if err := dl.Stream(&got); err != nil {
			t.Fatalf("length %d: Stream: %v", length, err)
		}
		if !bytes.Equal(got.Bytes(), data) {
			t.Errorf("length %d: downloaded data differs", length)
		}
	}
}

func TestFailedChunkStoreLeavesNothingBehind(t *testing.T) {
	svc, w, m := setup()
	w.failPutAt = 2
	_, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 5*testChunkSize)))
	if !errors.Is(err, workers.ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
	if len(m.objects) != 0 || len(w.chunks) != 0 {
		t.Fatalf("left behind %d objects and %d chunks", len(m.objects), len(w.chunks))
	}
}

func TestTruncatedUploadIsNotCommitted(t *testing.T) {
	svc, w, m := setup()
	src := io.MultiReader(bytes.NewReader(make([]byte, testChunkSize+3)), iotest.ErrReader(io.ErrUnexpectedEOF))
	if _, err := svc.Upload(context.Background(), src); !errors.Is(err, ErrIncompleteUpload) {
		t.Fatalf("error = %v, want ErrIncompleteUpload", err)
	}
	if len(m.objects) != 0 || len(w.chunks) != 0 {
		t.Fatalf("left behind %d objects and %d chunks", len(m.objects), len(w.chunks))
	}
}

func TestMetadataFailureRemovesChunks(t *testing.T) {
	svc, w, m := setup()
	m.createErr = errors.New("disk full")
	if _, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 20))); err == nil {
		t.Fatal("Upload succeeded, want error")
	}
	if len(w.chunks) != 0 {
		t.Fatalf("left behind %d chunks", len(w.chunks))
	}
}

func TestDownloadDetectsDamagedChunk(t *testing.T) {
	svc, w, _ := setup()
	obj, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 3*testChunkSize)))
	if err != nil {
		t.Fatal(err)
	}
	id := obj.Chunks[1].ChunkID
	c := w.chunks[id]
	c.Data = bytes.Repeat([]byte{9}, testChunkSize)
	w.chunks[id] = c

	dl, err := svc.OpenDownload(context.Background(), obj.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if err := dl.Stream(&got); !errors.Is(err, workers.ErrChunkCorrupted) {
		t.Fatalf("Stream error = %v, want ErrChunkCorrupted", err)
	}
	if got.Len() != testChunkSize {
		t.Fatalf("wrote %d bytes, want only the verified first chunk", got.Len())
	}
}

func TestOpenDownloadReportsFirstChunkFailure(t *testing.T) {
	svc, w, _ := setup()
	obj, err := svc.Upload(context.Background(), bytes.NewReader(make([]byte, 10)))
	if err != nil {
		t.Fatal(err)
	}
	w.getErr[obj.Chunks[0].ChunkID] = workers.ErrUnavailable
	if _, err := svc.OpenDownload(context.Background(), obj.ObjectID); !errors.Is(err, workers.ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
}
