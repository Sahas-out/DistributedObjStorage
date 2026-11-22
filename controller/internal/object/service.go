// Package object orchestrates object uploads, downloads, listing and deletion
// across the metadata store and the workers.
package object

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/chunk"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/cluster"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/events"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// cleanupTimeout bounds removing the chunks of a failed upload or a deleted
// object.
const cleanupTimeout = 30 * time.Second

// deleteParallelism is how many chunk deletions run at once.
const deleteParallelism = 8

var (
	// ErrIncompleteUpload means the client's data stream ended with an error.
	ErrIncompleteUpload = errors.New("upload incomplete")
	// ErrObjectUnavailable means some chunk has no replica that can be read
	// right now.
	ErrObjectUnavailable = errors.New("object unavailable")
)

var idPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// ValidObjectID reports whether id has the form the controller generates.
func ValidObjectID(id string) bool { return idPattern.MatchString(id) }

// ChunkStore is the part of the worker API the service needs. Every call
// names the worker by address.
type ChunkStore interface {
	PutChunk(ctx context.Context, addr, chunkID, objectID string, seq int, data []byte, checksum string) (protocol.ChunkMetadata, error)
	GetChunk(ctx context.Context, addr, chunkID string) (protocol.Chunk, error)
	DeleteChunk(ctx context.Context, addr, chunkID string) error
}

// MetadataStore is the part of the metadata repository the service needs.
type MetadataStore interface {
	Create(ctx context.Context, obj metadata.Object) error
	Get(ctx context.Context, objectID string) (metadata.Object, error)
	List(ctx context.Context) ([]metadata.Object, error)
	Delete(ctx context.Context, objectID string) error
}

// Cluster is the part of the in-memory cluster registry the service needs.
type Cluster interface {
	Place(objectID string, seq, rf int, newChunkID func() (string, error)) ([]cluster.Location, error)
	SetStatus(chunkID string, status cluster.ReplicaStatus) bool
	Remove(chunkID string)
	Readable(objectID string, seq int) []cluster.Location
	ObjectReplicas(objectID string) []cluster.Location
}

// Service implements the object flows.
type Service struct {
	chunks    ChunkStore
	meta      MetadataStore
	cluster   Cluster
	rf        int
	chunkSize int
	log       *slog.Logger
	events    *events.Log
}

// NewService builds a Service that splits objects into chunkSize pieces and
// stores each piece on rf workers. A nil logger discards logs.
func NewService(chunks ChunkStore, meta MetadataStore, c Cluster, rf, chunkSize int, log *slog.Logger) *Service {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Service{chunks: chunks, meta: meta, cluster: c, rf: rf, chunkSize: chunkSize, log: log}
}

// SetEvents makes object operations appear in the dashboard activity feed.
// Call it before serving requests.
func (s *Service) SetEvents(ev *events.Log) { s.events = ev }

// Upload stores src chunk by chunk, holding one chunk in memory at a time.
// Each chunk is written to rf workers in parallel, each copy under its own
// chunk ID. The metadata is committed only after every copy is stored, so an
// object either exists completely or not at all. On failure, the copies
// already sent are removed.
func (s *Service) Upload(ctx context.Context, src io.Reader) (_ metadata.Object, err error) {
	objectID, err := newID()
	if err != nil {
		return metadata.Object{}, err
	}

	// Every placed replica, recorded before it is sent: if a response is
	// lost the worker may still have stored the chunk.
	var placed []cluster.Location
	defer func() {
		if err != nil {
			s.events.Record(protocol.EventUploadFailed, events.Warn, "upload failed: "+err.Error(), "", "")
			s.discard(ctx, objectID, placed)
		}
	}()

	obj := metadata.Object{ObjectID: objectID}
	whole := sha256.New()
	reader := chunk.NewReader(src, s.chunkSize)
	for {
		c, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return metadata.Object{}, fmt.Errorf("%w: %v", ErrIncompleteUpload, err)
		}

		whole.Write(c.Data)
		sum := sha256.Sum256(c.Data)
		checksum := hex.EncodeToString(sum[:])

		locs, err := s.cluster.Place(objectID, c.SequenceNo, s.rf, newID)
		if err != nil {
			return metadata.Object{}, fmt.Errorf("place chunk %d: %w", c.SequenceNo, err)
		}
		placed = append(placed, locs...)
		if err := s.putReplicas(ctx, locs, objectID, c.SequenceNo, c.Data, checksum); err != nil {
			return metadata.Object{}, fmt.Errorf("store chunk %d: %w", c.SequenceNo, err)
		}

		obj.Size += int64(len(c.Data))
		obj.Chunks = append(obj.Chunks, metadata.Chunk{
			SequenceNo: c.SequenceNo,
			Size:       int64(len(c.Data)),
			Checksum:   checksum,
		})
	}

	obj.ChunkCount = len(obj.Chunks)
	obj.Checksum = hex.EncodeToString(whole.Sum(nil))
	obj.CreatedAt = time.Now().UTC()
	if err := s.meta.Create(ctx, obj); err != nil {
		return metadata.Object{}, fmt.Errorf("persist metadata: %w", err)
	}
	for _, l := range placed {
		s.cluster.SetStatus(l.ChunkID, cluster.ReplicaActive)
	}
	s.events.Record(protocol.EventObjectUploaded, events.Info,
		fmt.Sprintf("object uploaded: %d bytes in %d chunks, %d copies", obj.Size, obj.ChunkCount, len(placed)), objectID, "")
	return obj, nil
}

// putReplicas sends one chunk to all of its placed workers at once. The data
// slice is shared read-only, so memory use stays at one chunk.
func (s *Service) putReplicas(ctx context.Context, locs []cluster.Location, objectID string, seq int, data []byte, checksum string) error {
	errs := make([]error, len(locs))
	var wg sync.WaitGroup
	for i, l := range locs {
		wg.Go(func() {
			if _, err := s.chunks.PutChunk(ctx, l.Addr, l.ChunkID, objectID, seq, data, checksum); err != nil {
				errs[i] = fmt.Errorf("worker %s: %w", l.WorkerID, err)
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// discard removes the replicas of a failed upload from the workers and from
// the registry. It runs even when the client has disconnected, which is why
// it detaches from ctx's cancellation.
func (s *Service) discard(ctx context.Context, objectID string, locs []cluster.Location) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	s.removeReplicas(ctx, objectID, locs)
}

// removeReplicas deletes replicas from their workers, a few at a time. A
// replica that can't be deleted stays in the registry as ORPHAN so a later
// cleanup job can find it.
func (s *Service) removeReplicas(ctx context.Context, objectID string, locs []cluster.Location) {
	sem := make(chan struct{}, deleteParallelism)
	var wg sync.WaitGroup
	for _, l := range locs {
		if l.Addr == "" { // worker already gone
			s.cluster.Remove(l.ChunkID)
			continue
		}
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			if err := s.chunks.DeleteChunk(ctx, l.Addr, l.ChunkID); err != nil {
				s.cluster.SetStatus(l.ChunkID, cluster.ReplicaOrphan)
				s.log.Warn("could not remove chunk replica; marked orphan",
					"object_id", objectID, "chunk_id", l.ChunkID, "worker_id", l.WorkerID, "error", err)
				s.events.Record(protocol.EventReplicaOrphaned, events.Warn,
					"could not remove chunk "+l.ChunkID+"; marked orphan", objectID, l.WorkerID)
				return
			}
			s.cluster.Remove(l.ChunkID)
		})
	}
	wg.Wait()
}

// Stat returns an object's metadata without touching the workers.
func (s *Service) Stat(ctx context.Context, objectID string) (metadata.Object, error) {
	return s.meta.Get(ctx, objectID)
}

// List returns every stored object, without chunk lists.
func (s *Service) List(ctx context.Context) ([]metadata.Object, error) {
	return s.meta.List(ctx)
}

// Replicas returns an object's metadata and where each replica of its chunks
// currently lives.
func (s *Service) Replicas(ctx context.Context, objectID string) (metadata.Object, []cluster.Location, error) {
	obj, err := s.meta.Get(ctx, objectID)
	if err != nil {
		return metadata.Object{}, nil, err
	}
	return obj, s.cluster.ObjectReplicas(objectID), nil
}

// Delete removes an object. The metadata goes first, so the object
// disappears for clients at once; then the replicas are removed from the
// workers. Replicas that can't be removed are left as ORPHAN in the registry
// and the delete still succeeds.
func (s *Service) Delete(ctx context.Context, objectID string) error {
	if err := s.meta.Delete(ctx, objectID); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	locs := s.cluster.ObjectReplicas(objectID)
	s.removeReplicas(ctx, objectID, locs)
	s.events.Record(protocol.EventObjectDeleted, events.Info,
		fmt.Sprintf("object deleted (%d copies removed)", len(locs)), objectID, "")
	return nil
}

// Download is an object ready to be streamed.
type Download struct {
	Object metadata.Object

	ctx     context.Context
	service *Service
	first   []byte
	// failed holds workers that failed to serve a chunk earlier in this
	// download; they are tried last for the remaining chunks.
	failed map[string]bool
}

// OpenDownload loads the object's metadata and fetches its first chunk. Doing
// this before the response starts means an unavailable object can still be
// reported with a proper HTTP status.
func (s *Service) OpenDownload(ctx context.Context, objectID string) (*Download, error) {
	obj, err := s.meta.Get(ctx, objectID)
	if err != nil {
		return nil, err
	}
	d := &Download{Object: obj, ctx: ctx, service: s, failed: map[string]bool{}}
	if len(obj.Chunks) > 0 {
		if d.first, err = s.fetchChunk(ctx, objectID, obj.Chunks[0], d.failed); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// Stream writes the object to w in sequence order. The next chunk is fetched
// only after the previous one was written, so a slow client slows the
// transfer down and at most one chunk is held in memory.
func (d *Download) Stream(w io.Writer) error {
	for i, ref := range d.Object.Chunks {
		data := d.first
		d.first = nil
		if i > 0 {
			var err error
			if data, err = d.service.fetchChunk(d.ctx, d.Object.ObjectID, ref, d.failed); err != nil {
				return err
			}
		}
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("write chunk %d to client: %w", ref.SequenceNo, err)
		}
	}
	return nil
}

// fetchChunk reads one chunk from any of its replicas, trying them in random
// order so reads spread across workers. Workers in failed (those that
// couldn't be reached earlier in the same download) go last, and workers that
// can't be reached now are added to it. Each copy is checked against the
// persisted size and checksum; a copy that fails is marked CORRUPTED and the
// next one is tried, so damaged data is never returned while a good copy
// exists.
func (s *Service) fetchChunk(ctx context.Context, objectID string, ref metadata.Chunk, failed map[string]bool) ([]byte, error) {
	locs := s.cluster.Readable(objectID, ref.SequenceNo)
	if len(locs) == 0 {
		return nil, fmt.Errorf("%w: chunk %d has no readable replica", ErrObjectUnavailable, ref.SequenceNo)
	}
	mrand.Shuffle(len(locs), func(i, j int) { locs[i], locs[j] = locs[j], locs[i] })
	sort.SliceStable(locs, func(i, j int) bool { return !failed[locs[i].WorkerID] && failed[locs[j].WorkerID] })

	var lastErr error
	allDamaged := true
	for _, l := range locs {
		data, err := s.fetchReplica(ctx, objectID, ref, l)
		if err == nil {
			return data, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		lastErr = err
		switch {
		case errors.Is(err, workers.ErrChunkCorrupted):
			s.cluster.SetStatus(l.ChunkID, cluster.ReplicaCorrupted)
			s.log.Warn("corrupted replica detected; trying another copy",
				"object_id", objectID, "sequence_no", ref.SequenceNo, "chunk_id", l.ChunkID, "worker_id", l.WorkerID, "error", err)
			s.events.Record(protocol.EventReplicaCorrupted, events.Error,
				fmt.Sprintf("corrupted copy of chunk %d detected; marked CORRUPTED, served from another copy", ref.SequenceNo), objectID, l.WorkerID)
		case errors.Is(err, workers.ErrChunkNotFound):
			s.log.Warn("replica missing on worker; trying another copy",
				"object_id", objectID, "sequence_no", ref.SequenceNo, "chunk_id", l.ChunkID, "worker_id", l.WorkerID)
			s.events.Record(protocol.EventReplicaMissing, events.Warn,
				fmt.Sprintf("copy of chunk %d missing on worker", ref.SequenceNo), objectID, l.WorkerID)
		default:
			allDamaged = false
			failed[l.WorkerID] = true
			s.log.Warn("could not read replica; trying another copy",
				"object_id", objectID, "sequence_no", ref.SequenceNo, "chunk_id", l.ChunkID, "worker_id", l.WorkerID, "error", err)
		}
	}
	if allDamaged {
		return nil, fmt.Errorf("chunk %d: every replica is missing or corrupted: %w", ref.SequenceNo, lastErr)
	}
	return nil, fmt.Errorf("%w: chunk %d: %w", ErrObjectUnavailable, ref.SequenceNo, lastErr)
}

// fetchReplica reads one replica and verifies it against the persisted
// metadata. Workers keep no checksums, so this is where corruption is caught.
func (s *Service) fetchReplica(ctx context.Context, objectID string, ref metadata.Chunk, l cluster.Location) ([]byte, error) {
	c, err := s.chunks.GetChunk(ctx, l.Addr, l.ChunkID)
	if err != nil {
		return nil, err
	}
	if c.ObjectID != objectID || c.SequenceNo != ref.SequenceNo {
		return nil, fmt.Errorf("%w: chunk %s belongs to object %s sequence %d", workers.ErrChunkCorrupted, l.ChunkID, c.ObjectID, c.SequenceNo)
	}
	sum := sha256.Sum256(c.Data)
	if int64(len(c.Data)) != ref.Size || hex.EncodeToString(sum[:]) != ref.Checksum {
		return nil, fmt.Errorf("%w: chunk %s does not match its recorded checksum", workers.ErrChunkCorrupted, l.ChunkID)
	}
	return c.Data, nil
}

// newID returns 32 random hex characters. Object IDs and chunk IDs use the
// same form; every replica of a chunk gets its own ID.
func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return hex.EncodeToString(b), nil
}
