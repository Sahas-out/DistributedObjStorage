// Package object orchestrates object uploads and downloads between the client
// API, the metadata store and the worker.
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
	"regexp"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/chunk"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// cleanupTimeout bounds removing the chunks of a failed upload.
const cleanupTimeout = 10 * time.Second

// ErrIncompleteUpload means the client's data stream ended with an error.
var ErrIncompleteUpload = errors.New("upload incomplete")

var objectIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// ValidObjectID reports whether id has the form the controller generates.
func ValidObjectID(id string) bool { return objectIDPattern.MatchString(id) }

// ChunkStore is the part of the worker API the service needs.
type ChunkStore interface {
	PutChunk(ctx context.Context, chunkID, objectID string, seq int, data []byte, checksum string) (protocol.ChunkMetadata, error)
	GetChunk(ctx context.Context, chunkID string) (protocol.Chunk, error)
	DeleteChunk(ctx context.Context, chunkID string) error
}

// MetadataStore is the part of the metadata repository the service needs.
type MetadataStore interface {
	Create(ctx context.Context, obj metadata.Object) error
	Get(ctx context.Context, objectID string) (metadata.Object, error)
}

// Service implements the upload and download flows.
type Service struct {
	chunks    ChunkStore
	meta      MetadataStore
	chunkSize int
	log       *slog.Logger
}

// NewService builds a Service that splits objects into chunkSize pieces.
// A nil logger discards logs.
func NewService(chunks ChunkStore, meta MetadataStore, chunkSize int, log *slog.Logger) *Service {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Service{chunks: chunks, meta: meta, chunkSize: chunkSize, log: log}
}

// Upload stores src chunk by chunk, holding one chunk in memory at a time. The
// metadata is committed only after every chunk is on the worker, so an object
// either exists completely or not at all. On failure, the chunks already sent
// are removed from the worker.
func (s *Service) Upload(ctx context.Context, src io.Reader) (_ metadata.Object, err error) {
	objectID, err := newObjectID()
	if err != nil {
		return metadata.Object{}, err
	}

	var sent []string
	defer func() {
		if err != nil {
			s.removeChunks(ctx, objectID, sent)
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
		chunkID := fmt.Sprintf("%s_%d", objectID, c.SequenceNo)

		// Recorded before the call: if the response is lost the worker may
		// still have stored the chunk.
		sent = append(sent, chunkID)
		stored, err := s.chunks.PutChunk(ctx, chunkID, objectID, c.SequenceNo, c.Data, checksum)
		if err != nil {
			return metadata.Object{}, fmt.Errorf("store chunk %d: %w", c.SequenceNo, err)
		}

		obj.Size += int64(len(c.Data))
		obj.Chunks = append(obj.Chunks, metadata.Chunk{
			SequenceNo: stored.SequenceNo,
			ChunkID:    stored.ChunkID,
			Size:       int64(len(c.Data)),
			Checksum:   checksum,
		})
	}

	obj.Checksum = hex.EncodeToString(whole.Sum(nil))
	obj.CreatedAt = time.Now().UTC()
	if err := s.meta.Create(ctx, obj); err != nil {
		return metadata.Object{}, fmt.Errorf("persist metadata: %w", err)
	}
	return obj, nil
}

// removeChunks deletes the chunks of a failed upload. It runs even when the
// client has disconnected, which is why it detaches from ctx's cancellation.
func (s *Service) removeChunks(ctx context.Context, objectID string, chunkIDs []string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	for _, id := range chunkIDs {
		if err := s.chunks.DeleteChunk(ctx, id); err != nil {
			s.log.Warn("could not remove chunk of failed upload", "object_id", objectID, "chunk_id", id, "error", err)
		}
	}
}

// Stat returns an object's metadata without touching the worker.
func (s *Service) Stat(ctx context.Context, objectID string) (metadata.Object, error) {
	return s.meta.Get(ctx, objectID)
}

// Download is an object ready to be streamed.
type Download struct {
	Object metadata.Object

	ctx     context.Context
	service *Service
	first   []byte
}

// OpenDownload loads the object's metadata and fetches its first chunk. Doing
// this before the response starts means an unavailable object can still be
// reported with a proper HTTP status.
func (s *Service) OpenDownload(ctx context.Context, objectID string) (*Download, error) {
	obj, err := s.meta.Get(ctx, objectID)
	if err != nil {
		return nil, err
	}
	d := &Download{Object: obj, ctx: ctx, service: s}
	if len(obj.Chunks) > 0 {
		if d.first, err = s.fetchChunk(ctx, objectID, obj.Chunks[0]); err != nil {
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
			if data, err = d.service.fetchChunk(d.ctx, d.Object.ObjectID, ref); err != nil {
				return err
			}
		}
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("write chunk %d to client: %w", ref.SequenceNo, err)
		}
	}
	return nil
}

// fetchChunk retrieves one chunk and verifies it against the persisted
// metadata, so damage between the worker's disk and here is caught too.
func (s *Service) fetchChunk(ctx context.Context, objectID string, ref metadata.Chunk) ([]byte, error) {
	c, err := s.chunks.GetChunk(ctx, ref.ChunkID)
	if err != nil {
		return nil, fmt.Errorf("fetch chunk %d: %w", ref.SequenceNo, err)
	}
	if c.ObjectID != objectID || c.SequenceNo != ref.SequenceNo {
		return nil, fmt.Errorf("%w: chunk %s belongs to object %s sequence %d", workers.ErrChunkCorrupted, ref.ChunkID, c.ObjectID, c.SequenceNo)
	}
	sum := sha256.Sum256(c.Data)
	if int64(len(c.Data)) != ref.Size || hex.EncodeToString(sum[:]) != ref.Checksum {
		return nil, fmt.Errorf("%w: chunk %s does not match its recorded checksum", workers.ErrChunkCorrupted, ref.ChunkID)
	}
	return c.Data, nil
}

func newObjectID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate object id: %w", err)
	}
	return hex.EncodeToString(b), nil
}
