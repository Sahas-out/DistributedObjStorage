package api

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
	"github.com/Sahas-out/DistributedObjectStorage/worker/internal/storage"
)

// reservedChunkID cannot be used as a chunk ID because
// GET /internals/chunks/metadata is the metadata listing.
const reservedChunkID = "metadata"

// jsonOverhead is the slack allowed on top of the base64-encoded data for the
// rest of the PUT JSON body (keys, object_id, sequence_no, whitespace).
const jsonOverhead = 4 << 10

// putChunkRequest decodes the protocol.Chunk PUT body. SequenceNo is a pointer
// so a missing field can be told apart from sequence 0.
type putChunkRequest struct {
	ObjectID   string `json:"object_id"`
	SequenceNo *int   `json:"sequence_no"`
	Data       []byte `json:"data"`
}

// maxBodyBytes is the largest accepted PUT body: maxChunk bytes of data
// base64-encoded plus room for the other JSON fields.
func (s *Server) maxBodyBytes() int64 {
	return int64(base64.StdEncoding.EncodedLen(int(s.maxChunk))) + jsonOverhead
}

// handlePutChunk stores the chunk carried in a protocol.Chunk JSON body and
// responds 201 with its protocol.ChunkMetadata.
//
// The body is buffered and the data hashed before anything is written, so a
// bad X-Chunk-Checksum is rejected without touching storage.
//
// The body is read before any validation on purpose: replying early while up
// to ~1.4 MiB of request body is still unread makes the server close the
// connection, and the resulting TCP reset can destroy the error response
// before the client reads it (observed through Docker's port proxy).
func (s *Server) handlePutChunk(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBodyBytes()))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			s.chunkTooLarge(w)
			return
		}
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, "failed to read request body")
		return
	}

	id, ok := s.chunkID(w, r)
	if !ok {
		return
	}

	var req putChunkRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest,
			"request body must be a JSON object {object_id, sequence_no, data} with data base64-encoded")
		return
	}
	// Object IDs follow the same rules as chunk IDs.
	if !storage.ValidChunkID(req.ObjectID) {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest,
			"object_id must be 1-128 characters from [A-Za-z0-9_-]")
		return
	}
	if req.SequenceNo == nil || *req.SequenceNo < 0 {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest,
			"sequence_no is required and must be a non-negative integer")
		return
	}
	if req.Data == nil {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, "data is required")
		return
	}
	if int64(len(req.Data)) > s.maxChunk {
		s.chunkTooLarge(w)
		return
	}

	expected := strings.TrimSpace(r.Header.Get(protocol.HeaderChunkChecksum))
	if expected != "" && !validHexDigest(expected) {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest,
			protocol.HeaderChunkChecksum+" must be a 64-character SHA-256 hex digest")
		return
	}

	sum := sha256Hex(req.Data)
	if expected != "" && !digestsEqual(expected, sum) {
		writeError(w, http.StatusBadRequest, protocol.CodeChecksumMismatch,
			"data checksum "+sum+" does not match "+protocol.HeaderChunkChecksum)
		return
	}

	meta, err := s.store.Put(r.Context(), storage.ChunkMetadata{
		ChunkID:    id,
		ObjectID:   req.ObjectID,
		SequenceNo: *req.SequenceNo,
	}, bytes.NewReader(req.Data))
	if err != nil {
		s.storageError(w, r, id, err)
		return
	}
	if meta.Size != int64(len(req.Data)) {
		s.log.Error("storage recorded a different size than received",
			"chunk_id", id, "want_size", len(req.Data), "got_size", meta.Size)
		writeError(w, http.StatusInternalServerError, protocol.CodeStorageError, "storage recorded inconsistent chunk info")
		return
	}

	// The digest of what was received, so the caller can confirm the data
	// arrived intact. It is not stored.
	w.Header().Set(protocol.HeaderChunkChecksum, sum)
	writeJSON(w, http.StatusCreated, toWireMetadata(meta))
}

// handleGetChunk returns the chunk as a protocol.Chunk JSON body. The worker
// keeps no checksum, so the data is returned as it is on disk; the controller
// verifies it and falls back to another replica if it is corrupted.
func (s *Server) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	id, ok := s.chunkID(w, r)
	if !ok {
		return
	}

	rc, meta, err := s.store.Open(r.Context(), id)
	if err != nil {
		s.storageError(w, r, id, err)
		return
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		s.storageError(w, r, id, err)
		return
	}

	payload, err := json.Marshal(protocol.Chunk{
		ObjectID:   meta.ObjectID,
		SequenceNo: meta.SequenceNo,
		Data:       data,
	})
	if err != nil {
		s.log.Error("encode chunk response", "chunk_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, protocol.CodeInternalError, "internal server error")
		return
	}

	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(payload)))
	h.Set(protocol.HeaderChunkSize, strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(payload)
	}
}

// handleDeleteChunk removes a chunk. Deleting a missing chunk also returns 204.
func (s *Server) handleDeleteChunk(w http.ResponseWriter, r *http.Request) {
	id, ok := s.chunkID(w, r)
	if !ok {
		return
	}
	if err := s.store.Delete(r.Context(), id); err != nil && !errors.Is(err, storage.ErrNotFound) {
		s.storageError(w, r, id, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleListChunks returns a JSON array of protocol.ChunkMetadata for every
// stored chunk. The array is empty, never null, when there are no chunks.
func (s *Server) handleListChunks(w http.ResponseWriter, r *http.Request) {
	metas, err := s.store.List(r.Context())
	if err != nil {
		s.storageError(w, r, "", err)
		return
	}
	out := make([]protocol.ChunkMetadata, 0, len(metas))
	for _, m := range metas {
		out = append(out, toWireMetadata(m))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleHealth reports 200 when the metadata database answers a ping, 503
// otherwise. worker_id is empty until the worker has registered.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.ping(r.Context()); err != nil {
		s.log.Error("health check failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, protocol.CodeStorageError, "metadata store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "worker_id": s.workerID()})
}

func toWireMetadata(m storage.ChunkMetadata) protocol.ChunkMetadata {
	return protocol.ChunkMetadata{ChunkID: m.ChunkID, ObjectID: m.ObjectID, SequenceNo: m.SequenceNo}
}

// chunkID extracts and validates the chunk ID, writing a 400 if invalid.
func (s *Server) chunkID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("chunk_id")
	if !storage.ValidChunkID(id) {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidChunkID,
			"chunk id must be 1-128 characters from [A-Za-z0-9_-]")
		return "", false
	}
	if id == reservedChunkID {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidChunkID,
			"chunk id "+strconv.Quote(reservedChunkID)+" is reserved")
		return "", false
	}
	return id, true
}

func (s *Server) chunkTooLarge(w http.ResponseWriter) {
	writeError(w, http.StatusRequestEntityTooLarge, protocol.CodeChunkTooLarge,
		"chunk data exceeds maximum size of "+strconv.FormatInt(s.maxChunk, 10)+" bytes")
}

// storageError maps storage errors to HTTP responses. Unexpected errors are
// logged and reported without internal details.
func (s *Server) storageError(w http.ResponseWriter, r *http.Request, id string, err error) {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		writeError(w, http.StatusNotFound, protocol.CodeChunkNotFound, "chunk "+id+" not found")
	case errors.Is(err, storage.ErrInvalidChunkID):
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidChunkID, "invalid chunk id")
	default:
		s.log.Error("storage failure", "method", r.Method, "chunk_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, protocol.CodeStorageError, "storage failure")
	}
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func validHexDigest(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func digestsEqual(a, b string) bool {
	a, b = strings.ToLower(a), strings.ToLower(b)
	return len(a) == len(b) && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
