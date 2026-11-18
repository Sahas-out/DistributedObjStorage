package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/object"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// drainTimeout limits how long a failed upload keeps reading the client's
// remaining data before the error response is sent.
const drainTimeout = 5 * time.Second

// handleUpload stores the raw request body as a new object and responds 201
// with its protocol.ObjectInfo.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	obj, err := s.objects.Upload(r.Context(), r.Body)
	if err != nil {
		s.discardBody(w, r)
		s.writeObjectError(w, r, "", err)
		return
	}
	w.Header().Set("Location", "/api/v1/objects/"+obj.ObjectID)
	w.Header().Set("ETag", etag(obj))
	writeJSON(w, http.StatusCreated, protocol.ObjectInfo{
		ObjectID:   obj.ObjectID,
		Size:       obj.Size,
		Checksum:   obj.Checksum,
		ChunkCount: len(obj.Chunks),
		CreatedAt:  obj.CreatedAt,
	})
}

// discardBody reads what is left of a failed upload. Replying while the client
// is still sending makes the server close the connection, and the resulting
// TCP reset can destroy the error response before the client reads it.
func (s *Server) discardBody(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Connection", "close")
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(drainTimeout))
	_, _ = io.Copy(io.Discard, r.Body)
}

// handleDownload streams an object back to the client. It also answers HEAD,
// which returns the headers without contacting the worker.
func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	if !object.ValidObjectID(id) {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidObjectID, "object id must be 32 lowercase hex characters")
		return
	}

	if r.Method == http.MethodHead {
		obj, err := s.objects.Stat(r.Context(), id)
		if err != nil {
			s.writeObjectError(w, r, id, err)
			return
		}
		setObjectHeaders(w, obj)
		w.WriteHeader(http.StatusOK)
		return
	}

	download, err := s.objects.OpenDownload(r.Context(), id)
	if err != nil {
		s.writeObjectError(w, r, id, err)
		return
	}
	setObjectHeaders(w, download.Object)
	w.WriteHeader(http.StatusOK)
	if err := download.Stream(w); err != nil {
		// The 200 is already sent, so the status can't change. Aborting the
		// connection makes the client see fewer bytes than Content-Length
		// instead of mistaking a truncated object for a complete one.
		s.log.Error("download aborted", "object_id", id, "error", err)
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.ping(r.Context()); err != nil {
		s.log.Error("health check failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, protocol.CodeInternalError, "metadata store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func setObjectHeaders(w http.ResponseWriter, obj metadata.Object) {
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	h.Set("ETag", etag(obj))
}

func etag(obj metadata.Object) string { return `"` + obj.Checksum + `"` }

// writeObjectError maps service errors to HTTP responses. Unexpected errors
// are logged and reported without internal details.
func (s *Server) writeObjectError(w http.ResponseWriter, r *http.Request, id string, err error) {
	switch {
	case errors.Is(err, metadata.ErrNotFound):
		writeError(w, http.StatusNotFound, protocol.CodeObjectNotFound, "object "+id+" not found")
	case errors.Is(err, object.ErrIncompleteUpload), errors.Is(err, context.Canceled):
		s.log.Warn("request abandoned", "method", r.Method, "object_id", id, "error", err)
		writeError(w, http.StatusBadRequest, protocol.CodeUploadIncomplete, "the request ended before all data was transferred")
	case errors.Is(err, workers.ErrUnavailable):
		s.log.Error("worker unavailable", "method", r.Method, "object_id", id, "error", err)
		writeError(w, http.StatusServiceUnavailable, protocol.CodeWorkerUnavailable, "storage worker is unavailable; try again later")
	case errors.Is(err, workers.ErrChunkCorrupted), errors.Is(err, workers.ErrChunkNotFound):
		s.log.Error("object data damaged", "method", r.Method, "object_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, protocol.CodeObjectCorrupted, "stored data for object "+id+" is missing or corrupted")
	default:
		s.log.Error("request failed", "method", r.Method, "object_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, protocol.CodeInternalError, "internal server error")
	}
}
