package api

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/cluster"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/object"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// drainTimeout limits how long a failed upload keeps reading the client's
// remaining data before the error response is sent.
const drainTimeout = 5 * time.Second

// handleUpload stores the "file" field of a multipart/form-data request as a
// new object, with the custom metadata in the optional "metadata" field, and
// responds 201 with its protocol.ObjectInfo. The file is streamed to the
// workers as it arrives; a "metadata" field after it is read before the
// object is committed.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	obj, err := s.upload(r)
	if err != nil {
		s.discardBody(w, r)
		s.writeObjectError(w, r, "", err)
		return
	}
	w.Header().Set("Location", "/api/v1/objects/"+obj.ObjectID)
	w.Header().Set("ETag", etag(obj))
	writeJSON(w, http.StatusCreated, objectInfo(obj))
}

func (s *Server) upload(r *http.Request) (metadata.Object, error) {
	mr, err := r.MultipartReader()
	if err != nil {
		return metadata.Object{}, badRequest(`the upload must be multipart/form-data with a "file" field (and an optional "metadata" JSON field)`)
	}
	form := &uploadForm{mr: mr}
	file, err := form.nextFile()
	if err != nil {
		return metadata.Object{}, err
	}
	defer file.Close()
	name := file.FileName()
	spec := object.UploadSpec{
		Name:        name,
		ContentType: fileContentType(file, name),
		Custom:      form.custom,
		Finish: func(spec *object.UploadSpec) error {
			if err := form.finish(); err != nil {
				return err
			}
			spec.Custom = form.custom
			return nil
		},
	}
	return s.objects.Upload(r.Context(), file, spec)
}

// handleList responds with a JSON array of protocol.ObjectInfo for every
// stored object, oldest first.
func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	objs, err := s.objects.List(r.Context())
	if err != nil {
		s.writeObjectError(w, r, "", err)
		return
	}
	out := make([]protocol.ObjectInfo, 0, len(objs))
	for _, o := range objs {
		out = append(out, objectInfo(o))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleDelete removes an object and its stored chunks and responds 204.
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	if !object.ValidObjectID(id) {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidObjectID, "object id must be 32 lowercase hex characters")
		return
	}
	if err := s.objects.Delete(r.Context(), id); err != nil {
		s.writeObjectError(w, r, id, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func objectInfo(obj metadata.Object) protocol.ObjectInfo {
	return protocol.ObjectInfo{
		ObjectID:       obj.ObjectID,
		Name:           obj.Name,
		ContentType:    obj.ContentType,
		Size:           obj.Size,
		Checksum:       obj.Checksum,
		ChunkCount:     obj.ChunkCount,
		CreatedAt:      obj.CreatedAt,
		LastModifiedAt: obj.UpdatedAt,
	}
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

// setObjectHeaders describes a download. The object is always sent as an
// attachment, and nosniff is set, so a browser saves a stored HTML or SVG
// file instead of running it in the controller's origin.
func setObjectHeaders(w http.ResponseWriter, obj metadata.Object) {
	h := w.Header()
	ct := obj.ContentType
	if ct == "" {
		ct = object.DefaultContentType
	}
	h.Set("Content-Type", ct)
	h.Set("X-Content-Type-Options", "nosniff")
	if cd := mime.FormatMediaType("attachment", map[string]string{"filename": obj.Name}); cd != "" {
		h.Set("Content-Disposition", cd)
	} else {
		h.Set("Content-Disposition", "attachment")
	}
	h.Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	h.Set("ETag", etag(obj))
	h.Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
}

func etag(obj metadata.Object) string { return `"` + obj.Checksum + `"` }

// writeObjectError maps service errors to HTTP responses. Unexpected errors
// are logged and reported without internal details.
func (s *Server) writeObjectError(w http.ResponseWriter, r *http.Request, id string, err error) {
	var reqErr *requestError
	switch {
	case errors.As(err, &reqErr):
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, reqErr.msg)
	case errors.Is(err, object.ErrInvalidMetadata):
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidMetadata, object.ValidationMessage(err))
	case errors.Is(err, metadata.ErrNotFound):
		writeError(w, http.StatusNotFound, protocol.CodeObjectNotFound, "object "+id+" not found")
	case errors.Is(err, object.ErrIncompleteUpload), errors.Is(err, context.Canceled):
		s.log.Warn("request abandoned", "method", r.Method, "object_id", id, "error", err)
		writeError(w, http.StatusBadRequest, protocol.CodeUploadIncomplete, "the request ended before all data was transferred")
	case errors.Is(err, cluster.ErrInsufficientWorkers):
		s.log.Error("not enough workers", "method", r.Method, "error", err)
		writeError(w, http.StatusServiceUnavailable, protocol.CodeInsufficientWorkers, "not enough storage workers are ready to store the required copies; try again later")
	case errors.Is(err, object.ErrObjectUnavailable):
		s.log.Error("object unavailable", "method", r.Method, "object_id", id, "error", err)
		writeError(w, http.StatusServiceUnavailable, protocol.CodeObjectUnavailable, "no copy of part of object "+id+" can be read right now; try again later")
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
