// Package api implements the worker's internal HTTP API.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
	"github.com/Sahas-out/DistributedObjectStorage/worker/internal/storage"
)

// Server serves the worker chunk API on top of a Storage.
type Server struct {
	store    storage.Storage
	ping     func(context.Context) error
	workerID func() string
	maxChunk int64
	log      *slog.Logger
	traffic  *traffic
}

// NewServer builds a Server. ping reports whether the metadata database is
// usable and backs the health check. workerID returns the ID the controller
// assigned, or "" before the first registration. maxChunk is the largest
// accepted chunk body in bytes. A nil logger discards logs.
func NewServer(store storage.Storage, ping func(context.Context) error, workerID func() string, maxChunk int64, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	return &Server{store: store, ping: ping, workerID: workerID, maxChunk: maxChunk, log: log,
		traffic: &traffic{startedAt: time.Now()}}
}

// Handler returns the HTTP handler with all routes and middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Endpoints from docs/H-Design-Worker.md. The literal "metadata" pattern
	// is more specific than {chunk_id}, so ServeMux routes
	// GET /internals/chunks/metadata to the list handler.
	mux.HandleFunc("PUT /internals/chunks/{chunk_id}", s.handlePutChunk)
	mux.HandleFunc("GET /internals/chunks/{chunk_id}", s.handleGetChunk)
	mux.HandleFunc("DELETE /internals/chunks/{chunk_id}", s.handleDeleteChunk)
	mux.HandleFunc("GET /internals/chunks/metadata", s.handleListChunks)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	// Read-only counters for the controller's dashboard. Workers are only
	// reachable inside the cluster network; the GUI gets these through the
	// controller's /dashboard/overview.
	mux.HandleFunc("GET /dashboard/stats", s.handleStats)
	return s.recoverPanics(s.logRequests(s.traffic.countTraffic(mux)))
}

// statusRecorder captures the status code and bytes written for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

// used to write http status lines
func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		s.log.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"bytes", rec.bytes,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

func (s *Server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rv := recover()
			if rv == nil {
				return
			}
			if rv == http.ErrAbortHandler {
				panic(rv)
			}
			s.log.Error("panic serving request", "method", r.Method, "path", r.URL.Path, "panic", rv)
			writeError(w, http.StatusInternalServerError, protocol.CodeInternalError, "internal server error")
		}()
		next.ServeHTTP(w, r)
	})
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
