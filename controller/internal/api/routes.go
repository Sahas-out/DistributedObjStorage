// Package api implements the controller's client-facing HTTP API.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/object"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// Server serves the object API.
type Server struct {
	objects *object.Service
	ping    func(context.Context) error
	log     *slog.Logger
}

// NewServer builds a Server. ping reports whether the metadata store is usable
// and backs the health check. A nil logger discards logs.
func NewServer(objects *object.Service, ping func(context.Context) error, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Server{objects: objects, ping: ping, log: log}
}

// Handler returns the HTTP handler with all routes and middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/v1/objects", s.handleUpload)
	mux.HandleFunc("GET /api/v1/objects/{object_id}", s.handleDownload)
	mux.HandleFunc("GET /healthz", s.handleHealth)

	// Catch-alls so these responses use the JSON error format too. More
	// specific patterns above take priority.
	mux.HandleFunc("/api/v1/objects", methodNotAllowed(http.MethodPut))
	mux.HandleFunc("/api/v1/objects/{object_id}", methodNotAllowed(http.MethodGet+", "+http.MethodHead))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, protocol.CodeNotFound, "no such endpoint")
	})
	return s.recoverPanics(s.logRequests(mux))
}

func methodNotAllowed(allow string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Allow", allow)
		writeError(w, http.StatusMethodNotAllowed, protocol.CodeMethodNotAllowed, "method not allowed; use "+allow)
	}
}

// statusRecorder captures the status code and bytes written for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

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
		// Deferred so downloads aborted with http.ErrAbortHandler are logged.
		defer func() {
			s.log.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		}()
		next.ServeHTTP(rec, r)
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
