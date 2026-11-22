package api

import (
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// traffic holds cumulative request counters for the chunk API, reported on
// GET /dashboard/stats. The controller pulls them for the web GUI, which
// turns them into per-worker requests/second.
type traffic struct {
	startedAt time.Time
	put       atomic.Int64
	get       atomic.Int64
	del       atomic.Int64
	list      atomic.Int64
	errors    atomic.Int64
}

const chunkPrefix = "/internals/chunks/"

// countTraffic counts requests to the chunk API by kind. /healthz and
// /dashboard are left out so health checks and dashboard polling don't show
// up as load.
func (t *traffic) countTraffic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, chunkPrefix) {
			next.ServeHTTP(w, r)
			return
		}
		rec := &statusRecorder{ResponseWriter: w}
		defer func() {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == chunkPrefix+"metadata":
				t.list.Add(1)
			case r.Method == http.MethodPut:
				t.put.Add(1)
			case r.Method == http.MethodGet:
				t.get.Add(1)
			case r.Method == http.MethodDelete:
				t.del.Add(1)
			}
			if rec.status >= 400 {
				t.errors.Add(1)
			}
		}()
		next.ServeHTTP(rec, r)
	})
}

// handleStats serves GET /dashboard/stats: request counters plus how many
// chunks and bytes this worker stores.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	chunks, err := s.store.List(r.Context())
	if err != nil {
		s.log.Error("list chunks for stats", "error", err)
		writeError(w, http.StatusInternalServerError, protocol.CodeStorageError, "could not list chunks")
		return
	}
	var stored int64
	for _, c := range chunks {
		stored += c.Size
	}
	t := s.traffic
	writeJSON(w, http.StatusOK, protocol.WorkerStats{
		WorkerID:      s.workerID(),
		StartedAt:     t.startedAt.UTC(),
		UptimeSeconds: int64(time.Since(t.startedAt) / time.Second),
		ChunkCount:    len(chunks),
		StoredBytes:   stored,
		Requests: protocol.RequestCounts{
			Put:    t.put.Load(),
			Get:    t.get.Load(),
			Delete: t.del.Load(),
			List:   t.list.Load(),
			Errors: t.errors.Load(),
		},
	})
}
