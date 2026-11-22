package api

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/events"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// statsTimeout bounds asking one worker for its stats, so one stopped worker
// doesn't stall the whole dashboard.
const statsTimeout = time.Second

// StatsFetcher asks a worker for its dashboard stats (the workers.Client).
type StatsFetcher interface {
	Stats(ctx context.Context, addr string) (protocol.WorkerStats, error)
}

// Dashboard is what the read-only /dashboard endpoints need.
type Dashboard struct {
	Stats             StatsFetcher
	Events            *events.Log
	ReplicationFactor int
	AnnounceInterval  time.Duration
	StartedAt         time.Time
}

// EnableDashboard turns on the /dashboard endpoints. Call it before Handler.
func (s *Server) EnableDashboard(d Dashboard) { s.dash = &d }

// handleDashboardOverview combines the controller's worker table with stats
// pulled from every worker in parallel. Workers are only reachable inside
// the cluster network, so the GUI gets their numbers through here.
func (s *Server) handleDashboardOverview(w http.ResponseWriter, r *http.Request) {
	objs, err := s.objects.List(r.Context())
	if err != nil {
		s.writeObjectError(w, r, "", err)
		return
	}
	summary := protocol.ControllerSummary{
		StartedAt:               s.dash.StartedAt.UTC(),
		ReplicationFactor:       s.dash.ReplicationFactor,
		AnnounceIntervalSeconds: int(s.dash.AnnounceInterval / time.Second),
		ObjectCount:             len(objs),
	}
	for _, o := range objs {
		summary.LogicalBytes += o.Size
	}

	ws := s.members.Workers()
	out := make([]protocol.DashboardWorker, len(ws))
	var wg sync.WaitGroup
	for i, wk := range ws {
		out[i].WorkerInfo = protocol.WorkerInfo{
			WorkerID:   wk.ID,
			Addr:       wk.Addr,
			Status:     string(wk.Status),
			ChunkCount: wk.ChunkCount,
			LastSeen:   wk.LastSeen.UTC(),
		}
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(r.Context(), statsTimeout)
			defer cancel()
			st, err := s.dash.Stats.Stats(ctx, wk.Addr)
			if err != nil {
				out[i].StatsError = err.Error()
				return
			}
			out[i].Stats = &st
		})
	}
	wg.Wait()
	writeJSON(w, http.StatusOK, protocol.DashboardOverview{
		ServerTime: time.Now().UTC(),
		Controller: summary,
		Workers:    out,
	})
}

// eventsResponse is the body of GET /dashboard/events.
type eventsResponse struct {
	Events []protocol.DashboardEvent `json:"events"`
	// LastSeq is the newest event's seq; pass it as ?after= next time.
	LastSeq uint64 `json:"last_seq"`
}

// handleDashboardEvents returns activity-feed events newer than ?after=<seq>.
func (s *Server) handleDashboardEvents(w http.ResponseWriter, r *http.Request) {
	var after uint64
	if v := r.URL.Query().Get("after"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, "after must be a non-negative integer")
			return
		}
		after = n
	}
	evs, last := s.dash.Events.Since(after)
	writeJSON(w, http.StatusOK, eventsResponse{Events: evs, LastSeq: last})
}
