package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// stubStats answers Stats for addresses in byAddr and fails for the rest.
type stubStats struct {
	mu     sync.Mutex
	byAddr map[string]protocol.WorkerStats
}

func (s *stubStats) Stats(_ context.Context, addr string) (protocol.WorkerStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.byAddr[addr]
	if !ok {
		return protocol.WorkerStats{}, errors.New("worker unreachable")
	}
	return st, nil
}

func TestDashboardOverview(t *testing.T) {
	env := newTestServer(t)
	env.stats.byAddr["http://10.0.0.0:8080"] = protocol.WorkerStats{WorkerID: "w0", Requests: protocol.RequestCounts{Put: 42}}
	upload(t, env.srv.URL, []byte("hello dashboard"))

	resp := do(t, http.MethodGet, env.srv.URL+"/dashboard/overview", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got protocol.DashboardOverview
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Controller.ObjectCount != 1 || got.Controller.LogicalBytes != 15 || got.Controller.ReplicationFactor != 2 {
		t.Fatalf("controller = %+v", got.Controller)
	}
	if len(got.Workers) != 2 {
		t.Fatalf("workers = %+v", got.Workers)
	}
	w0, w1 := got.Workers[0], got.Workers[1]
	if w0.Stats == nil || w0.Stats.Requests.Put != 42 || w0.StatsError != "" {
		t.Fatalf("w0 = %+v", w0)
	}
	if w1.Stats != nil || w1.StatsError == "" {
		t.Fatalf("w1 should report a stats error: %+v", w1)
	}
	if w0.ChunkCount+w1.ChunkCount != 4 { // 2 chunks x 2 copies
		t.Fatalf("chunk counts = %d + %d", w0.ChunkCount, w1.ChunkCount)
	}
}

type eventsBody struct {
	Events  []protocol.DashboardEvent `json:"events"`
	LastSeq uint64                    `json:"last_seq"`
}

func getEvents(t *testing.T, url string) eventsBody {
	t.Helper()
	resp := do(t, http.MethodGet, url, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var b eventsBody
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDashboardEvents(t *testing.T) {
	env := newTestServer(t)
	info := upload(t, env.srv.URL, []byte("x"))
	first := getEvents(t, env.srv.URL+"/dashboard/events")
	if len(first.Events) != 1 || first.Events[0].Type != protocol.EventObjectUploaded || first.Events[0].ObjectID != info.ObjectID {
		t.Fatalf("events = %+v", first.Events)
	}

	do(t, http.MethodDelete, env.srv.URL+"/api/v1/objects/"+info.ObjectID, nil)
	next := getEvents(t, env.srv.URL+"/dashboard/events?after=1")
	if len(next.Events) != 1 || next.Events[0].Type != protocol.EventObjectDeleted || next.LastSeq != 2 {
		t.Fatalf("events after 1 = %+v", next)
	}

	wantError(t, do(t, http.MethodGet, env.srv.URL+"/dashboard/events?after=-1", nil), http.StatusBadRequest, protocol.CodeInvalidRequest)
	wantError(t, do(t, http.MethodPost, env.srv.URL+"/dashboard/overview", nil), http.StatusMethodNotAllowed, protocol.CodeMethodNotAllowed)
}
