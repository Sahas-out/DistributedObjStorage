package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
	"github.com/Sahas-out/DistributedObjectStorage/worker/internal/storage"
)

func TestStatsCountsChunkTrafficOnly(t *testing.T) {
	store, err := storage.NewFilesystem(context.Background(), t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	h := newTestHandler(store)

	data := []byte("some chunk bytes")
	body := chunkBody(t, "obj", 0, data)
	if rec := do(t, h, http.MethodPut, "/internals/chunks/c1", body, nil); rec.Code != http.StatusCreated {
		t.Fatalf("PUT = %d", rec.Code)
	}
	do(t, h, http.MethodGet, "/internals/chunks/c1", nil, nil)
	do(t, h, http.MethodGet, "/internals/chunks/missing", nil, nil) // 404 -> error
	do(t, h, http.MethodGet, "/internals/chunks/metadata", nil, nil)
	do(t, h, http.MethodGet, "/healthz", nil, nil) // not counted

	rec := do(t, h, http.MethodGet, "/dashboard/stats", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("stats = %d", rec.Code)
	}
	var st protocol.WorkerStats
	if err := json.NewDecoder(rec.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	wantReq := protocol.RequestCounts{Put: 1, Get: 2, List: 1, Errors: 1}
	if st.Requests != wantReq {
		t.Fatalf("requests = %+v, want %+v", st.Requests, wantReq)
	}
	if st.ChunkCount != 1 || st.StoredBytes != int64(len(data)) || st.WorkerID != "worker-test" {
		t.Fatalf("stats = %+v", st)
	}

	// The stats request itself is not counted either.
	again := do(t, h, http.MethodGet, "/dashboard/stats", nil, nil)
	var st2 protocol.WorkerStats
	_ = json.NewDecoder(again.Body).Decode(&st2)
	if st2.Requests != wantReq {
		t.Fatalf("stats changed after polling: %+v", st2)
	}
}
