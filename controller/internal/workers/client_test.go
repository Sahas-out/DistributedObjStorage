package workers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

func workerReturning(status int, code string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(protocol.ErrorResponse{Error: protocol.ErrorBody{Code: code}})
	}))
}

func TestGetChunkMapsWorkerErrors(t *testing.T) {
	tests := []struct {
		status int
		code   string
		want   error
	}{
		{http.StatusNotFound, protocol.CodeChunkNotFound, ErrChunkNotFound},
		{http.StatusInternalServerError, protocol.CodeChunkCorrupted, ErrChunkCorrupted},
		{http.StatusInternalServerError, protocol.CodeStorageError, ErrUnavailable},
	}
	for _, tt := range tests {
		srv := workerReturning(tt.status, tt.code)
		_, err := NewClient(srv.URL, time.Second).GetChunk(context.Background(), "c1")
		srv.Close()
		if !errors.Is(err, tt.want) {
			t.Errorf("%d %s: error = %v, want %v", tt.status, tt.code, err, tt.want)
		}
	}
}

func TestUnreachableWorkerIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	_, err := NewClient(url, time.Second).PutChunk(context.Background(), "c1", "o1", 0, []byte("x"), "sum")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
}

func TestPutChunkSendsContractAndValidatesAck(t *testing.T) {
	ack := protocol.ChunkMetadata{ChunkID: "c1", ObjectID: "o1", SequenceNo: 3}
	wantSeq := 3
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var c protocol.Chunk
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if r.Method != http.MethodPut || r.URL.Path != "/internals/chunks/c1" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if c.ObjectID != "o1" || c.SequenceNo != wantSeq || string(c.Data) != "hello" {
			t.Errorf("body = %+v", c)
		}
		if got := r.Header.Get(protocol.HeaderChunkChecksum); got != "sum" {
			t.Errorf("checksum header = %q", got)
		}
		w.Header().Set(protocol.HeaderChunkChecksum, "sum")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(ack)
	}))
	defer srv.Close()
	client := NewClient(srv.URL, time.Second)

	got, err := client.PutChunk(context.Background(), "c1", "o1", 3, []byte("hello"), "sum")
	if err != nil || got != ack {
		t.Fatalf("PutChunk = %+v, %v", got, err)
	}
	// An acknowledgement for some other chunk must not count as success.
	wantSeq = 4
	if _, err := client.PutChunk(context.Background(), "c1", "o1", 4, []byte("hello"), "sum"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("mismatched ack: error = %v, want ErrUnavailable", err)
	}
}
