package protocol

import "time"

// RegisterRequest is the body of POST /internal/workers/register on the
// controller. A worker sends it at startup and again every announce interval.
//
// WorkerID is empty the first time a worker ever registers; the controller
// assigns one and the worker stores it and sends it on every later call. Port
// is the port the worker's chunk API listens on. The controller combines it
// with the request's source IP to build the worker's address.
type RegisterRequest struct {
	WorkerID string `json:"worker_id"`
	Port     int    `json:"port"`
}

// RegisterResponse answers a RegisterRequest.
type RegisterResponse struct {
	WorkerID string `json:"worker_id"`
	Status   string `json:"status"`
	// AnnounceIntervalSeconds is how often the controller expects the worker
	// to register again.
	AnnounceIntervalSeconds int `json:"announce_interval_seconds"`
}

// WorkerInfo describes one registered worker (GET /internal/workers).
type WorkerInfo struct {
	WorkerID   string    `json:"worker_id"`
	Addr       string    `json:"addr"`
	Status     string    `json:"status"`
	ChunkCount int       `json:"chunk_count"`
	LastSeen   time.Time `json:"last_seen"`
}

// ReplicaInfo describes where one copy of a chunk lives
// (GET /internal/objects/{object_id}/replicas).
type ReplicaInfo struct {
	ChunkID    string `json:"chunk_id"`
	SequenceNo int    `json:"sequence_no"`
	WorkerID   string `json:"worker_id"`
	WorkerAddr string `json:"worker_addr"`
	Status     string `json:"status"`
}

// Error codes returned by the controller's internal worker API and by uploads
// that cannot be placed.
const (
	CodeInsufficientWorkers = "insufficient_workers"
	CodeObjectUnavailable   = "object_unavailable"
	CodeWorkerNotFound      = "worker_not_found"
	CodeWorkerRetired       = "worker_retired"
	CodeInvalidWorkerID     = "invalid_worker_id"
)
