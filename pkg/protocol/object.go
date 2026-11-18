package protocol

import "time"

// DefaultChunkSize is the size objects are split into (1 MiB). Only the last
// chunk of an object may be smaller. It must not exceed the worker's
// MAX_CHUNK_BYTES.
const DefaultChunkSize = 1 << 20

// ObjectInfo is the JSON body returned after an object is uploaded.
type ObjectInfo struct {
	ObjectID   string    `json:"object_id"`
	Size       int64     `json:"size"`
	Checksum   string    `json:"checksum"` // SHA-256 hex digest of the whole object
	ChunkCount int       `json:"chunk_count"`
	CreatedAt  time.Time `json:"created_at"`
}

// Error codes returned by the controller's client-facing API.
const (
	CodeInvalidObjectID   = "invalid_object_id"
	CodeObjectNotFound    = "object_not_found"
	CodeObjectCorrupted   = "object_corrupted"
	CodeUploadIncomplete  = "upload_incomplete"
	CodeWorkerUnavailable = "worker_unavailable"
	CodeMethodNotAllowed  = "method_not_allowed"
)
