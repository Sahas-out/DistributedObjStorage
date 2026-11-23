package protocol

import "time"

// DefaultChunkSize is the size objects are split into (1 MiB). Only the last
// chunk of an object may be smaller. It must not exceed the worker's
// MAX_CHUNK_BYTES.
const DefaultChunkSize = 1 << 20

// ObjectInfo describes a stored object: the body returned after an upload and
// each entry of the object list.
type ObjectInfo struct {
	ObjectID       string    `json:"object_id"`
	Name           string    `json:"name"`
	ContentType    string    `json:"content_type"`
	Size           int64     `json:"size"`
	Checksum       string    `json:"checksum"` // SHA-256 hex digest of the whole object
	ChunkCount     int       `json:"chunk_count"`
	CreatedAt      time.Time `json:"created_at"`
	LastModifiedAt time.Time `json:"last_modified_at"` // last change of name, content type or custom metadata
}

// ObjectMetadata is the body of GET and PATCH
// /api/v1/objects/{object_id}/metadata: the system-maintained metadata, the
// custom metadata and where every copy of every chunk is stored.
type ObjectMetadata struct {
	ObjectInfo
	CustomMetadata map[string]string `json:"custom_metadata"` // never null
	Chunks         []ChunkInfo       `json:"chunks"`
}

// ChunkInfo describes one chunk of an object and its copies.
type ChunkInfo struct {
	SequenceNo int             `json:"sequence_no"`
	Size       int64           `json:"size"`
	Checksum   string          `json:"checksum"` // SHA-256 hex digest of the chunk
	Replicas   []ChunkLocation `json:"replicas"`
}

// ChunkLocation is where one copy of a chunk is stored.
type ChunkLocation struct {
	ChunkID    string `json:"chunk_id"`
	WorkerID   string `json:"worker_id"`
	WorkerAddr string `json:"worker_addr"`
	Status     string `json:"status"`
}

// MetadataPatch is the body of PATCH /api/v1/objects/{object_id}/metadata.
// Omitted fields are left unchanged. A custom key set to null is removed.
type MetadataPatch struct {
	Name           *string            `json:"name,omitempty"`
	ContentType    *string            `json:"content_type,omitempty"`
	CustomMetadata map[string]*string `json:"custom_metadata,omitempty"`
}

// Error codes returned by the controller's client-facing API.
const (
	CodeInvalidObjectID   = "invalid_object_id"
	CodeObjectNotFound    = "object_not_found"
	CodeObjectCorrupted   = "object_corrupted"
	CodeUploadIncomplete  = "upload_incomplete"
	CodeWorkerUnavailable = "worker_unavailable"
	CodeMethodNotAllowed  = "method_not_allowed"
	CodeInvalidMetadata   = "invalid_metadata"
)
