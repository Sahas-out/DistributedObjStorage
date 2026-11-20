// Package protocol holds wire types shared by the worker, controller and client.
package protocol

// HTTP headers used by the worker chunk API.
const (
	// HeaderChunkChecksum carries the SHA-256 hex digest of a chunk's data on
	// PUT only. The caller may send it so the worker rejects data damaged in
	// transit, and the worker echoes the digest of what it received. Workers
	// do not store checksums, so GET responses don't carry it: the controller
	// verifies chunks against its own metadata.
	HeaderChunkChecksum = "X-Chunk-Checksum"
	// HeaderChunkSize carries the chunk data size in bytes on GET responses.
	HeaderChunkSize = "X-Chunk-Size"
)

// Chunk is the body of PUT /internals/chunks/{chunk_id} and the response
// body of GET /internals/chunks/{chunk_id} ("chunk_controller" in
// docs/H-Design-Worker.md).
//
// Data is the raw chunk bytes; encoding/json transmits it as a standard
// base64 string.
type Chunk struct {
	ObjectID   string `json:"object_id"`
	SequenceNo int    `json:"sequence_no"`
	Data       []byte `json:"data"`
}

// ChunkMetadata describes one stored chunk ("chunk_metadata_controller" in
// docs/H-Design-Worker.md). GET /internals/chunks/metadata returns a JSON
// array of these; PUT returns one for the stored chunk.
type ChunkMetadata struct {
	ChunkID    string `json:"chunk_id"`
	ObjectID   string `json:"object_id"`
	SequenceNo int    `json:"sequence_no"`
}

// ErrorResponse is the JSON body of every error response.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody holds a machine-readable code and a human-readable message.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error codes returned by the worker API.
const (
	CodeInvalidChunkID   = "invalid_chunk_id"
	CodeChunkNotFound    = "chunk_not_found"
	CodeChunkTooLarge    = "chunk_too_large"
	CodeChecksumMismatch = "checksum_mismatch"
	CodeInvalidRequest   = "invalid_request"
	CodeStorageError     = "storage_error"
	CodeInternalError    = "internal_error"
	CodeNotFound         = "not_found"
)
