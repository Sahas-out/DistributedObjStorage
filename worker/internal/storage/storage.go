// Package storage defines the chunk storage contract used by the worker.
//
// This file is the agreed interface between Person 1 (storage implementation)
// and Person 2 (HTTP API). Change it only by agreement. The metadata record
// follows CHUNKMetadata in docs/H-Design-Worker.md.
package storage

import (
	"context"
	"errors"
	"io"
	"regexp"
	"time"
)

// ChunkMetadata is the worker's internal record for one stored chunk
// (CHUNKMetadata in docs/H-Design-Worker.md). It is not a wire type; the API
// layer converts it to protocol.ChunkMetadata / protocol.Chunk.
type ChunkMetadata struct {
	ChunkID    string    // unique chunk identifier (primary key)
	ObjectID   string    // parent object identifier
	SequenceNo int       // chunk's position in the object
	FilePath   string    // location of the chunk file, relative to the data directory
	Size       int64     // chunk size in bytes
	Checksum   string    // SHA-256 hex digest, for integrity verification
	CreatedAt  time.Time // when the chunk was stored
}

// MaxChunkIDLength is the longest accepted chunk ID.
const MaxChunkIDLength = 128

var chunkIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// ValidChunkID reports whether id is an acceptable chunk ID: 1-128 characters
// from [A-Za-z0-9_-]. Such IDs contain no path separators or dots, so they
// cannot escape the storage directory.
func ValidChunkID(id string) bool {
	return chunkIDPattern.MatchString(id)
}

// Sentinel errors. Implementations may wrap them; callers use errors.Is.
var (
	// ErrNotFound is returned when the chunk does not exist.
	ErrNotFound = errors.New("chunk not found")
	// ErrInvalidChunkID is returned when the chunk ID is malformed or would
	// escape the storage directory.
	ErrInvalidChunkID = errors.New("invalid chunk id")
	// ErrMissingChecksum is returned when a chunk exists but its recorded
	// checksum metadata is missing or unreadable.
	ErrMissingChecksum = errors.New("chunk checksum metadata missing")
)

// Storage persists chunks and their metadata.
type Storage interface {
	// Put stores (or overwrites) a chunk: it registers the metadata entry and
	// stores the data. The caller sets ChunkID, ObjectID and SequenceNo in
	// meta; the store fills in FilePath, Size, Checksum and CreatedAt and
	// returns the complete record.
	Put(ctx context.Context, meta ChunkMetadata, src io.Reader) (ChunkMetadata, error)
	// Open returns the stored chunk data and its metadata.
	// The caller must close the reader.
	Open(ctx context.Context, chunkID string) (io.ReadCloser, ChunkMetadata, error)
	// Delete removes a chunk's metadata entry and data.
	// Returns ErrNotFound if it does not exist.
	Delete(ctx context.Context, chunkID string) error
	// List returns the metadata of all stored chunks.
	List(ctx context.Context) ([]ChunkMetadata, error)
}
