// Package workers is the controller's HTTP client for the worker chunk API.
package workers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// Errors callers can act on. Returned errors wrap these.
var (
	// ErrUnavailable means the worker could not be reached or failed to serve
	// the request.
	ErrUnavailable = errors.New("worker unavailable")
	// ErrChunkNotFound means the worker does not hold the chunk.
	ErrChunkNotFound = errors.New("chunk not found on worker")
	// ErrChunkCorrupted means the worker's copy failed its integrity check.
	ErrChunkCorrupted = errors.New("chunk corrupted on worker")
)

// maxResponseBytes bounds a chunk response: one base64-encoded chunk plus room
// for the other JSON fields.
var maxResponseBytes = int64(base64.StdEncoding.EncodedLen(protocol.DefaultChunkSize)) + 4<<10

// Client talks to a single worker.
type Client struct {
	baseURL string
	timeout time.Duration
	http    *http.Client
}

// NewClient returns a Client for the worker at baseURL. timeout applies to
// each chunk request.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		timeout: timeout,
		http:    &http.Client{},
	}
}

// PutChunk stores a chunk and returns the worker's record of it. The checksum
// is sent along so the worker rejects data damaged in transit.
func (c *Client) PutChunk(ctx context.Context, chunkID, objectID string, seq int, data []byte, checksum string) (protocol.ChunkMetadata, error) {
	body, err := json.Marshal(protocol.Chunk{ObjectID: objectID, SequenceNo: seq, Data: data})
	if err != nil {
		return protocol.ChunkMetadata{}, fmt.Errorf("encode chunk %s: %w", chunkID, err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, c.chunkURL(chunkID), bytes.NewReader(body))
	if err != nil {
		return protocol.ChunkMetadata{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(protocol.HeaderChunkChecksum, checksum)

	resp, err := c.http.Do(req)
	if err != nil {
		return protocol.ChunkMetadata{}, transportError(ctx, "put", chunkID, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return protocol.ChunkMetadata{}, responseError("put", chunkID, resp)
	}

	var meta protocol.ChunkMetadata
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&meta); err != nil {
		return protocol.ChunkMetadata{}, fmt.Errorf("%w: put chunk %s: decode response: %v", ErrUnavailable, chunkID, err)
	}
	if got := resp.Header.Get(protocol.HeaderChunkChecksum); !strings.EqualFold(got, checksum) {
		return protocol.ChunkMetadata{}, fmt.Errorf("%w: put chunk %s: worker stored checksum %q, sent %q", ErrUnavailable, chunkID, got, checksum)
	}
	if meta.ChunkID != chunkID || meta.ObjectID != objectID || meta.SequenceNo != seq {
		return protocol.ChunkMetadata{}, fmt.Errorf("%w: put chunk %s: worker acknowledged a different chunk: %+v", ErrUnavailable, chunkID, meta)
	}
	return meta, nil
}

// GetChunk retrieves a chunk from the worker.
func (c *Client) GetChunk(ctx context.Context, chunkID string) (protocol.Chunk, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, c.chunkURL(chunkID), nil)
	if err != nil {
		return protocol.Chunk{}, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return protocol.Chunk{}, transportError(ctx, "get", chunkID, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return protocol.Chunk{}, responseError("get", chunkID, resp)
	}

	var chunk protocol.Chunk
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&chunk); err != nil {
		if ctx.Err() != nil {
			return protocol.Chunk{}, ctx.Err()
		}
		return protocol.Chunk{}, fmt.Errorf("%w: get chunk %s: decode response: %v", ErrUnavailable, chunkID, err)
	}
	return chunk, nil
}

// DeleteChunk removes a chunk. Deleting a missing chunk succeeds.
func (c *Client) DeleteChunk(ctx context.Context, chunkID string) error {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodDelete, c.chunkURL(chunkID), nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return transportError(ctx, "delete", chunkID, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return responseError("delete", chunkID, resp)
	}
	return nil
}

func (c *Client) chunkURL(chunkID string) string {
	return c.baseURL + "/internals/chunks/" + url.PathEscape(chunkID)
}

// transportError separates the caller going away (ctx is the caller's context,
// not the per-request timeout) from the worker failing to answer.
func transportError(ctx context.Context, op, chunkID string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w: %s chunk %s: %v", ErrUnavailable, op, chunkID, err)
}

// responseError maps a worker error response onto the package's errors.
func responseError(op, chunkID string, resp *http.Response) error {
	var body protocol.ErrorResponse
	_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&body)
	detail := fmt.Sprintf("%s chunk %s: worker returned %d %s", op, chunkID, resp.StatusCode, body.Error.Code)
	switch {
	case body.Error.Code == protocol.CodeChunkCorrupted:
		return fmt.Errorf("%w: %s", ErrChunkCorrupted, detail)
	case resp.StatusCode == http.StatusNotFound && body.Error.Code == protocol.CodeChunkNotFound:
		return fmt.Errorf("%w: %s", ErrChunkNotFound, detail)
	case resp.StatusCode >= 500:
		return fmt.Errorf("%w: %s", ErrUnavailable, detail)
	default:
		return errors.New(detail)
	}
}

// drainAndClose lets the underlying connection be reused.
func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 64<<10))
	_ = body.Close()
}
