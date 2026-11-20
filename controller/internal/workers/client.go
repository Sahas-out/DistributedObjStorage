// Package workers is the controller's HTTP client for the worker chunk API.
// One Client serves every worker; each call names the worker's address.
package workers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	// ErrChunkCorrupted means a replica failed the controller's integrity
	// check. Workers don't verify chunks themselves.
	ErrChunkCorrupted = errors.New("chunk corrupted on worker")
)

// maxResponseBytes bounds a chunk response: one base64-encoded chunk plus room
// for the other JSON fields.
var maxResponseBytes = int64(base64.StdEncoding.EncodedLen(protocol.DefaultChunkSize)) + 4<<10

// dialTimeout bounds connecting to a worker.
const dialTimeout = time.Second

// maxListResponseBytes bounds a chunk list response.
const maxListResponseBytes = 64 << 20

// Client talks to workers. It is safe for concurrent use and shares one
// connection pool across all workers.
type Client struct {
	timeout time.Duration
	http    *http.Client
}

// NewClient returns a Client. timeout applies to each request.
func NewClient(timeout time.Duration) *Client {
	return &Client{
		timeout: timeout,
		http: &http.Client{Transport: &http.Transport{
			Proxy: nil,
			// Workers sit on the same Docker network, so connecting takes
			// well under a millisecond. A short dial timeout keeps a stopped
			// worker (whose IP no longer answers) from stalling a request
			// until the full timeout.
			DialContext:         (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
		}},
	}
}

// PutChunk stores a chunk and returns the worker's record of it. The checksum
// is sent along so the worker rejects data damaged in transit.
func (c *Client) PutChunk(ctx context.Context, addr, chunkID, objectID string, seq int, data []byte, checksum string) (protocol.ChunkMetadata, error) {
	body, err := json.Marshal(protocol.Chunk{ObjectID: objectID, SequenceNo: seq, Data: data})
	if err != nil {
		return protocol.ChunkMetadata{}, fmt.Errorf("encode chunk %s: %w", chunkID, err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, chunkURL(addr, chunkID), bytes.NewReader(body))
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
func (c *Client) GetChunk(ctx context.Context, addr, chunkID string) (protocol.Chunk, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, chunkURL(addr, chunkID), nil)
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
func (c *Client) DeleteChunk(ctx context.Context, addr, chunkID string) error {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodDelete, chunkURL(addr, chunkID), nil)
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

// ListChunks returns every chunk the worker at addr holds
// (GET /internals/chunks/metadata).
func (c *Client) ListChunks(ctx context.Context, addr string) ([]protocol.ChunkMetadata, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, strings.TrimRight(addr, "/")+"/internals/chunks/metadata", nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, transportError(ctx, "list", "", err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, responseError("list", "", resp)
	}
	var out []protocol.ChunkMetadata
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxListResponseBytes)).Decode(&out); err != nil {
		return nil, fmt.Errorf("%w: list chunks: decode response: %v", ErrUnavailable, err)
	}
	return out, nil
}

func chunkURL(addr, chunkID string) string {
	return strings.TrimRight(addr, "/") + "/internals/chunks/" + url.PathEscape(chunkID)
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
