# Distributed Object Store

A mini distributed cloud object store: one controller, four storage workers, and clients. Objects are split into 1 MiB chunks and each chunk is stored on two workers. See [docs/Mini_Distributed_Cloud_Object_Store.pdf](docs/Mini_Distributed_Cloud_Object_Store.pdf) for the project statement.

**Current status: Phase 2.** A client can upload and download objects through the controller, which splits them into chunks stored on a single worker. Multiple workers, replication and failure handling come in later phases.

## Quick start: try the whole flow

```sh
docker compose up --build -d      # controller on :8080, worker1 on :8081
docker compose ps                 # both should show (healthy)

head -c 5242880 /dev/urandom > original.bin      # a 5 MiB test file

# Upload. The response contains the object_id.
curl -s -T original.bin http://localhost:8080/api/v1/objects
ID=<object_id from the response>

# Download and compare.
curl -s -o downloaded.bin http://localhost:8080/api/v1/objects/$ID
cmp original.bin downloaded.bin && echo "bytes match"

# See the 5 chunks on the worker.
curl -s http://localhost:8081/internals/chunks/metadata

# Metadata and chunks survive recreating the containers...
docker compose down
docker compose up -d
curl -s -o again.bin http://localhost:8080/api/v1/objects/$ID
cmp original.bin again.bin && echo "still there"

# ...until the volumes are deleted.
docker compose down -v
```

## Controller HTTP API

| Method | Endpoint | Success | Notes |
|---|---|---|---|
| `PUT` | `/api/v1/objects` | `201` + `ObjectInfo` JSON | The request body is the raw object. The controller generates the object ID. |
| `GET` | `/api/v1/objects/{object_id}` | `200` + raw bytes | Sends `Content-Length` and `ETag` (the object's SHA-256). `HEAD` returns the headers only. |
| `GET` | `/healthz` | `200` | Used by the Docker health check. |

```json
{"object_id": "5f2c...", "size": 5242880, "checksum": "<sha256>", "chunk_count": 5, "created_at": "2026-11-18T10:00:00Z"}
```

| Status | Code | When |
|---|---|---|
| 400 | `invalid_object_id` | The ID is not 32 lowercase hex characters |
| 400 | `upload_incomplete` | The client stopped sending before the upload finished; nothing is stored |
| 404 | `object_not_found` | No object has this ID |
| 405 | `method_not_allowed` | The endpoint doesn't support this method |
| 500 | `object_corrupted` | A chunk is missing from the worker or failed its checksum |
| 500 | `internal_error` | Any other controller failure; details are logged |
| 503 | `worker_unavailable` | The worker can't be reached or failed to serve the request |

If a chunk fails after a download has started, the `200` has already been sent, so the controller aborts the connection. The client then receives fewer bytes than `Content-Length` and sees an error (curl exits with code 18).

### How an upload and a download work

- **Upload:** the controller reads the body into one reusable 1 MiB buffer and sends each chunk to the worker before reading the next, so memory use doesn't grow with object size. Metadata is committed in one SQLite transaction after the last chunk is stored. If anything fails, the chunks already sent are deleted and no object is created.
- **Download:** the controller loads the ordered chunk list, then fetches, verifies (SHA-256) and writes one chunk at a time.

The controller keeps its metadata in `DATA_DIR/metadata.db`, in two tables: `objects` (ID, size, checksum, chunk count, creation time) and `object_chunks` (sequence number, chunk ID, size, checksum).

### Controller configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | HTTP listen port |
| `DATA_DIR` | `/data` | Directory for `metadata.db` (created if missing) |
| `WORKER_URL` | `http://worker1:8080` | Base URL of the worker, using its Docker service name |
| `WORKER_TIMEOUT` | `30s` | Time limit for each chunk request to the worker |

### Worker storage

The worker keeps everything in `DATA_DIR`, which is the Docker volume:

```text
DATA_DIR/
├── metadata.db   SQLite: one row per chunk (chunk_id, object_id, sequence_no, file_path, size, checksum, created_at)
├── chunks/       chunk data, one file per chunk, named <chunk_id>.<random>
└── tmp/          in-progress writes
```

A PUT writes the data to `tmp/`, fsyncs it, renames it into `chunks/`, and only then commits the metadata row and returns `201` (see [docs/upload_sequence.mmd](docs/upload_sequence.mmd)). An overwrite always writes a new file and removes the old one after the row commits, so a failed or interrupted PUT never damages the previous version. On startup the worker empties `tmp/`, deletes chunk files that no row references, and drops rows whose file is missing. After that, `GET /internals/chunks/metadata` lists only chunks the worker can actually serve.

## Layout

```text
pkg/protocol/          shared wire types (Chunk, ChunkMetadata, ErrorResponse, header names, error codes)
worker/cmd/worker/     worker entry point
worker/internal/api/   HTTP routes, handlers, middleware, handler tests
worker/internal/storage/  Storage interface (shared contract), filesystem + SQLite store
worker/internal/config/   environment-based configuration
worker/Dockerfile
controller/cmd/controller/    controller entry point
controller/internal/api/      client-facing routes and handlers
controller/internal/object/   upload and download orchestration
controller/internal/chunk/    splits a stream into 1 MiB chunks
controller/internal/metadata/ SQLite object and chunk metadata
controller/internal/workers/  HTTP client for the worker API
controller/internal/config/   environment-based configuration
controller/Dockerfile
docker-compose.yml
```

## Requirements

- Go 1.26.5 or newer (the worker module's `go` directive)
- Docker with Docker Compose v2 (for the container setup)

## Build, test, run

The worker and the controller are separate Go modules, so run Go commands from their directories. The same `go vet` and `go test` commands work in `controller/`:

```sh
cd worker
go vet ./...
go test -race ./...

# run locally (PowerShell: $env:PORT=8081; $env:DATA_DIR="./data"; go run ./cmd/worker)
PORT=8081 WORKER_ID=worker1 DATA_DIR=./data go run ./cmd/worker
```

### Docker

```sh
docker compose up --build -d     # controller on :8080, worker1 on :8081
docker compose ps                # should show (healthy)
docker compose logs -f controller worker1
docker compose down              # add -v to also delete the data volumes
```

### Worker configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `WORKER_ID` | hostname | ID reported in listings and health |
| `PORT` | `8080` | HTTP listen port |
| `DATA_DIR` | `/data` | Directory for chunk files and `metadata.db` (created if missing) |
| `MAX_CHUNK_BYTES` | `1048576` | Largest accepted chunk (1 MiB) |

## Worker HTTP API

Endpoints and DTOs follow [docs/H-Design-Worker.md](docs/H-Design-Worker.md).

| Method | Endpoint | Success | Notes |
|---|---|---|---|
| `PUT` | `/internals/chunks/{chunk_id}` | `201` + `ChunkMetadata` JSON | Body is a `Chunk` JSON object; `data` is at most 1 MiB once decoded. Optional `X-Chunk-Checksum` request header (SHA-256 of the decoded data); a mismatch returns `400`. The response also carries `X-Chunk-Checksum`. Re-PUT overwrites. |
| `GET` | `/internals/chunks/{chunk_id}` | `200` + `Chunk` JSON | Headers: `X-Chunk-Checksum`, `X-Chunk-Size`. Data is checked against the recorded checksum before sending. |
| `DELETE` | `/internals/chunks/{chunk_id}` | `204` | Also `204` if the chunk doesn't exist (idempotent). |
| `GET` | `/internals/chunks/metadata` | `200` + JSON array of `ChunkMetadata` | `[]` when empty. |
| `GET` | `/healthz` | `200` | `{"status":"ok","worker_id":"..."}`. Not in the design doc; used by the Docker health check. |

DTOs (`pkg/protocol`):

```jsonc
// Chunk: PUT request body and GET response body. data is base64.
{"object_id": "obj1", "sequence_no": 0, "data": "aGVsbG8="}
// ChunkMetadata: PUT response, and each element of GET /internals/chunks/metadata.
{"chunk_id": "obj1_0", "object_id": "obj1", "sequence_no": 0}
```

Chunk IDs and object IDs must match `^[A-Za-z0-9_-]{1,128}$`, and `metadata` is reserved (it can't be a chunk ID). `sequence_no` is required and must be `>= 0`. Checksums are lowercase SHA-256 hex.

### Errors

All errors are JSON:

```json
{"error": {"code": "chunk_not_found", "message": "chunk abc not found"}}
```

| Status | Code | When |
|---|---|---|
| 400 | `invalid_chunk_id` | Chunk ID fails validation |
| 400 | `invalid_request` | PUT body isn't a valid `Chunk` (bad JSON or base64, missing/invalid `object_id`, `sequence_no` or `data`), malformed `X-Chunk-Checksum`, or the body couldn't be read |
| 400 | `checksum_mismatch` | PUT `data` doesn't match `X-Chunk-Checksum`; nothing is stored |
| 404 | `chunk_not_found` | GET of a missing chunk |
| 413 | `chunk_too_large` | PUT `data` over `MAX_CHUNK_BYTES` |
| 500 | `chunk_corrupted` | Stored bytes don't match the recorded checksum; the controller should use another replica |
| 500 | `storage_error` | Any other storage failure; details are logged, not returned |
| 500 | `internal_error` | A panic was recovered |

### Examples (bash)

```sh
W=http://localhost:8081
head -c 1048576 /dev/urandom > chunk.bin
SUM=$(sha256sum chunk.bin | cut -d' ' -f1)

printf '{"object_id":"obj1","sequence_no":0,"data":"%s"}' "$(base64 -w0 chunk.bin)" > put.json

curl -s -X PUT -H "Content-Type: application/json" -H "X-Chunk-Checksum: $SUM" \
     --data-binary @put.json $W/internals/chunks/obj1_0
curl -s $W/internals/chunks/obj1_0 | jq -r .data | base64 -d | cmp - chunk.bin && echo "bytes match"
curl -s $W/internals/chunks/metadata
curl -s -o /dev/null -w "%{http_code}\n" -X DELETE $W/internals/chunks/obj1_0   # 204
curl -s $W/internals/chunks/obj1_0                                              # 404 JSON
curl -s -X PUT --data-binary @put.json $W/internals/chunks/bad.id               # 400 JSON
```

The `cmp` check needs `jq`.

On Windows, use `curl.exe` in PowerShell (plain `curl` is an alias for `Invoke-WebRequest`).
