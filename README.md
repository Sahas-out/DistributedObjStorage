# Distributed Object Store

A mini distributed cloud object store: one controller, four storage workers, and clients. Objects are split into 1 MiB chunks and each chunk is stored on two workers. See [docs/Mini_Distributed_Cloud_Object_Store.pdf](docs/Mini_Distributed_Cloud_Object_Store.pdf) for the project statement.

**Current status: Phase 1.** One standalone worker that stores chunks and serves them over HTTP. See [docs/phase1_project_blueprint.md](docs/phase1_project_blueprint.md) and [docs/phase1_worker_http_api_plan.md](docs/phase1_worker_http_api_plan.md).

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
docker-compose.yml
```

## Requirements

- Go 1.26.5 or newer (the worker module's `go` directive)
- Docker with Docker Compose v2 (for the container setup)

## Build, test, run

The worker is its own Go module, so run Go commands from `worker/`:

```sh
cd worker
go vet ./...
go test -race ./...

# run locally (PowerShell: $env:PORT=8081; $env:DATA_DIR="./data"; go run ./cmd/worker)
PORT=8081 WORKER_ID=worker1 DATA_DIR=./data go run ./cmd/worker
```

### Docker

```sh
docker compose up --build -d     # worker1 on http://localhost:8081
docker compose ps                # should show (healthy)
docker compose logs -f worker1
docker compose down              # add -v to also delete the data volume
```

### Configuration (environment variables)

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
