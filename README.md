# Distributed Object Store

A mini distributed cloud object store: one controller, four storage workers, and clients. Objects are split into 1 MiB chunks and each chunk is stored on two workers. See [docs/Mini_Distributed_Cloud_Object_Store.pdf](docs/Mini_Distributed_Cloud_Object_Store.pdf) for the project statement.

**Current status: Phase 3.** Workers register themselves with the controller, and the number of workers can be scaled with one command. Every chunk is stored on 2 different workers (replication factor 2), and chunks are spread evenly across workers. Clients can upload, download, list and delete objects. A copy that fails its checksum is detected, marked, and skipped in favour of the other copy. Detecting worker failures, restoring lost or corrupted copies, and custom metadata come in later phases.

## Quick start: try the whole flow

```sh
docker compose up --build -d              # controller on :8080 + 4 workers
docker compose ps                         # all should show (healthy)
curl -s localhost:8080/internal/workers   # 4 workers, status READY

head -c 10485760 /dev/urandom > original.bin      # a 10 MiB test file

# Upload. The response contains the object_id.
curl -s -T original.bin http://localhost:8080/api/v1/objects
ID=<object_id from the response>

# Where are the 2 copies of each of the 10 chunks?
curl -s localhost:8080/internal/objects/$ID/replicas
curl -s localhost:8080/internal/workers           # chunk_count: 5 on each worker

# Download and compare. List all objects.
curl -s -o downloaded.bin http://localhost:8080/api/v1/objects/$ID
cmp original.bin downloaded.bin && echo "bytes match"
curl -s localhost:8080/api/v1/objects

# Scale the workers up or down. No Kubernetes or Swarm needed.
docker compose up -d --scale worker=6
docker compose up -d --scale worker=4

# Delete the object (204), after which it is gone (404).
curl -s -o /dev/null -w "%{http_code}\n" -X DELETE localhost:8080/api/v1/objects/$ID

docker compose down        # data survives; add -v to also delete the volumes
```

> **Upgrading from Phase 2:** the controller's and the worker's database layouts changed, so old volumes aren't compatible. The controller refuses to start on a Phase 2 volume with a message that says so. Run `docker compose down -v` once to start fresh. (The old `worker1-data` volume is no longer used; delete it with `docker volume rm distributedobjectstorage_worker1-data`.)

### Demo: corrupted copy

```sh
docker compose exec --index 2 worker sh -c \
  'f=$(ls /data/slot-*/chunks/* | head -1); printf XXXX | dd of=$f bs=1 seek=100 conv=notrunc'
curl -s -o downloaded.bin localhost:8080/api/v1/objects/$ID && cmp original.bin downloaded.bin && echo "still correct"
docker compose logs controller | grep "corrupted replica"
curl -s localhost:8080/internal/objects/$ID/replicas        # that copy now shows CORRUPTED
```

Replicas are read in random order, so you may need a few downloads before the damaged copy is picked. The download stays correct either way.

## How the cluster works

### Workers join on their own

The controller has no list of workers. Each worker knows `CONTROLLER_URL`. At startup, and then every `ANNOUNCE_INTERVAL` (30 s), it calls `POST /internal/workers/register` with `{"worker_id": "...", "port": 8080}`:

1. **First time:** `worker_id` is empty. The controller assigns one (`w-<16 hex>`), and the worker stores it in its own database and sends it on every later call.
2. **Address:** the controller takes the worker's address from the request's source IP (its container IP on the Docker network) plus the port. It calls each container directly, so Docker's service-name load balancing never applies.
3. **Chunk report:** for a worker it doesn't know yet, the controller answers right away with status `AVAILABLE`. It then pulls the worker's chunk list from `GET /internals/chunks/metadata` in the background. Once that is loaded, the worker becomes `READY` and starts receiving new chunks.
4. **Re-announcing:** for a known worker at the same address, an announcement only updates `last_seen`. The repeated call lets a restarted controller rebuild its view within one interval. Failure detection will use `last_seen` later.

`DELETE /internal/workers/{worker_id}` removes a worker that is leaving voluntarily. The controller forgets it and its copies and refuses the ID afterwards. The worker logs this and stops announcing itself. Stopping or scaling down a container does **not** deregister it.

### What the controller remembers

| Where | What | Why |
|---|---|---|
| SQLite (`DATA_DIR/metadata.db`) | `objects` (ID, size, checksum, chunk count, creation time) and `object_chunks` (object ID, sequence number, size, checksum) | These facts don't change while the object exists. The checksums let the controller verify any copy. |
| Memory: worker table | worker ID → address, status (`AVAILABLE`/`READY`), chunk count, last seen | Addresses change when containers restart. Rebuilt from registrations. |
| Memory: chunk table | chunk ID → object ID, sequence number, worker ID, status | Copies can be lost or corrupted, so where they live is runtime state. Rebuilt from the workers' chunk reports. |

Copy statuses in the chunk table:

| Status | Meaning |
|---|---|
| `PENDING` | An upload is writing it. |
| `ACTIVE` | Committed and readable. |
| `CORRUPTED` | Failed the checksum check on a read; no longer served. |
| `ORPHAN` | On a worker, but no object owns it, for example after a failed delete. |

Background jobs planned for later phases (lost-copy recovery, corruption repair, orphan cleanup, rebalancing) will watch these two tables and react to status changes.

Every copy has its own random chunk ID (32 hex characters, like object IDs). So the two copies of a chunk never share an ID, and the controller can track each one separately.

### Placement

For each chunk, the controller picks the 2 `READY` workers that hold the fewest chunks, breaking ties at random, and counts the copies against them immediately. Consecutive chunks therefore rotate across the workers, and chunk counts stay within one of each other. A newly added worker starts empty and is preferred until it catches up. Existing chunks are not moved. If fewer than 2 workers are `READY`, the upload is rejected with `503 insufficient_workers`.

### Upload, download, delete

- **Upload:** the controller reads the body into one reusable 1 MiB buffer. It sends each chunk to its 2 workers in parallel, then reads the next chunk, so memory use doesn't grow with object size. Metadata is committed in one SQLite transaction after every copy is stored, and then the copies become `ACTIVE`. If anything fails, all copies already sent are deleted and no object is created.
- **Download:** for each chunk, the controller tries the `ACTIVE` copies in random order. Each copy is checked against the stored size and SHA-256. A copy that fails is marked `CORRUPTED` and the next copy is tried, so damaged data is never returned while a good copy exists. Chunks are fetched and written one at a time. Workers keep **no** checksums; all verification is done by the controller.
- **Delete:** the metadata is deleted first, so the object disappears for clients at once. The copies are then deleted from the workers. A copy that can't be deleted is marked `ORPHAN`, and the delete still succeeds.

### Known limitations in this phase

- Stopping or scaling down workers can leave objects with fewer than 2 copies. Nothing restores them yet.
- A stopped worker stays listed as `READY` until failure detection exists. Downloads still work from the other copy: connecting to the stopped worker times out after 1 s, and that worker is tried last for the rest of the download. New uploads may still pick it and then fail with `503 worker_unavailable`.
- Right after a controller restart, downloads return `503` until the workers re-announce, which takes up to 30 s.
- Copies are marked `CORRUPTED` only in memory. After a controller restart, a damaged copy is detected again on its next read.

## Controller HTTP API

### Client API

| Method | Endpoint | Success | Notes |
|---|---|---|---|
| `PUT` | `/api/v1/objects` | `201` + `ObjectInfo` JSON | The request body is the raw object. The controller generates the object ID. |
| `GET` | `/api/v1/objects` | `200` + JSON array of `ObjectInfo` | Every stored object, oldest first. `[]` when empty. |
| `GET` | `/api/v1/objects/{object_id}` | `200` + raw bytes | Sends `Content-Length` and `ETag` (the object's SHA-256). `HEAD` returns the headers only. |
| `DELETE` | `/api/v1/objects/{object_id}` | `204` | Removes the object and its copies. |
| `GET` | `/healthz` | `200` | Used by the Docker health check. |

```json
{"object_id": "5f2c...", "size": 10485760, "checksum": "<sha256>", "chunk_count": 10, "created_at": "2026-11-21T10:00:00Z"}
```

| Status | Code | When |
|---|---|---|
| 400 | `invalid_object_id` | The ID is not 32 lowercase hex characters |
| 400 | `upload_incomplete` | The client stopped sending before the upload finished; nothing is stored |
| 404 | `object_not_found` | No object has this ID |
| 405 | `method_not_allowed` | The endpoint doesn't support this method |
| 500 | `object_corrupted` | Every copy of some chunk is missing or failed its checksum |
| 500 | `internal_error` | Any other controller failure; details are logged |
| 503 | `insufficient_workers` | Fewer than 2 workers are `READY`, so an upload can't be stored |
| 503 | `object_unavailable` | No copy of some chunk can be reached right now |
| 503 | `worker_unavailable` | A worker failed while storing an upload; nothing is stored |

If a chunk fails after a download has started, the `200` has already been sent, so the controller aborts the connection. The client then receives fewer bytes than `Content-Length` and sees an error (curl exits with code 18).

### Internal API

| Method | Endpoint | Success | Notes |
|---|---|---|---|
| `POST` | `/internal/workers/register` | `200` + `{"worker_id","status","announce_interval_seconds"}` | Called by workers. `400 invalid_request` for a bad body; `410 worker_retired` for a deregistered ID. |
| `DELETE` | `/internal/workers/{worker_id}` | `204` | Voluntary deregistration. `404 worker_not_found` if unknown. |
| `GET` | `/internal/workers` | `200` + JSON array | `worker_id`, `addr`, `status`, `chunk_count`, `last_seen` for each worker. |
| `GET` | `/internal/objects/{object_id}/replicas` | `200` + JSON | The object's `ObjectInfo` plus `replicas`: `chunk_id`, `sequence_no`, `worker_id`, `worker_addr`, `status` for every copy. |

These endpoints are served on the same port as the client API, which is fine for a local deployment.

### Controller configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | HTTP listen port |
| `DATA_DIR` | `/data` | Directory for `metadata.db` (created if missing) |
| `REPLICATION_FACTOR` | `2` | Copies of each chunk, each on a different worker |
| `WORKER_TIMEOUT` | `30s` | Time limit for each request to a worker, and for loading a worker's chunk list |
| `ANNOUNCE_INTERVAL` | `30s` | Announce interval reported to workers in the register response |

## Worker

### Storage

With `DATA_ROOT` set (as in `docker-compose.yml`), all workers share one volume. At startup each worker claims the lowest free `slot-<n>` directory by holding an exclusive lock on its `.lock` file. The kernel releases the lock when the worker exits, so a restarted or newly scaled worker picks up a free slot together with the chunks and worker ID stored in it. Without `DATA_ROOT`, the worker uses `DATA_DIR` directly.

```text
DATA_ROOT/slot-<n>/        (or DATA_DIR/)
├── .lock         held while a worker uses this slot
├── metadata.db   SQLite: chunks (chunk_id, object_id, sequence_no, file_path, size, created_at)
│                 and worker_identity (the controller-assigned worker ID)
├── chunks/       chunk data, one file per chunk, named <chunk_id>.<random>
└── tmp/          in-progress writes
```

A PUT writes the data to `tmp/`, fsyncs it, renames it into `chunks/`, and only then commits the metadata row and returns `201` (see [docs/upload_sequence.mmd](docs/upload_sequence.mmd)). An overwrite always writes a new file and removes the old one after the row commits, so a failed or interrupted PUT never damages the previous version. On startup the worker empties `tmp/`, deletes chunk files that no row references, and drops rows whose file is missing. After that, `GET /internals/chunks/metadata` lists only chunks the worker can actually serve.

The worker stores **no checksums**. It checks `X-Chunk-Checksum` on PUT to catch data damaged in transit, but returns stored bytes as they are, so the controller can detect and handle a corrupted copy.

### Worker configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | HTTP listen port; also sent to the controller when registering |
| `CONTROLLER_URL` | `http://controller:8080` | Where to register |
| `ANNOUNCE_INTERVAL` | `30s` | Time between re-registrations |
| `DATA_ROOT` | (empty) | Shared volume root; enables slot mode (see above) |
| `DATA_DIR` | `/data` | Data directory when `DATA_ROOT` is empty |
| `MAX_CHUNK_BYTES` | `1048576` | Largest accepted chunk (1 MiB) |

The worker ID is not configured. The controller assigns it at the first registration.

### Worker HTTP API

Endpoints and DTOs follow [docs/H-Design-Worker.md](docs/H-Design-Worker.md).

| Method | Endpoint | Success | Notes |
|---|---|---|---|
| `PUT` | `/internals/chunks/{chunk_id}` | `201` + `ChunkMetadata` JSON | Body is a `Chunk` JSON object; `data` is at most 1 MiB once decoded. Optional `X-Chunk-Checksum` request header (SHA-256 of the decoded data); a mismatch returns `400`. The response's `X-Chunk-Checksum` is the digest of what was received; it is not stored. Re-PUT overwrites. |
| `GET` | `/internals/chunks/{chunk_id}` | `200` + `Chunk` JSON | Header: `X-Chunk-Size`. Data is returned as stored, unverified. |
| `DELETE` | `/internals/chunks/{chunk_id}` | `204` | Also `204` if the chunk doesn't exist (idempotent). |
| `GET` | `/internals/chunks/metadata` | `200` + JSON array of `ChunkMetadata` | `[]` when empty. The controller pulls this after a worker registers. |
| `GET` | `/healthz` | `200` | `{"status":"ok","worker_id":"..."}` (`worker_id` is empty until the first registration). `503` if the SQLite database doesn't answer a ping. |

DTOs (`pkg/protocol`):

```jsonc
// Chunk: PUT request body and GET response body. data is base64.
{"object_id": "obj1", "sequence_no": 0, "data": "aGVsbG8="}
// ChunkMetadata: PUT response, and each element of GET /internals/chunks/metadata.
{"chunk_id": "c1", "object_id": "obj1", "sequence_no": 0}
```

Chunk IDs and object IDs must match `^[A-Za-z0-9_-]{1,128}$`, and `metadata` is reserved (it can't be a chunk ID). `sequence_no` is required and must be `>= 0`. Checksums are lowercase SHA-256 hex.

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
| 500 | `storage_error` | Any other storage failure; details are logged, not returned |
| 500 | `internal_error` | A panic was recovered |
| 503 | `storage_error` | Health check: the database doesn't answer |

### Talking to one worker directly (bash)

Workers publish their port on a host port from the range 8081-8099. Find a worker's port with `docker compose port --index 1 worker 8080`.

```sh
W=http://$(docker compose port --index 1 worker 8080)
head -c 1048576 /dev/urandom > chunk.bin
SUM=$(sha256sum chunk.bin | cut -d' ' -f1)
printf '{"object_id":"obj1","sequence_no":0,"data":"%s"}' "$(base64 -w0 chunk.bin)" > put.json

curl -s -X PUT -H "Content-Type: application/json" -H "X-Chunk-Checksum: $SUM" \
     --data-binary @put.json $W/internals/chunks/c1
curl -s $W/internals/chunks/c1 | jq -r .data | base64 -d | cmp - chunk.bin && echo "bytes match"
curl -s $W/internals/chunks/metadata
curl -s -o /dev/null -w "%{http_code}\n" -X DELETE $W/internals/chunks/c1   # 204
```

The `cmp` check needs `jq`. Chunks written this way don't belong to any object, so the controller marks them `ORPHAN` the next time it loads this worker's chunk list. On Windows, use `curl.exe` in PowerShell (plain `curl` is an alias for `Invoke-WebRequest`).

## Layout

```text
pkg/protocol/                   shared wire types, header names, error codes
worker/cmd/worker/              worker entry point
worker/internal/api/            HTTP routes, handlers, middleware
worker/internal/storage/        Storage interface, filesystem + SQLite store, worker identity
worker/internal/registration/   registers with the controller and re-announces
worker/internal/slot/           claims a slot directory on a shared volume
worker/internal/config/         environment-based configuration
controller/cmd/controller/      controller entry point
controller/internal/api/        client and internal routes and handlers
controller/internal/object/     upload, download, list, delete orchestration
controller/internal/cluster/    in-memory worker and chunk tables, placement, membership
controller/internal/chunk/      splits a stream into 1 MiB chunks
controller/internal/metadata/   SQLite object and chunk metadata
controller/internal/workers/    HTTP client for the worker API
controller/internal/config/     environment-based configuration
docker-compose.yml
```

## Requirements

- Go 1.26.5 or newer
- Docker with Docker Compose v2

## Build, test, run

The worker and the controller are separate Go modules, so run Go commands from their directories:

```sh
cd controller && go vet ./... && go test -race ./...
cd ../worker  && go vet ./... && go test -race ./...
```

Run locally without Docker (two terminals plus one per worker). Use at least 2 workers so uploads can be stored:

```sh
cd controller && PORT=8080 DATA_DIR=./data go run ./cmd/controller
cd worker && PORT=8081 DATA_DIR=./data1 CONTROLLER_URL=http://localhost:8080 go run ./cmd/worker
cd worker && PORT=8082 DATA_DIR=./data2 CONTROLLER_URL=http://localhost:8080 go run ./cmd/worker
```

Docker:

```sh
docker compose up --build -d             # controller on :8080, 4 workers
docker compose ps                        # should show (healthy)
docker compose logs -f controller worker
docker compose up -d --scale worker=N    # change the number of workers
docker compose down                      # add -v to also delete the data volumes
```
