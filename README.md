# Distributed Object Store

A mini distributed cloud object store: one controller, four storage workers, and clients. Objects are split into 1 MiB chunks and each chunk is stored on two workers. See [docs/Mini_Distributed_Cloud_Object_Store.pdf](docs/Mini_Distributed_Cloud_Object_Store.pdf) for the project statement.

**Current status: Phase 7.** Workers register themselves with the controller, and the number of workers can be scaled with one command. Every chunk is stored on 2 different workers (replication factor 2), and chunks are spread evenly across workers. Clients can upload, download, list and delete objects. A copy that fails its checksum is detected, marked, and skipped in favour of the other copy. Each object has a name, a content type, creation and last-modified times, and custom key/value metadata, which can be read and changed without re-uploading the data.

The controller also detects workers that stop sending heartbeats. A background recovery job restores lost copies, repairs corrupted ones, removes extra and orphan copies, and keeps an object whose only remaining copy sits on a dead worker, in case that worker comes back. Rebalancing existing chunks onto new workers comes later.

## Quick start: try the whole flow

```sh
docker compose up --build -d              # controller on :8080 + 4 workers
docker compose ps                         # all should show (healthy)
curl -s localhost:8080/internal/workers   # 4 workers, status READY

head -c 10485760 /dev/urandom > original.bin      # a 10 MiB test file

# Upload (multipart/form-data) with custom metadata. The response contains the object_id.
curl -s -F file=@original.bin -F 'metadata={"owner":"alice","project":"demo"}' localhost:8080/api/v1/objects
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

### Demo: metadata

```sh
# System metadata, custom metadata and where every copy of every chunk is stored.
curl -s localhost:8080/api/v1/objects/$ID/metadata

# Rename, change the content type, add a key and remove one (null). No data is re-uploaded;
# last_modified_at changes, checksum and chunks don't.
curl -s -X PATCH -H 'Content-Type: application/json' \
  -d '{"name":"renamed.bin","content_type":"application/x-test","custom_metadata":{"project":null,"reviewed":"yes"}}' \
  localhost:8080/api/v1/objects/$ID/metadata

curl -sI localhost:8080/api/v1/objects/$ID   # Content-Type and filename follow the new metadata
```

The schema changed in phase 7. If you have a `controller-data` volume from an earlier phase, remove it with `docker compose down -v` first.

### Demo: corrupted copy

```sh
docker compose exec --index 2 worker sh -c \
  'f=$(ls /data/slot-*/chunks/* | head -1); printf XXXX | dd of=$f bs=1 seek=100 conv=notrunc'
curl -s -o downloaded.bin localhost:8080/api/v1/objects/$ID && cmp original.bin downloaded.bin && echo "still correct"
docker compose logs controller | grep "corrupted replica"
curl -s localhost:8080/internal/objects/$ID/replicas        # that copy now shows CORRUPTED
```

Replicas are read in random order, so you may need a few downloads before the damaged copy is picked. The download stays correct either way. Within one recovery tick (5 s) the copy is rewritten in place from the good copy and shows `ACTIVE` again:

```sh
docker compose logs controller | grep "repaired copy in place"
curl -s localhost:8080/internal/objects/$ID/replicas        # back to ACTIVE, same chunk_id, same worker
```

If **every** copy of a chunk is corrupted (repeat the `dd` on both workers that hold it), the object can't be restored. Recovery deletes it and logs `object deleted`, and `GET` returns `404`.

### Demo: worker failure and recovery

```sh
curl -s localhost:8080/internal/workers               # 4 READY workers
docker ps --format '{{.Names}}' | grep worker        # e.g. distributedobjectstorage-worker-1
docker stop distributedobjectstorage-worker-1         # stop one worker that holds copies
sleep 35                                              # 3 missed heartbeats (10 s each)
curl -s localhost:8080/internal/workers               # that worker is now DEAD
curl -s localhost:8080/internal/objects/$ID/replicas  # still 2 ACTIVE copies of every chunk, on the other workers
docker compose logs controller | grep -E "declared dead|restored a copy"
curl -s -o downloaded.bin localhost:8080/api/v1/objects/$ID && cmp original.bin downloaded.bin && echo "bytes match"

docker start distributedobjectstorage-worker-1        # it comes back with its old chunks: 3 copies...
sleep 10
docker compose logs controller | grep "removed extra copy"   # ...trimmed back to 2
```

If you stop **both** workers that hold a chunk, that chunk has no readable copy. Downloads answer `503 object_unavailable`, but the object is **kept**. The dashboard shows "unavailable", and the copies are marked `LOST`. When either worker starts again, its copy comes back and the chunk is re-replicated.

## How the cluster works

### Workers join on their own

The controller has no list of workers. Each worker knows `CONTROLLER_URL`. At startup, and then every `ANNOUNCE_INTERVAL` (30 s), it calls `POST /internal/workers/register` with `{"worker_id": "...", "port": 8080}`:

1. **First time:** `worker_id` is empty. The controller assigns one (`w-<16 hex>`), and the worker keeps it in memory and sends it on every later call. The ID lasts only as long as the worker process: a restarted worker registers again and gets a new ID. If another worker entry still has the same address, that entry belongs to the dead process, so the controller replaces it. If the restarted worker reports chunks that are still listed under its old entry, those chunks move to the new ID, because the files are on its disk.
2. **Address:** the controller takes the worker's address from the request's source IP (its container IP on the Docker network) plus the port. It calls each container directly, so Docker's service-name load balancing never applies.
3. **Chunk report:** for a worker it doesn't know yet, the controller answers right away with status `AVAILABLE`. It then pulls the worker's chunk list from `GET /internals/chunks/metadata` in the background. Once that is loaded, the worker becomes `READY` and starts receiving new chunks.
4. **Re-announcing:** for a known worker at the same address, an announcement only updates `last_seen`. This is the worker's heartbeat. The register response carries `announce_interval_seconds`, and the worker adopts it, so the controller and the worker agree on the interval.
5. **Failure:** a worker that hasn't announced for `WORKER_DEAD_AFTER` (30 s in compose) is declared `DEAD`. If it announces again with the same ID (it was only unreachable), it goes back to `AVAILABLE` and its chunk list is reloaded. A restarted worker gets a new ID as usual and reports the chunks on its disk.

`DELETE /internal/workers/{worker_id}` removes a worker that is leaving voluntarily. The controller forgets it and its copies and refuses the ID afterwards. The worker logs this and stops announcing itself. Stopping or scaling down a container does **not** deregister it.

### What the controller remembers

| Where | What | Why |
|---|---|---|
| SQLite (`DATA_DIR/metadata.db`) | `objects` (ID, name, content type, size, checksum, chunk count, creation and last-modified time), `object_chunks` (object ID, sequence number, size, checksum) and `object_custom_metadata` (object ID, key, value) | The data-describing facts never change while the object exists, and the checksums let the controller verify any copy. Name, content type and custom metadata change only through `PATCH`, in one transaction that also sets the last-modified time. Deleting an object deletes its chunk and custom-metadata rows too. |
| Memory: worker table | worker ID → address, status (`AVAILABLE`/`READY`/`DEAD`), chunk count, last seen | IDs and addresses last only as long as a worker process. Built from registrations. |
| Memory: chunk table | chunk ID → object ID, sequence number, worker ID, status | Copies can be lost or corrupted, so where they live is runtime state. Rebuilt from the workers' chunk reports. |

Copy statuses in the chunk table:

| Status | Meaning |
|---|---|
| `PENDING` | An upload is writing it. |
| `ACTIVE` | Committed and readable. |
| `CORRUPTED` | Failed the checksum check on a read; no longer served. Recovery rewrites it. |
| `LOST` | Its worker is `DEAD`, or the worker answered that it doesn't have the file. Recovery restores it. |
| `ORPHAN` | On a worker, but no object owns it, for example after a failed delete or a delete while the worker was down. Recovery deletes it. |

### Failure detection and recovery

One background goroutine in the controller, the recovery job (`controller/internal/recovery`), handles every failure. Events only change a status in the tables above. A missed heartbeat makes a worker `DEAD`, and a failed checksum or a `404` on download marks a copy `CORRUPTED` or `LOST`. On every tick (`RECOVERY_INTERVAL`, 5 s) the job:

1. **Detects failures.** Workers silent for longer than `WORKER_DEAD_AFTER` become `DEAD`, and their `ACTIVE` copies become `LOST`.
2. **Checks every chunk in SQLite** against its copies. A good copy is `ACTIVE` on a `READY` worker, and every copy it reads is verified against the stored SHA-256.

| Situation | Action |
|---|---|
| A `CORRUPTED` or missing copy on a `READY` worker, and a good copy exists | Rewrite it **in place**: same worker, same chunk ID. |
| Fewer good copies than `REPLICATION_FACTOR` | Copy the chunk to the least-loaded `READY` worker that doesn't hold it yet. Then forget the `LOST` entries on dead workers. |
| More good copies than `REPLICATION_FACTOR` (a dead worker came back) | Delete the extra copy from the most-loaded worker. |
| No good copy, but a copy is on a `DEAD` worker | Keep the object and wait (the chunk is "unavailable"). The worker may come back with it. |
| Every copy is corrupted or missing | The chunk can't be restored, so **delete the object**. |

3. **Deletes `ORPHAN` copies** from their workers.

Safety rules:
- A chunk with a `PENDING` copy (an upload in progress) is skipped until the next tick.
- Nothing is deleted while a worker is still loading its chunk list, because it may hold a good copy.
- No repairs run in the first 2 × `ANNOUNCE_INTERVAL` after the controller starts, while the in-memory tables are still filling from announcements.

Every copy has its own random chunk ID (32 hex characters, like object IDs). So the two copies of a chunk never share an ID, and the controller can track each one separately.

### Placement

For each chunk, the controller picks the 2 `READY` workers that hold the fewest chunks, breaking ties at random, and counts the copies against them immediately. Consecutive chunks therefore rotate across the workers, and chunk counts stay within one of each other. A newly added worker starts empty and is preferred until it catches up. Existing chunks are not moved (rebalancing is planned, see [docs/phase6_rebalancing_notes.md](docs/phase6_rebalancing_notes.md)). Placement runs inside the upload request, not in a background goroutine. The recovery job uses the same policy for the new copies it creates. If fewer than 2 workers are `READY`, the upload is rejected with `503 insufficient_workers`.

### Upload, download, delete

- **Upload:** the body is `multipart/form-data`. The `file` part's filename and `Content-Type` become the object's name and content type. If the part's type is missing or `application/octet-stream`, the type is guessed from the file extension. The controller reads the `file` part into one reusable 1 MiB buffer. It sends each chunk to its 2 workers in parallel, then reads the next chunk, so memory use doesn't grow with object size. Metadata is committed in one SQLite transaction after every copy is stored, and then the copies become `ACTIVE`. A `metadata` field may come before or after `file`; one that comes after is read before the commit. If anything fails, including invalid metadata, all copies already sent are deleted and no object is created.
- **Download:** for each chunk, the controller tries the `ACTIVE` copies in random order. Each copy is checked against the stored size and SHA-256. A copy that fails is marked `CORRUPTED` and the next copy is tried, so damaged data is never returned while a good copy exists. Chunks are fetched and written one at a time. Workers keep **no** checksums; all verification is done by the controller.
- **Delete:** the metadata is deleted first, so the object disappears for clients at once. The copies are then deleted from the workers. A copy that can't be deleted is marked `ORPHAN`, and the delete still succeeds.

### Known limitations in this phase

- A stopped worker is only declared `DEAD` after `WORKER_DEAD_AFTER` (30 s). Until then it is still `READY`. Downloads still work from the other copy: connecting to the stopped worker times out after 1 s, and that worker is tried last for the rest of the download. New uploads may still pick it and then fail with `503 worker_unavailable`.
- Corruption is only found when a copy is read, by a download or by the recovery job while it copies a chunk. There is no background scrubbing, so a rarely read chunk can lose both copies unnoticed.
- Orphans are deleted without a grace period. In a rare race, a worker re-syncs in the middle of an upload, and that upload's copy on the worker can be deleted as an orphan. Recovery then restores the copy from the other one.
- The project covers worker failures only. The controller is a single point of failure: its worker and chunk tables are in memory, so after a controller restart they are empty until each worker's next announcement (up to 30 s), and `CORRUPTED` marks are lost.

## Controller HTTP API

### Client API

| Method | Endpoint | Success | Notes |
|---|---|---|---|
| `POST` | `/api/v1/objects` | `201` + `ObjectInfo` JSON | `multipart/form-data`: required `file` field, optional `metadata` field with a JSON object of string values. The controller generates the object ID. |
| `GET` | `/api/v1/objects` | `200` + JSON array of `ObjectInfo` | Every stored object, oldest first. `[]` when empty. |
| `GET` | `/api/v1/objects/{object_id}` | `200` + raw bytes | Sends the stored `Content-Type`, `Content-Disposition: attachment; filename=…`, `Content-Length`, `ETag` (the object's SHA-256) and `Last-Modified`. `HEAD` returns the headers only. |
| `GET` | `/api/v1/objects/{object_id}/metadata` | `200` + `ObjectMetadata` JSON | System metadata, custom metadata, and every chunk with its copies. The workers aren't contacted. |
| `PATCH` | `/api/v1/objects/{object_id}/metadata` | `200` + `ObjectMetadata` JSON | JSON body, all fields optional: `name`, `content_type`, `custom_metadata` (each key is added or replaced; `null` removes it). Any change sets `last_modified_at`. |
| `DELETE` | `/api/v1/objects/{object_id}` | `204` | Removes the object and its copies. |
| `GET` | `/healthz` | `200` | Used by the Docker health check. |

`ObjectInfo`:

```json
{"object_id": "5f2c...", "name": "original.bin", "content_type": "application/octet-stream", "size": 10485760,
 "checksum": "<sha256>", "chunk_count": 10, "created_at": "2026-11-21T10:00:00Z", "last_modified_at": "2026-11-21T10:00:00Z"}
```

`ObjectMetadata` is `ObjectInfo` plus:

```json
{"custom_metadata": {"owner": "alice"},
 "chunks": [{"sequence_no": 0, "size": 1048576, "checksum": "<sha256>",
             "replicas": [{"chunk_id": "9a1e...", "worker_id": "w-1", "worker_addr": "http://172.18.0.3:8080", "status": "ACTIVE"}, ...]}, ...]}
```

Metadata limits: name 1–255 bytes; content type must be a valid media type such as `image/png`; at most 64 custom keys per object; keys are 1–128 characters of `A-Z a-z 0-9 . _ -`; values are strings of up to 1024 bytes. An upload's `metadata` field is limited to 16 KiB, and a `PATCH` body to 64 KiB.

| Status | Code | When |
|---|---|---|
| 400 | `invalid_object_id` | The ID is not 32 lowercase hex characters |
| 400 | `invalid_request` | The upload isn't multipart or has no `file` field, a `metadata` field isn't a JSON object of strings, or a `PATCH` body isn't valid JSON (unknown field, non-string value, `null` name or content type) |
| 400 | `invalid_metadata` | A name, content type, key or value breaks the limits above; nothing is stored or changed |
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
| `ANNOUNCE_INTERVAL` | `30s` (compose: `10s`) | Announce interval reported to workers in the register response; workers adopt it |
| `WORKER_DEAD_AFTER` | 3 × `ANNOUNCE_INTERVAL` (compose: `30s`) | A worker silent this long is declared `DEAD`; must be longer than `ANNOUNCE_INTERVAL` |
| `RECOVERY_INTERVAL` | `5s` | Time between runs of the recovery job |

## Worker

### Storage

With `DATA_ROOT` set (as in `docker-compose.yml`), all workers share one volume. At startup each worker claims the lowest free `slot-<n>` directory by holding an exclusive lock on its `.lock` file. The kernel releases the lock when the worker exits, so a restarted or newly scaled worker picks up a free slot together with the chunks stored in it. Without `DATA_ROOT`, the worker uses `DATA_DIR` directly.

```text
DATA_ROOT/slot-<n>/        (or DATA_DIR/)
├── .lock         held while a worker uses this slot
├── metadata.db   SQLite: chunks (chunk_id, object_id, sequence_no, file_path, size, created_at)
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
| `ANNOUNCE_INTERVAL` | `30s` (compose: `10s`) | Time between re-registrations until the controller's first answer, whose interval then replaces it |
| `DATA_ROOT` | (empty) | Shared volume root; enables slot mode (see above) |
| `DATA_DIR` | `/data` | Data directory when `DATA_ROOT` is empty |
| `MAX_CHUNK_BYTES` | `1048576` | Largest accepted chunk (1 MiB) |

The worker ID is not configured or stored. The controller assigns one each time the worker process starts and registers.

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
| 404 | `chunk_not_found` | GET of a missing chunk, or of one whose data file has disappeared from disk |
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
worker/internal/storage/        Storage interface, filesystem + SQLite store
worker/internal/registration/   registers with the controller and re-announces
worker/internal/slot/           claims a slot directory on a shared volume
worker/internal/config/         environment-based configuration
controller/cmd/controller/      controller entry point
controller/internal/api/        client and internal routes and handlers
controller/internal/object/     upload, download, list, delete orchestration
controller/internal/cluster/    in-memory worker and chunk tables, placement, membership
controller/internal/recovery/   background job: failure detection, lost/corrupted/extra/orphan copies
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
