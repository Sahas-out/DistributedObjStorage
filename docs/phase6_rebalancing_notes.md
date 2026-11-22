# Phase 6 (planned): rebalancing existing chunks

Deferred from phase 5 by decision. These notes keep the context.

## How placement works today (end of phase 5)

- `cluster.LeastLoaded.Choose` (`controller/internal/cluster/placement.go`) shuffles the `READY` workers, stable-sorts them by `ChunkCount`, and takes the first RF.
  - `ChunkCount` counts copies of any status.
  - The shuffle breaks ties.
- `Registry.Place` calls it under the registry lock, once per chunk, inside the upload request. The copies count against the workers immediately. No goroutine is involved.
- The recovery job (`controller/internal/recovery`) uses the same policy for new copies, through `Registry.PlaceReplica(obj, seq, exclude, newID)`.
- **Existing chunks never move.** A newly joined worker starts at 0 chunks, so it receives one of the 2 copies of every new chunk until it catches up. Until then it is a hotspot for writes, and for reads of new objects. If no new uploads arrive, it stays empty.

## Idea

Add a rebalance step to the existing recovery loop. Don't add a new goroutine; phase 5 agreed on a single one.

1. After the repair pass, compute each `READY` worker's `ChunkCount` and the mean.
2. While max − min > threshold (e.g. > 1, or > 10 % of the mean), pick a copy on the most-loaded worker of a chunk that is fully healthy, i.e. has RF good copies.
3. Move it, with copy first and delete last, so no chunk ever drops below RF:
   - `PlaceReplica` on the least-loaded worker, excluding the current holders
   - GET from the source and verify with `workers.VerifyChunk`
   - PUT the copy and `SetStatusIf(PENDING → ACTIVE)`
   - `DeleteChunk` on the old worker and `Remove` its entry

   The existing trim rule would also delete the extra copy, but it picks the most-loaded worker, so prefer doing the delete explicitly.
4. Limits:
   - Move at most N chunks per tick, so client traffic isn't starved. This is the one place a throttle makes sense.
   - Never move a copy of a degraded or unavailable chunk.
   - Skip the step entirely while any worker is `AVAILABLE` (syncing), or during the startup grace period.

## Open questions for phase 6

- Should the threshold be configurable (`REBALANCE_THRESHOLD`), and should rebalancing be on by default?
- Should load be counted in chunks or in stored bytes? The last chunk of an object is smaller.
- Should a new worker be filled eagerly, or only when the spread exceeds the threshold?
