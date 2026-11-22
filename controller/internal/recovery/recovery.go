// Package recovery runs the controller's single background job for failure
// handling. On every tick it:
//
//  1. declares DEAD the workers that stopped announcing themselves, which
//     marks their ACTIVE copies LOST;
//  2. compares every chunk in the metadata with the copies in the chunk table
//     and repairs what it finds:
//     - a CORRUPTED or missing (LOST) copy on a READY worker is overwritten
//     in place with data from a good copy;
//     - a chunk with fewer good copies than the replication factor gets new
//     copies on other workers;
//     - a chunk with more good copies than the replication factor (a dead
//     worker came back) loses the extra ones;
//     - a chunk with no good copy but one on a dead worker is left alone:
//     the object is kept so the copy can come back with the worker;
//     - a chunk whose every copy is corrupted or missing can't be restored,
//     so its object is deleted;
//  3. deletes ORPHAN copies, which belong to no object.
//
// Events elsewhere (a missed heartbeat, a failed checksum on download) only
// change a status in the registry; this job is what reacts to them.
package recovery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/cluster"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/events"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// deadWorkerRetention is how long a DEAD worker with no copies left stays
// listed (for the dashboard) before it is forgotten.
const deadWorkerRetention = 10 * time.Minute

// ChunkStore is the part of the worker API recovery needs (workers.Client).
type ChunkStore interface {
	PutChunk(ctx context.Context, addr, chunkID, objectID string, seq int, data []byte, checksum string) (protocol.ChunkMetadata, error)
	GetChunk(ctx context.Context, addr, chunkID string) (protocol.Chunk, error)
	DeleteChunk(ctx context.Context, addr, chunkID string) error
}

// Metadata is the part of the metadata store recovery needs.
type Metadata interface {
	AllChunks(ctx context.Context) ([]metadata.ChunkRef, error)
	HasChunk(ctx context.Context, objectID string, seq int) (bool, error)
}

// ObjectDeleter deletes an object and its copies (object.Service).
type ObjectDeleter interface {
	Delete(ctx context.Context, objectID string) error
}

// Config tunes the recovery job.
type Config struct {
	ReplicationFactor int
	// Interval is the time between ticks.
	Interval time.Duration
	// DeadAfter is how long a worker may go without announcing before it is
	// declared DEAD.
	DeadAfter time.Duration
	// StartupGrace is how long after the controller starts repairs wait.
	// The chunk table is in memory, so right after a restart it only fills
	// as workers announce themselves; repairing earlier would copy chunks
	// whose other copies simply haven't been reported yet. Failure detection
	// runs from the start.
	StartupGrace time.Duration
}

// Recovery is the background failure-handling job.
type Recovery struct {
	reg     *cluster.Registry
	chunks  ChunkStore
	meta    Metadata
	objects ObjectDeleter
	cfg     Config
	log     *slog.Logger
	events  *events.Log
	now     func() time.Time
	newID   func() (string, error)
	started time.Time

	// unavailable holds the chunks reported unavailable, so the event is
	// recorded once rather than on every tick. Only the Run goroutine uses it.
	unavailable map[cluster.ChunkKey]bool

	mu     sync.Mutex
	status protocol.RecoveryStatus
}

// New builds the recovery job. A nil logger discards logs.
func New(reg *cluster.Registry, chunks ChunkStore, meta Metadata, objects ObjectDeleter, cfg Config, log *slog.Logger) *Recovery {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Recovery{
		reg: reg, chunks: chunks, meta: meta, objects: objects, cfg: cfg, log: log,
		now: time.Now, newID: newID, started: time.Now(),
		unavailable: map[cluster.ChunkKey]bool{},
	}
}

// SetEvents makes recovery actions appear in the dashboard activity feed.
// Call it before Run.
func (r *Recovery) SetEvents(ev *events.Log) { r.events = ev }

// Status returns the result of the latest scan.
func (r *Recovery) Status() protocol.RecoveryStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

// Run ticks every Interval until ctx is cancelled.
func (r *Recovery) Run(ctx context.Context) {
	r.log.Info("recovery job started", "interval", r.cfg.Interval.String(),
		"dead_after", r.cfg.DeadAfter.String(), "startup_grace", r.cfg.StartupGrace.String())
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.tick(ctx)
		}
	}
}

// chunkState is what a scan concluded about one chunk.
type chunkState int

const (
	stateHealthy     chunkState = iota
	stateDegraded               // fewer good copies than the replication factor
	stateUnavailable            // no good copy; one may come back
	stateLost                   // every copy is corrupted or missing
	stateBusy                   // a copy is being written; looked at next tick
)

func (r *Recovery) tick(ctx context.Context) {
	now := r.now()
	r.detectFailures(now)
	if now.Sub(r.started) < r.cfg.StartupGrace {
		return
	}

	refs, err := r.meta.AllChunks(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Error("recovery: could not list chunks", "error", err)
		}
		return
	}
	view := r.reg.View()
	load := map[string]int{}
	for _, w := range r.reg.Workers() {
		load[w.ID] = w.ChunkCount
	}

	st := protocol.RecoveryStatus{LastScan: now.UTC()}
	unavailable := map[cluster.ChunkKey]bool{}
	lost := map[string]int{} // object ID -> a sequence number that is lost
	for _, ref := range refs {
		if ctx.Err() != nil {
			return
		}
		k := cluster.ChunkKey{ObjectID: ref.ObjectID, SeqNo: ref.SequenceNo}
		switch r.checkChunk(ctx, ref, view.Replicas[k], view.Syncing, load) {
		case stateDegraded:
			st.DegradedChunks++
		case stateUnavailable:
			st.UnavailableChunks++
			unavailable[k] = true
			if !r.unavailable[k] {
				r.log.Warn("chunk has no readable copy; keeping the object until a worker holding it returns",
					"object_id", ref.ObjectID, "sequence_no", ref.SequenceNo)
				r.events.Record(protocol.EventChunkUnavailable, events.Error,
					fmt.Sprintf("chunk %d has no readable copy; waiting for a dead worker to return", ref.SequenceNo), ref.ObjectID, "")
			}
		case stateLost:
			if _, seen := lost[ref.ObjectID]; !seen {
				lost[ref.ObjectID] = ref.SequenceNo
			}
		}
	}
	r.unavailable = unavailable

	for id, seq := range lost {
		r.deleteLostObject(ctx, id, seq)
	}
	st.OrphanReplicas = r.removeOrphans(ctx, view)
	for _, id := range r.reg.PruneDead(now.Add(-deadWorkerRetention)) {
		r.log.Info("forgot dead worker with no copies left", "worker_id", id)
	}

	r.mu.Lock()
	r.status = st
	r.mu.Unlock()
}

// detectFailures declares DEAD the workers that missed their heartbeats.
func (r *Recovery) detectFailures(now time.Time) {
	for _, d := range r.reg.MarkDead(now.Add(-r.cfg.DeadAfter)) {
		r.log.Warn("worker missed its heartbeats; declared dead",
			"worker_id", d.ID, "last_seen", d.LastSeen, "copies_lost", d.Lost)
		r.events.Record(protocol.EventWorkerDead, events.Error,
			fmt.Sprintf("no heartbeat for %s; declared DEAD, %d copies marked LOST",
				now.Sub(d.LastSeen).Round(time.Second), d.Lost), "", d.ID)
	}
}

// checkChunk classifies one chunk's copies and repairs what it can.
func (r *Recovery) checkChunk(ctx context.Context, ref metadata.ChunkRef, locs []cluster.Location, syncing bool, load map[string]int) chunkState {
	var good, repair, mayReturn, gone []cluster.Location
	for _, l := range locs {
		ready := l.WorkerStatus == cluster.WorkerReady
		switch {
		case l.Status == cluster.ReplicaPending:
			return stateBusy // an upload or an earlier repair is writing it
		case l.Status == cluster.ReplicaOrphan:
			// removeOrphans deals with it.
		case ready && l.Status == cluster.ReplicaActive:
			good = append(good, l)
		case ready: // CORRUPTED, or LOST because the worker didn't have it
			repair = append(repair, l)
		case l.Status == cluster.ReplicaActive || l.Status == cluster.ReplicaLost:
			mayReturn = append(mayReturn, l) // on a dead worker
		default:
			gone = append(gone, l) // corrupted, on a dead worker
		}
	}

	if len(good) == 0 {
		// While a worker is still reporting its chunks it may hold a good
		// copy, so nothing is declared lost until it has.
		if len(mayReturn) > 0 || len(repair)+len(gone) == 0 || syncing {
			return stateUnavailable
		}
		return stateLost
	}

	data, good := r.readGood(ctx, ref, good)
	if data == nil {
		return stateDegraded // no good copy could be read now; next tick
	}

	holders := map[string]bool{}
	for _, l := range locs {
		holders[l.WorkerID] = true
	}
	for _, l := range repair {
		if r.rewrite(ctx, ref, l, data) {
			good = append(good, l)
		}
	}
	for len(good) < r.cfg.ReplicationFactor {
		loc, err := r.reg.PlaceReplica(ref.ObjectID, ref.SequenceNo, holders, r.newID)
		if err != nil {
			r.log.Debug("recovery: no worker for another copy", "object_id", ref.ObjectID, "sequence_no", ref.SequenceNo, "error", err)
			break
		}
		holders[loc.WorkerID] = true
		if r.copyTo(ctx, ref, loc, data) {
			good = append(good, loc)
		}
	}
	if len(good) > r.cfg.ReplicationFactor {
		good = r.trim(ctx, ref, good, load)
	}
	if len(good) < r.cfg.ReplicationFactor {
		return stateDegraded
	}
	// Fully replicated again: copies left on dead workers are no longer
	// needed. If such a worker comes back, its copies return as extras and
	// are trimmed.
	for _, l := range append(mayReturn, gone...) {
		r.reg.Remove(l.ChunkID)
	}
	return stateHealthy
}

// readGood reads one good copy, verifying it, and returns its data with the
// copies still believed good. Copies that turn out corrupted or missing are
// marked so and dropped from the list; unreachable ones are kept (if their
// worker is down it will be declared dead soon). data is nil if no copy could
// be read.
func (r *Recovery) readGood(ctx context.Context, ref metadata.ChunkRef, good []cluster.Location) ([]byte, []cluster.Location) {
	var data []byte
	kept := good[:0:0]
	for _, l := range good {
		if data != nil {
			kept = append(kept, l)
			continue
		}
		c, err := r.chunks.GetChunk(ctx, l.Addr, l.ChunkID)
		if err == nil {
			err = workers.VerifyChunk(c, l.ChunkID, ref.ObjectID, ref.SequenceNo, ref.Size, ref.Checksum)
		}
		switch {
		case err == nil:
			data = c.Data
			kept = append(kept, l)
		case errors.Is(err, workers.ErrChunkCorrupted):
			r.reg.SetStatusIf(l.ChunkID, cluster.ReplicaActive, cluster.ReplicaCorrupted)
			r.log.Warn("recovery: copy failed its checksum; marked corrupted",
				"object_id", ref.ObjectID, "sequence_no", ref.SequenceNo, "chunk_id", l.ChunkID, "worker_id", l.WorkerID)
			r.events.Record(protocol.EventReplicaCorrupted, events.Error,
				fmt.Sprintf("corrupted copy of chunk %d found during recovery; marked CORRUPTED", ref.SequenceNo), ref.ObjectID, l.WorkerID)
		case errors.Is(err, workers.ErrChunkNotFound):
			r.reg.SetStatusIf(l.ChunkID, cluster.ReplicaActive, cluster.ReplicaLost)
			r.log.Warn("recovery: copy missing on worker; marked lost",
				"object_id", ref.ObjectID, "sequence_no", ref.SequenceNo, "chunk_id", l.ChunkID, "worker_id", l.WorkerID)
		default:
			kept = append(kept, l)
			r.log.Warn("recovery: could not read copy", "object_id", ref.ObjectID, "sequence_no", ref.SequenceNo,
				"chunk_id", l.ChunkID, "worker_id", l.WorkerID, "error", err)
		}
	}
	return data, kept
}

// rewrite overwrites a corrupted or missing copy in place: same worker, same
// chunk ID (the worker's PUT replaces an existing chunk).
func (r *Recovery) rewrite(ctx context.Context, ref metadata.ChunkRef, l cluster.Location, data []byte) bool {
	if _, err := r.chunks.PutChunk(ctx, l.Addr, l.ChunkID, ref.ObjectID, ref.SequenceNo, data, ref.Checksum); err != nil {
		r.log.Warn("recovery: could not rewrite copy; will retry", "object_id", ref.ObjectID, "sequence_no", ref.SequenceNo,
			"chunk_id", l.ChunkID, "worker_id", l.WorkerID, "error", err)
		return false
	}
	if !r.reg.SetStatusIf(l.ChunkID, l.Status, cluster.ReplicaActive) {
		return false // the table changed meanwhile; the next tick looks again
	}
	r.log.Info("recovery: repaired copy in place", "object_id", ref.ObjectID, "sequence_no", ref.SequenceNo,
		"chunk_id", l.ChunkID, "worker_id", l.WorkerID, "was", l.Status)
	r.events.Record(protocol.EventReplicaRepaired, events.Info,
		fmt.Sprintf("%s copy of chunk %d rewritten from a good copy", l.Status, ref.SequenceNo), ref.ObjectID, l.WorkerID)
	return true
}

// copyTo writes a new copy to the worker PlaceReplica chose.
func (r *Recovery) copyTo(ctx context.Context, ref metadata.ChunkRef, loc cluster.Location, data []byte) bool {
	if _, err := r.chunks.PutChunk(ctx, loc.Addr, loc.ChunkID, ref.ObjectID, ref.SequenceNo, data, ref.Checksum); err != nil {
		r.reg.Remove(loc.ChunkID)
		r.log.Warn("recovery: could not store new copy", "object_id", ref.ObjectID, "sequence_no", ref.SequenceNo,
			"worker_id", loc.WorkerID, "error", err)
		return false
	}
	// The object may have been deleted while the copy was written; its
	// delete wouldn't have seen this copy.
	if ok, err := r.meta.HasChunk(ctx, ref.ObjectID, ref.SequenceNo); err == nil && !ok {
		_ = r.chunks.DeleteChunk(ctx, loc.Addr, loc.ChunkID)
		r.reg.Remove(loc.ChunkID)
		return false
	}
	if !r.reg.SetStatusIf(loc.ChunkID, cluster.ReplicaPending, cluster.ReplicaActive) {
		return false
	}
	r.log.Info("recovery: restored a copy", "object_id", ref.ObjectID, "sequence_no", ref.SequenceNo,
		"chunk_id", loc.ChunkID, "worker_id", loc.WorkerID)
	r.events.Record(protocol.EventReplicaRestored, events.Info,
		fmt.Sprintf("new copy of chunk %d stored", ref.SequenceNo), ref.ObjectID, loc.WorkerID)
	return true
}

// trim deletes good copies beyond the replication factor, from the most
// loaded workers first, and returns the copies kept.
func (r *Recovery) trim(ctx context.Context, ref metadata.ChunkRef, good []cluster.Location, load map[string]int) []cluster.Location {
	sort.Slice(good, func(i, j int) bool {
		if load[good[i].WorkerID] != load[good[j].WorkerID] {
			return load[good[i].WorkerID] > load[good[j].WorkerID]
		}
		return good[i].WorkerID < good[j].WorkerID
	})
	extra := len(good) - r.cfg.ReplicationFactor
	var kept []cluster.Location
	for i, l := range good {
		if i >= extra {
			kept = append(kept, l)
			continue
		}
		if err := r.chunks.DeleteChunk(ctx, l.Addr, l.ChunkID); err != nil {
			kept = append(kept, l)
			r.log.Warn("recovery: could not remove extra copy; will retry", "object_id", ref.ObjectID,
				"sequence_no", ref.SequenceNo, "chunk_id", l.ChunkID, "worker_id", l.WorkerID, "error", err)
			continue
		}
		r.reg.Remove(l.ChunkID)
		load[l.WorkerID]--
		r.log.Info("recovery: removed extra copy", "object_id", ref.ObjectID, "sequence_no", ref.SequenceNo,
			"chunk_id", l.ChunkID, "worker_id", l.WorkerID)
		r.events.Record(protocol.EventReplicaTrimmed, events.Info,
			fmt.Sprintf("extra copy of chunk %d removed (more than %d copies)", ref.SequenceNo, r.cfg.ReplicationFactor), ref.ObjectID, l.WorkerID)
	}
	return kept
}

// deleteLostObject removes an object one of whose chunks can't be restored.
func (r *Recovery) deleteLostObject(ctx context.Context, objectID string, seq int) {
	err := r.objects.Delete(ctx, objectID)
	if err != nil && !errors.Is(err, metadata.ErrNotFound) {
		r.log.Error("recovery: could not delete unrecoverable object", "object_id", objectID, "error", err)
		return
	}
	r.log.Error("every copy of a chunk is corrupted or missing; object deleted", "object_id", objectID, "sequence_no", seq)
	r.events.Record(protocol.EventObjectLost, events.Error,
		fmt.Sprintf("every copy of chunk %d is corrupted or missing; object deleted", seq), objectID, "")
}

// removeOrphans deletes ORPHAN copies from their workers and returns how many
// are left. Orphans on workers that are gone are only dropped from the table.
func (r *Recovery) removeOrphans(ctx context.Context, view cluster.View) int {
	left := 0
	removed := map[string]int{} // worker ID -> orphans deleted
	for _, locs := range view.Replicas {
		for _, l := range locs {
			if l.Status != cluster.ReplicaOrphan {
				continue
			}
			switch l.WorkerStatus {
			case cluster.WorkerReady:
				if err := r.chunks.DeleteChunk(ctx, l.Addr, l.ChunkID); err != nil {
					left++
					r.log.Warn("recovery: could not delete orphan copy; will retry", "chunk_id", l.ChunkID, "worker_id", l.WorkerID, "error", err)
					continue
				}
				r.reg.Remove(l.ChunkID)
				removed[l.WorkerID]++
			case cluster.WorkerAvailable:
				left++
			default: // DEAD or no longer registered
				r.reg.Remove(l.ChunkID)
			}
		}
	}
	for id, n := range removed {
		r.log.Info("recovery: deleted orphan copies", "worker_id", id, "count", n)
		r.events.Record(protocol.EventOrphanRemoved, events.Info,
			fmt.Sprintf("%d orphan copies deleted", n), "", id)
	}
	return left
}

// newID returns 32 random hex characters, the form of every chunk ID.
func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return hex.EncodeToString(b), nil
}
