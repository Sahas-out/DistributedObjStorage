package cluster

import (
	"fmt"
	"time"
)

// This file holds the registry operations used by the recovery job: failure
// detection, a consistent snapshot of every replica, and placing one extra
// copy of an existing chunk.

// DeadWorker describes a worker that MarkDead just declared dead.
type DeadWorker struct {
	ID       string
	LastSeen time.Time
	// Lost is how many of its ACTIVE copies became LOST.
	Lost int
}

// MarkDead declares DEAD every worker that hasn't announced since cutoff and
// isn't DEAD already. Their ACTIVE copies become LOST; other statuses are
// kept (a CORRUPTED copy stays corrupted, a PENDING one is resolved by its
// upload). A chunk-list sync still running for such a worker is discarded.
func (r *Registry) MarkDead(cutoff time.Time) []DeadWorker {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []DeadWorker
	for id, w := range r.workers {
		if w.Status == WorkerDead || !w.LastSeen.Before(cutoff) {
			continue
		}
		w.Status = WorkerDead
		r.nextGen++
		w.gen, w.syncGen = r.nextGen, 0
		d := DeadWorker{ID: id, LastSeen: w.LastSeen}
		for cid := range r.byWorker[id] {
			if rep := r.chunks[cid]; rep.Status == ReplicaActive {
				rep.Status = ReplicaLost
				d.Lost++
			}
		}
		out = append(out, d)
	}
	return out
}

// PruneDead forgets DEAD workers that hold no replicas any more and haven't
// been seen since cutoff, and returns their IDs. Until then they stay listed
// so the dashboard can show them.
func (r *Registry) PruneDead(cutoff time.Time) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for id, w := range r.workers {
		if w.Status == WorkerDead && len(r.byWorker[id]) == 0 && w.LastSeen.Before(cutoff) {
			delete(r.byWorker, id)
			delete(r.workers, id)
			out = append(out, id)
		}
	}
	return out
}

// View is a consistent snapshot of the chunk table.
type View struct {
	// Replicas holds every replica, of any status, grouped by chunk. ORPHAN
	// replicas appear under keys that have no object in the metadata.
	Replicas map[ChunkKey][]Location
	// Syncing is true while some worker is AVAILABLE: its chunk list hasn't
	// been loaded, so it may hold copies the table doesn't show yet.
	Syncing bool
}

// View returns a snapshot of every replica and its worker's status.
func (r *Registry) View() View {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v := View{Replicas: make(map[ChunkKey][]Location, len(r.chunks))}
	for _, rep := range r.chunks {
		k := ChunkKey{ObjectID: rep.ObjectID, SeqNo: rep.SeqNo}
		v.Replicas[k] = append(v.Replicas[k], r.locationLocked(rep))
	}
	for _, w := range r.workers {
		if w.Status == WorkerAvailable {
			v.Syncing = true
			break
		}
	}
	return v
}

// PlaceReplica chooses one READY worker not in exclude for an extra copy of
// chunk seq of objectID, using the placement policy, and registers a PENDING
// replica there with an ID from newChunkID.
func (r *Registry) PlaceReplica(objectID string, seq int, exclude map[string]bool, newChunkID func() (string, error)) (Location, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ready []Worker
	for _, w := range r.workers {
		if w.Status == WorkerReady && !exclude[w.ID] {
			ready = append(ready, r.snapshotLocked(w))
		}
	}
	if len(ready) == 0 {
		return Location{}, fmt.Errorf("%w: no READY worker without a copy of the chunk", ErrInsufficientWorkers)
	}
	chosen := r.policy.Choose(ready, 1)
	if len(chosen) != 1 {
		return Location{}, fmt.Errorf("placement policy chose %d workers, want 1", len(chosen))
	}
	id, err := newChunkID()
	if err != nil {
		return Location{}, err
	}
	if _, dup := r.chunks[id]; dup {
		return Location{}, fmt.Errorf("%w: %s", ErrDuplicateChunk, id)
	}
	w := r.workers[chosen[0].ID]
	rep := Replica{ChunkID: id, ObjectID: objectID, SeqNo: seq, WorkerID: w.ID, Status: ReplicaPending}
	r.addLocked(rep)
	return Location{Replica: rep, Addr: w.Addr, WorkerStatus: w.Status}, nil
}

// SetStatusIf changes a replica's status from one value to another. It
// returns false, changing nothing, if the replica is gone or its status is
// no longer from: recovery uses it so it never overwrites a change made
// since its snapshot.
func (r *Registry) SetStatusIf(chunkID string, from, to ReplicaStatus) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep, ok := r.chunks[chunkID]
	if !ok || rep.Status != from {
		return false
	}
	rep.Status = to
	return true
}
