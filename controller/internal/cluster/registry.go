// Package cluster holds the controller's runtime view of the cluster: which
// workers exist and where every chunk replica lives.
//
// None of this is persisted. Worker IDs and addresses only last as long as a
// worker process, and replicas can be lost or corrupted, so storing them
// would only record stale facts. Instead the view is built from the workers
// themselves: each worker registers (and keeps re-announcing as a heartbeat),
// and the controller pulls its chunk list.
//
// The Registry holds two tables behind one lock so they always agree:
//
//   - workers: worker_id -> address, status, chunk count, last seen
//   - chunks:  chunk_id  -> object_id, sequence_no, worker_id, status
//
// Placement, uploads, downloads and deletes all read and update these
// tables. Future background jobs (lost-chunk recovery, corruption repair,
// orphan cleanup, rebalancing) are meant to watch them the same way: an event
// such as a worker failure only changes a status here, and the job that owns
// that status reacts to it.
package cluster

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// WorkerStatus is the lifecycle state of a registered worker.
type WorkerStatus string

const (
	// WorkerAvailable means the worker has registered but the controller
	// hasn't loaded its chunk list yet. It gets no new chunks.
	WorkerAvailable WorkerStatus = "AVAILABLE"
	// WorkerReady means the chunk list is loaded; the worker serves reads and
	// receives new chunks.
	WorkerReady WorkerStatus = "READY"
)

// ReplicaStatus is the state of one copy of a chunk.
type ReplicaStatus string

const (
	// ReplicaPending is a copy being written by an upload that hasn't
	// committed yet. Not readable.
	ReplicaPending ReplicaStatus = "PENDING"
	// ReplicaActive is a committed, readable copy.
	ReplicaActive ReplicaStatus = "ACTIVE"
	// ReplicaCorrupted is a copy that failed the checksum check on read.
	// It is no longer served; repairing it is a later phase.
	ReplicaCorrupted ReplicaStatus = "CORRUPTED"
	// ReplicaOrphan is a copy on a worker that belongs to no object (left by
	// a failed upload or delete). Removing it is a later phase.
	ReplicaOrphan ReplicaStatus = "ORPHAN"
)

// Errors returned by the registry.
var (
	ErrUnknownWorker       = errors.New("unknown worker")
	ErrWorkerRetired       = errors.New("worker was deregistered")
	ErrInsufficientWorkers = errors.New("not enough ready workers")
	ErrDuplicateChunk      = errors.New("chunk id already registered")
)

// Worker is a snapshot of one worker's entry.
type Worker struct {
	ID         string
	Addr       string // base URL, e.g. http://172.18.0.5:8080
	Status     WorkerStatus
	ChunkCount int // replicas of any status on this worker
	LastSeen   time.Time
}

// Replica is one copy of a chunk on one worker.
type Replica struct {
	ChunkID  string
	ObjectID string
	SeqNo    int
	WorkerID string
	Status   ReplicaStatus
}

// Location is a replica together with its worker's current address.
type Location struct {
	Replica
	Addr string
}

type workerEntry struct {
	Worker
	// gen changes whenever the worker's replica set is reset (new address);
	// a chunk-list sync started for an older gen is discarded.
	gen uint64
	// syncGen is the gen of the sync in progress, or 0 if none.
	syncGen uint64
}

// Registry is the in-memory worker and chunk table. It is safe for
// concurrent use.
type Registry struct {
	mu       sync.RWMutex
	workers  map[string]*workerEntry
	chunks   map[string]*Replica
	byObject map[string]map[int]map[string]struct{} // object -> seq -> chunk IDs
	byWorker map[string]map[string]struct{}         // worker -> chunk IDs
	retired  map[string]struct{}
	nextGen  uint64

	policy Policy
	now    func() time.Time
}

// NewRegistry returns an empty registry that places chunks with policy.
func NewRegistry(policy Policy) *Registry {
	return &Registry{
		workers:  map[string]*workerEntry{},
		chunks:   map[string]*Replica{},
		byObject: map[string]map[int]map[string]struct{}{},
		byWorker: map[string]map[string]struct{}{},
		retired:  map[string]struct{}{},
		policy:   policy,
		now:      time.Now,
	}
}

// AnnounceResult tells Membership what an announcement changed.
type AnnounceResult struct {
	Worker Worker
	New    bool // the worker wasn't known before
	Moved  bool // the worker came back at a different address
	// Replaced lists workers removed because this worker now uses their
	// address: a restarted worker registers under a new ID, and only one
	// process can listen on an address, so the old entry is dead.
	Replaced []string
	// SyncGen is non-zero when the caller must load the worker's chunk list
	// and then call CompleteSync or FailSync with it.
	SyncGen uint64
}

// Announce records that worker id is alive at addr.
//
//   - Unknown worker: added as AVAILABLE; a chunk-list sync is requested.
//     Any other worker registered at the same address is removed first.
//   - Known worker at the same address: only LastSeen changes, unless it is
//     still AVAILABLE with no sync running (an earlier sync failed), in which
//     case a sync is requested again.
//   - Known worker at a new address: its replicas are dropped, it goes back
//     to AVAILABLE and a sync is requested.
func (r *Registry) Announce(id, addr string) (AnnounceResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.retired[id]; ok {
		return AnnounceResult{}, ErrWorkerRetired
	}
	var res AnnounceResult
	w, ok := r.workers[id]
	if !ok || w.Addr != addr {
		for otherID, other := range r.workers {
			if otherID != id && other.Addr == addr {
				r.dropReplicasLocked(otherID)
				delete(r.byWorker, otherID)
				delete(r.workers, otherID)
				res.Replaced = append(res.Replaced, otherID)
			}
		}
	}
	switch {
	case !ok:
		r.nextGen++
		w = &workerEntry{Worker: Worker{ID: id, Addr: addr, Status: WorkerAvailable}, gen: r.nextGen}
		r.workers[id] = w
		r.byWorker[id] = map[string]struct{}{}
		res.New = true
	case w.Addr != addr:
		r.dropReplicasLocked(id)
		r.nextGen++
		w.Addr, w.Status, w.gen, w.syncGen = addr, WorkerAvailable, r.nextGen, 0
		res.Moved = true
	}
	w.LastSeen = r.now()
	if w.Status == WorkerAvailable && w.syncGen != w.gen {
		w.syncGen = w.gen
		res.SyncGen = w.gen
	}
	res.Worker = r.snapshotLocked(w)
	return res, nil
}

// CompleteSync adds the replicas reported by worker id and marks it READY.
// It returns false, changing nothing, if the worker is gone or moved since
// the sync started. A reported chunk already in the table on the same worker
// keeps its current entry (it may be PENDING from an upload in progress).
// One registered on a different worker is taken over by the reporting
// worker, because the chunk's file is on its disk: this happens when a
// worker restarts under a new ID at a new address while its old entry is
// still listed. Taken-over chunk IDs are returned.
func (r *Registry) CompleteSync(id string, gen uint64, reported []Replica) (ok bool, takenOver []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, exists := r.workers[id]
	if !exists || w.gen != gen {
		return false, nil
	}
	for _, rep := range reported {
		if existing, dup := r.chunks[rep.ChunkID]; dup {
			if existing.WorkerID == id {
				continue
			}
			r.removeLocked(rep.ChunkID)
			takenOver = append(takenOver, rep.ChunkID)
		}
		rep.WorkerID = id
		r.addLocked(rep)
	}
	w.Status = WorkerReady
	w.syncGen = 0
	w.ChunkCount = len(r.byWorker[id])
	return true, takenOver
}

// FailSync records that the sync for gen failed, so the next announcement
// requests another one.
func (r *Registry) FailSync(id string, gen uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.workers[id]; ok && w.syncGen == gen {
		w.syncGen = 0
	}
}

// RemoveWorker forgets a worker and all its replicas. With retire set, the
// ID is refused if it announces again.
func (r *Registry) RemoveWorker(id string, retire bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.workers[id]; !ok {
		return ErrUnknownWorker
	}
	r.dropReplicasLocked(id)
	delete(r.byWorker, id)
	delete(r.workers, id)
	if retire {
		r.retired[id] = struct{}{}
	}
	return nil
}

// Worker returns a snapshot of one worker.
func (r *Registry) Worker(id string) (Worker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w, ok := r.workers[id]
	if !ok {
		return Worker{}, false
	}
	return r.snapshotLocked(w), true
}

// Workers returns snapshots of all workers, sorted by ID.
func (r *Registry) Workers() []Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Worker, 0, len(r.workers))
	for _, w := range r.workers {
		out = append(out, r.snapshotLocked(w))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Place chooses rf distinct READY workers for chunk seq of objectID and
// registers a PENDING replica on each, with IDs from newChunkID. Registering
// them immediately raises the workers' chunk counts, so the next placement
// (by this upload or a concurrent one) sees the new load.
func (r *Registry) Place(objectID string, seq, rf int, newChunkID func() (string, error)) ([]Location, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ready []Worker
	for _, w := range r.workers {
		if w.Status == WorkerReady {
			ready = append(ready, r.snapshotLocked(w))
		}
	}
	if len(ready) < rf {
		return nil, fmt.Errorf("%w: need %d, have %d", ErrInsufficientWorkers, rf, len(ready))
	}
	chosen := r.policy.Choose(ready, rf)
	if len(chosen) != rf {
		return nil, fmt.Errorf("placement policy chose %d workers, want %d", len(chosen), rf)
	}
	out := make([]Location, 0, rf)
	for _, w := range chosen {
		id, err := newChunkID()
		if err != nil {
			for _, l := range out {
				r.removeLocked(l.ChunkID)
			}
			return nil, err
		}
		if _, dup := r.chunks[id]; dup {
			for _, l := range out {
				r.removeLocked(l.ChunkID)
			}
			return nil, fmt.Errorf("%w: %s", ErrDuplicateChunk, id)
		}
		rep := Replica{ChunkID: id, ObjectID: objectID, SeqNo: seq, WorkerID: w.ID, Status: ReplicaPending}
		r.addLocked(rep)
		out = append(out, Location{Replica: rep, Addr: w.Addr})
	}
	return out, nil
}

// SetStatus changes a replica's status. It returns false if the replica is
// not in the table.
func (r *Registry) SetStatus(chunkID string, status ReplicaStatus) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep, ok := r.chunks[chunkID]
	if !ok {
		return false
	}
	rep.Status = status
	return true
}

// Remove drops a replica from the table.
func (r *Registry) Remove(chunkID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeLocked(chunkID)
}

// Readable returns the ACTIVE replicas of one chunk that sit on READY
// workers, in no particular order.
func (r *Registry) Readable(objectID string, seq int) []Location {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Location
	for id := range r.byObject[objectID][seq] {
		rep := r.chunks[id]
		w := r.workers[rep.WorkerID]
		if rep.Status == ReplicaActive && w != nil && w.Status == WorkerReady {
			out = append(out, Location{Replica: *rep, Addr: w.Addr})
		}
	}
	return out
}

// ObjectReplicas returns every replica of an object, whatever its status,
// sorted by sequence number and then worker ID.
func (r *Registry) ObjectReplicas(objectID string) []Location {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Location
	for _, ids := range r.byObject[objectID] {
		for id := range ids {
			rep := r.chunks[id]
			loc := Location{Replica: *rep}
			if w := r.workers[rep.WorkerID]; w != nil {
				loc.Addr = w.Addr
			}
			out = append(out, loc)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SeqNo != out[j].SeqNo {
			return out[i].SeqNo < out[j].SeqNo
		}
		return out[i].WorkerID < out[j].WorkerID
	})
	return out
}

func (r *Registry) snapshotLocked(w *workerEntry) Worker {
	s := w.Worker
	s.ChunkCount = len(r.byWorker[w.ID])
	return s
}

func (r *Registry) addLocked(rep Replica) {
	r.chunks[rep.ChunkID] = &rep
	seqs := r.byObject[rep.ObjectID]
	if seqs == nil {
		seqs = map[int]map[string]struct{}{}
		r.byObject[rep.ObjectID] = seqs
	}
	if seqs[rep.SeqNo] == nil {
		seqs[rep.SeqNo] = map[string]struct{}{}
	}
	seqs[rep.SeqNo][rep.ChunkID] = struct{}{}
	if r.byWorker[rep.WorkerID] == nil {
		r.byWorker[rep.WorkerID] = map[string]struct{}{}
	}
	r.byWorker[rep.WorkerID][rep.ChunkID] = struct{}{}
	if w := r.workers[rep.WorkerID]; w != nil {
		w.ChunkCount = len(r.byWorker[rep.WorkerID])
	}
}

func (r *Registry) removeLocked(chunkID string) {
	rep, ok := r.chunks[chunkID]
	if !ok {
		return
	}
	delete(r.chunks, chunkID)
	if seqs := r.byObject[rep.ObjectID]; seqs != nil {
		delete(seqs[rep.SeqNo], chunkID)
		if len(seqs[rep.SeqNo]) == 0 {
			delete(seqs, rep.SeqNo)
		}
		if len(seqs) == 0 {
			delete(r.byObject, rep.ObjectID)
		}
	}
	delete(r.byWorker[rep.WorkerID], chunkID)
	if w := r.workers[rep.WorkerID]; w != nil {
		w.ChunkCount = len(r.byWorker[rep.WorkerID])
	}
}

func (r *Registry) dropReplicasLocked(workerID string) {
	for id := range r.byWorker[workerID] {
		r.removeLocked(id)
	}
}
