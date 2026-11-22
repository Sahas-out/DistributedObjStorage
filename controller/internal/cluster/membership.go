package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/events"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// ErrInvalidRegistration means a register request had a malformed worker ID
// or port.
var ErrInvalidRegistration = errors.New("invalid registration")

var workerIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ChunkLister fetches a worker's chunk list (the workers.Client).
type ChunkLister interface {
	ListChunks(ctx context.Context, addr string) ([]protocol.ChunkMetadata, error)
}

// ChunkIndex tells whether a reported chunk belongs to a stored object (the
// metadata store).
type ChunkIndex interface {
	HasChunk(ctx context.Context, objectID string, seq int) (bool, error)
}

// Membership handles workers joining, re-announcing and leaving.
type Membership struct {
	reg      *Registry
	lister   ChunkLister
	index    ChunkIndex
	interval time.Duration
	timeout  time.Duration
	log      *slog.Logger
	events   *events.Log

	ctx    context.Context // cancelled by Close; parent of every sync
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewMembership builds a Membership. interval is the announce interval
// suggested to workers; syncTimeout bounds loading one worker's chunk list.
// A nil logger discards logs.
func NewMembership(reg *Registry, lister ChunkLister, index ChunkIndex, interval, syncTimeout time.Duration, log *slog.Logger) *Membership {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Membership{
		reg: reg, lister: lister, index: index,
		interval: interval, timeout: syncTimeout, log: log,
		ctx: ctx, cancel: cancel,
	}
}

// SetEvents makes membership changes appear in the dashboard activity feed.
// Call it before serving requests.
func (m *Membership) SetEvents(ev *events.Log) { m.events = ev }

// Close stops running chunk-list syncs and waits for them.
func (m *Membership) Close() {
	m.cancel()
	m.wg.Wait()
}

// Register handles POST /internal/workers/register. remoteIP is the source
// address of the request: inside the Docker network that is the worker
// container's own IP, so the controller reaches each worker directly rather
// than through a load-balanced service name.
//
// It answers right away. When the worker is new (or moved, or an earlier
// sync failed) the chunk list is loaded in the background and the worker
// becomes READY when that finishes.
func (m *Membership) Register(req protocol.RegisterRequest, remoteIP string) (protocol.RegisterResponse, error) {
	if req.Port < 1 || req.Port > 65535 {
		return protocol.RegisterResponse{}, fmt.Errorf("%w: port %d out of range", ErrInvalidRegistration, req.Port)
	}
	if net.ParseIP(remoteIP) == nil {
		return protocol.RegisterResponse{}, fmt.Errorf("%w: cannot determine worker address from %q", ErrInvalidRegistration, remoteIP)
	}
	id := req.WorkerID
	if id == "" {
		var err error
		if id, err = newWorkerID(); err != nil {
			return protocol.RegisterResponse{}, err
		}
	} else if !workerIDPattern.MatchString(id) {
		return protocol.RegisterResponse{}, fmt.Errorf("%w: worker id must be 1-64 characters from [A-Za-z0-9_-]", ErrInvalidRegistration)
	}
	addr := "http://" + net.JoinHostPort(remoteIP, strconv.Itoa(req.Port))

	res, err := m.reg.Announce(id, addr)
	if err != nil {
		return protocol.RegisterResponse{}, err
	}
	if len(res.Replaced) > 0 {
		m.log.Info("removed workers whose address was taken by a new registration", "worker_id", id, "addr", addr, "removed", res.Replaced)
		for _, old := range res.Replaced {
			m.events.Record(protocol.EventWorkerReplaced, events.Info,
				fmt.Sprintf("worker %s removed: its address %s now belongs to %s", old, addr, id), "", old)
		}
	}
	switch {
	case res.New:
		m.log.Info("worker registered", "worker_id", id, "addr", addr)
		m.events.Record(protocol.EventWorkerRegistered, events.Info, "worker registered at "+addr, "", id)
	case res.Revived:
		m.log.Info("dead worker announced again; reloading its chunks", "worker_id", id, "addr", addr)
		m.events.Record(protocol.EventWorkerRevived, events.Info, "dead worker is back at "+addr+"; reloading its chunks", "", id)
	case res.Moved:
		m.log.Info("worker re-registered at a new address; reloading its chunks", "worker_id", id, "addr", addr)
		m.events.Record(protocol.EventWorkerMoved, events.Warn, "worker moved to "+addr+"; reloading its chunks", "", id)
	}
	if res.SyncGen != 0 {
		m.wg.Add(1)
		go m.sync(id, addr, res.SyncGen)
	}
	return protocol.RegisterResponse{
		WorkerID:                id,
		Status:                  string(res.Worker.Status),
		AnnounceIntervalSeconds: int(m.interval / time.Second),
	}, nil
}

// Deregister removes a worker that is leaving voluntarily, with all of its
// replica entries. The chunks on its disk are not touched. The ID is retired,
// so the worker's next announcement is refused.
func (m *Membership) Deregister(id string) error {
	if err := m.reg.RemoveWorker(id, true); err != nil {
		return err
	}
	m.log.Info("worker deregistered", "worker_id", id)
	m.events.Record(protocol.EventWorkerDeregistered, events.Info, "worker deregistered voluntarily", "", id)
	return nil
}

// sync loads a worker's chunk list into the registry. Each reported chunk is
// ACTIVE if its object and position exist in the metadata, ORPHAN otherwise.
func (m *Membership) sync(id, addr string, gen uint64) {
	defer m.wg.Done()
	ctx, cancel := context.WithTimeout(m.ctx, m.timeout)
	defer cancel()

	reported, err := m.lister.ListChunks(ctx, addr)
	if err != nil {
		m.reg.FailSync(id, gen)
		m.log.Warn("could not load worker chunk list; will retry on next announce", "worker_id", id, "addr", addr, "error", err)
		m.events.Record(protocol.EventWorkerSyncFailed, events.Warn, "could not load chunk list; will retry on next announce", "", id)
		return
	}
	replicas := make([]Replica, 0, len(reported))
	orphans := 0
	for _, c := range reported {
		known, err := m.index.HasChunk(ctx, c.ObjectID, c.SequenceNo)
		if err != nil {
			m.reg.FailSync(id, gen)
			m.log.Error("could not classify reported chunks; will retry on next announce", "worker_id", id, "error", err)
			return
		}
		status := ReplicaActive
		if !known {
			status = ReplicaOrphan
			orphans++
		}
		replicas = append(replicas, Replica{ChunkID: c.ChunkID, ObjectID: c.ObjectID, SeqNo: c.SequenceNo, Status: status})
	}

	ok, takenOver := m.reg.CompleteSync(id, gen, replicas)
	if !ok {
		m.log.Info("discarding stale chunk list; worker left or moved during the sync", "worker_id", id)
		return
	}
	if len(takenOver) > 0 {
		m.log.Warn("worker reported chunks registered to another worker; reassigned to the reporting worker",
			"worker_id", id, "chunks", len(takenOver))
	}
	m.log.Info("worker ready", "worker_id", id, "addr", addr, "chunks", len(replicas), "orphans", orphans)
	m.events.Record(protocol.EventWorkerReady, events.Info,
		fmt.Sprintf("worker ready with %d chunks (%d orphans)", len(replicas), orphans), "", id)
}

func newWorkerID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate worker id: %w", err)
	}
	return "w-" + hex.EncodeToString(b), nil
}

// Workers returns a snapshot of every registered worker, sorted by ID.
func (m *Membership) Workers() []Worker { return m.reg.Workers() }
