// Package events keeps a short in-memory history of notable controller
// events (uploads, deletes, worker registrations, corrupted replicas) for the
// dashboard's activity feed. It is not persisted and not an audit log.
package events

import (
	"sync"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// DefaultCapacity is how many events a Log made by New keeps.
const DefaultCapacity = 200

// Levels for Event.Level.
const (
	Info  = "info"
	Warn  = "warn"
	Error = "error"
)

// Log is a fixed-size ring of the most recent events. It is safe for
// concurrent use. A nil *Log ignores Record and returns nothing from Since,
// so components can be used without one.
type Log struct {
	mu   sync.Mutex
	buf  []protocol.DashboardEvent
	next int    // index the next event is written to
	full bool   // buf has wrapped
	seq  uint64 // seq of the last recorded event
	now  func() time.Time
}

// New returns a Log keeping the last capacity events (DefaultCapacity if
// capacity < 1).
func New(capacity int) *Log {
	if capacity < 1 {
		capacity = DefaultCapacity
	}
	return &Log{buf: make([]protocol.DashboardEvent, capacity), now: time.Now}
}

// Record appends an event. objectID and workerID may be empty.
func (l *Log) Record(typ, level, message, objectID, workerID string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	l.buf[l.next] = protocol.DashboardEvent{
		Seq: l.seq, Time: l.now().UTC(), Type: typ, Level: level,
		Message: message, ObjectID: objectID, WorkerID: workerID,
	}
	l.next = (l.next + 1) % len(l.buf)
	if l.next == 0 {
		l.full = true
	}
}

// Since returns the retained events with Seq > after, oldest first, and the
// seq of the newest event recorded so far (0 if none).
func (l *Log) Since(after uint64) ([]protocol.DashboardEvent, uint64) {
	if l == nil {
		return []protocol.DashboardEvent{}, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := []protocol.DashboardEvent{}
	n, start := l.next, 0
	if l.full {
		n, start = len(l.buf), l.next
	}
	for i := range n {
		e := l.buf[(start+i)%len(l.buf)]
		if e.Seq > after {
			out = append(out, e)
		}
	}
	return out, l.seq
}
