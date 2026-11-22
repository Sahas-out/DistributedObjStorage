// Package slot lets several workers share one volume, each with its own data
// directory.
//
// Docker Compose can't give each replica of a scaled service its own named
// volume. Instead, every worker mounts the same volume and claims a slot
// directory (slot-0, slot-1, ...) by holding an exclusive lock on the
// directory's lock file for as long as the process runs. The kernel releases
// the lock when the process exits, however it exits, so a restarted or newly
// scaled worker picks up a free slot together with the chunks stored in it.
package slot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// lockFileName is the file locked inside each slot directory.
const lockFileName = ".lock"

// maxSlots bounds the search so a misconfiguration fails instead of looping.
const maxSlots = 1024

// errLocked is returned by tryLock when another process holds the lock.
var errLocked = errors.New("slot is locked")

// Slot is a claimed data directory.
type Slot struct {
	Dir   string // the slot's data directory
	Index int
	lock  *os.File
}

// Release unlocks the slot. It is safe to call more than once.
func (s *Slot) Release() error {
	if s.lock == nil {
		return nil
	}
	err := s.lock.Close() // closing the descriptor drops the lock
	s.lock = nil
	return err
}

// Claim locks the lowest-numbered free slot under root, creating it if
// needed. Lower slots are preferred so a worker that restarts usually gets
// its old slot back, and with it its chunks.
func Claim(root string) (*Slot, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create data root: %w", err)
	}
	for i := range maxSlots {
		dir := filepath.Join(root, fmt.Sprintf("slot-%d", i))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create slot dir: %w", err)
		}
		f, err := os.OpenFile(filepath.Join(dir, lockFileName), os.O_RDWR|os.O_CREATE, 0o640)
		if err != nil {
			return nil, fmt.Errorf("open slot lock: %w", err)
		}
		err = tryLock(f)
		if errors.Is(err, errLocked) {
			f.Close()
			continue
		}
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("lock %s: %w", dir, err)
		}
		return &Slot{Dir: dir, Index: i, lock: f}, nil
	}
	return nil, fmt.Errorf("no free slot under %s (tried %d)", root, maxSlots)
}
