//go:build unix

package slot

import (
	"errors"
	"os"
	"syscall"
)

// tryLock takes a non-blocking exclusive flock on f. flock locks belong to
// the open file description, so two descriptors opened separately (even in
// the same process) exclude each other, and every container sharing the
// volume on the same host sees the same lock.
func tryLock(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return errLocked
	}
	return err
}
