//go:build !unix

package slot

import (
	"errors"
	"os"
)

func tryLock(*os.File) error {
	return errors.New("slot mode (DATA_ROOT) needs a Unix system")
}
