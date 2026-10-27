//go:build !windows

package db

import (
	"os"
	"syscall"
)

// A source replaced by a FIFO between stat and open must not wait for a writer.
// Callers validate the opened descriptor is still the expected regular file.
func openTimingSourceFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
