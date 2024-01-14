//go:build unix

package possum

import (
	"syscall"
)

func duplicateFileHandle(fh uintptr) (newFh uintptr, err error) {
	newFd, err := syscall.Dup(int(fh))
	newFh = uintptr(newFd)
	return
}
