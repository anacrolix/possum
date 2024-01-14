package possum

import (
	"syscall"
)

func duplicateFileHandle(fh uintptr) (newFh uintptr, err error) {
	var newHandle syscall.Handle
	sourceHandle := syscall.Handle(fh)
	currentProcess, err := syscall.GetCurrentProcess()
	err = syscall.DuplicateHandle(
		currentProcess,
		sourceHandle,
		currentProcess,
		&newHandle,
		0,
		true,
		syscall.DUPLICATE_SAME_ACCESS,
	)
	newFh = uintptr(newHandle)
	return
}
