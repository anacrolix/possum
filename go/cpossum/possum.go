package possumC

/*
#cgo LDFLAGS: -L ../../target/debug -lpossum
#include "../../possum.h"
*/
import "C"
import (
	"unsafe"
)

type Handle = C.Handle

func NewHandle(dir string) *Handle {
	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))
	handle := C.possum_new(cDir)
	return handle
}

func DropHandle(handle *Handle) {
	C.possum_drop(handle)
}
