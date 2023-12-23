package possumC

/*
#cgo LDFLAGS: -L ../../target/debug -lpossum
#include "../../possum.h"
*/
import "C"
import (
	"github.com/anacrolix/generics"
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

type FileInfo struct {
	cStat C.Stat
}

func SingleStat(handle *Handle, key string) (opt generics.Option[FileInfo]) {
	opt.Ok = C.possum_single_stat(handle, unsafe.StringData(key), len(key), &opt.Value.cStat)
}
