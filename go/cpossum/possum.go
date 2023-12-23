package possumC

/*
#cgo LDFLAGS: -L ../../target/debug -lpossum
#include "../../possum.h"
*/
import "C"
import (
	"github.com/anacrolix/generics"
	"time"
	"unsafe"
)

type Handle = C.Handle

type Stat = C.Stat

func (me Stat) LastUsed() time.Time {
	ts := me.last_used
	return time.Unix(int64(ts.tv_sec), int64(ts.tv_nsec))
}

func (me Stat) Size() int64 {
	return int64(me.size)
}

func NewHandle(dir string) *Handle {
	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))
	handle := C.possum_new(cDir)
	return handle
}

func DropHandle(handle *Handle) {
	C.possum_drop(handle)
}

func SingleStat(handle *Handle, key string) (opt generics.Option[Stat]) {
	opt.Ok = bool(C.possum_single_stat(handle, (*C.uchar)(unsafe.StringData(key)), C.size_t(len(key)), &opt.Value))
	return
}
