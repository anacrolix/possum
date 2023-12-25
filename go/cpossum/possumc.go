package possumC

// #cgo LDFLAGS: -L ../../target/debug -lpossum
// #include "../../possum.h"
import "C"
import (
	"errors"
	"github.com/anacrolix/generics"
	"math"
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

func WriteSingleBuf(handle *Handle, key string, buf []byte) (written uint, err error) {
	written = uint(C.possum_single_write_buf(
		handle,
		(*C.uchar)(unsafe.StringData(key)),
		C.size_t(len(key)),
		(*C.uchar)(unsafe.SliceData(buf)),
		C.size_t(len(buf)),
	))
	if written == math.MaxUint {
		err = errors.New("unknown possum error")
	}
	return
}

//func ListKeys(handle *Handle, prefix string) (keys []string) {
//var cKeys *C.uchar
//var cKeysLen C.size_t
//C.possum_list_keys(handle, (*C.uchar)(unsafe.StringData(prefix)), C.size_t(len(prefix)), &cKeys, &cKeysLen)
//defer C.free(unsafe.Pointer(cKeys))
//keys = make([]string, cKeysLen)
//for i := range keys {
//	keys[i] = C.GoStringN((*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cKeys))+uintptr(i))), C.int(len(prefix)))
//}
//return
//}
