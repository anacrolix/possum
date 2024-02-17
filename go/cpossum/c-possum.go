package possumC

/*
// Not sure how to statically just this library, so for now it's preferred to use CGO_LDFLAGS to do
// your own thing.
//#cgo LDFLAGS: -lpossum
#include "possum.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"github.com/anacrolix/generics"
	"io/fs"
	"math"
	"runtime"
	"time"
	"unsafe"
)

var NoSuchKey = Error{
	pec: C.NoSuchKey,
}

type Error struct {
	pec             C.PossumError
	displayGoesHere string
}

const cErrorEnumNoSuchKey = C.NoSuchKey

func (me Error) Is(err error) bool {
	if err == NoSuchKey || err == fs.ErrNotExist {
		return me.pec == cErrorEnumNoSuchKey
	}
	return false
}

func (me Error) Error() string {
	return fmt.Sprintf("possum error code %v", me.pec)
}

func mapError(err C.PossumError) error {
	if err == C.NoError {
		return nil
	}
	return Error{pec: err}
}

type Stat = C.PossumStat

func (me Stat) LastUsed() time.Time {
	ts := me.last_used
	return time.Unix(int64(ts.secs), int64(ts.nanos))
}

func (me Stat) Size() int64 {
	return int64(me.size)
}

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

func SingleDelete(handle *Handle, key string) (opt generics.Option[Stat], err error) {
	pe := C.possum_single_delete(handle, BufFromString(key), &opt.Value)
	switch pe {
	case C.NoError:
		opt.Ok = true
	case C.NoSuchKey:
	default:
		err = mapError(pe)
	}
	return
}

func SingleStat(handle *Handle, key string) (opt generics.Option[Stat]) {
	opt.Ok = bool(C.possum_single_stat(
		handle,
		BufFromString(key),
		&opt.Value,
	))
	return
}

func WriteSingleBuf(handle *Handle, key string, buf []byte) (written uint, err error) {
	written = uint(C.possum_single_write_buf(
		handle,
		BufFromString(key),
		BufFromBytes(buf),
	))
	if written == math.MaxUint {
		err = errors.New("unknown possum error")
	}
	return
}

func goListItems(items *C.PossumItem, itemsLen C.size_t) (goItems []Item) {
	itemsSlice := unsafe.Slice(items, uint(itemsLen))
	goItems = make([]Item, itemsLen)
	for i, from := range itemsSlice {
		to := &goItems[i]
		to.Key = C.GoStringN(
			(*C.char)(from.key.ptr),
			C.int(from.key.size),
		)
		C.free(unsafe.Pointer(from.key.ptr))
		to.Stat = from.stat
	}
	C.free(unsafe.Pointer(items))
	return
}

func HandleListItems(handle *Handle, prefix string) (items []Item, err error) {
	var cItems *C.PossumItem
	var itemsLen C.size_t
	err = mapError(C.possum_list_items(
		handle,
		BufFromString(prefix),
		&cItems, &itemsLen))
	if err != nil {
		return
	}
	items = goListItems(cItems, itemsLen)
	return
}

func SingleReadAt(handle *Handle, key string, buf []byte, offset uint64) (n int, err error) {
	var pinner runtime.Pinner
	defer pinner.Unpin()
	pBuf := BufFromBytes(buf)
	pinner.Pin(pBuf.ptr)
	err = mapError(C.possum_single_read_at(
		handle,
		BufFromString(key),
		&pBuf,
		C.uint64_t(offset),
	))
	n = int(pBuf.size)
	return
}

type Reader = *C.PossumReader

func NewReader(handle *Handle) (r Reader, err error) {
	err = mapError(C.possum_reader_new(handle, &r))
	return
}

func BufFromString(s string) C.PossumBuf {
	return C.PossumBuf{
		(*C.char)(unsafe.Pointer(unsafe.StringData(s))),
		C.size_t(len(s)),
	}
}

func BufFromBytes(b []byte) C.PossumBuf {
	return C.PossumBuf{
		(*C.char)(unsafe.Pointer(unsafe.SliceData(b))),
		C.size_t(len(b)),
	}
}

func ReaderAdd(r Reader, key string) (v Value, err error) {
	err = mapError(C.possum_reader_add(r, BufFromString(key), &v))
	return
}

func ReaderBegin(r Reader) error {
	return mapError(C.possum_reader_begin(r))
}

func ReaderEnd(r Reader) {
	C.possum_reader_end(r)
}

func ReaderListItems(r Reader, prefix string) (items []Item, err error) {
	var cItems *C.PossumItem
	var itemsLen C.size_t
	err = mapError(C.possum_reader_list_items(r, BufFromString(prefix), &cItems, &itemsLen))
	if err != nil {
		return
	}
	items = goListItems(cItems, itemsLen)
	return
}

type Value = *C.PossumValue

func ValueReadAt(v Value, buf []byte, offset int64) (n int, err error) {
	pBuf := BufFromBytes(buf)
	var pin runtime.Pinner
	defer pin.Unpin()
	pin.Pin(pBuf.ptr)
	err = mapError(C.possum_value_read_at(v, &pBuf, C.uint64_t(offset)))
	n = int(pBuf.size)
	return
}

func ValueStat(v Value) (ret Stat) {
	C.possum_value_stat(v, &ret)
	return
}

type Item struct {
	Key string
	Stat
}

type Limits struct {
	MaxValueLengthSum   generics.Option[uint64]
	DisableHolePunching bool
}

func CleanupSnapshots(h *Handle) error {
	return mapError(C.possum_cleanup_snapshots(h))
}

func SetInstanceLimits(h *Handle, limits Limits) error {
	var cLimits C.PossumLimits
	cLimits.max_value_length_sum = C.uint64_t(limits.MaxValueLengthSum.UnwrapOr(math.MaxUint64))
	cLimits.disable_hole_punching = C.bool(limits.DisableHolePunching)
	return mapError(C.possum_set_instance_limits(h, &cLimits))
}

type Writer = *C.PossumWriter

func NewWriter(h *Handle) Writer {
	return C.possum_new_writer(h)
}

type ValueWriter = *C.PossumValueWriter

func StartNewValue(w Writer) (vw ValueWriter, err error) {
	err = mapError(C.possum_start_new_value(w, &vw))
	return
}

func ValueWriterFd(vw ValueWriter) int {
	return int(C.possum_value_writer_fd(vw))
}

func StageWrite(w Writer, key []byte, vw ValueWriter) error {
	return mapError(C.possum_writer_stage(w, BufFromBytes(key), vw))
}

func CommitWriter(w Writer) error {
	return mapError(C.possum_writer_commit(w))
}
