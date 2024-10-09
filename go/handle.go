package possum

import (
	"errors"
	"github.com/anacrolix/generics"
	possumC "github.com/anacrolix/possum/go/cpossum"
	"sync"
)

type Handle struct {
	mu      sync.RWMutex
	cHandle Rc[*possumC.Handle]
	closed  bool
}

var ErrHandleClosed = errors.New("possum Handle closed")

type Limits = possumC.Limits

func Open(dir string) (handle *Handle, err error) {
	cHandle := possumC.NewHandle(dir)
	if cHandle == nil {
		err = errors.New("unhandled possum error")
		return
	}
	generics.InitNew(&handle)
	handle.cHandle.Init(cHandle, func(cHandle *possumC.Handle) {
		possumC.DropHandle(cHandle)
	})
	return
}

func (me *Handle) Close() error {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.closed {
		return nil
	}
	me.cHandle.Drop()
	me.closed = true
	return nil
}

func (me *Handle) cloneRc() (rc *Rc[*possumC.Handle], err error) {
	me.mu.RLock()
	defer me.mu.RUnlock()
	if me.closed {
		err = ErrHandleClosed
		return
	}
	rc = me.cHandle.Clone()
	return
}

// Runs the given function with a handle that won't be dropped during the function, or returns an
// error if the handle is closed.
func (me *Handle) withHandle(f func(handle *possumC.Handle) error) error {
	rc, err := me.cloneRc()
	if err != nil {
		return err
	}
	defer rc.Drop()
	return f(rc.Deref())
}

func (me *Handle) SingleStat(key string) (fi FileInfo, ok bool) {
	_ = me.withHandle(func(handle *possumC.Handle) error {
		stat := possumC.SingleStat(handle, key)
		if !stat.Ok {
			return nil
		}
		fi = FileInfo{stat.Value, key}
		ok = true
		return nil
	})
	return
}

func (me *Handle) PutBuf(key string, buf []byte) error {
	return me.withHandle(func(handle *possumC.Handle) error {
		written, err := possumC.WriteSingleBuf(handle, key, buf)
		if err != nil {
			return err
		}
		if written != uint(len(buf)) {
			panic("expected an error")
		}
		return err
	})
}

func (me *Handle) ListKeys(prefix string) (keys []string, err error) {
	err = me.withHandle(func(handle *possumC.Handle) error {
		items, err := possumC.HandleListItems(handle, prefix)
		for _, item := range items {
			keys = append(keys, item.Key)
		}
		return err
	})
	return
}

func (me *Handle) SingleDelete(key string) (fi generics.Option[FileInfo], err error) {
	err = me.withHandle(func(handle *possumC.Handle) (err error) {
		stat, err := possumC.SingleDelete(handle, key)
		if err != nil {
			return
		}
		if !stat.Ok {
			return
		}
		fi.Set(FileInfo{stat.Value, key})
		return
	})
	return
}

func (me *Handle) SingleReadAt(key string, off int64, p []byte) (n int, err error) {
	err = me.withHandle(func(handle *possumC.Handle) (err error) {
		n, err = possumC.SingleReadAt(handle, key, p, uint64(off))
		err = mapRustEofReadAt(len(p), n, err)
		return
	})
	return
}

func (me *Handle) NewReader() (r Reader, err error) {
	err = me.withHandle(func(handle *possumC.Handle) (err error) {
		r.pc, err = possumC.NewReader(handle)
		return
	})
	return
}

func (me *Handle) SetInstanceLimits(limits Limits) error {
	return me.withHandle(func(handle *possumC.Handle) error {
		return possumC.SetInstanceLimits(handle, limits)
	})
}

func (me *Handle) CleanupSnapshots() error {
	return me.withHandle(func(handle *possumC.Handle) error {
		return possumC.CleanupSnapshots(handle)
	})
}

func (me *Handle) MovePrefix(from, to []byte) error {
	return me.withHandle(func(handle *possumC.Handle) error {
		return possumC.HandleMovePrefix(handle, from, to)
	})
}

func (me *Handle) DeletePrefix(prefix []byte) error {
	return me.withHandle(func(handle *possumC.Handle) error {
		return possumC.HandleDeletePrefix(handle, prefix)
	})
}
