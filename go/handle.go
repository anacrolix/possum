package possum

import (
	"github.com/anacrolix/generics"
	possumC "github.com/anacrolix/possum/go/cpossum"
)

type Handle struct {
	cHandle *possumC.Handle
}

type Limits = possumC.Limits

func Open(dir string) (*Handle, error) {
	cHandle := possumC.NewHandle(dir)
	return &Handle{cHandle}, nil
}

func (me Handle) Close() error {
	possumC.DropHandle(me.cHandle)
	return nil
}

func (me Handle) SingleStat(key string) (fi FileInfo, ok bool) {
	stat := possumC.SingleStat(me.cHandle, key)
	if !stat.Ok {
		return
	}
	return FileInfo{stat.Value, key}, true
}

func (me Handle) PutBuf(key string, buf []byte) error {
	written, err := possumC.WriteSingleBuf(me.cHandle, key, buf)
	if err != nil {
		return err
	}
	if written != uint(len(buf)) {
		panic("expected an error")
	}
	return err
}

func (me Handle) ListKeys(prefix string) (keys []string, err error) {
	items, err := possumC.HandleListItems(me.cHandle, prefix)
	for _, item := range items {
		keys = append(keys, item.Key)
	}
	return
}

func (me Handle) SingleDelete(key string) (fi generics.Option[FileInfo], err error) {
	stat, err := possumC.SingleDelete(me.cHandle, key)
	if err != nil {
		return
	}
	if !stat.Ok {
		return
	}
	fi.Value = FileInfo{stat.Value, key}
	fi.Ok = true
	return
}

func (me Handle) SingleReadAt(key string, off int64, p []byte) (n int, err error) {
	n, err = possumC.SingleReadAt(me.cHandle, key, p, uint64(off))
	err = mapRustEofReadAt(len(p), n, err)
	return
}

func (me Handle) NewReader() (r Reader, err error) {
	r.pc, err = possumC.NewReader(me.cHandle)
	return
}

func (me Handle) SetInstanceLimits(limits Limits) error {
	return possumC.SetInstanceLimits(me.cHandle, limits)
}

func (me Handle) CleanupSnapshots() error {
	return possumC.CleanupSnapshots(me.cHandle)
}

func (me Handle) MovePrefix(from, to []byte) error {
	return possumC.HandleMovePrefix(me.cHandle, from, to)
}

func (me Handle) DeletePrefix(prefix []byte) error {
	return possumC.HandleDeletePrefix(me.cHandle, prefix)
}
