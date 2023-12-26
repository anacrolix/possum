package possum

import (
	possumC "github.com/anacrolix/possum/go/cpossum"
	"io/fs"
	"time"
)

type Handle struct {
	cHandle *possumC.Handle
}

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
	return possumC.ListKeys(me.cHandle, prefix)
}

type FileInfo struct {
	cStat possumC.Stat
	name  string
}

func (f FileInfo) Name() string {
	return f.name
}

func (f FileInfo) Size() int64 {
	return f.cStat.Size()
}

func (f FileInfo) Mode() fs.FileMode {
	return 0o444
}

func (f FileInfo) ModTime() time.Time {
	return f.cStat.LastUsed()
}

func (f FileInfo) IsDir() bool {
	return false
}

func (f FileInfo) Sys() any {
	return f.cStat
}
