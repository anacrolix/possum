package possum

import (
	possumC "github.com/anacrolix/possum/go/cpossum"
	"io"
	"io/fs"
	"time"
)

type Reader struct {
	pc possumC.Reader
}

func (r Reader) Add(key string) (v Value, err error) {
	v.c, err = possumC.ReaderAdd(r.pc, key)
	return
}

func (r Reader) Begin() error {
	return possumC.ReaderBegin(r.pc)
}

func (r Reader) End() {
	possumC.ReaderEnd(r.pc)
}

func (r Reader) Close() error {
	// This probably isn't safe to call multiple times.
	r.End()
	return nil
}

func (r Reader) ListItems(prefix string) ([]Item, error) {
	return possumC.ReaderListItems(r.pc, prefix)
}

type Value struct {
	c   possumC.Value
	key string
}

func (v Value) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = possumC.ValueReadAt(v.c, p, off)
	err = mapRustEofReadAt(len(p), n, err)
	return
}

func (v Value) Stat() FileInfo {
	return FileInfo{possumC.ValueStat(v.c), v.key}
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

type Item = possumC.Item

// See the very strict definition of io.ReaderAt.ReadAt.
func mapRustEofReadAt(bufLen int, n int, err error) error {
	if n == 0 && bufLen != 0 && err == nil {
		return io.EOF
	}
	return err
}
