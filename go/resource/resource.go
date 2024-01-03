package possumResource

import (
	"github.com/anacrolix/missinggo/v2/resource"
	possum "github.com/anacrolix/possum/go"
	"io"
	"io/fs"
	"os"
)

type Provider struct {
	Handle *possum.Handle
}

func (p Provider) NewInstance(s string) (resource.Instance, error) {
	return &instance{
		key:    s,
		handle: p.Handle,
	}, nil
}

var _ resource.Provider = Provider{}

type instance struct {
	key    string
	handle *possum.Handle
}

func (i *instance) Get() (rc io.ReadCloser, err error) {
	// TODO: Return a wrapper around a snapshot value, and link Close to closing the snapshot.
	fi, err := i.Stat()
	if err != nil {
		return
	}
	rc = io.NopCloser(io.NewSectionReader(i, 0, fi.Size()))
	return
}

func (i *instance) Put(reader io.Reader) (err error) {
	// TODO: Stream the value into a new value writer (after all that's what Possum excels at).
	b, err := io.ReadAll(reader)
	if err != nil {
		return
	}
	return i.handle.PutBuf(i.key, b)
}

func (i *instance) Stat() (fi os.FileInfo, err error) {
	fi, ok := i.handle.SingleStat(i.key)
	if !ok {
		err = fs.ErrNotExist
		return
	}
	return
}

func (i *instance) ReadAt(p []byte, off int64) (n int, err error) {
	return i.handle.SingleReadAt(i.key, off, p)
}

func (i *instance) WriteAt(bytes []byte, i2 int64) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (i *instance) Delete() (err error) {
	fi, err := i.handle.SingleDelete(i.key)
	if err != nil {
		return
	}
	if !fi.Ok {
		err = fs.ErrNotExist
	}
	return
}

var _ interface {
	resource.Instance
	resource.DirInstance
} = (*instance)(nil)

func (i *instance) Readdirnames() (names []string, err error) {
	subKeys, err := i.handle.ListKeys(i.key + "/")
	if err != nil {
		return
	}
	// For now let's just return all the keys. If they have slashes, it might not be what the caller
	// expects.
	names = subKeys
	return
}
