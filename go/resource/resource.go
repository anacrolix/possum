// This implements the missinggo resource.Provider interface over a Possum handle.

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
	w := i.handle.NewWriter()
	defer func() {
		// TODO: There's no Writer.Drop.
		commitErr := w.Commit()
		if err == nil {
			err = commitErr
		}
	}()
	vw, err := w.StartNewValue()
	if err != nil {
		return
	}
	f, err := vw.NewFile(i.key)
	if err != nil {
		return
	}
	_, err = io.Copy(f, reader)
	f.Close()
	if err == nil {
		err = w.Stage([]byte(i.key), vw)
	}
	// TODO: Committing here since we only staged one thing and if it failed, we should be
	// committing nothing. There's no way to drop a Writer at the time of writing.
	return
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
