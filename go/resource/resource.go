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
	return instance{
		key:    s,
		handle: p.Handle,
	}, nil
}

var _ resource.Provider = Provider{}

type instance struct {
	key    string
	handle *possum.Handle
}

func (i instance) Get() (io.ReadCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (i instance) Put(reader io.Reader) error {
	//TODO implement me
	panic("implement me")
}

func (i instance) Stat() (fi os.FileInfo, err error) {
	fi, ok := i.handle.SingleStat(i.key)
	if !ok {
		err = fs.ErrNotExist
		return
	}
	return
}

func (i instance) ReadAt(p []byte, off int64) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (i instance) WriteAt(bytes []byte, i2 int64) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (i instance) Delete() error {
	//TODO implement me
	panic("implement me")
}

var _ resource.Instance = instance{}
