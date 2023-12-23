package possum

import (
	possumC "github.com/anacrolix/possum/go/cpossum"
)

type Handle struct {
	cHandle *possumC.Handle
}

func Open(dir string) (Handle, error) {
	cHandle := possumC.NewHandle(dir)
	return Handle{cHandle}, nil
}

func (me Handle) Close() error {
	possumC.DropHandle(me.cHandle)
	return nil
}
