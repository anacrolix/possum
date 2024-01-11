package possum

import (
	possumC "github.com/anacrolix/possum/go/cpossum"
	"os"
	"syscall"
)

type Writer struct {
	c possumC.Writer
}

func (me Handle) NewWriter() Writer {
	return Writer{possumC.NewWriter(me.cHandle)}
}

type ValueWriter struct {
	c     possumC.ValueWriter
	files []*os.File
}

func (me Writer) StartNewValue() (vw *ValueWriter, err error) {
	c, err := possumC.StartNewValue(me.c)
	if err != nil {
		return
	}
	vw = &ValueWriter{c, nil}
	return
}

func (me Writer) Stage(key []byte, value *ValueWriter) error {
	for _, f := range value.files {
		f.Close()
	}
	return possumC.StageWrite(me.c, key, value.c)
}

// Should this be exposed?
func (me *ValueWriter) Fd() uintptr {
	return uintptr(possumC.ValueWriterFd(me.c))
}

func (me *ValueWriter) NewFile(name string) (f *os.File, err error) {
	// I wonder if closing this will close the fd belong to possum. If so, we should dup, and then
	// kill it remotely if the writer is committed.
	fd, err := syscall.Dup(int(me.Fd()))
	if err != nil {
		return
	}
	f = os.NewFile(uintptr(fd), name)
	me.files = append(me.files, f)
	return
}

func (me Writer) Commit() error {
	return possumC.CommitWriter(me.c)
}
