package possum

import (
	possumC "github.com/anacrolix/possum/go/cpossum"
	"os"
)

type Writer struct {
	c         possumC.Writer
	handleRef *Rc[*possumC.Handle]
}

func (me *Handle) NewWriter() *Writer {
	rc, err := me.cloneRc()
	if err != nil {
		panic(err)
	}
	return &Writer{
		c:         possumC.NewWriter(rc.Deref()),
		handleRef: rc,
	}
}

type ValueWriter struct {
	c     possumC.ValueWriter
	files []*os.File
}

func (me *Writer) StartNewValue() (vw *ValueWriter, err error) {
	c, err := possumC.StartNewValue(me.c)
	if err != nil {
		return
	}
	vw = &ValueWriter{c, nil}
	return
}

func (me *Writer) Stage(key []byte, value *ValueWriter) error {
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
	fd, err := duplicateFileHandle(me.Fd())
	if err != nil {
		return
	}
	f = os.NewFile(fd, name)
	me.files = append(me.files, f)
	return
}

// This consumes the Writer.
func (me *Writer) Commit() error {
	err := possumC.CommitWriter(me.c)
	me.c = nil
	me.handleRef.Drop()
	return err
}

func (me *Writer) Rename(v Value, newKey []byte) {
	possumC.WriterRename(me.c, v.c, newKey)
}
