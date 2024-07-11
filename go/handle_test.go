package possum

import (
	qt "github.com/frankban/quicktest"
	"sync"
	"testing"
)

func TestHandleDropWhileReading(t *testing.T) {
	c := qt.New(t)
	dir, err := Open(t.TempDir())
	c.Assert(err, qt.IsNil)
	var wg sync.WaitGroup
	wg.Add(2)
	b := make([]byte, 420)
	go func() {
		defer wg.Done()
		n, err := dir.SingleReadAt("a", 69, b)
		_, _ = n, err
	}()
	go func() {
		defer wg.Done()
		dir.Close()
	}()
	wg.Wait()
	c.Check(dir.cHandle.ValueDropped().IsSet(), qt.IsTrue)
}

func TestReadDroppedHandle(t *testing.T) {
	c := qt.New(t)
	dir, err := Open(t.TempDir())
	c.Assert(err, qt.IsNil)
	c.Assert(dir.Close(), qt.IsNil)
	b := make([]byte, 420)
	n, err := dir.SingleReadAt("a", 69, b)
	c.Assert(err, qt.ErrorIs, ErrHandleClosed)
	c.Check(n, qt.Equals, 0)
	c.Check(dir.cHandle.ValueDropped().IsSet(), qt.IsTrue)
}
