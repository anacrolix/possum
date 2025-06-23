package possum

import (
	qt "github.com/go-quicktest/qt"
	"sync"
	"testing"
)

func TestHandleDropWhileReading(t *testing.T) {
	dir, err := Open(t.TempDir())
	qt.Assert(t, qt.IsNil(err))
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
	qt.Check(t, qt.IsTrue(dir.cHandle.ValueDropped().IsSet()))
}

func TestReadDroppedHandle(t *testing.T) {
	dir, err := Open(t.TempDir())
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsNil(dir.Close()))
	b := make([]byte, 420)
	n, err := dir.SingleReadAt("a", 69, b)
	qt.Assert(t, qt.ErrorIs(err, ErrHandleClosed))
	qt.Check(t, qt.Equals(n, 0))
	qt.Check(t, qt.IsTrue(dir.cHandle.ValueDropped().IsSet()))
}
