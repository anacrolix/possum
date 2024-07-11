package possum

import (
	"github.com/anacrolix/chansync"
	"github.com/anacrolix/generics"
	"runtime"
	"sync/atomic"
)

type Rc[T any] struct {
	inner  *rcInner[T]
	inited bool
}

type rcInner[T any] struct {
	count        atomic.Int32
	value        T
	dropValue    func(T)
	valueDropped chansync.SetOnce
}

func (me *Rc[T]) Init(t T, dropValue func(T)) {
	if me.inited {
		panic("rc already inited")
	}
	inner := rcInner[T]{
		value:     t,
		dropValue: dropValue,
	}
	inner.count.Store(1)
	*me = Rc[T]{
		inner:  &inner,
		inited: true,
	}
}

func NewRc[T any](t T, dropValue func(T)) (rc *Rc[T]) {
	generics.InitNew(&rc)
	rc.Init(t, dropValue)
	// This probably isn't needed, and probably has a crazy high overhead. Can't set it in Init in
	// case it's an in place value initialization that doesn't align the start of the memory block.
	runtime.SetFinalizer(
		rc,
		// This should be abstracted as an "is dropped", or "Drop" helper.
		func(*Rc[T]) {
			panic("rc not dropped")
		})
	return
}

// Return the value. TODO: Should we allow modifying the value? In Rust we'd get a reference, but in
// Go we're more than likely working with a pointer resource, but it would be possible to do
// synchronization external to Rc to make it useful as a value type.
func (me *Rc[T]) Deref() T {
	if !me.inited {
		panic("dropped")
	}
	return me.inner.value
}

// It's a logical error to do this more than once. Figure your shit out.
func (me *Rc[T]) Drop() {
	if !me.inited {
		panic("Rc not inited")
	}
	me.inited = false
	me.inner.decRef()
	// I'm leaving this alive for now so you can interrogate the inner state from a dead Rc.
	//me.inner = nil
}

func (me *Rc[T]) Clone() (cloned *Rc[T]) {
	// What happens here if this is a dead ref? It will probably panic, but it might not if there
	// are still other live Rcs. Should probably check for inited here or set inner to nil in
	// Rc.Drop.
	me.inner.incRef()
	// Do we need to do anything with dropped here? If we cloned from an already dropped Rc, we
	// should panic when we try to drop this new Rc.
	generics.InitNew(&cloned)
	*cloned = *me
	if me == cloned {
		// Really, it does this if you do &*. I mean I know it's a rvalue and stuff but not what I
		// expected.
		panic("fuck you go")
	}
	return
}

// The return is set when the inner value is dropped.
func (me *Rc[T]) ValueDropped() *chansync.SetOnce {
	return &me.inner.valueDropped
}

func (me *rcInner[T]) decRef() {
	newCount := me.count.Add(-1)
	if newCount > 0 {
		return
	} else if newCount == 0 {
		me.dropValue(me.value)
		me.valueDropped.Set()
		generics.SetZero(&me.value)
		me.dropValue = nil
	} else {
		panic(newCount)
	}
}

func (me *rcInner[T]) incRef() {
	newCount := me.count.Add(1)
	if newCount > 1 {
		return
	}
	panic("value already dropped")
}
