package rocksdb

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type CloseGuard struct {
	// yztodo: optimize this lock
	lock      sync.Mutex
	closed    *atomic.Bool
	counter   *atomic.Int32
	allDoneCh chan struct{}
}

func newCloseGuard() *CloseGuard {
	var closed atomic.Bool
	closed.Store(false)

	var counter atomic.Int32
	counter.Store(0)

	return &CloseGuard{
		closed:    &closed,
		counter:   &counter,
		allDoneCh: make(chan struct{}, 0),
	}
}

// return true if reference success
func (cg *CloseGuard) reference() bool {
	cg.lock.Lock()
	defer cg.lock.Unlock()

	if cg.closed.Load() {
		return false
	}
	cg.counter.Add(1)

	// check closed again to see if it is closed during we increment counter
	if cg.closed.Load() {
		if 0 != cg.counter.Add(-1) {
			panic("close guard: has closed but counter is not zero")
		}
		return false
	}

	return true
}

func (cg *CloseGuard) deReference() {
	cg.lock.Lock()
	defer cg.lock.Unlock()

	if cg.subAndCheckCloseability(-1) {
		close(cg.allDoneCh)
	}
}

// return true if this is the first time call close
func (cg *CloseGuard) close() bool {
	cg.lock.Lock()
	defer cg.lock.Unlock()

	firstClose := cg.closed.CompareAndSwap(false, true)

	// yztodo: concurrent problem
	if firstClose && cg.subAndCheckCloseability(0) {
		close(cg.allDoneCh)
	}

	// no matter this is the first time call close,
	// we should wait all done
	cg.waitAllDoneOnClose()

	return firstClose
}

func (cg *CloseGuard) subAndCheckCloseability(delta int32) bool {
	currentCounter := cg.counter.Add(delta)
	if currentCounter < 0 {
		panic(fmt.Sprintf("close guard: counter is %d, which is illegal: less than 0", currentCounter))
	}

	return (currentCounter == 0) && cg.closed.Load()
}

func (cg *CloseGuard) waitAllDoneOnClose() {
	if !cg.subAndCheckCloseability(0) {
		<-cg.allDoneCh
	}
}
