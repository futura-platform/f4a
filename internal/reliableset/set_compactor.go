package reliableset

import (
	"context"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/internal/reliablelock"
)

// setCompactor owns only the state needed for background compaction.
type setCompactor struct {
	set *Set

	lock *reliablelock.Lock

	runOnce   sync.Once
	runCtx    context.Context
	runCancel context.CancelFunc
	runDone   chan struct{}

	releaseOnce sync.Once
}

func newSetCompactor(set *Set, lockDir directory.DirectorySubspace) *setCompactor {
	return &setCompactor{set: set, lock: reliablelock.NewLock(lockDir)}
}

func (c *setCompactor) Run() func() {
	c.runOnce.Do(func() {
		c.runCtx, c.runCancel = context.WithCancel(context.Background())
		c.runDone = make(chan struct{})

		go func() {
			defer close(c.runDone)
			_ = c.runCompactionLoop()
		}()
	})
	return c.release
}

func (c *setCompactor) release() {
	c.releaseOnce.Do(func() {
		if c.runCancel == nil {
			return
		}
		c.runCancel()
		<-c.runDone
	})
}
