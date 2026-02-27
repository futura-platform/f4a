package reliableset

import (
	"context"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/futura-platform/f4a/internal/reliablelock"
)

// setCompactor owns only the state needed for background compaction.
type setCompactor struct {
	set *Set

	lockInitOnce sync.Once
	lock         *reliablelock.Lock[string]

	runOnce   sync.Once
	runCtx    context.Context
	runCancel context.CancelFunc
	runDone   chan struct{}

	releaseOnce sync.Once
}

func newSetCompactor(set *Set) *setCompactor {
	return &setCompactor{set: set}
}

func (c *setCompactor) Run() func() {
	c.runOnce.Do(func() {
		c.ensureLock()
		c.runCtx, c.runCancel = context.WithCancel(context.Background())
		c.runDone = make(chan struct{})

		go func() {
			defer close(c.runDone)
			_ = c.runCompactionLoop()
		}()
	})
	return c.release
}

func (c *setCompactor) ensureLock() {
	c.lockInitOnce.Do(func() {
		c.lock = reliablelock.NewLock(
			c.set.db,
			c.set.metadataSubspace.Pack(tuple.Tuple{"compactionLock"}),
			c.set.consumerID,
			reliablelock.WithLeaseDuration(2*compactionInterval),
			reliablelock.WithRefreshInterval(compactionInterval/2),
		)
	})
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
