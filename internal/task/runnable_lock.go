package task

import (
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/futura-platform/f4a/internal/reliablelock"
)

func (k TaskKey) RunnableLock(db fdb.Transactor, holder string) *reliablelock.Lock[string] {
	return reliablelock.NewLock(
		db,
		k.d.Pack(tuple.Tuple{k.id.Bytes(), "lock"}),
		holder,
		reliablelock.WithLeaseDuration(10*time.Second),
		reliablelock.WithRefreshInterval(1*time.Second),
	)
}
