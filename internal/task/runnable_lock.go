package task

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliablelock"
)

func (k TaskKey) RunnableLock(db fdb.Transactor) *reliablelock.Lock {
	lockDir, err := k.d.CreateOrOpen(db, []string{"runnable_lock"}, nil)
	if err != nil {
		panic(err)
	}
	return reliablelock.NewLock(lockDir)
}
