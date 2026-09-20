package task

import "github.com/futura-platform/f4a/internal/reliablelock"

func (k TaskKey) RunnableLock() *reliablelock.Lock {
	return reliablelock.NewLock(k.keyspace().Sub("runnable_lock"))
}
