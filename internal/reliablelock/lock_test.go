package reliablelock_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/futura-platform/f4a/internal/reliablelock"
	"github.com/futura-platform/f4a/internal/util"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
)

func lockKey(db util.DbRoot) fdb.Key {
	return db.Root.Pack(tuple.Tuple{"lock"})
}

func readLockValue[T comparable](t *testing.T, db util.DbRoot, key fdb.KeyConvertible) (reliablelock.LeaseValue[T], bool) {
	t.Helper()

	var raw []byte
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		value, err := tx.Get(key).Get()
		if err != nil {
			return nil, err
		}
		raw = value
		return nil, nil
	})
	if !assert.NoError(t, err) {
		return reliablelock.LeaseValue[T]{}, false
	}
	if raw == nil {
		return reliablelock.LeaseValue[T]{}, false
	}

	decoder := privateencoding.NewDecoder[reliablelock.LeaseValue[T]](bytes.NewReader(raw))
	value, err := decoder.Decode()
	if !assert.NoError(t, err) {
		return reliablelock.LeaseValue[T]{}, false
	}
	return value, true
}

type lockHolderMetadata struct {
	ID     string
	Region string
}

func TestLockAcquireRelease(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		key := lockKey(db)
		metadata := "holder-1"
		lock := reliablelock.NewLock(db.Database, key, metadata)

		err := lock.Acquire(t.Context())
		assert.NoError(t, err)

		value, ok := readLockValue[string](t, db, key)
		assert.True(t, ok)
		assert.Equal(t, metadata, value.Holder)
		assert.True(t, time.Unix(0, value.LeaseExpiration).After(time.Now()))

		err = lock.Release()
		assert.NoError(t, err)

		_, ok = readLockValue[string](t, db, key)
		assert.False(t, ok)
	})
}

func TestLockStoresCorrectValue(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		key := lockKey(db)
		holder := lockHolderMetadata{
			ID:     "task-123",
			Region: "us-west-2",
		}
		leaseDuration := 400 * time.Millisecond
		lock := reliablelock.NewLock(db.Database, key, holder,
			reliablelock.WithLeaseDuration(leaseDuration),
			reliablelock.WithRefreshInterval(leaseDuration*10),
		)

		start := time.Now()
		err := lock.Acquire(t.Context())
		assert.NoError(t, err)

		value, ok := readLockValue[lockHolderMetadata](t, db, key)
		assert.True(t, ok)
		assert.Equal(t, holder, value.Holder)
		assert.WithinDuration(t, start.Add(leaseDuration), time.Unix(0, value.LeaseExpiration), 500*time.Millisecond)

		assert.NoError(t, lock.Release())
	})
}

func TestLockBlocksUntilReleased(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		key := lockKey(db)
		holder := reliablelock.NewLock(db.Database, key, "holder")
		waiter := reliablelock.NewLock(db.Database, key, "waiter")

		err := holder.Acquire(t.Context())
		assert.NoError(t, err)

		acquired := make(chan error, 1)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()

		go func() {
			acquired <- waiter.Acquire(ctx)
		}()

		select {
		case err := <-acquired:
			t.Fatalf("lock acquired early: %v", err)
		case <-time.After(200 * time.Millisecond):
		}

		err = holder.Release()
		assert.NoError(t, err)

		select {
		case err := <-acquired:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for lock acquisition")
		}
	})
}

func TestLockReleaseNotOwner(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		key := lockKey(db)
		holder := reliablelock.NewLock(db.Database, key, "holder")
		other := reliablelock.NewLock(db.Database, key, "other")

		err := holder.Acquire(t.Context())
		assert.NoError(t, err)

		err = other.Release()
		assert.ErrorIs(t, err, reliablelock.ErrNotOwner)
	})
}

func TestLockAcquireContextCanceled(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		key := lockKey(db)
		holder := reliablelock.NewLock(db.Database, key, "holder")
		waiter := reliablelock.NewLock(db.Database, key, "waiter")

		err := holder.Acquire(t.Context())
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()

		err = waiter.Acquire(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestLockHolderDies(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		key := lockKey(db)
		leaseDuration := 200 * time.Millisecond
		holder := reliablelock.NewLock(db.Database, key, "holder",
			reliablelock.WithLeaseDuration(leaseDuration),
			reliablelock.WithRefreshInterval(leaseDuration*4),
		)
		waiter := reliablelock.NewLock(db.Database, key, "waiter", reliablelock.WithLeaseDuration(leaseDuration))

		err := holder.Acquire(context.Background())
		assert.NoError(t, err)

		acquired := make(chan error, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		go func() {
			acquired <- waiter.Acquire(ctx)
		}()

		select {
		case err := <-acquired:
			t.Fatalf("lock acquired early: %v", err)
		case <-time.After(leaseDuration / 2):
		}

		select {
		case err := <-acquired:
			assert.NoError(t, err)
		case <-time.After(leaseDuration * 4):
			t.Fatal("timeout waiting for lock acquisition after lease expiry")
		}
	})
}

func TestLockContention(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		assert.NoError(t, db.Options().SetTransactionRetryLimit(10))
		key := lockKey(db)

		const goroutines = 10
		const iterations = 3

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		start := make(chan struct{})
		errCh := make(chan error, goroutines*iterations)
		var wg sync.WaitGroup
		var active atomic.Int32

		for i := range goroutines {
			lock := reliablelock.NewLock(db.Database, key, fmt.Sprintf("holder-%d", i))
			wg.Go(func() {
				<-start
				for range iterations {
					if err := lock.Acquire(ctx); err != nil {
						errCh <- err
						return
					}
					current := active.Add(1)
					if current != 1 {
						errCh <- fmt.Errorf("lock violated exclusivity: %d holders", current)
					}
					time.Sleep(100 * time.Millisecond)
					active.Add(-1)
					if err := lock.Release(); err != nil {
						errCh <- err
						return
					}
				}
			})
		}

		close(start)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			t.Fatalf("timeout waiting for contention test: %v", ctx.Err())
		}

		close(errCh)
		for err := range errCh {
			assert.NoError(t, err)
		}
	})
}

func BenchmarkLockAcquireRelease(b *testing.B) {
	testutil.WithEphemeralDBRoot(b, func(db util.DbRoot) {
		key := db.Root.Pack(tuple.Tuple{"bench_lock"})
		lock := reliablelock.NewLock(db.Database, key, "bench")
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			if err := lock.Acquire(ctx); err != nil {
				b.Fatal(err)
			}
			if err := lock.Release(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
