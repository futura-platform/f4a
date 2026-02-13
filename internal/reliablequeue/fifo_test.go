package reliablequeue

import (
	"fmt"
	"sync"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fifoPath(db dbutil.DbRoot, name string) []string {
	path := append([]string{}, db.Root.GetPath()...)
	path = append(path, "fifo", name)
	return path
}

func newFIFO(db dbutil.DbRoot, name string) *FIFO {
	fifo, err := CreateOrOpenFIFO(db.Database, fifoPath(db, name))
	if err != nil {
		panic(err)
	}
	return fifo
}

type transactorWithNotify struct {
	fdb.Transactor
	ready chan struct{}
	once  sync.Once
}

func (t *transactorWithNotify) Transact(fn func(fdb.Transaction) (any, error)) (any, error) {
	result, err := t.Transactor.Transact(fn)
	t.once.Do(func() {
		close(t.ready)
	})
	return result, err
}

func enqueue(t testing.TB, db dbutil.DbRoot, queue *FIFO, item []byte) {
	t.Helper()
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		return nil, queue.Enqueue(tx, item)
	})
	require.NoError(t, err)
}

func dequeue(t testing.TB, db dbutil.DbRoot, queue *FIFO) ([]byte, error) {
	t.Helper()
	var item []byte
	var err error
	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		item, err = queue.Dequeue(tx)
		return nil, err
	})
	require.NoError(t, err)
	return item, err
}

func TestFIFOEnqueueDequeueOrder(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		queue := newFIFO(db, "order")
		items := [][]byte{
			[]byte("first"),
			[]byte("second"),
			[]byte("third"),
		}

		for _, item := range items {
			enqueue(t, db, queue, item)
		}

		for _, expected := range items {
			item, err := dequeue(t, db, queue)
			require.NoError(t, err)
			assert.Equal(t, expected, item)
		}

		_, err := dequeueAllowEmpty(t, db, queue)
		require.ErrorIs(t, err, ErrQueueEmpty)
	})
}

func TestFIFOMultipleEnqueueSingleTransaction(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		queue := newFIFO(db, "multi_enqueue_tx")
		items := [][]byte{
			[]byte("first"),
			[]byte("second"),
			[]byte("third"),
		}

		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			for _, item := range items {
				err := queue.Enqueue(tx, item)
				if err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		require.NoError(t, err)

		requireQueueMatchesDB(t, db, queue, items)
	})
}

func TestFIFOMultipleDequeueSingleTransaction(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		queue := newFIFO(db, "multi_dequeue_tx")
		items := [][]byte{
			[]byte("first"),
			[]byte("second"),
			[]byte("third"),
			[]byte("fourth"),
		}
		enqueueBatch(t, db, queue, items)

		const dequeueCount = 3
		dequeued, err := dequeueBatch(t, db, queue, dequeueCount)
		require.NoError(t, err)
		require.Equal(t, items[:dequeueCount], dequeued)

		requireQueueMatchesDB(t, db, queue, items[dequeueCount:])
	})
}

func TestFIFOCreateOrOpenReusesPath(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		path := fifoPath(db, "reopen")
		queue, err := CreateOrOpenFIFO(db.Database, path)
		require.NoError(t, err)
		enqueue(t, db, queue, []byte("payload"))

		reopened, err := CreateOrOpenFIFO(db.Database, path)
		require.NoError(t, err)
		item, err := dequeue(t, db, reopened)
		require.NoError(t, err)
		assert.Equal(t, []byte("payload"), item)
	})
}

// TestFIFOEpochKeyChangesOnOperations verifies that the epoch key changes on
// every enqueue and dequeue, and does not change on an empty dequeue.
func TestFIFOEpochKeyChangesOnOperations(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		queue := newFIFO(db, "epoch_key_invariant")

		readEpoch := func() []byte {
			var val []byte
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				val = tx.Get(queue.epochKey).MustGet()
				return nil, nil
			})
			require.NoError(t, err)
			return val
		}

		epoch0 := readEpoch()

		enqueue(t, db, queue, []byte("item1"))
		epoch1 := readEpoch()
		assert.NotEqual(t, epoch0, epoch1, "Enqueue should change epochKey")

		enqueue(t, db, queue, []byte("item2"))
		epoch2 := readEpoch()
		assert.NotEqual(t, epoch1, epoch2, "second Enqueue should change epochKey")

		item, err := dequeue(t, db, queue)
		require.NoError(t, err)
		assert.Equal(t, []byte("item1"), item)
		epoch3 := readEpoch()
		assert.NotEqual(t, epoch2, epoch3, "Dequeue should change epochKey")

		item, err = dequeue(t, db, queue)
		require.NoError(t, err)
		assert.Equal(t, []byte("item2"), item)
		epoch4 := readEpoch()
		assert.NotEqual(t, epoch3, epoch4, "second Dequeue should change epochKey")

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			_, err := queue.Dequeue(tx)
			return nil, err
		})
		require.ErrorIs(t, err, ErrQueueEmpty)
		epoch5 := readEpoch()
		assert.Equal(t, epoch4, epoch5, "empty Dequeue should NOT change epochKey")

		enqueue(t, db, queue, []byte("item3"))
		epoch6 := readEpoch()
		assert.NotEqual(t, epoch5, epoch6, "Enqueue after dequeues should change epochKey")
	})
}

func TestFIFOConcurrentDequeue(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		assert.NoError(t, db.Options().SetTransactionRetryLimit(5))

		queue := newFIFO(db, "concurrent")
		const total = 10

		for i := range total {
			enqueue(t, db, queue, fmt.Appendf(nil, "item-%d", i))
		}

		start := make(chan struct{})
		results := make(chan string, total)
		errCh := make(chan error, total)
		var wg sync.WaitGroup
		for range total {
			wg.Go(func() {
				<-start

				var item []byte
				var err error
				_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
					item, err = queue.Dequeue(tx)
					return nil, nil
				})
				if err != nil {
					errCh <- err
					return
				}
				results <- string(item)
			})
		}

		close(start)

		wg.Wait()
		close(results)
		close(errCh)

		for err := range errCh {
			require.NoError(t, err)
		}

		seen := make(map[string]struct{}, total)
		for item := range results {
			if _, exists := seen[item]; exists {
				t.Fatalf("duplicate item: %q", item)
			}
			seen[item] = struct{}{}
		}

		assert.Len(t, seen, total)

		_, err := dequeueAllowEmpty(t, db, queue)
		require.ErrorIs(t, err, ErrQueueEmpty)
	})
}

func BenchmarkFIFOEnqueue(b *testing.B) {
	testutil.WithEphemeralDBRoot(b, func(db dbutil.DbRoot) {
		queue := newFIFO(db, "bench_enqueue")
		payload := []byte("payload")

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			if _, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				return nil, queue.Enqueue(tx, payload)
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkFIFODequeue(b *testing.B) {
	testutil.WithEphemeralDBRoot(b, func(db dbutil.DbRoot) {
		queue := newFIFO(db, "bench_dequeue")
		payload := []byte("payload")

		b.ReportAllocs()
		b.StopTimer()

		for i := 0; i < b.N; i++ {
			if _, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				return nil, queue.Enqueue(tx, payload)
			}); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		for b.Loop() {
			if _, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				_, err := queue.Dequeue(tx)
				return nil, err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
