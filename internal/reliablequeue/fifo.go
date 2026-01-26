package reliablequeue

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/pkg/util/db"
)

// FIFO is an implementation of a FIFO queue built on FoundationDB.
// It is gauranteed to be contention free on write operations
// (unless versiontimestamp collisions occur across FDB shards).
type FIFO struct {
	t fdb.Transactor

	// maxItemSizeBytes caps the size of values stored in the queue.
	// It also drives batch sizing for unbounded reads.
	maxItemSizeBytes int
	// epochKey changes on Enqueue and Dequeue.
	// It lives outside the queue subspace so consumers can watch for those events.
	epochKey fdb.Key
	// this is the subspace of the queue, it is used to store the items in the queue
	subspace directory.DirectorySubspace
	// enqueueCounter disambiguates versionstamp keys within a transaction.
	enqueueCounter uint64
}

const (
	targetIterateBatchBytes = 4 * 1024 * 1024
	queueEntryOverheadBytes = 64
)

type FIFOOptions struct {
	// MaxItemSize caps the size of values stored in the queue.
	// It also drives batch sizing for unbounded reads.
	MaxItemSize int
}

func CreateOrOpenFIFO(t fdb.Transactor, path []string, opts FIFOOptions) (*FIFO, error) {
	if opts.MaxItemSize <= 0 {
		return nil, fmt.Errorf("max item size must be greater than 0")
	}
	var fifo *FIFO
	_, err := t.Transact(func(tx fdb.Transaction) (any, error) {
		subspace, err := directory.CreateOrOpen(tx, path, nil)
		if err != nil {
			return nil, err
		}

		metaPath := append(append([]string{}, path...), "_meta")
		metaSubspace, err := directory.CreateOrOpen(tx, metaPath, nil)
		if err != nil {
			return nil, err
		}

		fifo = &FIFO{
			t:                t,
			maxItemSizeBytes: opts.MaxItemSize,
			epochKey:         metaSubspace.Pack(tuple.Tuple{"epoch"}),
			subspace:         subspace,
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	return fifo, nil
}

func (q *FIFO) batchSizeForItemBytes(itemBytes int) int {
	batch := targetIterateBatchBytes / itemBytes
	if batch < 1 {
		return 1
	}
	return batch
}

func (q *FIFO) rangeBatchSize() int {
	return q.batchSizeForItemBytes(q.maxItemSizeBytes + queueEntryOverheadBytes)
}

// Enqueue enqueues an item into the queue, within a given transaction.
func (q *FIFO) Enqueue(tx fdb.Transaction, item []byte) error {
	if len(item) > q.maxItemSizeBytes {
		return fmt.Errorf("%w: %d > %d", ErrItemTooLarge, len(item), q.maxItemSizeBytes)
	}
	k, err := q.subspace.PackWithVersionstamp(tuple.Tuple{
		tuple.IncompleteVersionstamp(0),
		atomic.AddUint64(&q.enqueueCounter, 1),
	})
	if err != nil {
		return err
	}
	tx.SetVersionstampedKey(k, item)
	dbutil.AtomicIncrement(tx, q.epochKey)
	return nil
}

var ErrQueueEmpty = errors.New("queue is empty")
var ErrItemTooLarge = errors.New("queue item is too large")

// Dequeue dequeues an item from the queue, within a given transaction.
// It returns the item and a boolean indicating if the item was successfully dequeued.
func (q *FIFO) Dequeue(tx fdb.Transaction) ([]byte, error) {
	begin, end := q.subspace.FDBRangeKeys()
	kvs, err := tx.GetRange(
		fdb.KeyRange{Begin: begin, End: end},
		fdb.RangeOptions{Limit: 1},
	).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		// queue is empty
		return nil, ErrQueueEmpty
	}
	kv := kvs[0]
	tx.Clear(kv.Key)
	dbutil.AtomicIncrement(tx, q.epochKey)
	return kv.Value, nil
}
