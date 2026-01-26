package reliablequeue

import (
	"errors"
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
	db fdb.Database
	// epochKey changes on Enqueue and Dequeue.
	// It lives outside the queue subspace so consumers can watch for those events.
	epochKey fdb.Key
	// this is the subspace of the queue, it is used to store the items in the queue
	subspace directory.DirectorySubspace
	// enqueueCounter disambiguates versionstamp keys within a transaction.
	enqueueCounter uint64
}

func CreateOrOpenFIFO(t fdb.Transactor, path []string) (*FIFO, error) {
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
			db:       tx.GetDatabase(),
			epochKey: metaSubspace.Pack(tuple.Tuple{"epoch"}),
			subspace: subspace,
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	return fifo, nil
}

// Enqueue enqueues an item into the queue, within a given transaction.
func (q *FIFO) Enqueue(tx fdb.Transaction, item []byte) error {
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
