package dbutil

import (
	"context"
	"iter"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/samber/mo"
)

type KeyValue struct {
	Key   fdb.Key
	Value []byte
}

// UnboundedIterate iterates over a range of keys in the database.
// It does not gaurantee consistency, since it uses a snapshot and is not in a single transaction.
// This helper should be used for unbounded iteration of large ranges of keys, to avoid running into FDB's transaction size limits.
func UnboundedIterate[T fdb.ReadTransactor](
	ctx context.Context,
	tr T,
	iterRange fdb.KeyRange,
	batchSize int,
) iter.Seq[mo.Either[error, KeyValue]] {
	return func(yield func(mo.Either[error, KeyValue]) bool) {
		exhausted := false
		for !exhausted {
			var batch []fdb.KeyValue
			_, err := TransactContext(ctx, tr.ReadTransact, func(rt fdb.ReadTransaction) (any, error) {
				batch = rt.Snapshot().GetRange(iterRange, fdb.RangeOptions{
					// read one extra to check if we're at the end
					Limit: batchSize + 1,
				}).GetSliceOrPanic()
				exhausted = len(batch) <= batchSize
				return nil, nil
			})
			if err != nil {
				yield(mo.Left[error, KeyValue](err))
				return
			}
			for _, kv := range batch {
				if !yield(mo.Right[error](KeyValue{Key: kv.Key, Value: kv.Value})) {
					return
				}
				iterRange.Begin = KeyAfter(kv.Key)
			}
		}
	}
}
