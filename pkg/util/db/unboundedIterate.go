package dbutil

import (
	"context"
	"iter"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type IterateUnboundedOptions struct {
	// BatchSize is the maximum number of key-values per chunk.
	// This should be set to a value that is large enough to be efficient,
	// but small enough to always fit in the transaction size limit.
	BatchSize int
	// ReadVersion pins all batches to a fixed read version (if non-nil).
	ReadVersion *int64
	// RangeOptions controls streaming mode and other range read options.
	// (Mode is not used)
	RangeOptions fdb.RangeOptions
	// Snapshot enables snapshot reads for each batch.
	Snapshot bool
}

// IterateUnbounded iterates over a large range in batches over multiple transactions.
// This allows for iterating over a large range without running into the transaction size limit.
func IterateUnbounded(ctx context.Context, tr fdb.Transactor, r fdb.Range, opts IterateUnboundedOptions) iter.Seq2[[]fdb.KeyValue, error] {
	if opts.BatchSize <= 0 {
		panic("batchSize must be greater than 0")
	}
	begin, end := r.FDBRangeKeySelectors()
	currentRange := fdb.SelectorRange{
		Begin: begin,
		End:   end,
	}
	rangeOpts := opts.RangeOptions
	rangeOpts.Limit = opts.BatchSize

	return func(yield func([]fdb.KeyValue, error) bool) {
		for {
			if err := ctx.Err(); err != nil {
				_ = yield(nil, err)
				return
			}
			var chunk []fdb.KeyValue
			readChunk := func(tx fdb.ReadTransaction) error {
				readTx := tx
				if opts.Snapshot {
					readTx = tx.Snapshot()
				}
				var err error
				chunk, err = readTx.GetRange(currentRange, rangeOpts).GetSliceWithError()
				return err
			}

			var err error
			if opts.ReadVersion != nil {
				_, err = tr.Transact(func(tx fdb.Transaction) (any, error) {
					tx.SetReadVersion(*opts.ReadVersion)
					return nil, readChunk(tx)
				})
			} else {
				_, err = tr.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
					return nil, readChunk(tx)
				})
			}

			if err != nil {
				_ = yield(nil, err)
				return
			}
			if len(chunk) == 0 {
				return
			} else if !yield(chunk, nil) {
				return
			}

			if rangeOpts.Reverse {
				currentRange.End = fdb.FirstGreaterOrEqual(chunk[len(chunk)-1].Key)
			} else {
				currentRange.Begin = fdb.FirstGreaterThan(chunk[len(chunk)-1].Key)
			}
		}
	}
}
