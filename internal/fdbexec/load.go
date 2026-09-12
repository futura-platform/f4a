package fdbexec

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/moment"
	"github.com/futura-platform/futura/privateencoding"
)

// loadBatchSize bounds the rows read per transaction while loading, so a large
// state never trips the transaction time limit.
const loadBatchSize = 256

// load reads the container's whole state from the database into memory.
//
// It reads in batches, each in its own transaction, so the image is only
// consistent if nothing else writes the task meanwhile, which holding its
// runnable lease guarantees.
func (c *ExecutionContainer) load(ctx context.Context) (*executiontype.InMemoryContainer, error) {
	s := executiontype.NewInMemoryContainer()

	// the index keys sort after the length key, and by index, so they are the
	// range from index 0 to the end of the subspace
	_, callOrderEnd := c.callOrder.FDBRangeKeys()
	indexKeys := fdb.KeyRange{Begin: callOrderIndexKey(c.callOrder, 0), End: callOrderEnd}
	err := iterate(ctx, c.db, c.callOrder, indexKeys, func(_ int64, value []byte) error {
		identity, err := privateencoding.NewDecoder[moment.Identity](bytes.NewReader(value)).Decode()
		if err != nil {
			return err
		}
		s.AppendCallOrder(identity)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("call order: %w", err)
	}

	memoBegin, memoEnd := c.memoTable.FDBRangeKeys()
	err = iterate(ctx, c.db, c.memoTable, fdb.KeyRange{Begin: memoBegin, End: memoEnd}, func(encodedIdentity []byte, value []byte) error {
		identity, err := privateencoding.NewDecoder[moment.Identity](bytes.NewReader(encodedIdentity)).Decode()
		if err != nil {
			return err
		}
		s.SetMoment(identity, value)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("memo table: %w", err)
	}

	durableBegin, durableEnd := c.durableObjects.FDBRangeKeys()
	err = iterate(ctx, c.db, c.durableObjects, fdb.KeyRange{Begin: durableBegin, End: durableEnd}, func(key string, value []byte) error {
		return s.StoreDurable(key, value)
	})
	if err != nil {
		return nil, fmt.Errorf("durable objects: %w", err)
	}
	return s, nil
}

// iterate calls fn with the single tuple element and the value of every key in
// the range, in key order.
func iterate[E int64 | string | []byte](
	ctx context.Context,
	db fdb.Database,
	sub directory.DirectorySubspace,
	keys fdb.KeyRange,
	fn func(elem E, value []byte) error,
) error {
	for kvOrErr := range dbutil.UnboundedIterate(ctx, db, keys, loadBatchSize) {
		if err, ok := kvOrErr.Left(); ok {
			return err
		}
		kv := kvOrErr.MustRight()
		elems, err := sub.Unpack(kv.Key)
		if err != nil {
			return err
		}
		if len(elems) != 1 {
			return fmt.Errorf("key %v has %d elements, expected 1", kv.Key, len(elems))
		}
		elem, ok := elems[0].(E)
		if !ok {
			return fmt.Errorf("key %v has a %T element, expected %T", kv.Key, elems[0], elem)
		}
		if err := fn(elem, kv.Value); err != nil {
			return err
		}
	}
	return nil
}
