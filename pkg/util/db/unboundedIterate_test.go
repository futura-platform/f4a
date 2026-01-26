package dbutil

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/futura-platform/f4a/pkg/util"
	testutil "github.com/futura-platform/f4a/pkg/util/test"
	"github.com/stretchr/testify/require"
)

func seedRange(t testing.TB, db util.DbRoot, sub interface{ Pack(tuple.Tuple) fdb.Key }, start, count int) []fdb.KeyValue {
	t.Helper()
	kvs := make([]fdb.KeyValue, count)
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		for i := range count {
			id := start + i
			key := sub.Pack(tuple.Tuple{id})
			value := []byte(fmt.Sprintf("value-%d", id))
			tx.Set(key, value)
			kvs[i] = fdb.KeyValue{Key: key, Value: value}
		}
		return nil, nil
	})
	require.NoError(t, err)
	return kvs
}

func collectSeq(seq iter.Seq2[[]fdb.KeyValue, error], onChunk func([]fdb.KeyValue)) ([]fdb.KeyValue, error) {
	var out []fdb.KeyValue
	var err error
	seq(func(chunk []fdb.KeyValue, chunkErr error) bool {
		if chunkErr != nil {
			err = chunkErr
			return false
		}
		if onChunk != nil {
			onChunk(chunk)
		}
		out = append(out, chunk...)
		return true
	})
	return out, err
}

func TestIterateUnboundedForward(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		sub := db.Root.Sub("iter", "forward")
		expected := seedRange(t, db, sub, 0, 25)

		opts := IterateUnboundedOptions{BatchSize: 7}
		var batchSizes []int
		got, err := collectSeq(
			IterateUnbounded(context.Background(), db.Database, sub, opts),
			func(chunk []fdb.KeyValue) { batchSizes = append(batchSizes, len(chunk)) },
		)
		require.NoError(t, err)
		require.Len(t, got, len(expected))
		require.Equal(t, []int{7, 7, 7, 4}, batchSizes)
		for i := range expected {
			require.True(t, bytes.Equal(expected[i].Key, got[i].Key))
			require.True(t, bytes.Equal(expected[i].Value, got[i].Value))
		}
	})
}

func TestIterateUnboundedReverse(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		sub := db.Root.Sub("iter", "reverse")
		expected := seedRange(t, db, sub, 0, 25)

		opts := IterateUnboundedOptions{
			BatchSize:    6,
			RangeOptions: fdb.RangeOptions{Reverse: true},
		}
		var batchSizes []int
		got, err := collectSeq(
			IterateUnbounded(context.Background(), db.Database, sub, opts),
			func(chunk []fdb.KeyValue) { batchSizes = append(batchSizes, len(chunk)) },
		)
		require.NoError(t, err)
		require.Len(t, got, len(expected))
		require.Equal(t, []int{6, 6, 6, 6, 1}, batchSizes)
		for i := range expected {
			expectedIdx := len(expected) - 1 - i
			require.True(t, bytes.Equal(expected[expectedIdx].Key, got[i].Key))
			require.True(t, bytes.Equal(expected[expectedIdx].Value, got[i].Value))
		}
	})
}

func TestIterateUnboundedReadVersionPinned(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		sub := db.Root.Sub("iter", "read_version")
		expected := seedRange(t, db, sub, 0, 5)

		var readVersion int64
		_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			readVersion = tx.GetReadVersion().MustGet()
			return nil, nil
		})
		require.NoError(t, err)

		_ = seedRange(t, db, sub, 5, 5)

		opts := IterateUnboundedOptions{
			BatchSize:   2,
			ReadVersion: &readVersion,
		}
		got, err := collectSeq(IterateUnbounded(context.Background(), db.Database, sub, opts), nil)
		require.NoError(t, err)
		require.Len(t, got, len(expected))
		for i := range expected {
			require.True(t, bytes.Equal(expected[i].Key, got[i].Key))
			require.True(t, bytes.Equal(expected[i].Value, got[i].Value))
		}
	})
}

func TestIterateUnboundedContextCancel(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		sub := db.Root.Sub("iter", "ctx_cancel")
		_ = seedRange(t, db, sub, 0, 10)

		ctx, cancel := context.WithCancel(context.Background())
		opts := IterateUnboundedOptions{BatchSize: 3}

		var gotErr error
		chunks := 0
		IterateUnbounded(ctx, db.Database, sub, opts)(func(chunk []fdb.KeyValue, err error) bool {
			if err != nil {
				gotErr = err
				return false
			}
			chunks++
			if chunks == 1 {
				cancel()
			}
			return true
		})

		require.ErrorIs(t, gotErr, context.Canceled)
	})
}

func TestIterateUnboundedKeyRange(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		sub := db.Root.Sub("iter", "range")
		all := seedRange(t, db, sub, 0, 20)

		begin := all[5].Key
		end := all[10].Key
		opts := IterateUnboundedOptions{BatchSize: 4}
		got, err := collectSeq(
			IterateUnbounded(context.Background(), db.Database, fdb.KeyRange{Begin: begin, End: end}, opts),
			nil,
		)
		require.NoError(t, err)
		require.Len(t, got, 5)
		for i := range 5 {
			require.True(t, bytes.Equal(all[5+i].Key, got[i].Key))
			require.True(t, bytes.Equal(all[5+i].Value, got[i].Value))
		}
	})
}
