package dbutil_test

import (
	"fmt"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeRangeThenCheckIterate(t *testing.T, db dbutil.DbRoot, expectedValues [][]byte, batchSize int) {
	t.Helper()

	directory, err := db.Root.Create(db, []string{t.Name(), "range_container"}, nil)
	require.NoError(t, err)

	expected := make([]fdb.KeyValue, 0, len(expectedValues))
	for i, value := range expectedValues {
		kv := fdb.KeyValue{
			Key:   directory.Pack(tuple.Tuple{fmt.Sprintf("key-%d", i)}),
			Value: value,
		}
		expected = append(expected, kv)
		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			tx.Set(kv.Key, kv.Value)
			return nil, nil
		})
		require.NoError(t, err)
	}

	seq := dbutil.UnboundedIterate(t.Context(), db, fdb.KeyRange{Begin: expected[0].Key, End: expected[len(expected)-1].Key}, batchSize)
	i := 0
	for kvOrErr := range seq {
		kv := kvOrErr.MustRight()
		assert.Equal(t, string(kv.Value), string(expectedValues[i]))
		i++
	}
}

func TestUnboundedIteration(t *testing.T) {
	basicValues := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
	}

	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		t.Run("minimum batch size", func(t *testing.T) {
			writeRangeThenCheckIterate(t, db, basicValues, 1)
		})
		t.Run("large batch size", func(t *testing.T) {
			writeRangeThenCheckIterate(t, db, basicValues, len(basicValues))
		})
		t.Run("intermediate batch size", func(t *testing.T) {
			writeRangeThenCheckIterate(t, db, basicValues, len(basicValues)/2)
		})
	})
}
