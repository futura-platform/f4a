package fdbexec

import (
	"bytes"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The loader relies on two properties of the tuple layer.
func TestTupleLayout(t *testing.T) {
	t.Run("an element unpacks as the one type its layout packs", func(t *testing.T) {
		// the type is stored with the element, so it is never a raw []byte
		for _, tc := range []struct {
			packed   tuple.TupleElement
			unpacked tuple.TupleElement
		}{
			{int(7), int64(7)}, // callOrderIndexKey packs an int
			{int64(7), int64(7)},
			{"length", "length"},
			{[]byte{1, 2}, []byte{1, 2}},
		} {
			elems, err := tuple.Unpack(tuple.Tuple{tc.packed}.Pack())
			require.NoError(t, err)
			assert.IsType(t, tc.unpacked, elems[0])
			assert.Equal(t, tc.unpacked, elems[0])
		}
	})
	t.Run("the length key sorts before every index key", func(t *testing.T) {
		// so the index keys are the range from index 0 to the end of the subspace
		length := tuple.Tuple{callOrderLengthElement}.Pack()
		for _, index := range []int{0, 1, 255, 256, 1 << 40} {
			assert.Less(t, bytes.Compare(length, tuple.Tuple{index}.Pack()), 0, "index %d", index)
		}
	})
}
