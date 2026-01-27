package dbutil

import (
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// AtomicIncrement increments the value at the given key by 1.
// it assumed the value is a little endian encoded uint64 (the default for fdb).
func AtomicIncrement(tx fdb.Transaction, key fdb.Key) {
	var one [8]byte
	binary.LittleEndian.PutUint64(one[:], 1)
	tx.Add(key, one[:])
}
