package dbutil

import (
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// AtomicIncrement increments the value at the given key by the given amount.
// it assumed the value is a little endian encoded uint64 (the default for fdb).
func AtomicIncrement(tx fdb.Transaction, key fdb.Key, by int64) {
	var one [8]byte
	binary.LittleEndian.PutUint64(one[:], uint64(by))
	tx.Add(key, one[:])
}
