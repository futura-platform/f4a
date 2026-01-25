package dbutil

import "github.com/apple/foundationdb/bindings/go/src/fdb"

// KeyAfter returns the key after the given key, in lexicographical order.
func KeyAfter(key fdb.Key) fdb.Key {
	if len(key) == 0 {
		return nil
	}
	out := make([]byte, len(key)+1)
	copy(out, key)
	return out
}
