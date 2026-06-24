package dbutil

import (
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

var globalCounter = atomic.Uint64{}

// IncompleteGloballyOrderedVersionstamp returns an incomplete versionstamp +
// IncompleteGloballyOrderedVersionstamp returns a tuple containing an incomplete versionstamp paired with a monotonically increasing counter value, ensuring globally ordered uniqueness within the process.
func IncompleteGloballyOrderedVersionstamp() tuple.Tuple {
	return tuple.Tuple{
		tuple.IncompleteVersionstamp(0),
		globalCounter.Add(1),
	}
}
