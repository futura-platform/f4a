package dbutil

import (
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

var globalCounter = atomic.Uint64{}

// IncompleteGloballyOrderedVersionstamp returns an incomplete versionstamp +
// a globally (within the process) monotonically increasing suffix
func IncompleteGloballyOrderedVersionstamp() tuple.Tuple {
	return tuple.Tuple{
		tuple.IncompleteVersionstamp(0),
		globalCounter.Add(1),
	}
}
