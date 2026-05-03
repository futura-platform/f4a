package reliableset

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// snapshot returns the current snapshot of the set. (NOT including the log entries)
func (s *Set) snapshot(ctx context.Context, tr fdb.ReadTransactor) (mapset.Set[string], error) {
	begin, end := s.snapshotSubspace.FDBRangeKeys()
	snapshot := mapset.NewSet[string]()
	for kvOrErr := range dbutil.UnboundedIterate(ctx, tr, fdb.KeyRange{Begin: begin, End: end}, 100) {
		if err, ok := kvOrErr.Left(); ok {
			return nil, err
		}
		kv := kvOrErr.MustRight()
		snapshot.Add(string(kv.Value))
	}
	return snapshot, nil
}
