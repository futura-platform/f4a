package reliableset

import (
	"context"

	mapset "github.com/deckarep/golang-set/v2"
	dbutil "github.com/futura-platform/f4a/pkg/util/db"
)

// snapshot returns the current snapshot of the set. (NOT including the log entries)
func (s *Set) snapshot(ctx context.Context, readVersion *int64) (mapset.Set[string], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts := dbutil.IterateUnboundedOptions{
		BatchSize:   s.snapshotBatchSize(),
		ReadVersion: readVersion,
		Snapshot:    true,
	}
	snapshot := mapset.NewSet[string]()
	for chunk, err := range dbutil.IterateUnbounded(ctx, s.t, s.snapshotSubspace, opts) {
		if err != nil {
			return nil, err
		}
		for _, snapshotEntry := range chunk {
			snapshot.Add(string(snapshotEntry.Value))
		}
	}
	return snapshot, nil
}
