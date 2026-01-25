package reliableset

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
)

// snapshot returns the current snapshot of the set. (NOT including the log entries)
func (s *Set) snapshot(tx fdb.ReadTransaction) (mapset.Set[string], error) {
	begin, end := s.snapshotSubspace.FDBRangeKeys()
	snapshotEntries, err := tx.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{}).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	snapshot := mapset.NewSetWithSize[string](len(snapshotEntries))
	for _, snapshotEntry := range snapshotEntries {
		snapshot.Add(string(snapshotEntry.Value))
	}
	return snapshot, nil
}
