package pool

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

type ActiveRunners struct {
	livenessMarkers directory.DirectorySubspace
}

func activeRunnersPath() []string {
	return []string{"active_runners"}
}

func CreateOrOpenActiveRunners(db dbutil.DbRoot) (ActiveRunners, error) {
	subspace, err := db.Root.CreateOrOpen(db, activeRunnersPath(), nil)
	if err != nil {
		return ActiveRunners{}, err
	}
	return ActiveRunners{
		livenessMarkers: subspace,
	}, nil
}

func (a ActiveRunners) livenessMarkerKey(runnerId string) fdb.Key {
	return a.livenessMarkers.Pack(tuple.Tuple{runnerId})
}

func (a ActiveRunners) IsActive(tx fdb.ReadTransaction, runnerId string) *dbutil.Future[bool] {
	f := tx.Get(a.livenessMarkerKey(runnerId))

	return dbutil.NewFuture(f, func(b []byte) (bool, error) {
		return b != nil, nil
	})
}

func (a ActiveRunners) SetActive(tx fdb.Transaction, runnerId string, active bool) {
	if active {
		tx.Set(a.livenessMarkerKey(runnerId), nil)
	} else {
		tx.Clear(a.livenessMarkerKey(runnerId))
	}
}
