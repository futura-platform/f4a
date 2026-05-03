package pool

import (
	"context"
	"fmt"
	"iter"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/samber/mo"
)

// ActiveRunners is a set of runners that can accept tasks.
// It is NOT strongly consistent, and should only be used for best effort fast paths.
// (this is because runner pods can fail to drain without committing their liveness marker)
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

// RunnerIDFromLivenessKey unpacks runner ID from a key in the active_runners liveness range.
func (a ActiveRunners) RunnerIDFromLivenessKey(key fdb.Key) (string, error) {
	decoded, err := a.livenessMarkers.Unpack(key)
	if err != nil {
		return "", fmt.Errorf("unpack active runner liveness key: %w", err)
	}
	if len(decoded) != 1 {
		return "", fmt.Errorf("active runner liveness key: expected 1-tuple, got %d", len(decoded))
	}
	runnerID, ok := decoded[0].(string)
	if !ok {
		return "", fmt.Errorf("active runner liveness key: expected string runner id")
	}
	return runnerID, nil
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

func (a ActiveRunners) Iterate(ctx context.Context, tr fdb.ReadTransactor) iter.Seq[mo.Either[error, fdb.KeyValue]] {
	begin, end := a.livenessMarkers.FDBRangeKeys()
	return dbutil.UnboundedIterate(ctx, tr, fdb.KeyRange{Begin: begin, End: end}, 100)
}
