package scheduler

import (
	"log/slog"
	"reflect"

	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sync/singleflight"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// runnerSetCache is a cache of active runner sets.
// It is used as an in memory cache of active runner sets.
// Keys are only invalidated when the runner pod is deleted.
type runnerSetCache struct {
	db           dbutil.DbRoot
	accessFlight singleflight.Group
	activeSets   *xsync.Map[string, *reliableset.Set]
}

func (r *runnerSetCache) open(runnerId string) (*reliableset.Set, error) {
	set, err, _ := r.accessFlight.Do(runnerId, func() (any, error) {
		var loadErr error
		set, _ := r.activeSets.LoadOrCompute(runnerId, func() (newValue *reliableset.Set, cancel bool) {
			set, err := pool.OpenTaskSetForRunner(r.db, r.db, runnerId)
			if err != nil {
				loadErr = err
				return nil, true
			}
			return set, false
		})
		return set, loadErr
	})
	if err != nil {
		return nil, err
	}
	return set.(*reliableset.Set), nil
}

func newRunnerSetCache(db dbutil.DbRoot, runnerInformer cache.SharedIndexInformer) *runnerSetCache {
	cache := &runnerSetCache{
		db:         db,
		activeSets: xsync.NewMap[string, *reliableset.Set](),
	}
	runnerInformer.AddEventHandler(cache)
	return cache
}

// OnAdd implements [cache.ResourceEventHandler].
func (r *runnerSetCache) OnAdd(obj any, isInInitialList bool) {} // noop

// OnDelete implements [cache.ResourceEventHandler].
func (r *runnerSetCache) OnDelete(obj any) {
	// handle unknown states conservatively by clearing the cache if we see this.
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	switch obj := obj.(type) {
	case *corev1.Pod:
		r.activeSets.Delete(obj.Name)
	default:
		slog.Error("unexpected object type", "type", reflect.TypeOf(obj))
	}
}

// OnUpdate implements [cache.ResourceEventHandler].
func (r *runnerSetCache) OnUpdate(oldObj any, newObj any) {} // noop
