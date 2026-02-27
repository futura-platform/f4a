package reaper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1 "k8s.io/client-go/listers/core/v1"
)

// SpawnReaperRoutine spins off a goroutine that runs a loop that scans for orphaned task sets and re queues all the tasks in them to be scheduled.
// Orphaned task sets are task sets that are not associated with any active runners.
// This can happen when a runner fails to drain itself before being force killed.
// activeRunnerSets is expected to be updated in real time as a liveActiveRunnerSets return value.
func SpawnReaperRoutine(
	db dbutil.DbRoot,
	cachedPods corev1.PodNamespaceLister,
	livePods corev1client.PodInterface,
	pollInterval time.Duration,
) (_ context.CancelFunc, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	activeRunners, err := pool.CreateOrOpenActiveRunners(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open active runners: %w", err)
	}

	pendingSet, cancelPendingSet, err := servicestate.CreateOrOpenReadySet(db, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open pending set: %w", err)
	}
	cancelPendingSet()
	taskDirectory, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open task directory: %w", err)
	}
	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := reapAll(ctx, db, cachedPods, livePods, activeRunners, pendingSet, taskDirectory)
				if err != nil {
					slog.Error("reaper: failed to reap", "error", err)
				}
			}
		}
	}()

	return cancel, nil
}

func reapAll(
	ctx context.Context,
	db dbutil.DbRoot,
	cachedPods corev1.PodNamespaceLister,
	livePods corev1client.PodInterface,
	activeRunners pool.ActiveRunners,
	pendingSet *reliableset.Set,
	taskDirectory task.TasksDirectory,
) error {
	var runnerIds []string
	_, err := db.ReadTransactContext(ctx, func(tx fdb.ReadTransaction) (_ any, err error) {
		runnerIds, err = pool.ListTaskSets(tx, db)
		return nil, err
	})
	if err != nil {
		return err
	}
	reapErrs := make([]error, 0, len(runnerIds))
	for _, runnerId := range runnerIds {
		// check if the runner is dead (in cache, fast eventually consistent path)
		if _, err := cachedPods.Get(runnerId); !apierrors.IsNotFound(err) {
			continue
		}
		// check if the runner is dead (from api server, slow consistent path)
		if _, err := livePods.Get(ctx, runnerId, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			continue
		}

		err = reapForRunner(db, activeRunners, pendingSet, taskDirectory, runnerId)
		if err != nil {
			reapErrs = append(reapErrs, err)
		}
	}
	if len(reapErrs) > 0 {
		return fmt.Errorf("failed to reap some task sets: %w", errors.Join(reapErrs...))
	}
	return nil
}

func reapForRunner(
	db dbutil.DbRoot,
	activeRunners pool.ActiveRunners,
	pendingSet *reliableset.Set,
	taskDirectory task.TasksDirectory,
	runnerId string,
) error {
	taskSet, cancel, err := pool.CreateOrOpenTaskSetForRunner(db, db, runnerId)
	if err != nil {
		return err
	}
	cancel()

	// call a shared drain function here to drain the task set for the given runner id
	err = pool.DrainTaskRunner(db, runnerId, activeRunners, taskSet, pendingSet, taskDirectory)
	if err != nil {
		return err
	}
	return nil
}
