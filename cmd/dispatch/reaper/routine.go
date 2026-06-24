package reaper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"go.opentelemetry.io/otel"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1 "k8s.io/client-go/listers/core/v1"
)

var (
	tracer = otel.Tracer("f4a.dispatch.reaper")
)

// SpawnReaperRoutine spins off a goroutine that runs a loop that scans for orphaned task sets and re queues all the tasks in them to be scheduled.
// Orphaned task sets are task sets that are not associated with any active runners.
// This can happen when a runner fails to drain itself before being force killed.
// SpawnReaperRoutine starts a background goroutine that periodically scans for orphaned task sets and re-queues their tasks for scheduling. It returns a cancel function to stop the goroutine and an error if initialization fails.
func SpawnReaperRoutine(
	ctx context.Context,
	db dbutil.DbRoot,
	cachedPods corev1.PodNamespaceLister,
	livePods corev1client.PodInterface,
	pollInterval time.Duration,
) (_ context.CancelFunc, err error) {
	ctx, span := tracer.Start(ctx, "spawn")
	defer span.End()

	activeRunners, err := pool.CreateOrOpenActiveRunners(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open active runners: %w", err)
	}

	placer, _, err := servicestate.CreateOrOpenTaskPlacer(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open pending set: %w", err)
	}
	taskDirectory, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open task directory: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ctx, span := tracer.Start(ctx, "reapAll")
				err := reapAll(ctx, db, placer, cachedPods, livePods, activeRunners, taskDirectory)
				if err != nil {
					span.RecordError(err)
					slog.Error("reaper: failed to reap", "error", err)
				}
				span.End()
			}
		}
	}()

	return cancel, nil
}

// reapAll lists all task sets and re-queues their tasks if the associated runner pod no longer exists in Kubernetes, returning an aggregated error if any reaping operation failed.
func reapAll(
	ctx context.Context,
	db dbutil.DbRoot,
	placer *servicestate.TaskPlacer,
	cachedPods corev1.PodNamespaceLister,
	livePods corev1client.PodInterface,
	activeRunners pool.ActiveRunners,
	taskDirectory task.TasksDirectory,
) error {
	var runnerIds []string
	_, err := db.ReadTransactContext(ctx, func(tx fdb.ReadTransaction) (_ any, err error) {
		runnerIds, err = servicestate.ListTaskSets(tx, db)
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

		err = reapForRunner(ctx, db, placer, activeRunners, taskDirectory, runnerId)
		if err != nil {
			reapErrs = append(reapErrs, err)
		}
	}
	if len(reapErrs) > 0 {
		return fmt.Errorf("failed to reap some task sets: %w", errors.Join(reapErrs...))
	}
	return nil
}

// reapForRunner drains and re-queues tasks for the given orphaned runner's task set.
func reapForRunner(
	ctx context.Context,
	db dbutil.DbRoot,
	placer *servicestate.TaskPlacer,
	activeRunners pool.ActiveRunners,
	taskDirectory task.TasksDirectory,
	runnerId string,
) error {
	taskSet, err := servicestate.CreateOrOpenTaskSetForRunner(db, db, runnerId)
	if err != nil {
		return err
	}

	// call a shared drain function here to drain the task set for the given runner id
	err = pool.DrainTaskRunner(ctx, db, placer, runnerId, activeRunners, taskSet, taskDirectory)
	if err != nil {
		return err
	}
	return nil
}
