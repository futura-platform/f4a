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
	otelutil "github.com/futura-platform/f4a/internal/util/otel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1 "k8s.io/client-go/listers/core/v1"
)

var (
	tracer = otel.Tracer("f4a.dispatch.reaper")
	meter  = otel.Meter("f4a.dispatch.reaper")
)

// SpawnReaperRoutine spins off a goroutine that runs a loop that scans for orphaned task sets and re queues all the tasks in them to be scheduled.
// Orphaned task sets are task sets that are not associated with any active runners.
// This can happen when a runner fails to drain itself before being force killed.
// activeRunnerSets is expected to be updated in real time as a liveActiveRunnerSets return value.
func SpawnReaperRoutine(
	ctx context.Context,
	db dbutil.DbRoot,
	cachedPods corev1.PodNamespaceLister,
	livePods corev1client.PodInterface,
	pollInterval time.Duration,
) (_ context.CancelFunc, err error) {
	ctx, span := tracer.Start(ctx, "spawn")
	defer func() { otelutil.End(span, err) }()

	activeRunners, err := pool.CreateOrOpenActiveRunners(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open active runners: %w", err)
	}

	placer, _, err := servicestate.CreateOrOpenTaskPlacer(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open task placer: %w", err)
	}
	taskDirectory, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open task directory: %w", err)
	}

	lookupFailures, err := meter.Int64Counter(
		"pod_lookup_failures",
		metric.WithUnit("{lookup}"),
		metric.WithDescription("Runner pod liveness lookups that failed (excluding not-found), each skipping that runner for one reap cycle."),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create pod lookup failure counter: %w", err)
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
				failedLookups, err := reapAll(ctx, db, placer, cachedPods, livePods, activeRunners, taskDirectory)
				if failedLookups > 0 {
					lookupFailures.Add(ctx, failedLookups)
				}
				if err != nil {
					slog.Error("reaper: failed to reap", "error", err)
				}
				otelutil.End(span, err)
			}
		}
	}()

	return cancel, nil
}

// reapAll drains the task sets of all provably dead runners. It returns how
// many runners were skipped because their liveness lookup failed; the caller
// is expected to surface that count in a metric.
func reapAll(
	ctx context.Context,
	db dbutil.DbRoot,
	placer *servicestate.TaskPlacer,
	cachedPods corev1.PodNamespaceLister,
	livePods corev1client.PodInterface,
	activeRunners pool.ActiveRunners,
	taskDirectory task.TasksDirectory,
) (failedLookups int64, err error) {
	var runnerIds []string
	_, err = db.ReadTransactContext(ctx, func(tx fdb.ReadTransaction) (_ any, err error) {
		runnerIds, err = servicestate.ListTaskSets(tx, db)
		return nil, err
	})
	if err != nil {
		return 0, err
	}
	reapErrs := make([]error, 0, len(runnerIds))
	var firstLookupErr error
	for _, runnerId := range runnerIds {
		dead, err := runnerIsDead(ctx, cachedPods, livePods, runnerId)
		if err != nil {
			if ctx.Err() != nil {
				// shutting down mid-cycle; not a lookup incident
				return 0, nil
			}
			failedLookups++
			if firstLookupErr == nil {
				firstLookupErr = err
			}
			continue
		}
		if !dead {
			continue
		}

		err = reapForRunner(ctx, db, placer, activeRunners, taskDirectory, runnerId)
		if err != nil {
			reapErrs = append(reapErrs, err)
		}
	}
	if failedLookups > 0 {
		slog.Error("reaper: pod lookups failed, skipping runners this cycle",
			"skipped_runners", failedLookups, "first_error", firstLookupErr)
	}
	if len(reapErrs) > 0 {
		return failedLookups, fmt.Errorf("failed to reap some task sets: %w", errors.Join(reapErrs...))
	}
	return failedLookups, nil
}

// runnerIsDead reports whether the runner's pod is provably gone.
// A pod found by either lookup means alive. Only a not-found answer from the
// api server (the consistent source) proves death; the cache alone can lag.
// Any other outcome is an error: the runner's liveness is unknown, and the
// caller must not conflate that with "alive".
func runnerIsDead(
	ctx context.Context,
	cachedPods corev1.PodNamespaceLister,
	livePods corev1client.PodInterface,
	runnerId string,
) (bool, error) {
	// fast, eventually consistent path
	if _, err := cachedPods.Get(runnerId); err == nil {
		return false, nil
	} else if !apierrors.IsNotFound(err) {
		// the cache is only an optimization; fall through to the
		// authoritative lookup instead of skipping the runner
		slog.Warn("reaper: cached pod lookup failed, falling back to api server", "runner_id", runnerId, "error", err)
	}

	// slow, consistent path
	_, err := livePods.Get(ctx, runnerId, metav1.GetOptions{})
	if err == nil {
		return false, nil
	}
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	return false, fmt.Errorf("failed to look up runner pod %q: %w", runnerId, err)
}

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
