package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/internal/util"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"k8s.io/apimachinery/pkg/labels"
)

// assignPending assigns all given tasks in the pending set to active runner pods.
// If resources are unavailable, the task is not assigned and added to the retryAssignLater return set.
func (s *Scheduler) assignPending(
	ctx context.Context,
	pendingIds []string,
) (retryAssignLater mapset.Set[string], err error) {
	ctx, span := tracer.Start(ctx, "assignPending")
	span.SetAttributes(
		attribute.Int("pending_ids_count", len(pendingIds)),
	)

	pods, err := s.runnerPodLister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("failed to list runner pods: %w", err)
	}

	runnerIDs := make([]string, 0, len(pods))
	for _, pod := range pods {
		runnerIDs = append(runnerIDs, pod.Name)
	}
	sort.Strings(runnerIDs)

	assignmentRecord := []string{}
	var couldntSelect, batchTransactionFailed atomic.Int32
	var nextRunnerSelection atomic.Uint64
	defer func() {
		span.SetAttributes(
			attribute.Int("assignment_count", len(assignmentRecord)),
			attribute.Int("retry_assign_later_count", retryAssignLater.Cardinality()),
			attribute.Int("couldnt_select_count", int(couldntSelect.Load())),
			attribute.Int("batch_transaction_failed_count", int(batchTransactionFailed.Load())),
		)
		if err != nil {
			span.RecordError(err)
		}
		span.End()

		if len(pendingIds) == 0 {
			return
		}
		slog.Info("assigned pending tasks",
			"assignments", util.JoinWithMaxPreview(assignmentRecord, 5),
			"retry_assign_later", util.JoinWithMaxPreview(retryAssignLater.ToSlice(), 5),
			"retry_reasons", fmt.Sprint(map[string]int32{
				"couldnt_select":           couldntSelect.Load(),
				"batch_transaction_failed": batchTransactionFailed.Load(),
			}),
			"err", err,
		)
	}()
	activeTxSem := semaphore.NewWeighted(int64(s.cfg.BatchTxParallelism))

	group, ctx := errgroup.WithContext(ctx)
	retryAssignLater = mapset.NewSet[string]()
	for i := 0; i < len(pendingIds); i += assignmentBatchSize {
		batch := pendingIds[i:min(i+assignmentBatchSize, len(pendingIds))]

		err := activeTxSem.Acquire(ctx, 1)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return nil, err
		}
		group.Go(func() error {
			defer activeTxSem.Release(1)
			var txScopedRetryAssignLater mapset.Set[string]

			_, err := s.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
				txScopedRetryAssignLater = mapset.NewSet[string]()
				rejectedRunners := mapset.NewSet[string]()

				// Keep this scoped to a single transaction attempt so retries do not
				// observe stale per-attempt state.
				txRunnerActiveStates := make(map[string]bool)
				for _, id := range batch {
				retryRunnerSelection:
					runnerId, ok := selectRunnerExcluding(
						runnerIDs,
						rejectedRunners,
						int(nextRunnerSelection.Add(1)-1),
					)
					if !ok {
						couldntSelect.Add(1)
						txScopedRetryAssignLater.Add(id)
						continue
					}

					runnerState, ok := txRunnerActiveStates[runnerId]
					if !ok {
						// initialize the runner states JIT
						runnerState = s.activeRunners.IsActive(tx, runnerId).MustGet()
						txRunnerActiveStates[runnerId] = runnerState
					}
					if !runnerState {
						slog.Info("runner is not active, skipping assignment", "runner_id", runnerId)
						rejectedRunners.Add(runnerId)
						// then retry the assignment for this task.
						goto retryRunnerSelection
					}

					runnerSet, err := s.activeRunnerSets.open(runnerId)
					if err != nil {
						if errors.Is(err, directory.ErrDirNotExists) {
							// The runner set is no longer active. Try another visible runner.
							rejectedRunners.Add(runnerId)
							goto retryRunnerSelection
						}
						return nil, err
					}

					assignmentRecord = append(assignmentRecord, fmt.Sprintf("%s -> %s", id, runnerId))
					if err := s.assignTask(tx, task.Id(id), runnerId, runnerSet); err != nil {
						if errors.Is(err, ErrTaskNotInAssignableState) {
							continue
						}
						return nil, err
					}
				}
				return nil, nil
			})
			if err != nil {
				batchTransactionFailed.Add(int32(len(batch)))
				retryAssignLater.Append(batch...)
			} else {
				retryAssignLater.Append(txScopedRetryAssignLater.ToSlice()...)
			}
			return err
		})
	}

	return retryAssignLater, group.Wait()
}

var (
	ErrTaskNotInAssignableState = errors.New("task not in assignable state")
)

func (s *Scheduler) assignTask(tx fdb.Transaction, id task.Id, runnerId string, runnerSet *reliableset.Set) error {
	taskKey, err := s.taskDir.Open(tx, id)
	if err != nil {
		if errors.Is(err, directory.ErrDirNotExists) {
			return ErrTaskNotInAssignableState
		}
		return fmt.Errorf("failed to open task %s: %w", id, err)
	}

	assignmentState, err := task.ReadAssignmentState(tx, taskKey)
	if err != nil {
		return err
	}
	if err := assignmentState.ValidateRunnerLifecycleInvariant(); err != nil {
		return fmt.Errorf("task assignment invariant violation: %w", err)
	}
	lifecycleStatus, err := assignmentState.LifecycleStatusFuture.Get()
	if err != nil {
		return fmt.Errorf("failed to get task lifecycle status: %w", err)
	}
	if lifecycleStatus != task.LifecycleStatusPending {
		return ErrTaskNotInAssignableState
	}
	// Preserve lifecycle invariant atomically: running status implies queue membership.
	taskKey.RunnerId().Set(tx, &runnerId)
	taskKey.LifecycleStatus().Set(tx, task.LifecycleStatusRunning)
	if err := runnerSet.Add(tx, []byte(id)); err != nil {
		return err
	}

	return s.pendingSet.Remove(tx, []byte(id))
}

func selectRunnerExcluding(runnerIDs []string, excluded mapset.Set[string], offset int) (string, bool) {
	if len(runnerIDs) == 0 {
		return "", false
	}
	for i := range runnerIDs {
		idx := (offset + i) % len(runnerIDs)
		runnerID := runnerIDs[idx]
		if excluded == nil || !excluded.ContainsOne(runnerID) {
			return runnerID, true
		}
	}
	return "", false
}
