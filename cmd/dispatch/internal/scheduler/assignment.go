package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/internal/util"
	weightedrand "github.com/mroth/weightedrand/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// assignPending assigns all given tasks in the pending set to the most fit workers.
// Fitness is determines by selectWeightedWorker.
// If resources are unavailable, the task is not assigned and added to the retryAssignLater return set.
func (s *Scheduler) assignPending(
	ctx context.Context,
	pendingIds []string,
	// scores *xsync.Map[string, float64], TODO: implement new scoring logic, inline in this method
	activeRunnerSets *runnerSetCache,
) (retryAssignLater mapset.Set[string], err error) {
	ctx, span := tracer.Start(ctx, "assignPending")
	span.SetAttributes(
		attribute.Int("pending_ids_count", len(pendingIds)),
	)

	assignmentRecord := []string{}

	var couldntSelect, batchTransactionFailed atomic.Int32
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
			var txScopedRunnerScoreEvictions mapset.Set[string]

			_, err := s.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
				txScopedRetryAssignLater = mapset.NewSet[string]()
				txScopedRunnerScoreEvictions = mapset.NewSet[string]()
				rejectedRunners := mapset.NewSet[string]()

				// Keep this scoped to a single transaction attempt so retries do not
				// observe stale per-attempt state.
				txRunnerActiveStates := make(map[string]bool)
				for _, id := range batch {
				retryRunnerSelection:
					runnerId, ok := selectWeightedRunnerExcluding(scores, rejectedRunners)
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
						// this runner is no longer active, skip assignment for it.
						// evict from score cache only after transaction commit.
						rejectedRunners.Add(runnerId)
						txScopedRunnerScoreEvictions.Add(runnerId)
						// then retry the assignment for this task.
						goto retryRunnerSelection
					}

					runnerSet, err := activeRunnerSets.open(runnerId)
					if err != nil {
						if errors.Is(err, directory.ErrDirNotExists) {
							// the runner set is no longer active, evict score on commit and retry assignment.
							rejectedRunners.Add(runnerId)
							txScopedRunnerScoreEvictions.Add(runnerId)
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
				for runnerID := range txScopedRunnerScoreEvictions.Iter() {
					scores.Delete(runnerID)
				}
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

func selectWeightedRunner(scores *xsync.Map[string, float64]) (string, bool) {
	return selectWeightedRunnerExcluding(scores, nil)
}

func selectWeightedRunnerExcluding(scores *xsync.Map[string, float64], excluded mapset.Set[string]) (string, bool) {
	if scores.Size() == 0 {
		return "", false
	}

	keys := make([]string, 0, scores.Size())
	maxScore := math.Inf(-1)
	scores.Range(func(worker string, scoreValue float64) bool {
		if excluded != nil && excluded.ContainsOne(worker) {
			return true
		}
		keys = append(keys, worker)
		if scoreValue > maxScore {
			maxScore = scoreValue
		}
		return true
	})
	if len(keys) == 0 {
		return "", false
	}
	sort.Strings(keys)

	const weightScale = 1000.0
	choices := make([]weightedrand.Choice[string, uint], 0, len(keys))
	for _, worker := range keys {
		scoreVal, ok := scores.Load(worker)
		if !ok {
			continue
		}
		delta := maxScore - scoreVal
		if delta < 0 {
			delta = 0
		}
		weight := uint(math.Round(delta*weightScale)) + 1
		choices = append(choices, weightedrand.NewChoice(worker, weight))
	}

	chooser, err := weightedrand.NewChooser(choices...)
	if err != nil {
		return "", false
	}
	return chooser.Pick(), true
}
