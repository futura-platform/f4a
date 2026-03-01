package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/cmd/dispatch/internal/k8s"
	"github.com/futura-platform/f4a/cmd/dispatch/internal/score"
	"github.com/futura-platform/f4a/cmd/dispatch/reaper"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/internal/util"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/constants"
	weightedrand "github.com/mroth/weightedrand/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	assignmentTxBudgetBytes = constants.MaxTransactionAffectedSizeBytes / 3
	// Per task assignment we touch more than one reliable set log append:
	//   1) worker queue Add: log entry write + epoch increment
	//   2) pending queue Remove: log entry write + epoch increment
	// plus task metadata writes (runner_id, lifecycle_status) and a lifecycle read.
	//
	// This 1KiB heuristic intentionally stays conservative so batch transactions
	// remain below FDB's affected-size limit across schema/runtime variations.
	perTaskEstimatedTxnOverheadBytes = 1024
	DefaultBatchParallelism          = 4

	assignmentBatchSize = assignmentTxBudgetBytes / (task.MAX_ID_LENGTH + perTaskEstimatedTxnOverheadBytes)
)

type Config struct {
	Namespace          string
	StatefulSetName    string
	MetricsInterval    time.Duration
	ScoreAlpha         float64
	BatchTxParallelism int
	Logger             *slog.Logger
}

type Scheduler struct {
	cfg           Config
	db            dbutil.DbRoot
	activeRunners pool.ActiveRunners
	taskDir       task.TasksDirectory
	pendingSet    *reliableset.Set
	clients       *k8s.Clients

	scoreCache *scoreCache
	logger     *slog.Logger
}

const (
	reaperPollInterval = 10 * time.Second
)

func Run(ctx context.Context, cfg Config, db dbutil.DbRoot, clients *k8s.Clients) error {
	if cfg.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if cfg.StatefulSetName == "" {
		return fmt.Errorf("statefulset name is required")
	}
	if cfg.MetricsInterval <= 0 {
		return fmt.Errorf("metrics interval is required")
	}
	if cfg.ScoreAlpha <= 0 || cfg.ScoreAlpha > 1 {
		return fmt.Errorf("score EMA alpha must be between 0 and 1")
	}
	if cfg.BatchTxParallelism <= 0 {
		return fmt.Errorf("batch tx parallelism must be greater than 0")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	taskDir, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		return fmt.Errorf("failed to open task directory: %w", err)
	}
	pendingSet, err := servicestate.CreateOrOpenReadySet(db, db)
	if err != nil {
		return fmt.Errorf("failed to open pending set: %w", err)
	}
	activeRunners, err := pool.CreateOrOpenActiveRunners(db)
	if err != nil {
		return fmt.Errorf("failed to open active runners: %w", err)
	}

	s := &Scheduler{
		cfg:           cfg,
		db:            db,
		activeRunners: activeRunners,
		taskDir:       taskDir,
		pendingSet:    pendingSet,
		clients:       clients,
		scoreCache:    newScoreCache(cfg.ScoreAlpha),
		logger:        cfg.Logger,
	}
	return s.run(ctx)
}

// run is the main loop of the scheduler. It is expected to run as a singleton scoped to the whole cluster.
// It assigns tasks to the fittest workers exactly once per pending task.
// It also periodically refreshes the worker scores to evaluate fitness.
func (s *Scheduler) run(ctx context.Context) error {
	cancelPendingCompaction := s.pendingSet.RunCompactor()
	defer cancelPendingCompaction()

	initialValues, eventsCh, streamErrCh, err := s.pendingSet.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to stream pending set: %w", err)
	}

	runnerPodInformer, cancel, err := liveRunnerPods(
		ctx,
		s.clients,
		s.cfg.Namespace,
		s.cfg.StatefulSetName,
	)
	if err != nil {
		return fmt.Errorf("failed to watch runner pods: %w", err)
	}
	defer cancel()

	cancelReaper, err := reaper.SpawnReaperRoutine(
		s.db,
		runnerPodInformer.Lister().Pods(s.cfg.Namespace),
		s.clients.Core.CoreV1().Pods(s.cfg.Namespace),
		reaperPollInterval,
	)
	if err != nil {
		return fmt.Errorf("failed to spawn reaper routine: %w", err)
	}
	defer cancelReaper()

	scores, err := s.refreshScores(ctx)
	if err != nil {
		s.logger.Error("failed to refresh worker scores, using default scores", "error", err)
		scores = xsync.NewMap[string, float64](xsync.WithPresize(1))
	}

	activeRunnerSets := newRunnerSetCache(s.db, runnerPodInformer.Informer())

	backlog, err := s.assignPending(ctx, initialValues.ToSlice(), scores, activeRunnerSets)
	if err != nil {
		return fmt.Errorf("failed to assign initial pending tasks: %w", err)
	}

	ticker := time.NewTicker(s.cfg.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-eventsCh:
			if !ok {
				return nil
			}

			for _, entry := range batch {
				switch entry.Op {
				case reliableset.LogOperationAdd:
					backlog.Add(string(entry.Value))
				case reliableset.LogOperationRemove:
					backlog.Remove(string(entry.Value))
				}
			}
			backlog, err = s.assignPending(ctx, backlog.ToSlice(), scores, activeRunnerSets)
			if err != nil {
				return fmt.Errorf("failed to assign pending tasks: %w", err)
			}
		case err, ok := <-streamErrCh:
			if !ok {
				return nil
			}
			if err != nil {
				return fmt.Errorf("pending set stream failed: %w", err)
			}
		case <-ticker.C:
			slog.Info("refreshing worker scores")
			updated, err := s.refreshScores(ctx)
			if err != nil {
				s.logger.Error("failed to refresh worker scores", "error", err)
				continue
			}
			scores = updated
			backlog, err = s.assignPending(ctx, backlog.ToSlice(), scores, activeRunnerSets)
			if err != nil {
				return fmt.Errorf("failed to assign pending backlog: %w", err)
			}
		}
	}
}

func (s *Scheduler) refreshScores(ctx context.Context) (*xsync.Map[string, float64], error) {
	utilization, err := k8s.WorkerUtilizationSnapshot(ctx, s.clients, s.cfg.Namespace, s.cfg.StatefulSetName)
	if err != nil {
		return nil, err
	}

	snapshot := xsync.NewMap[string, float64](xsync.WithPresize(len(utilization)))
	for worker, util := range utilization {
		snapshot.Store(worker, score.WorkerUtilizationScore(util.CPU, util.Memory))
	}

	return s.scoreCache.Update(snapshot), nil
}

// assignPending assigns all given tasks in the pending set to the most fit workers.
// Fitness is determines by selectWeightedWorker.
// If resources are unavailable, the task is not assigned and added to the retryAssignLater return set.
func (s *Scheduler) assignPending(
	ctx context.Context,
	pendingIds []string,
	scores *xsync.Map[string, float64],
	activeRunnerSets *runnerSetCache,
) (retryAssignLater mapset.Set[string], err error) {
	defer func() {
		slog.Info("assigned pending tasks",
			"pendingIds", util.JoinWithMaxPreview(pendingIds, 5),
			"scores", scores,
			"retryAssignLater", util.JoinWithMaxPreview(retryAssignLater.ToSlice(), 5),
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
	if err := s.pendingSet.Remove(tx, []byte(id)); err != nil {
		return err
	}
	return nil
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
