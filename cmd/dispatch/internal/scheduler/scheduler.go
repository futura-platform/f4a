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
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/cmd/dispatch/internal/k8s"
	"github.com/futura-platform/f4a/cmd/dispatch/internal/score"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/constants"
	weightedrand "github.com/mroth/weightedrand/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	assignmentTxBudgetBytes = constants.MaxTransactionAffectedSizeBytes / 3
	DefaultBatchParallelism = 4
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
	cfg        Config
	db         dbutil.DbRoot
	taskDir    task.TasksDirectory
	pendingSet *reliableset.Set
	clients    *k8s.Clients

	scoreCache *scoreCache
	taskSets   map[string]*reliableset.Set
	logger     *slog.Logger
}

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
	pendingSet, err := servicestate.CreateOrOpenReadySet(db)
	if err != nil {
		return fmt.Errorf("failed to open pending set: %w", err)
	}

	s := &Scheduler{
		cfg:        cfg,
		db:         db,
		taskDir:    taskDir,
		pendingSet: pendingSet,
		clients:    clients,
		scoreCache: newScoreCache(cfg.ScoreAlpha),
		taskSets:   make(map[string]*reliableset.Set),
		logger:     cfg.Logger,
	}
	return s.run(ctx)
}

// run is the main loop of the scheduler. It is expected to run as a singleton scoped to the whole cluster.
// It assigns tasks to the fittest workers exactly once per pending task.
// It also periodically refreshes the worker scores to evaluate fitness.
func (s *Scheduler) run(ctx context.Context) error {
	initialValues, eventsCh, streamErrCh, err := s.pendingSet.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to stream pending set: %w", err)
	}

	scores, err := s.refreshScores(ctx)
	if err != nil {
		s.logger.Error("failed to refresh worker scores", "error", err)
	}

	backlog, err := s.assignPending(ctx, initialValues.ToSlice(), scores)
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
			newlyPending := mapset.NewSet[string]()
			for _, entry := range batch {
				if entry.Op != reliableset.LogOperationAdd {
					continue
				}
				newlyPending.Add(string(entry.Value))
			}
			backlog, err = s.assignPending(ctx, backlog.Union(newlyPending).ToSlice(), scores)
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
			updated, err := s.refreshScores(ctx)
			if err != nil {
				s.logger.Error("failed to refresh worker scores", "error", err)
				continue
			}
			scores = updated
			backlog, err = s.assignPending(ctx, backlog.ToSlice(), scores)
			if err != nil {
				return fmt.Errorf("failed to assign pending backlog: %w", err)
			}
		}
	}
}

func (s *Scheduler) refreshScores(ctx context.Context) (map[string]float64, error) {
	utilization, err := k8s.WorkerUtilizationSnapshot(ctx, s.clients, s.cfg.Namespace, s.cfg.StatefulSetName)
	if err != nil {
		return nil, err
	}

	snapshot := make(map[string]float64, len(utilization))
	for worker, util := range utilization {
		snapshot[worker] = score.WorkerUtilizationScore(util.CPU, util.Memory)
	}

	snapshot = s.ensureTaskSets(snapshot)
	return s.scoreCache.Update(snapshot), nil
}

func (s *Scheduler) ensureTaskSets(scores map[string]float64) map[string]float64 {
	for worker := range scores {
		if _, ok := s.taskSets[worker]; ok {
			continue
		}
		taskSet, err := pool.CreateOrOpenTaskSetForRunner(s.db, worker)
		if err != nil {
			s.logger.Error("failed to open worker task set", "worker", worker, "error", err)
			delete(scores, worker)
			continue
		}
		s.taskSets[worker] = taskSet
	}
	return scores
}

// assignPending assigns all given tasks in the pending set to the most fit workers.
// fitness is determines by selectWeightedWorker.
func (s *Scheduler) assignPending(ctx context.Context, pendingIds []string, scores map[string]float64) (couldntAssign mapset.Set[string], err error) {
	activeTxSem := semaphore.NewWeighted(int64(s.cfg.BatchTxParallelism))

	group, ctx := errgroup.WithContext(ctx)
	couldntAssign = mapset.NewSet[string]()
	for i := 0; i < len(pendingIds); i += s.cfg.BatchTxParallelism {
		batch := pendingIds[i:min(i+s.cfg.BatchTxParallelism, len(pendingIds))]

		err := activeTxSem.Acquire(ctx, 1)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return nil, err
		}
		group.Go(func() error {
			defer activeTxSem.Release(1)

			worker, ok := selectWeightedWorker(scores)
			if !ok {
				couldntAssign.Append(batch...)
				return nil
			}

			_, err := s.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
				for _, id := range batch {
					if err := s.assignTask(tx, task.Id(id), worker); err != nil {
						return nil, err
					}
				}
				return nil, nil
			})
			if err != nil {
				couldntAssign.Append(batch...)
			}
			return err
		})
	}

	return couldntAssign, group.Wait()
}

func (s *Scheduler) assignTask(tx fdb.Transaction, id task.Id, worker string) error {
	taskSet, ok := s.taskSets[worker]
	if !ok {
		return fmt.Errorf("task set missing for worker %q", worker)
	}

	taskKey, err := s.taskDir.Open(s.db, id)
	if err != nil {
		return fmt.Errorf("failed to open task %s: %w", id, err)
	}

	status := taskKey.LifecycleStatus().Get(tx).MustGet()
	if status != task.LifecycleStatusPending {
		return fmt.Errorf("task %s is not pending", id)
	}
	taskKey.RunnerId().Set(tx, worker)
	taskKey.LifecycleStatus().Set(tx, task.LifecycleStatusRunning)
	if err := taskSet.Add(tx, []byte(id)); err != nil {
		return err
	}
	if err := s.pendingSet.Remove(tx, []byte(id)); err != nil {
		return err
	}
	return nil
}

func selectWeightedWorker(scores map[string]float64) (string, bool) {
	if len(scores) == 0 {
		return "", false
	}

	keys := make([]string, 0, len(scores))
	maxScore := math.Inf(-1)
	for worker, scoreValue := range scores {
		keys = append(keys, worker)
		if scoreValue > maxScore {
			maxScore = scoreValue
		}
	}
	sort.Strings(keys)

	const weightScale = 1000.0
	choices := make([]weightedrand.Choice[string, uint], 0, len(keys))
	for _, worker := range keys {
		delta := maxScore - scores[worker]
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
