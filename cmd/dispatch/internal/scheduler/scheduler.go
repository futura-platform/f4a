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
	weightedrand "github.com/mroth/weightedrand/v2"
)

const maxAssignmentsPerSchedule = 64

var errNotPending = errors.New("task not pending")

type Config struct {
	Namespace       string
	StatefulSetName string
	MetricsInterval time.Duration
	ScoreAlpha      float64
	Logger          *slog.Logger
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

func (s *Scheduler) run(ctx context.Context) error {
	initialValues, eventsCh, streamErrCh, err := s.pendingSet.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to stream pending set: %w", err)
	}

	pending := mapset.NewSet[task.Id]()
	if err := addInitialPending(pending, initialValues); err != nil {
		return err
	}

	scores := make(map[string]float64)
	if updated, err := s.refreshScores(ctx); err != nil {
		s.logger.Error("failed to load initial worker scores", "error", err)
	} else {
		scores = updated
	}
	s.assignPending(ctx, pending, scores)

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
			if err := applyPendingBatch(pending, batch); err != nil {
				return err
			}
			s.assignPending(ctx, pending, scores)
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
			s.assignPending(ctx, pending, scores)
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

func (s *Scheduler) assignPending(ctx context.Context, pending mapset.Set[task.Id], scores map[string]float64) {
	if pending.Cardinality() == 0 || len(scores) == 0 {
		return
	}

	assignments := 0
	for _, id := range pending.ToSlice() {
		if assignments >= maxAssignmentsPerSchedule {
			return
		}
		worker, ok := selectWeightedWorker(scores)
		if !ok {
			return
		}
		err := s.assignTask(ctx, id, worker)
		if err != nil {
			if errors.Is(err, errNotPending) {
				pending.Remove(id)
				continue
			}
			s.logger.Error("failed to assign task", "task_id", id.String(), "worker", worker, "error", err)
			return
		}
		pending.Remove(id)
		assignments++
	}
}

func (s *Scheduler) assignTask(ctx context.Context, id task.Id, worker string) error {
	taskKey, err := s.taskDir.Open(s.db, id)
	if err != nil {
		return fmt.Errorf("failed to open task %s: %w", id.String(), err)
	}

	taskSet, ok := s.taskSets[worker]
	if !ok {
		return fmt.Errorf("task set missing for worker %q", worker)
	}

	_, err = s.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		status := taskKey.LifecycleStatus().Get(tx).MustGet()
		if status != task.LifecycleStatusPending {
			return nil, errNotPending
		}
		taskKey.RunnerId().Set(tx, worker)
		taskKey.LifecycleStatus().Set(tx, task.LifecycleStatusRunning)
		if err := taskSet.Add(tx, id.Bytes()); err != nil {
			return nil, err
		}
		if err := s.pendingSet.Remove(tx, id.Bytes()); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func addInitialPending(pending mapset.Set[task.Id], initial mapset.Set[string]) error {
	for _, item := range initial.ToSlice() {
		id, err := task.IdFromBytes([]byte(item))
		if err != nil {
			return fmt.Errorf("failed to parse pending task id: %w", err)
		}
		pending.Add(id)
	}
	return nil
}

func applyPendingBatch(pending mapset.Set[task.Id], batch []reliableset.LogEntry) error {
	addedSet := mapset.NewSetWithSize[task.Id](len(batch))
	removedSet := mapset.NewSetWithSize[task.Id](len(batch))
	for _, entry := range batch {
		id, err := task.IdFromBytes(entry.Value)
		if err != nil {
			return fmt.Errorf("failed to parse task id: %w", err)
		}
		switch entry.Op {
		case reliableset.LogOperationAdd:
			addedSet.Add(id)
			removedSet.Remove(id)
		case reliableset.LogOperationRemove:
			removedSet.Add(id)
			addedSet.Remove(id)
		default:
			return fmt.Errorf("unknown log operation: %d", entry.Op)
		}
	}

	for _, id := range addedSet.ToSlice() {
		pending.Add(id)
	}
	for _, id := range removedSet.ToSlice() {
		pending.Remove(id)
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
