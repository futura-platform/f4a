package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"

	"github.com/futura-platform/f4a/cmd/dispatch/internal/k8s"
	"github.com/futura-platform/f4a/cmd/dispatch/reaper"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"

	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/constants"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
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
	pendingSet,
	suspendedSet *reliableset.Set
	clients *k8s.Clients

	logger *slog.Logger
}

const (
	reaperPollInterval = 10 * time.Second
)

func Run(ctx context.Context, cfg Config, db dbutil.DbRoot, clients *k8s.Clients) error {
	taskDir, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		return fmt.Errorf("failed to open task directory: %w", err)
	}
	pendingSet, err := servicestate.CreateOrOpenReadySet(db, db)
	if err != nil {
		return fmt.Errorf("failed to open pending set: %w", err)
	}
	suspendedSet, err := servicestate.CreateOrOpenSuspendedSet(db, db)
	if err != nil {
		return fmt.Errorf("failed to open suspended set: %w", err)
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
		suspendedSet:  suspendedSet,
		clients:       clients,
		logger:        cfg.Logger,
	}
	return s.commandRunners(ctx)
}

var (
	tracer = otel.Tracer("f4a.dispatch.scheduler")
	meter  = otel.Meter("f4a.dispatch.scheduler")
)

// commandRunners is the main loop of the scheduler. It is expected to commandRunners as a singleton scoped to the whole cluster.
// It assigns tasks to the fittest workers exactly once per pending task.
// It also periodically refreshes the worker scores to evaluate fitness.
func (s *Scheduler) commandRunners(ctx context.Context) (err error) {
	ctx, span := tracer.Start(ctx, "commandRunners")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

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

	activeRunnerSets := newRunnerSetCache(s.db, runnerPodInformer.Informer())

	taskCountGauge, err := meter.Int64ObservableGauge("task_count")
	if err != nil {
		return fmt.Errorf("failed to create pending task count counter: %w", err)
	}
	availableRunnerGauge, err := meter.Int64ObservableGauge("available_runner_count")
	if err != nil {
		return fmt.Errorf("failed to create runner count counter: %w", err)
	}
	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		o.ObserveInt64(availableRunnerGauge, int64(scores.Size()))

		const stateAttribute = "state"
		pendingSetItems, _, err := s.pendingSet.Items(ctx, s.db.Database)
		if err != nil {
			return err
		}
		o.ObserveInt64(taskCountGauge, int64(pendingSetItems.Cardinality()), metric.WithAttributes(attribute.String(stateAttribute, "pending")))

		suspendedSetItems, _, err := s.suspendedSet.Items(ctx, s.db.Database)
		if err != nil {
			return err
		}
		o.ObserveInt64(taskCountGauge, int64(suspendedSetItems.Cardinality()), metric.WithAttributes(attribute.String(stateAttribute, "suspended")))

		var runningCount int64
		for kvOrErr := range s.activeRunners.Iterate(ctx, s.db) {
			if err, ok := kvOrErr.Left(); ok {
				return err
			}
			kv := kvOrErr.MustRight()
			runnerID, err := s.activeRunners.RunnerIDFromLivenessKey(kv.Key)
			if err != nil {
				return err
			}
			runnerSet, err := activeRunnerSets.open(runnerID)
			if err != nil {
				if errors.Is(err, directory.ErrDirNotExists) {
					continue
				}
				return err
			}
			runningSetItems, _, err := runnerSet.Items(ctx, s.db.Database)
			if err != nil {
				return err
			}
			runningCount += int64(runningSetItems.Cardinality())
		}
		o.ObserveInt64(taskCountGauge, runningCount, metric.WithAttributes(attribute.String(stateAttribute, "running")))
		return nil
	}, taskCountGauge, availableRunnerGauge)
	if err != nil {
		return fmt.Errorf("failed to register callback: %w", err)
	}
	defer reg.Unregister()

	cancelPendingCompaction := s.pendingSet.RunCompactor()
	defer cancelPendingCompaction()

	initialValues, eventsCh, streamErrCh, err := s.pendingSet.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to stream pending set: %w", err)
	}

	cancelReaper, err := reaper.SpawnReaperRoutine(
		ctx,
		s.db,
		runnerPodInformer.Lister().Pods(s.cfg.Namespace),
		s.clients.Core.CoreV1().Pods(s.cfg.Namespace),
		reaperPollInterval,
	)
	if err != nil {
		return fmt.Errorf("failed to spawn reaper routine: %w", err)
	}
	defer cancelReaper()

	backlog, err := s.assignPending(ctx, initialValues.ToSlice(), activeRunnerSets)
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

			span.AddEvent("event_batch_received")
			for _, entry := range batch {
				switch entry.Op {
				case reliableset.LogOperationAdd:
					backlog.Add(string(entry.Value))
				case reliableset.LogOperationRemove:
					backlog.Remove(string(entry.Value))
				}
			}
			backlog, err = s.assignPending(ctx, backlog.ToSlice(), activeRunnerSets)
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
			backlog, err = s.assignPending(ctx, backlog.ToSlice(), activeRunnerSets)
			if err != nil {
				return fmt.Errorf("failed to assign pending backlog: %w", err)
			}
		}
	}
}
