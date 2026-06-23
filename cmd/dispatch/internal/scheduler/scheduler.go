package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	v1 "k8s.io/client-go/listers/core/v1"

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
	BatchTxParallelism int
	Logger             *slog.Logger
}

type Scheduler struct {
	cfg              Config
	db               dbutil.DbRoot
	activeRunners    pool.ActiveRunners
	taskDir          task.TasksDirectory
	taskPlacer       *servicestate.TaskPlacer
	activeRunnerSets *runnerSetCache
	runnerPodLister  v1.PodNamespaceLister
	clients          *k8s.Clients

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
	activeRunners, err := pool.CreateOrOpenActiveRunners(db)
	if err != nil {
		return fmt.Errorf("failed to open active runners: %w", err)
	}

	taskPlacer, runCompactor, err := servicestate.CreateOrOpenTaskPlacer(db)
	if err != nil {
		return fmt.Errorf("failed to create or open task placer: %w", err)
	}
	cancelCompactor := runCompactor()
	defer cancelCompactor()

	runnerPodInformer, cancel, err := liveRunnerPods(
		ctx,
		clients,
		cfg.Namespace,
		cfg.StatefulSetName,
	)
	if err != nil {
		return fmt.Errorf("failed to watch runner pods: %w", err)
	}
	defer cancel()

	s := &Scheduler{
		cfg:              cfg,
		db:               db,
		activeRunners:    activeRunners,
		taskDir:          taskDir,
		taskPlacer:       taskPlacer,
		clients:          clients,
		logger:           cfg.Logger,
		activeRunnerSets: newRunnerSetCache(db, runnerPodInformer.Informer()),
		runnerPodLister:  runnerPodInformer.Lister().Pods(cfg.Namespace),
	}
	return s.commandRunners(ctx)
}

var (
	tracer = otel.Tracer("f4a.dispatch.scheduler")
	meter  = otel.Meter("f4a.dispatch.scheduler")
)

// commandRunners is the main loop of the scheduler. It is expected to commandRunners as a singleton scoped to the whole cluster.
// It assigns tasks to the fittest workers exactly once per pending task.
// It also periodically retries tasks that were left in the pending backlog.
func (s *Scheduler) commandRunners(ctx context.Context) (err error) {
	ctx, span := tracer.Start(ctx, "commandRunners")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	taskCountGauge, err := meter.Int64ObservableGauge(
		"task_count",
		metric.WithUnit("{task}"),
		metric.WithDescription("Tasks by lifecycle state."),
	)
	if err != nil {
		return fmt.Errorf("failed to create pending task count counter: %w", err)
	}
	requestedCpuGauge, err := meter.Float64ObservableGauge(
		"requested_cpu",
		metric.WithUnit("{cpu}"),
		metric.WithDescription("Requested CPU by placement class."),
	)
	if err != nil {
		return fmt.Errorf("failed to create requested cpu gauge: %w", err)
	}
	requestedMemoryBytesGauge, err := meter.Int64ObservableGauge(
		"requested_memory",
		metric.WithUnit("By"),
		metric.WithDescription("Requested memory by placement class."),
	)
	if err != nil {
		return fmt.Errorf("failed to create requested memory bytes gauge: %w", err)
	}
	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		const (
			stateAttribute          = "state"
			placementClassAttribute = "placement_class"
		)
		pendingTaskIds, _, err := s.taskPlacer.PendingTasks(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(taskCountGauge, int64(pendingTaskIds.Cardinality()), metric.WithAttributes(attribute.String(stateAttribute, "pending")))

		suspendedTaskIds, _, err := s.taskPlacer.SuspendedTasks(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(taskCountGauge, int64(suspendedTaskIds.Cardinality()), metric.WithAttributes(attribute.String(stateAttribute, "suspended")))

		var activeDemandCpuMillis, activeDemandMemoryBytes, suspendedCpuMillis, suspendedMemoryBytes int64
		_, err = s.db.ReadTransactContext(ctx, func(t fdb.ReadTransaction) (any, error) {
			activeDemandCpuMillis, err = s.taskPlacer.GetActiveDemandUtilization(t, servicestate.UtilizationDimensionCPU)
			if err != nil {
				return nil, err
			}
			activeDemandMemoryBytes, err = s.taskPlacer.GetActiveDemandUtilization(t, servicestate.UtilizationDimensionMemory)
			if err != nil {
				return nil, err
			}
			suspendedCpuMillis, err = s.taskPlacer.GetSuspendedUtilization(t, servicestate.UtilizationDimensionCPU)
			if err != nil {
				return nil, err
			}
			suspendedMemoryBytes, err = s.taskPlacer.GetSuspendedUtilization(t, servicestate.UtilizationDimensionMemory)
			if err != nil {
				return nil, err
			}
			return nil, nil
		})
		if err != nil {
			return err
		}
		o.ObserveFloat64(requestedCpuGauge, float64(activeDemandCpuMillis)/1000, metric.WithAttributes(attribute.String(placementClassAttribute, "active_demand")))
		o.ObserveInt64(requestedMemoryBytesGauge, activeDemandMemoryBytes, metric.WithAttributes(attribute.String(placementClassAttribute, "active_demand")))
		o.ObserveFloat64(requestedCpuGauge, float64(suspendedCpuMillis)/1000, metric.WithAttributes(attribute.String(placementClassAttribute, "suspended")))
		o.ObserveInt64(requestedMemoryBytesGauge, suspendedMemoryBytes, metric.WithAttributes(attribute.String(placementClassAttribute, "suspended")))

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
			runnerSet, err := s.activeRunnerSets.open(runnerID)
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
	}, taskCountGauge, requestedCpuGauge, requestedMemoryBytesGauge)
	if err != nil {
		return fmt.Errorf("failed to register callback: %w", err)
	}
	defer reg.Unregister()

	initialValues, eventsCh, streamErrCh, err := s.taskPlacer.StreamPendingTasks(ctx)
	if err != nil {
		return fmt.Errorf("failed to stream pending set: %w", err)
	}

	cancelReaper, err := reaper.SpawnReaperRoutine(
		ctx,
		s.db,
		s.runnerPodLister,
		s.clients.Core.CoreV1().Pods(s.cfg.Namespace),
		reaperPollInterval,
	)
	if err != nil {
		return fmt.Errorf("failed to spawn reaper routine: %w", err)
	}
	defer cancelReaper()

	backlog, err := s.assignPending(ctx, initialValues.ToSlice())
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
					backlog.Add(entry.Value)
				case reliableset.LogOperationRemove:
					backlog.Remove(entry.Value)
				}
			}
			backlog, err = s.assignPending(ctx, backlog.ToSlice())
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
			backlog, err = s.assignPending(ctx, backlog.ToSlice())
			if err != nil {
				return fmt.Errorf("failed to assign pending backlog: %w", err)
			}
		}
	}
}
