package scheduler

import (
	"context"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/kubectl/pkg/util/podutils"
)

type remainingResources struct {
	cpuMillis,
	memoryBytes int64
}

type taskWithResourceRequest struct {
	taskId          task.Id
	resourceRequest *taskv1.TaskResourceRequest
}

func (s *Scheduler) batchTxParallelism() int {
	if s.cfg.BatchTxParallelism < 1 {
		return DefaultBatchParallelism
	}
	return s.cfg.BatchTxParallelism
}

// assignPending assigns all given tasks in the pending set to active runner pods.
// If resources are unavailable, the task is not assigned and added to the retryAssignLater return set.
func (s *Scheduler) assignPending(
	ctx context.Context,
	pendingIds []task.Id,
) (retryAssignLater mapset.Set[task.Id], err error) {
	ctx, span := tracer.Start(ctx, "assignPending")
	span.SetAttributes(
		attribute.Int("pending_ids_count", len(pendingIds)),
	)

	pods, err := s.runnerPodLister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("failed to list runner pods: %w", err)
	}

	assignmentFailureGauge, err := meter.Int64Gauge(
		"pending_assignment_failure",
		metric.WithUnit("{task}"),
		metric.WithDescription("Assignment failures."),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create assignment failure gauge: %w", err)
	}

	type runnerWithResources struct {
		runnerId  string
		resources *remainingResources
	}
	remainingResourcesPerRunner := mapset.NewSet[*runnerWithResources]()
	runnerGroup, _ := errgroup.WithContext(ctx)
	runnerGroup.SetLimit(s.batchTxParallelism())
	for _, pod := range pods {
		if !podutils.IsPodReady(pod) {
			continue
		}
		runnerGroup.Go(func() error {
			resources, ok, err := remainingResourcesFromRunner(s.db, s.db, s.activeRunners, pod)
			if err != nil {
				return err
			} else if !ok {
				return nil
			}
			remainingResourcesPerRunner.Add(&runnerWithResources{
				runnerId:  pod.Name,
				resources: resources,
			})
			return nil
		})
	}
	if err := runnerGroup.Wait(); err != nil {
		return nil, err
	}

	assignmentPlan := make(map[string]mapset.Set[taskWithResourceRequest], remainingResourcesPerRunner.Cardinality())
	for _, runner := range remainingResourcesPerRunner.ToSlice() {
		assignmentPlan[runner.runnerId] = mapset.NewSet[taskWithResourceRequest]()
	}

	// load the tasks with their resource requests
	taskResourceRequests := mapset.NewSet[taskWithResourceRequest]()
	taskGroup, taskCtx := errgroup.WithContext(ctx)
	taskGroup.SetLimit(s.batchTxParallelism())
	for _, taskId := range pendingIds {
		taskGroup.Go(func() error {
			_, err := s.db.ReadTransactContext(taskCtx, func(t fdb.ReadTransaction) (any, error) {
				taskKey, err := s.taskDir.Open(t, task.Id(taskId))
				if err != nil {
					if errors.Is(err, directory.ErrDirNotExists) {
						return nil, nil
					}
					return nil, fmt.Errorf("failed to open task %s: %w", taskId, err)
				}

				taskResourceRequest, err := taskKey.ResourceRequest().Get(t).Get()
				if err != nil {
					return nil, fmt.Errorf("failed to get task resource request for task %s: %w", taskId, err)
				}
				taskResourceRequests.Add(taskWithResourceRequest{
					taskId:          task.Id(taskId),
					resourceRequest: taskResourceRequest,
				})
				return nil, nil
			})
			return err
		})
	}
	if err := taskGroup.Wait(); err != nil {
		return nil, fmt.Errorf("failed to load task resource requests: %w", err)
	}

	// fill out the assignment plan
	retryAssignLater = mapset.NewSet[task.Id]()
	remainingResourcesPerRunnerSlice := remainingResourcesPerRunner.ToSlice()
	for _, t := range taskResourceRequests.ToSlice() {
		// select the first runner with enough resources
		var selectedRunner *runnerWithResources
		for _, runner := range remainingResourcesPerRunnerSlice {
			if runner.resources.cpuMillis >= int64(t.resourceRequest.CpuMillis) &&
				runner.resources.memoryBytes >= int64(t.resourceRequest.MemoryBytes) {
				selectedRunner = runner
				break
			}
		}
		if selectedRunner == nil {
			retryAssignLater.Add(t.taskId)
			continue
		}
		assignmentPlan[selectedRunner.runnerId].Add(t)
		selectedRunner.resources.cpuMillis -= int64(t.resourceRequest.CpuMillis)
		selectedRunner.resources.memoryBytes -= int64(t.resourceRequest.MemoryBytes)
	}
	assignmentFailureGauge.Record(ctx, int64(retryAssignLater.Cardinality()), metric.WithAttributes(attribute.String("reason", "no_resources")))

	executionRetryLater, err := s.executeAssignmentPlan(ctx, assignmentPlan)
	if err != nil {
		return nil, fmt.Errorf("failed to execute assignment plan: %w", err)
	}
	assignmentFailureGauge.Record(ctx, int64(executionRetryLater.Cardinality()), metric.WithAttributes(attribute.String("reason", "runner_went_inactive")))

	return retryAssignLater.Union(executionRetryLater), nil
}

func (s *Scheduler) executeAssignmentPlan(ctx context.Context, assignmentPlan map[string]mapset.Set[taskWithResourceRequest]) (retryAssignLater mapset.Set[task.Id], err error) {
	ctx, span := tracer.Start(ctx, "executeAssignmentPlan")
	defer span.End()

	span.SetAttributes(
		attribute.Int("assignment_plan_size", len(assignmentPlan)),
	)

	retryAssignLater = mapset.NewSet[task.Id]()
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(s.batchTxParallelism())
	for runnerId, tasks := range assignmentPlan {
		// split into batches to avoid overwhelming the transaction size limit + increase parallelism.
		const batchSize = 256
		taskSlice := tasks.ToSlice()
		for batchStart := 0; batchStart < len(taskSlice); batchStart += batchSize {
			batchEnd := min(batchStart+batchSize, len(taskSlice))
			batch := taskSlice[batchStart:batchEnd]
			batchIndex := batchStart / batchSize
			batchForWorker := batch
			batchIndexForWorker := batchIndex
			group.Go(func() error {
				ctx, span := tracer.Start(ctx,
					"executeAssignmentPlan.assignTasks",
					trace.WithAttributes(
						attribute.String("runner_id", runnerId),
						attribute.Int("batch_index", batchIndexForWorker),
						attribute.Int("batch_size", len(batchForWorker)),
					),
				)
				defer span.End()

				_, err := s.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
					active, err := s.activeRunners.IsActive(tx, runnerId).Get()
					if err != nil {
						return nil, err
					}
					if !active {
						span.AddEvent("runner is no longer active")
						for _, t := range batchForWorker {
							retryAssignLater.Add(t.taskId)
						}
						return nil, nil
					}

					runnerSet, err := s.activeRunnerSets.open(runnerId)
					if err != nil {
						if errors.Is(err, directory.ErrDirNotExists) {
							// The runner set is no longer active. the tasks in the plan cannot be assigned to this runner.
							span.AddEvent("runner set is no longer active")
							for _, t := range batchForWorker {
								retryAssignLater.Add(t.taskId)
							}
							return nil, nil
						}
						return nil, err
					}

					for _, t := range batchForWorker {
						if err := s.assignTask(tx, t.taskId, runnerId, runnerSet); err != nil {
							if errors.Is(err, ErrTaskNotInAssignableState) {
								continue
							}
							return nil, err
						}
					}
					return nil, nil
				})
				if err != nil {
					span.RecordError(err)
				}
				return err
			})
		}
	}
	if err = group.Wait(); err != nil {
		span.RecordError(err)
	}

	return retryAssignLater, err
}

func remainingResourcesFromRunner(tr fdb.ReadTransactor, db dbutil.DbRoot, activeRunners pool.ActiveRunners, pod *corev1.Pod) (_ *remainingResources, isActive bool, err error) {
	var availableCpuMillis int64
	var availableMemoryBytes int64
	for _, container := range pod.Spec.Containers {
		if cpu, ok := requestOrLimitMilli(container, corev1.ResourceCPU); ok {
			availableCpuMillis += cpu
		}
		if mem, ok := requestOrLimitBytes(container, corev1.ResourceMemory); ok {
			availableMemoryBytes += mem
		}
	}

	var inUseCpuMillis int64
	var inUseMemoryBytes int64
	_, err = tr.ReadTransact(func(t fdb.ReadTransaction) (any, error) {
		active, err := activeRunners.IsActive(t, pod.Name).Get()
		if err != nil {
			return nil, err
		}
		if !active {
			isActive = false
			return nil, nil
		}

		runnerSet, err := servicestate.OpenTaskSetForRunner(t, db, pod.Name)
		if err != nil {
			if errors.Is(err, directory.ErrDirNotExists) {
				// The runner set is no longer active. the tasks in the plan cannot be assigned to this runner.
				isActive = false
				return nil, nil
			}
			return nil, err
		}
		inUseCpuMillis, err = runnerSet.GetUtilization(t, servicestate.UtilizationDimensionCPU)
		if err != nil {
			return nil, err
		}
		inUseMemoryBytes, err = runnerSet.GetUtilization(t, servicestate.UtilizationDimensionMemory)
		if err != nil {
			return nil, err
		}
		isActive = true
		return nil, nil
	})
	if err != nil {
		return nil, false, err
	}
	return &remainingResources{
		cpuMillis:   availableCpuMillis - inUseCpuMillis,
		memoryBytes: availableMemoryBytes - inUseMemoryBytes,
	}, isActive, nil
}

func requestOrLimitMilli(container corev1.Container, name corev1.ResourceName) (int64, bool) {
	if q, ok := container.Resources.Requests[name]; ok && !q.IsZero() {
		return q.MilliValue(), true
	}
	if q, ok := container.Resources.Limits[name]; ok && !q.IsZero() {
		return q.MilliValue(), true
	}
	return 0, false
}

func requestOrLimitBytes(container corev1.Container, name corev1.ResourceName) (int64, bool) {
	if q, ok := container.Resources.Requests[name]; ok && !q.IsZero() {
		return q.Value(), true
	}
	if q, ok := container.Resources.Limits[name]; ok && !q.IsZero() {
		return q.Value(), true
	}
	return 0, false
}

var (
	ErrTaskNotInAssignableState = errors.New("task not in assignable state")
)

func (s *Scheduler) assignTask(tx fdb.Transaction, id task.Id, runnerId string, runnerSet *servicestate.RunnerSet) error {
	taskKey, err := s.taskDir.Open(tx, id)
	if err != nil {
		if errors.Is(err, directory.ErrDirNotExists) {
			return ErrTaskNotInAssignableState
		}
		return fmt.Errorf("failed to open task %s: %w", id, err)
	}

	err = s.taskPlacer.PlaceTaskOnRunner(tx, runnerId, runnerSet, taskKey)
	if errors.Is(err, servicestate.ErrTaskNotInPendingState) {
		return ErrTaskNotInAssignableState
	}
	return err
}
