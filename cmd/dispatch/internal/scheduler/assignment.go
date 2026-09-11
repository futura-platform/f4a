package scheduler

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	otelutil "github.com/futura-platform/f4a/internal/util/otel"
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

const assignmentItemParallelism = 16

func (s *Scheduler) batchTxParallelism() int {
	if s.cfg.BatchTxParallelism < 1 {
		return DefaultBatchParallelism
	}
	return s.cfg.BatchTxParallelism
}

type assignmentFailures struct {
	noResources    mapset.Set[task.Id]
	runnerInactive mapset.Set[task.Id]
}

func newAssignmentFailures() assignmentFailures {
	return assignmentFailures{
		noResources:    mapset.NewSet[task.Id](),
		runnerInactive: mapset.NewSet[task.Id](),
	}
}

func (f assignmentFailures) Union(other assignmentFailures) assignmentFailures {
	return assignmentFailures{
		noResources:    f.noResources.Union(other.noResources),
		runnerInactive: f.runnerInactive.Union(other.runnerInactive),
	}
}

func (f assignmentFailures) All() mapset.Set[task.Id] {
	return f.noResources.Union(f.runnerInactive)
}

func (f assignmentFailures) Record(ctx context.Context, gauge metric.Int64Gauge) {
	gauge.Record(ctx,
		int64(f.noResources.Cardinality()),
		metric.WithAttributes(attribute.String("reason", "no_resources")),
	)
	gauge.Record(ctx,
		int64(f.runnerInactive.Cardinality()),
		metric.WithAttributes(attribute.String("reason", "runner_inactive")),
	)
}

// assignPending assigns all given tasks in the pending set to active runner pods.
// If any failure occurs, the task is not assigned and added to the assignmentFailures return value.
func (s *Scheduler) assignPending(
	ctx context.Context,
	pendingIds mapset.Set[task.Id],
) (failures assignmentFailures, err error) {
	ctx, span := tracer.Start(ctx, "assignPending")
	defer func() { otelutil.End(span, err) }()
	span.SetAttributes(
		attribute.Int("pending_ids_count", pendingIds.Cardinality()),
	)

	pods, err := s.runnerPodLister.List(labels.Everything())
	if err != nil {
		return assignmentFailures{}, fmt.Errorf("failed to list runner pods: %w", err)
	}

	type runnerWithResources struct {
		runnerId  string
		resources *remainingResources
	}
	const runnerLoadBatchSize = 64
	readyPods := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if podutils.IsPodReady(pod) {
			readyPods = append(readyPods, pod)
		}
	}
	remainingResourcesPerRunner := mapset.NewSet[*runnerWithResources]()
	runnerGroup, runnerCtx := errgroup.WithContext(ctx)
	runnerGroup.SetLimit(s.batchTxParallelism())
	for batchStart := 0; batchStart < len(readyPods); batchStart += runnerLoadBatchSize {
		batch := readyPods[batchStart:min(batchStart+runnerLoadBatchSize, len(readyPods))]
		runnerGroup.Go(func() error {
			loaded, err := s.db.ReadTransactContext(runnerCtx, func(t fdb.ReadTransaction) (any, error) {
				batchLoaded := make([]*runnerWithResources, len(batch))
				var reads errgroup.Group
				reads.SetLimit(assignmentItemParallelism)
				for i, pod := range batch {
					reads.Go(func() error {
						resources, ok, err := remainingResourcesFromRunner(t, s.db, s.activeRunners, pod)
						if err != nil || !ok {
							return err
						}
						batchLoaded[i] = &runnerWithResources{runnerId: pod.Name, resources: resources}
						return nil
					})
				}
				if err := reads.Wait(); err != nil {
					return nil, err
				}
				return slices.DeleteFunc(batchLoaded, func(r *runnerWithResources) bool { return r == nil }), nil
			})
			if err != nil {
				return err
			}
			remainingResourcesPerRunner.Append(loaded.([]*runnerWithResources)...)
			return nil
		})
	}
	if err := runnerGroup.Wait(); err != nil {
		return assignmentFailures{}, err
	}

	assignmentPlan := make(map[string]mapset.Set[taskWithResourceRequest], remainingResourcesPerRunner.Cardinality())
	for _, runner := range remainingResourcesPerRunner.ToSlice() {
		assignmentPlan[runner.runnerId] = mapset.NewSet[taskWithResourceRequest]()
	}

	const resourceRequestLoadBatchSize = 256
	pendingIdSlice := pendingIds.ToSlice()
	taskResourceRequests := mapset.NewSet[taskWithResourceRequest]()
	taskGroup, taskCtx := errgroup.WithContext(ctx)
	taskGroup.SetLimit(s.batchTxParallelism())
	for batchStart := 0; batchStart < len(pendingIdSlice); batchStart += resourceRequestLoadBatchSize {
		batch := pendingIdSlice[batchStart:min(batchStart+resourceRequestLoadBatchSize, len(pendingIdSlice))]
		taskGroup.Go(func() error {
			loaded, err := s.db.ReadTransactContext(taskCtx, func(t fdb.ReadTransaction) (any, error) {
				batchLoaded := make([]*taskWithResourceRequest, len(batch))
				var reads errgroup.Group
				reads.SetLimit(assignmentItemParallelism)
				for i, taskId := range batch {
					reads.Go(func() error {
						taskKey, err := s.taskDir.Open(t, taskId)
						if err != nil {
							if errors.Is(err, directory.ErrDirNotExists) {
								return nil
							}
							return fmt.Errorf("failed to open task %s: %w", taskId, err)
						}

						taskResourceRequest, err := taskKey.ResourceRequest().Get(t).Get()
						if err != nil {
							return fmt.Errorf("failed to get task resource request for task %s: %w", taskId, err)
						}
						batchLoaded[i] = &taskWithResourceRequest{taskId: taskId, resourceRequest: taskResourceRequest}
						return nil
					})
				}
				if err := reads.Wait(); err != nil {
					return nil, err
				}
				return slices.DeleteFunc(batchLoaded, func(t *taskWithResourceRequest) bool { return t == nil }), nil
			})
			if err != nil {
				return err
			}
			for _, task := range loaded.([]*taskWithResourceRequest) {
				taskResourceRequests.Add(*task)
			}
			return nil
		})
	}
	if err := taskGroup.Wait(); err != nil {
		return assignmentFailures{}, fmt.Errorf("failed to load task resource requests: %w", err)
	}

	// fill out the assignment plan
	failures = newAssignmentFailures()
	remainingResourcesPerRunnerSlice := remainingResourcesPerRunner.ToSlice()
	// Fill the lowest StatefulSet ordinals first: scale-down always removes
	// the highest ordinals, so keeping them empty makes scale-down
	// reschedule-free and lets the fleet consolidate. (The set's ToSlice is
	// hash-ordered; without sorting, the placement policy is arbitrary.)
	slices.SortFunc(remainingResourcesPerRunnerSlice, func(a, b *runnerWithResources) int {
		return compareRunnersByOrdinal(a.runnerId, b.runnerId)
	})
	for _, t := range taskResourceRequests.ToSlice() {
		// select the first runner with enough resources
		var selectedRunner *runnerWithResources
		for _, runner := range remainingResourcesPerRunnerSlice {
			if runner.resources.cpuMillis >= int64(t.resourceRequest.GetCpuMillis()) &&
				runner.resources.memoryBytes >= int64(t.resourceRequest.GetMemoryBytes()) {
				selectedRunner = runner
				break
			}
		}
		if selectedRunner == nil {
			// A task that is larger than any runner stays here forever; f4a only
			// tracks demand and intentionally leaves instance-shape validation to consumers.
			// TODO: handle this better somehow
			failures.noResources.Add(t.taskId)
			continue
		}
		assignmentPlan[selectedRunner.runnerId].Add(t)
		selectedRunner.resources.cpuMillis -= int64(t.resourceRequest.GetCpuMillis())
		selectedRunner.resources.memoryBytes -= int64(t.resourceRequest.GetMemoryBytes())
	}

	executionFailures, err := s.executeAssignmentPlan(ctx, assignmentPlan)
	if err != nil {
		return assignmentFailures{}, fmt.Errorf("failed to execute assignment plan: %w", err)
	}

	return failures.Union(executionFailures), nil
}

func (s *Scheduler) executeAssignmentPlan(ctx context.Context, assignmentPlan map[string]mapset.Set[taskWithResourceRequest]) (failures assignmentFailures, err error) {
	ctx, span := tracer.Start(ctx, "executeAssignmentPlan")
	defer func() { otelutil.End(span, err) }()

	span.SetAttributes(
		attribute.Int("assignment_plan_size", len(assignmentPlan)),
	)

	failures = newAssignmentFailures()
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
			group.Go(func() (err error) {
				ctx, span := tracer.Start(ctx,
					"executeAssignmentPlan.assignTasks",
					trace.WithAttributes(
						attribute.String("runner_id", runnerId),
						attribute.Int("batch_index", batchIndexForWorker),
						attribute.Int("batch_size", len(batchForWorker)),
					),
				)
				defer func() { otelutil.End(span, err) }()

				result, err := s.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
					active, err := s.activeRunners.IsActive(tx, runnerId).Get()
					if err != nil {
						return nil, err
					}
					if !active {
						span.AddEvent("runner is no longer active")
						return nil, nil
					}

					runnerSet, err := servicestate.OpenTaskSetForRunner(tx, s.db, runnerId)
					if err != nil {
						if errors.Is(err, directory.ErrDirNotExists) {
							// The runner set is no longer active. the tasks in the plan cannot be assigned to this runner.
							span.AddEvent("runner set is no longer active")
							return nil, nil
						}
						return nil, err
					}

					assigned := mapset.NewSet[task.Id]()
					var assignments errgroup.Group
					assignments.SetLimit(assignmentItemParallelism)
					for _, t := range batchForWorker {
						assignments.Go(func() error {
							if err := s.assignTask(tx, t.taskId, runnerId, runnerSet); err != nil {
								if errors.Is(err, ErrTaskNotInAssignableState) {
									return nil
								}
								return err
							}
							assigned.Add(t.taskId)
							return nil
						})
					}
					if err := assignments.Wait(); err != nil {
						return nil, err
					}
					return assigned, nil
				})
				if err != nil {
					return err
				}
				if result == nil {
					for _, t := range batchForWorker {
						failures.runnerInactive.Add(t.taskId)
					}
					return nil
				}
				// Emit a marker span per assigned task so the full task
				// lifecycle can be queried by task_id across traces.
				// Per-item spans alongside a batch span are an OTel-sanctioned
				// pattern, mirroring messaging semconv "create" spans (one per
				// message in a batch publish):
				// https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/#batch-publishing-with-create-spans
				for _, taskId := range result.(mapset.Set[task.Id]).ToSlice() {
					_, taskSpan := tracer.Start(ctx, "assignTask",
						trace.WithAttributes(
							attribute.String("task_id", string(taskId)),
							attribute.String("runner_id", runnerId),
						),
					)
					taskSpan.End()
				}
				return nil
			})
		}
	}
	if err := group.Wait(); err != nil {
		return failures, err
	}

	return failures, nil
}

// remainingResourcesFromRunner reads one runner's remaining capacity inside
// the caller's read transaction, so callers can batch many runners per
// transaction instead of opening one each.
func remainingResourcesFromRunner(t fdb.ReadTransaction, db dbutil.DbRoot, activeRunners pool.ActiveRunners, pod *corev1.Pod) (_ *remainingResources, isActive bool, err error) {
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

	active, err := activeRunners.IsActive(t, pod.Name).Get()
	if err != nil {
		return nil, false, err
	}
	if !active {
		return nil, false, nil
	}

	runnerSet, err := servicestate.OpenTaskSetForRunner(t, db, pod.Name)
	if err != nil {
		if errors.Is(err, directory.ErrDirNotExists) {
			// The runner set is no longer active. the tasks in the plan cannot be assigned to this runner.
			return nil, false, nil
		}
		return nil, false, err
	}
	inUseCpuMillis, err := runnerSet.GetUtilization(t, servicestate.UtilizationDimensionCPU)
	if err != nil {
		return nil, false, err
	}
	inUseMemoryBytes, err := runnerSet.GetUtilization(t, servicestate.UtilizationDimensionMemory)
	if err != nil {
		return nil, false, err
	}
	return &remainingResources{
		cpuMillis:   availableCpuMillis - inUseCpuMillis,
		memoryBytes: availableMemoryBytes - inUseMemoryBytes,
	}, true, nil
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

// compareRunnersByOrdinal orders runner ids by ascending StatefulSet ordinal,
// falling back to the full name so ties (foreign name shapes, duplicate
// ordinals across sets) still order deterministically.
func compareRunnersByOrdinal(a, b string) int {
	if c := cmp.Compare(runnerOrdinal(a), runnerOrdinal(b)); c != 0 {
		return c
	}
	return cmp.Compare(a, b)
}

// runnerOrdinal extracts the StatefulSet ordinal from a pod name
// ("<name>-<ordinal>"). Names without one sort last.
func runnerOrdinal(podName string) int {
	idx := strings.LastIndexByte(podName, '-')
	if idx < 0 {
		return math.MaxInt
	}
	ordinal, err := strconv.Atoi(podName[idx+1:])
	if err != nil || ordinal < 0 {
		return math.MaxInt
	}
	return ordinal
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
