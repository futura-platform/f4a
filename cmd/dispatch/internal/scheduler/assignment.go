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
		return assignmentFailures{}, err
	}

	assignmentPlan := make(map[string]mapset.Set[taskWithResourceRequest], remainingResourcesPerRunner.Cardinality())
	for _, runner := range remainingResourcesPerRunner.ToSlice() {
		assignmentPlan[runner.runnerId] = mapset.NewSet[taskWithResourceRequest]()
	}

	// Load the tasks with their resource requests, batched: one read
	// transaction per task turned a large backlog into an unbounded silent
	// startup stall (observed: a promoted leader ground through an ~87k
	// pending set for 13+ minutes with no output — the line looked dead).
	// Chunking keeps each transaction well inside the 5s budget while
	// cutting the transaction count by resourceRequestLoadBatchSize.
	const resourceRequestLoadBatchSize = 256
	pendingIdSlice := pendingIds.ToSlice()
	taskResourceRequests := mapset.NewSet[taskWithResourceRequest]()
	taskGroup, taskCtx := errgroup.WithContext(ctx)
	taskGroup.SetLimit(s.batchTxParallelism())
	for batchStart := 0; batchStart < len(pendingIdSlice); batchStart += resourceRequestLoadBatchSize {
		batch := pendingIdSlice[batchStart:min(batchStart+resourceRequestLoadBatchSize, len(pendingIdSlice))]
		taskGroup.Go(func() error {
			// Collect inside the closure and merge after the commit: the
			// closure re-runs on transaction retry, and mutating the shared
			// set from inside it would duplicate entries.
			loaded, err := s.db.ReadTransactContext(taskCtx, func(t fdb.ReadTransaction) (any, error) {
				batchLoaded := make([]taskWithResourceRequest, 0, len(batch))
				for _, taskId := range batch {
					taskKey, err := s.taskDir.Open(t, task.Id(taskId))
					if err != nil {
						if errors.Is(err, directory.ErrDirNotExists) {
							continue
						}
						return nil, fmt.Errorf("failed to open task %s: %w", taskId, err)
					}

					taskResourceRequest, err := taskKey.ResourceRequest().Get(t).Get()
					if err != nil {
						return nil, fmt.Errorf("failed to get task resource request for task %s: %w", taskId, err)
					}
					batchLoaded = append(batchLoaded, taskWithResourceRequest{
						taskId:          task.Id(taskId),
						resourceRequest: taskResourceRequest,
					})
				}
				return batchLoaded, nil
			})
			if err != nil {
				return err
			}
			taskResourceRequests.Append(loaded.([]taskWithResourceRequest)...)
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

				// Collected inside the transaction closure and reset on each
				// attempt so FDB retries don't produce duplicates. Marker spans
				// are emitted only after the transaction commits.
				assignedInBatch := make([]task.Id, 0, len(batchForWorker))
				_, err = s.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
					assignedInBatch = assignedInBatch[:0]
					active, err := s.activeRunners.IsActive(tx, runnerId).Get()
					if err != nil {
						return nil, err
					}
					if !active {
						span.AddEvent("runner is no longer active")
						for _, t := range batchForWorker {
							failures.runnerInactive.Add(t.taskId)
						}
						return nil, nil
					}

					runnerSet, err := s.activeRunnerSets.open(runnerId)
					if err != nil {
						if errors.Is(err, directory.ErrDirNotExists) {
							// The runner set is no longer active. the tasks in the plan cannot be assigned to this runner.
							span.AddEvent("runner set is no longer active")
							for _, t := range batchForWorker {
								failures.runnerInactive.Add(t.taskId)
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
						assignedInBatch = append(assignedInBatch, t.taskId)
					}
					return nil, nil
				})
				if err != nil {
					return err
				}
				// Emit a marker span per assigned task so the full task
				// lifecycle can be queried by task_id across traces.
				// Per-item spans alongside a batch span are an OTel-sanctioned
				// pattern, mirroring messaging semconv "create" spans (one per
				// message in a batch publish):
				// https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/#batch-publishing-with-create-spans
				for _, taskId := range assignedInBatch {
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
